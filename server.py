"""
server.py — Main Coordinator Server
Mini GFS | Distributed File Sharing System

Run this first:
    python server.py

Responsibilities:
  - Authenticate users (via SQLite)
  - Select which node to store a file on (round-robin load balancing)
  - Tell that node to replicate to another node
  - Find which node holds a file (via SQLite metadata) on download
  - Handle node failures by falling back to replica node
  - Relay file bytes between client and nodes
"""

import socket
import threading
import os
from database import (
    init_db, verify_user, register_user,
    save_file_metadata, get_file_metadata, list_user_files
)

# ── Config ─────────────────────────────────────
SERVER_HOST  = "127.0.0.1"
SERVER_PORT  = 9000
BUFFER_SIZE  = 4096

# The 3 storage nodes (port → replica port mapping)
# Round-robin index for load balancing
NODES = [9001, 9002, 9003]
REPLICA_MAP = {
    9001: 9002,   # Node 9001's replica lives on 9002
    9002: 9003,   # Node 9002's replica lives on 9003
    9003: 9001,   # Node 9003's replica lives on 9001 (circular)
}

# Shared round-robin counter (thread-safe via lock)
_node_index      = 0
_node_index_lock = threading.Lock()


# ──────────────────────────────────────────────
# Node health check
# ──────────────────────────────────────────────

def ping_node(port):
    """
    Send a PING to a node. Returns True if alive, False if down.
    Timeout = 1 second so the demo doesn't hang.
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(1)
        s.connect(("127.0.0.1", port))
        s.send("PING".ljust(64).encode())
        resp = s.recv(8).decode().strip()
        s.close()
        return resp == "ALIVE"
    except:
        return False


def get_alive_nodes():
    """Return list of currently alive node ports."""
    return [p for p in NODES if ping_node(p)]


# ──────────────────────────────────────────────
# Load balancing — round-robin node selection
# ──────────────────────────────────────────────

def pick_primary_node():
    """
    Round-robin: pick the next available alive node.
    Skips dead nodes automatically.
    """
    global _node_index
    alive = get_alive_nodes()
    if not alive:
        return None

    with _node_index_lock:
        node = alive[_node_index % len(alive)]
        _node_index += 1

    print(f"  [SERVER] Load balancer → selected node {node}")
    return node


# ──────────────────────────────────────────────
# File transfer helpers (server ↔ node)
# ──────────────────────────────────────────────

def recv_file_from_client(client_conn, size):
    """Receive exactly `size` bytes from client into memory."""
    data     = b""
    received = 0
    while received < size:
        chunk = client_conn.recv(min(BUFFER_SIZE, size - received))
        if not chunk:
            break
        data     += chunk
        received += len(chunk)
    return data


def forward_file_to_node(node_port, filename, file_data, replica_port):
    """
    Send a file to a storage node and tell it to replicate.
    Returns True on success, False on failure.
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(("127.0.0.1", node_port))

        # 1. Send STORE command
        s.send(f"STORE {filename}".ljust(64).encode())

        # 2. Wait for READY
        ack = s.recv(8).decode().strip()
        if ack != "READY":
            s.close()
            return False

        # 3. Send file size + file bytes
        s.send(str(len(file_data)).ljust(16).encode())
        s.sendall(file_data)

        # 4. Wait for STORED confirmation
        s.recv(8)  # "STORED"

        # 5. Tell node to replicate to replica_port
        s.send(f"REPLICATE {replica_port}".ljust(32).encode())
        s.recv(10)  # "REPLICATED" or "NOREPLICA"

        s.close()
        print(f"  [SERVER] File '{filename}' stored on node {node_port}, replicated to {replica_port}")
        return True

    except Exception as e:
        print(f"  [SERVER] Failed to store on node {node_port}: {e}")
        return False


def fetch_file_from_node(node_port, filename):
    """
    Fetch a file's bytes from a storage node.
    Returns file bytes on success, None on failure.
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(3)
        s.connect(("127.0.0.1", node_port))

        # Send FETCH command
        s.send(f"FETCH {filename}".ljust(64).encode())

        # Check response
        status = s.recv(8).decode().strip()
        if status != "FOUND":
            s.close()
            return None

        # Read file size
        raw_size  = s.recv(16).decode().strip()
        file_size = int(raw_size)

        # Receive file bytes
        data     = b""
        received = 0
        while received < file_size:
            chunk = s.recv(min(BUFFER_SIZE, file_size - received))
            if not chunk:
                break
            data     += chunk
            received += len(chunk)

        s.close()
        return data

    except Exception as e:
        print(f"  [SERVER] Failed to fetch from node {node_port}: {e}")
        return None


# ──────────────────────────────────────────────
# Request handlers
# ──────────────────────────────────────────────

def handle_login(client_conn, parts):
    """LOGIN <username> <password>"""
    if len(parts) < 3:
        client_conn.send("FAIL    ".encode())
        return None

    username = parts[1]
    password = parts[2]

    if verify_user(username, password):
        client_conn.send("OK      ".encode())
        print(f"  [SERVER] LOGIN OK → {username}")
        return username
    else:
        client_conn.send("FAIL    ".encode())
        print(f"  [SERVER] LOGIN FAIL → {username}")
        return None


def handle_register(client_conn, parts):
    """REGISTER <username> <password>"""
    if len(parts) < 3:
        client_conn.send("FAIL    ".encode())
        return

    username = parts[1]
    password = parts[2]

    success, msg = register_user(username, password)
    if success:
        client_conn.send("OK      ".encode())
        print(f"  [SERVER] REGISTER OK → {username}")
    else:
        client_conn.send("FAIL    ".encode())
        print(f"  [SERVER] REGISTER FAIL → {username}: {msg}")


def handle_upload(client_conn, parts, username):
    """
    UPLOAD <filename> <filesize>
    Receives file from client, picks a node, stores + replicates it.
    """
    if len(parts) < 3:
        client_conn.send("FAIL    ".encode())
        return

    filename  = parts[1]
    file_size = int(parts[2])

    # Pick a primary node (round-robin)
    primary = pick_primary_node()
    if not primary:
        client_conn.send("NONODES ".encode())
        return

    replica = REPLICA_MAP[primary]

    # Tell client we're ready to receive
    client_conn.send("READY   ".encode())

    # Receive the file bytes from client
    file_data = recv_file_from_client(client_conn, file_size)

    # Forward to node (which will also replicate)
    success = forward_file_to_node(primary, filename, file_data, replica)

    if success:
        # Save metadata to DB
        save_file_metadata(filename, username, primary, replica)
        client_conn.send("UPLOADED".encode())
        print(f"  [SERVER] '{filename}' uploaded by {username}")
    else:
        client_conn.send("FAIL    ".encode())


def handle_download(client_conn, parts, username):
    """
    DOWNLOAD <filename>
    Looks up node from DB, fetches file, sends to client.
    Falls back to replica if primary node is down.
    """
    if len(parts) < 2:
        client_conn.send("FAIL    ".encode())
        return

    filename = parts[1]

    # Look up metadata
    meta = get_file_metadata(filename, username)
    if not meta:
        client_conn.send("NOTFOUND".encode())
        return

    primary = meta["primary_node"]
    replica = meta["replica_node"]

    # Try primary node first
    file_data = None
    source    = None

    if ping_node(primary):
        print(f"  [SERVER] Fetching '{filename}' from primary node {primary}")
        file_data = fetch_file_from_node(primary, filename)
        source    = f"Node {primary} (primary)"
    
    # Fault tolerance: fall back to replica if primary fails
    if file_data is None:
        print(f"  [SERVER] Primary node {primary} DOWN! Trying replica {replica}...")
        if ping_node(replica):
            file_data = fetch_file_from_node(replica, filename)
            source    = f"Node {replica} (replica — fault recovery ✓)"

    if file_data is None:
        client_conn.send("FAIL    ".encode())
        print(f"  [SERVER] Both nodes down for '{filename}'")
        return

    # Send file size + file bytes to client
    client_conn.send("FOUND   ".encode())
    client_conn.send(str(len(file_data)).ljust(16).encode())
    client_conn.sendall(file_data)

    print(f"  [SERVER] '{filename}' sent to {username} from {source}")


def handle_list(client_conn, username):
    """
    LIST
    Returns all files owned by this user as a newline-separated string.
    """
    files = list_user_files(username)
    if not files:
        client_conn.send("EMPTY   ".encode())
        return

    # Format: "filename|primary_node|replica_node|uploaded_at"
    lines = [f"{f[0]}|{f[1]}|{f[2]}|{f[3]}" for f in files]
    response = "\n".join(lines)

    client_conn.send("LIST    ".encode())
    # Send count first so client knows how many lines to expect
    client_conn.send(str(len(response)).ljust(16).encode())
    client_conn.sendall(response.encode())


def handle_nodes_status(client_conn):
    """
    NODESTATUS
    Returns alive/dead status for all nodes (for the GUI status panel).
    """
    statuses = []
    for port in NODES:
        alive = ping_node(port)
        statuses.append(f"{port}:{'ALIVE' if alive else 'DOWN'}")
    response = ",".join(statuses)
    client_conn.send(response.ljust(64).encode())


# ──────────────────────────────────────────────
# Per-client session handler
# ──────────────────────────────────────────────

def handle_client(client_conn, addr):
    """
    Manages one client's full session.
    A session can have multiple commands (login → upload → download → ...).
    Commands are terminated when client disconnects.
    """
    print(f"\n[SERVER] New connection from {addr}")
    username = None   # Not logged in yet

    try:
        while True:
            # Each command is a 128-byte header
            raw = client_conn.recv(128).decode().strip()
            if not raw:
                break   # Client disconnected

            parts   = raw.split()
            command = parts[0].upper()

            print(f"[SERVER] CMD={command} | User={username or 'N/A'}")

            if command == "REGISTER":
                handle_register(client_conn, parts)

            elif command == "LOGIN":
                username = handle_login(client_conn, parts)

            elif command == "UPLOAD":
                if not username:
                    client_conn.send("NOAUTH  ".encode())
                else:
                    handle_upload(client_conn, parts, username)

            elif command == "DOWNLOAD":
                if not username:
                    client_conn.send("NOAUTH  ".encode())
                else:
                    handle_download(client_conn, parts, username)

            elif command == "LIST":
                if not username:
                    client_conn.send("NOAUTH  ".encode())
                else:
                    handle_list(client_conn, username)

            elif command == "NODESTATUS":
                handle_nodes_status(client_conn)

            elif command == "LOGOUT":
                print(f"[SERVER] {username} logged out.")
                username = None
                client_conn.send("BYE     ".encode())

            elif command == "QUIT":
                break

            else:
                client_conn.send("UNKNOWN ".encode())

    except Exception as e:
        print(f"[SERVER] Error with {addr}: {e}")
    finally:
        client_conn.close()
        print(f"[SERVER] Connection closed: {addr}")


# ──────────────────────────────────────────────
# Main server loop
# ──────────────────────────────────────────────

def start_server():
    init_db()

    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind((SERVER_HOST, SERVER_PORT))
    server_sock.listen(20)

    print("=" * 50)
    print("  Mini GFS — Main Server")
    print(f"  Listening on {SERVER_HOST}:{SERVER_PORT}")
    print(f"  Nodes: {NODES}")
    print("=" * 50)

    # Check which nodes are currently alive
    alive = get_alive_nodes()
    print(f"  Alive nodes at startup: {alive if alive else 'None — start nodes first!'}")
    print()

    while True:
        conn, addr = server_sock.accept()
        t = threading.Thread(
            target=handle_client,
            args=(conn, addr),
            daemon=True
        )
        t.start()


if __name__ == "__main__":
    start_server()
