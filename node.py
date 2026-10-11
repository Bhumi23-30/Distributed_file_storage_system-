"""
node.py — Storage Node
Mini GFS | Distributed File Sharing System

Run this 3 times in separate terminals:
    python node.py 9001
    python node.py 9002
    python node.py 9003

Each node:
  - Listens on its port for commands from the main server
  - Stores files in  storage/node<PORT>/
  - Can replicate a file to another node
  - Sends files back on download request
"""

import socket
import os
import sys
import threading


# ── Constants ──────────────────────────────────
BUFFER_SIZE  = 4096       # bytes per chunk during file transfer
STORAGE_ROOT = "storage"  # parent folder for all node storage


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────

def get_storage_path(port):
    """Return (and create if needed) the storage folder for this node."""
    path = os.path.join(STORAGE_ROOT, f"node{port}")
    os.makedirs(path, exist_ok=True)
    return path


def receive_file(conn, save_path):
    """
    Receive a file sent over socket and write it to save_path.
    Protocol:
      1. Sender sends file size as a fixed 16-byte string
      2. Sender streams file bytes
    """
    # Step 1: read file size
    raw_size = conn.recv(16).decode().strip()
    file_size = int(raw_size)

    # Step 2: receive exactly file_size bytes
    received = 0
    with open(save_path, "wb") as f:
        while received < file_size:
            chunk = conn.recv(min(BUFFER_SIZE, file_size - received))
            if not chunk:
                break
            f.write(chunk)
            received += len(chunk)

    print(f"  [NODE] Received file → {save_path} ({received} bytes)")


def send_file(conn, file_path):
    """
    Send a file over socket.
    Protocol mirrors receive_file.
    """
    file_size = os.path.getsize(file_path)
    # Step 1: send size padded to 16 bytes
    conn.send(str(file_size).ljust(16).encode())

    # Step 2: stream the file
    sent = 0
    with open(file_path, "rb") as f:
        while chunk := f.read(BUFFER_SIZE):
            conn.sendall(chunk)
            sent += len(chunk)

    print(f"  [NODE] Sent file ← {file_path} ({sent} bytes)")


def replicate_to(filename, file_path, target_port):
    """
    Push a file to another node (target_port) as a replication step.
    Opens a fresh socket connection to the target node.
    """
    try:
        rep_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        rep_sock.connect(("127.0.0.1", target_port))

        # Tell the target node: "STORE <filename>"
        rep_sock.send(f"STORE {filename}".ljust(64).encode())

        # Wait for its acknowledgement
        ack = rep_sock.recv(8).decode().strip()
        if ack == "READY":
            send_file(rep_sock, file_path)
            print(f"  [NODE] Replicated '{filename}' → port {target_port}")
        rep_sock.close()
    except Exception as e:
        print(f"  [NODE] Replication to port {target_port} FAILED: {e}")


# ──────────────────────────────────────────────
# Command handlers
# ──────────────────────────────────────────────

def handle_store(conn, filename, storage_path):
    """
    Handle STORE command: receive a file from sender and save it.
    Sends "READY" to signal sender can start streaming.
    """
    save_path = os.path.join(storage_path, filename)
    conn.send("READY   ".encode())   # 8 bytes ACK
    receive_file(conn, save_path)
    conn.send("STORED  ".encode())   # 8 bytes confirmation
    return save_path


def handle_fetch(conn, filename, storage_path):
    """
    Handle FETCH command: send a file to the requester.
    Sends "FOUND" or "MISSING" first.
    """
    file_path = os.path.join(storage_path, filename)
    if os.path.exists(file_path):
        conn.send("FOUND   ".encode())   # 8 bytes
        send_file(conn, file_path)
    else:
        conn.send("MISSING ".encode())   # 8 bytes
        print(f"  [NODE] File not found: {filename}")


def handle_delete(conn, filename, storage_path):
    """Handle DELETE command: remove file from this node's storage."""
    file_path = os.path.join(storage_path, filename)
    if os.path.exists(file_path):
        os.remove(file_path)
        conn.send("DELETED ".encode())
        print(f"  [NODE] Deleted '{filename}'")
    else:
        conn.send("MISSING ".encode())


def handle_ping(conn):
    """Handle PING: used by server to check if this node is alive."""
    conn.send("ALIVE   ".encode())


# ──────────────────────────────────────────────
# Per-connection handler (runs in a thread)
# ──────────────────────────────────────────────

def handle_connection(conn, addr, port, storage_path):
    """
    Each incoming connection is handled here.
    First 64 bytes = command string, e.g. "STORE resume.pdf"
    """
    try:
        raw_cmd = conn.recv(64).decode().strip()
        parts   = raw_cmd.split(" ", 1)
        command = parts[0].upper()
        arg     = parts[1] if len(parts) > 1 else ""

        print(f"  [NODE:{port}] Command: {command} | Arg: {arg}")

        if command == "STORE":
            save_path = handle_store(conn, arg, storage_path)
            # After storing, check if server wants us to replicate
            # The server sends "REPLICATE <target_port>" or "NOREPLICA"
            replication_cmd = conn.recv(32).decode().strip()
            if replication_cmd.startswith("REPLICATE"):
                target_port = int(replication_cmd.split()[1])
                replicate_to(arg, save_path, target_port)
                conn.send("REPLICATED".encode())
            else:
                conn.send("NOREPLICA ".encode())

        elif command == "FETCH":
            handle_fetch(conn, arg, storage_path)

        elif command == "DELETE":
            handle_delete(conn, arg, storage_path)

        elif command == "PING":
            handle_ping(conn)

        else:
            print(f"  [NODE:{port}] Unknown command: {command}")

    except Exception as e:
        print(f"  [NODE:{port}] Error handling connection: {e}")
    finally:
        conn.close()


# ──────────────────────────────────────────────
# Main node server loop
# ──────────────────────────────────────────────

def start_node(port):
    storage_path = get_storage_path(port)
    print(f"[NODE:{port}] Storage → {os.path.abspath(storage_path)}")

    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind(("127.0.0.1", port))
    server_sock.listen(10)

    print(f"[NODE:{port}] Listening... (Ctrl+C to simulate failure)")

    while True:
        conn, addr = server_sock.accept()
        # Handle each connection in its own thread
        t = threading.Thread(
            target=handle_connection,
            args=(conn, addr, port, storage_path),
            daemon=True
        )
        t.start()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python node.py <port>")
        print("Example: python node.py 9001")
        sys.exit(1)

    port = int(sys.argv[1])
    if port not in [9001, 9002, 9003]:
        print("Use ports 9001, 9002, or 9003")
        sys.exit(1)

    start_node(port)
