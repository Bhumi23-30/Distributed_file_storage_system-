"""
client_gui.py — GUI Client
Mini GFS | Distributed File Sharing System

Run after server and nodes are up:
    python client_gui.py

Features shown in GUI:
  - Login / Register
  - Upload a file (with node selection log)
  - Download a file
  - File list with node locations
  - Node status panel (ALIVE / DOWN)
  - Fault tolerance live demo (kill a node, download still works)
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
import socket
import os
import threading


# ── Config ─────────────────────────────────────
SERVER_HOST = "127.0.0.1"
SERVER_PORT = 9000
BUFFER_SIZE = 4096


# ──────────────────────────────────────────────
# Low-level socket communication helpers
# ──────────────────────────────────────────────

class ServerConnection:
    """
    Manages a persistent TCP connection to the main server.
    All GUI actions go through this.
    """

    def __init__(self):
        self.sock     = None
        self.username = None

    def connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((SERVER_HOST, SERVER_PORT))

    def disconnect(self):
        if self.sock:
            try:
                self._send_cmd("QUIT")
            except:
                pass
            self.sock.close()
            self.sock = None

    def _send_cmd(self, cmd):
        """Send a command string (padded to 128 bytes)."""
        self.sock.send(cmd.ljust(128).encode())

    def _recv(self, n):
        """Receive exactly n bytes."""
        data = b""
        while len(data) < n:
            chunk = self.sock.recv(n - len(data))
            if not chunk:
                break
            data += chunk
        return data

    def _recv_str(self, n):
        return self._recv(n).decode().strip()

    # ── Auth ──────────────────────────────────

    def login(self, username, password):
        self._send_cmd(f"LOGIN {username} {password}")
        resp = self._recv_str(8)
        if resp == "OK":
            self.username = username
            return True, "Login successful!"
        return False, "Invalid username or password."

    def register(self, username, password):
        self._send_cmd(f"REGISTER {username} {password}")
        resp = self._recv_str(8)
        if resp == "OK":
            return True, "Registered successfully! You can now log in."
        return False, "Username already taken."

    # ── File operations ───────────────────────

    def upload(self, filepath, progress_callback=None):
        filename  = os.path.basename(filepath)
        file_size = os.path.getsize(filepath)

        self._send_cmd(f"UPLOAD {filename} {file_size}")
        resp = self._recv_str(8)

        if resp != "READY":
            return False, f"Server not ready: {resp}"

        # Stream the file
        sent = 0
        with open(filepath, "rb") as f:
            while chunk := f.read(BUFFER_SIZE):
                self.sock.sendall(chunk)
                sent += len(chunk)
                if progress_callback:
                    progress_callback(sent, file_size)

        # Wait for confirmation
        conf = self._recv_str(8)
        if conf == "UPLOADED":
            return True, f"'{filename}' uploaded and replicated ✓"
        return False, f"Upload failed: {conf}"

    def download(self, filename, save_dir):
        self._send_cmd(f"DOWNLOAD {filename}")
        status = self._recv_str(8)

        if status == "NOTFOUND":
            return False, "File not found on server."
        if status == "FAIL":
            return False, "Both primary and replica nodes are down."
        if status != "FOUND":
            return False, f"Unexpected response: {status}"

        # Get file size
        file_size = int(self._recv_str(16))

        # Receive file bytes
        data     = b""
        received = 0
        while received < file_size:
            chunk = self.sock.recv(min(BUFFER_SIZE, file_size - received))
            if not chunk:
                break
            data     += chunk
            received += len(chunk)

        # Save to disk
        save_path = os.path.join(save_dir, filename)
        with open(save_path, "wb") as f:
            f.write(data)

        return True, f"'{filename}' downloaded to {save_path}"

    def list_files(self):
        self._send_cmd("LIST")
        resp = self._recv_str(8)

        if resp == "EMPTY":
            return []

        # Receive list data
        size     = int(self._recv_str(16))
        raw      = self._recv(size).decode()
        files    = []
        for line in raw.strip().split("\n"):
            parts = line.split("|")
            if len(parts) == 4:
                files.append({
                    "filename":     parts[0],
                    "primary_node": parts[1],
                    "replica_node": parts[2],
                    "uploaded_at":  parts[3],
                })
        return files

    def get_node_status(self):
        self._send_cmd("NODESTATUS")
        raw = self._recv_str(64)
        # Format: "9001:ALIVE,9002:DOWN,9003:ALIVE"
        statuses = {}
        for item in raw.split(","):
            parts = item.split(":")
            if len(parts) == 2:
                statuses[parts[0]] = parts[1]
        return statuses


# ──────────────────────────────────────────────
# GUI Application
# ──────────────────────────────────────────────

class MiniGFSApp:

    def __init__(self, root):
        self.root    = root
        self.conn    = ServerConnection()
        self.root.title("Mini GFS — Distributed File Sharing System")
        self.root.geometry("900x650")
        self.root.configure(bg="#1e2a3a")
        self.root.resizable(True, True)

        # Try connecting to server
        try:
            self.conn.connect()
        except Exception as e:
            messagebox.showerror("Connection Error",
                f"Cannot connect to server at {SERVER_HOST}:{SERVER_PORT}\n\n"
                f"Make sure server.py is running first!\n\nError: {e}")
            root.destroy()
            return

        self._build_login_screen()

    # ──────────────────────────────────────────
    # Screen builders
    # ──────────────────────────────────────────

    def _clear(self):
        """Destroy all widgets in root."""
        for w in self.root.winfo_children():
            w.destroy()

    def _build_login_screen(self):
        self._clear()
        self.root.title("Mini GFS — Login")

        frame = tk.Frame(self.root, bg="#1e2a3a")
        frame.place(relx=0.5, rely=0.5, anchor="center")

        # Logo / Title
        tk.Label(frame, text="🗄️  Mini GFS", font=("Helvetica", 28, "bold"),
                 bg="#1e2a3a", fg="#00d4aa").pack(pady=(0, 4))
        tk.Label(frame, text="Distributed File Sharing System",
                 font=("Helvetica", 11), bg="#1e2a3a", fg="#aaaaaa").pack(pady=(0, 30))

        # Card
        card = tk.Frame(frame, bg="#253447", padx=40, pady=30,
                        relief="flat", bd=0)
        card.pack()

        tk.Label(card, text="Username", bg="#253447", fg="#cccccc",
                 font=("Helvetica", 10)).grid(row=0, column=0, sticky="w", pady=4)
        self.entry_user = tk.Entry(card, width=26, font=("Helvetica", 11),
                                   bg="#1e2a3a", fg="white", insertbackground="white",
                                   relief="flat", bd=6)
        self.entry_user.grid(row=1, column=0, pady=(0, 14))

        tk.Label(card, text="Password", bg="#253447", fg="#cccccc",
                 font=("Helvetica", 10)).grid(row=2, column=0, sticky="w", pady=4)
        self.entry_pass = tk.Entry(card, width=26, font=("Helvetica", 11),
                                   bg="#1e2a3a", fg="white", insertbackground="white",
                                   relief="flat", bd=6, show="•")
        self.entry_pass.grid(row=3, column=0, pady=(0, 20))

        btn_frame = tk.Frame(card, bg="#253447")
        btn_frame.grid(row=4, column=0)

        tk.Button(btn_frame, text="Login", width=12,
                  bg="#00d4aa", fg="#1e2a3a", font=("Helvetica", 10, "bold"),
                  relief="flat", bd=0, cursor="hand2",
                  command=self._do_login).pack(side="left", padx=5)

        tk.Button(btn_frame, text="Register", width=12,
                  bg="#3a4f6a", fg="white", font=("Helvetica", 10),
                  relief="flat", bd=0, cursor="hand2",
                  command=self._do_register).pack(side="left", padx=5)

        self.login_status = tk.Label(card, text="", bg="#253447",
                                     fg="#ff6b6b", font=("Helvetica", 9))
        self.login_status.grid(row=5, column=0, pady=(10, 0))

        # Bind Enter key
        self.entry_pass.bind("<Return>", lambda e: self._do_login())

    def _build_main_screen(self):
        self._clear()
        self.root.title(f"Mini GFS — {self.conn.username}")

        # ── Top bar ────────────────────────────
        topbar = tk.Frame(self.root, bg="#253447", height=50)
        topbar.pack(fill="x")

        tk.Label(topbar, text="🗄️  Mini GFS",
                 font=("Helvetica", 14, "bold"),
                 bg="#253447", fg="#00d4aa").pack(side="left", padx=16, pady=12)

        tk.Label(topbar, text=f"👤  {self.conn.username}",
                 font=("Helvetica", 10),
                 bg="#253447", fg="#aaaaaa").pack(side="left", padx=8)

        tk.Button(topbar, text="Logout", bg="#3a4f6a", fg="white",
                  font=("Helvetica", 9), relief="flat", cursor="hand2",
                  command=self._do_logout).pack(side="right", padx=12, pady=10)

        # ── Main layout ────────────────────────
        main = tk.Frame(self.root, bg="#1e2a3a")
        main.pack(fill="both", expand=True, padx=12, pady=12)

        # Left panel: file list + actions
        left = tk.Frame(main, bg="#1e2a3a")
        left.pack(side="left", fill="both", expand=True)

        # Right panel: node status + log
        right = tk.Frame(main, bg="#1e2a3a", width=260)
        right.pack(side="right", fill="y", padx=(12, 0))
        right.pack_propagate(False)

        self._build_file_panel(left)
        self._build_node_status_panel(right)
        self._build_log_panel(right)

        # Initial data load
        self._refresh_files()
        self._refresh_nodes()

    def _build_file_panel(self, parent):
        # Title
        tk.Label(parent, text="My Files", font=("Helvetica", 13, "bold"),
                 bg="#1e2a3a", fg="white").pack(anchor="w", pady=(0, 8))

        # Upload bar
        upload_frame = tk.Frame(parent, bg="#253447", padx=10, pady=10)
        upload_frame.pack(fill="x", pady=(0, 10))

        self.upload_label = tk.Label(upload_frame, text="No file selected",
                                     bg="#253447", fg="#aaaaaa",
                                     font=("Helvetica", 9), width=35, anchor="w")
        self.upload_label.pack(side="left")

        tk.Button(upload_frame, text="Browse", bg="#3a4f6a", fg="white",
                  font=("Helvetica", 9), relief="flat", cursor="hand2",
                  command=self._browse_file).pack(side="left", padx=6)

        tk.Button(upload_frame, text="⬆ Upload", bg="#00d4aa", fg="#1e2a3a",
                  font=("Helvetica", 9, "bold"), relief="flat", cursor="hand2",
                  command=self._do_upload).pack(side="left", padx=2)

        # Progress bar
        self.progress = ttk.Progressbar(parent, mode="determinate", length=400)
        self.progress.pack(fill="x", pady=(0, 10))

        # File table
        cols = ("Filename", "Primary Node", "Replica Node", "Uploaded At")
        self.file_tree = ttk.Treeview(parent, columns=cols,
                                       show="headings", height=12)
        for col in cols:
            self.file_tree.heading(col, text=col)
            self.file_tree.column(col, width=120 if col == "Filename" else 100)

        # Style
        style = ttk.Style()
        style.theme_use("clam")
        style.configure("Treeview",
                        background="#253447", foreground="white",
                        fieldbackground="#253447", rowheight=26)
        style.configure("Treeview.Heading",
                        background="#1e2a3a", foreground="#00d4aa",
                        font=("Helvetica", 9, "bold"))
        style.map("Treeview", background=[("selected", "#3a6f8a")])

        self.file_tree.pack(fill="both", expand=True)

        # Action buttons
        btn_row = tk.Frame(parent, bg="#1e2a3a")
        btn_row.pack(fill="x", pady=8)

        tk.Button(btn_row, text="⬇ Download Selected", bg="#3a7bd5",
                  fg="white", font=("Helvetica", 9), relief="flat",
                  cursor="hand2", command=self._do_download).pack(side="left", padx=4)

        tk.Button(btn_row, text="🔄 Refresh", bg="#3a4f6a",
                  fg="white", font=("Helvetica", 9), relief="flat",
                  cursor="hand2", command=self._refresh_files).pack(side="left", padx=4)

    def _build_node_status_panel(self, parent):
        tk.Label(parent, text="Node Status", font=("Helvetica", 11, "bold"),
                 bg="#1e2a3a", fg="white").pack(anchor="w", pady=(0, 6))

        self.node_labels = {}
        nodes_frame = tk.Frame(parent, bg="#253447", padx=12, pady=12)
        nodes_frame.pack(fill="x")

        for port in [9001, 9002, 9003]:
            row = tk.Frame(nodes_frame, bg="#253447")
            row.pack(fill="x", pady=3)

            dot = tk.Label(row, text="●", font=("Helvetica", 14),
                           bg="#253447", fg="#888888")
            dot.pack(side="left")

            tk.Label(row, text=f"Node {port}", bg="#253447", fg="#cccccc",
                     font=("Helvetica", 10)).pack(side="left", padx=6)

            status_lbl = tk.Label(row, text="Checking...",
                                  bg="#253447", fg="#888888",
                                  font=("Helvetica", 9))
            status_lbl.pack(side="right")

            self.node_labels[port] = (dot, status_lbl)

        tk.Button(parent, text="🔄 Refresh Nodes", bg="#3a4f6a",
                  fg="white", font=("Helvetica", 8), relief="flat",
                  cursor="hand2", command=self._refresh_nodes).pack(pady=6)

    def _build_log_panel(self, parent):
        tk.Label(parent, text="Activity Log", font=("Helvetica", 11, "bold"),
                 bg="#1e2a3a", fg="white").pack(anchor="w", pady=(12, 6))

        self.log_box = scrolledtext.ScrolledText(
            parent, height=18, bg="#0d1b2a", fg="#00d4aa",
            font=("Courier", 8), wrap="word", relief="flat"
        )
        self.log_box.pack(fill="both", expand=True)
        self.log_box.configure(state="disabled")

    # ──────────────────────────────────────────
    # Actions
    # ──────────────────────────────────────────

    def _log(self, msg):
        """Append a message to the activity log."""
        self.log_box.configure(state="normal")
        self.log_box.insert("end", f"▶  {msg}\n")
        self.log_box.see("end")
        self.log_box.configure(state="disabled")

    def _do_login(self):
        u = self.entry_user.get().strip()
        p = self.entry_pass.get().strip()
        if not u or not p:
            self.login_status.config(text="Please enter username and password.")
            return

        ok, msg = self.conn.login(u, p)
        if ok:
            self._build_main_screen()
        else:
            self.login_status.config(text=msg)

    def _do_register(self):
        u = self.entry_user.get().strip()
        p = self.entry_pass.get().strip()
        if not u or not p:
            self.login_status.config(text="Please fill in both fields.")
            return
        if len(p) < 4:
            self.login_status.config(text="Password must be at least 4 characters.")
            return

        ok, msg = self.conn.register(u, p)
        color = "#00d4aa" if ok else "#ff6b6b"
        self.login_status.config(text=msg, fg=color)

    def _do_logout(self):
        self.conn.username = None
        self._build_login_screen()

    def _browse_file(self):
        path = filedialog.askopenfilename()
        if path:
            self.selected_file = path
            name = os.path.basename(path)
            size = os.path.getsize(path)
            self.upload_label.config(
                text=f"{name}  ({size:,} bytes)", fg="white"
            )
        else:
            self.selected_file = None

    def _do_upload(self):
        if not hasattr(self, "selected_file") or not self.selected_file:
            messagebox.showwarning("No file", "Please select a file first.")
            return

        self._log(f"Uploading: {os.path.basename(self.selected_file)}")
        self.progress["value"] = 0

        def task():
            def on_progress(sent, total):
                pct = (sent / total) * 100
                self.progress["value"] = pct

            ok, msg = self.conn.upload(self.selected_file, on_progress)
            self.progress["value"] = 100 if ok else 0
            self._log(msg)
            if ok:
                self._refresh_files()
                self._refresh_nodes()

        threading.Thread(target=task, daemon=True).start()

    def _do_download(self):
        selected = self.file_tree.selection()
        if not selected:
            messagebox.showwarning("No selection", "Please select a file to download.")
            return

        filename = self.file_tree.item(selected[0])["values"][0]
        save_dir = filedialog.askdirectory(title="Save file to...")
        if not save_dir:
            return

        self._log(f"Downloading: {filename}")

        def task():
            ok, msg = self.conn.download(filename, save_dir)
            self._log(msg)
            if ok:
                self._refresh_nodes()

        threading.Thread(target=task, daemon=True).start()

    def _refresh_files(self):
        files = self.conn.list_files()
        # Clear table
        for row in self.file_tree.get_children():
            self.file_tree.delete(row)
        for f in files:
            self.file_tree.insert("", "end", values=(
                f["filename"],
                f"Node {f['primary_node']}",
                f"Node {f['replica_node']}",
                f["uploaded_at"][:16]   # trim seconds
            ))

    def _refresh_nodes(self):
        statuses = self.conn.get_node_status()
        for port_str, status in statuses.items():
            port = int(port_str)
            if port in self.node_labels:
                dot, lbl = self.node_labels[port]
                if status == "ALIVE":
                    dot.config(fg="#00d4aa")
                    lbl.config(text="ALIVE", fg="#00d4aa")
                else:
                    dot.config(fg="#ff6b6b")
                    lbl.config(text="DOWN", fg="#ff6b6b")
                self._log(f"Node {port}: {status}")

        # Auto-refresh every 5 seconds
        self.root.after(5000, self._refresh_nodes)


# ──────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────

if __name__ == "__main__":
    root = tk.Tk()
    app  = MiniGFSApp(root)
    root.protocol("WM_DELETE_WINDOW", lambda: (app.conn.disconnect(), root.destroy()))
    root.mainloop()
