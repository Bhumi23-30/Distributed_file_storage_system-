"""
database.py — SQLite setup and helper functions
Mini GFS | Distributed File Sharing System
"""

import sqlite3
import hashlib
import os

DB_PATH = "gfs.db"


def get_connection():
    """Return a connection to the SQLite database."""
    return sqlite3.connect(DB_PATH)


def init_db():
    """
    Create all tables if they don't exist.
    Called once when the server starts.
    
    Tables:
      - users  : stores login credentials
      - files  : stores file metadata (which node holds the file)
    """
    conn = get_connection()
    cursor = conn.cursor()

    # --- Users table ---
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT    UNIQUE NOT NULL,
            password TEXT    NOT NULL
        )
    """)

    # --- Files table ---
    # primary_node  : port of the node that holds the original file
    # replica_node  : port of the node that holds the replica
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS files (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            filename     TEXT    NOT NULL,
            owner        TEXT    NOT NULL,
            primary_node INTEGER NOT NULL,
            replica_node INTEGER NOT NULL,
            uploaded_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.commit()
    conn.close()
    print("[DB] Database initialized.")


# ──────────────────────────────────────────────
# User helpers
# ──────────────────────────────────────────────

def hash_password(password):
    """SHA-256 hash a password before storing."""
    return hashlib.sha256(password.encode()).hexdigest()


def register_user(username, password):
    """
    Register a new user.
    Returns (True, "OK") or (False, "error reason").
    """
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "INSERT INTO users (username, password) VALUES (?, ?)",
            (username, hash_password(password))
        )
        conn.commit()
        return True, "User registered successfully."
    except sqlite3.IntegrityError:
        return False, "Username already exists."
    finally:
        conn.close()


def verify_user(username, password):
    """
    Check login credentials.
    Returns True if valid, False otherwise.
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT password FROM users WHERE username = ?",
        (username,)
    )
    row = cursor.fetchone()
    conn.close()

    if row and row[0] == hash_password(password):
        return True
    return False


# ──────────────────────────────────────────────
# File metadata helpers
# ──────────────────────────────────────────────

def save_file_metadata(filename, owner, primary_node, replica_node):
    """
    Save a record that 'filename' owned by 'owner'
    is stored on primary_node, with replica on replica_node.
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """INSERT INTO files (filename, owner, primary_node, replica_node)
           VALUES (?, ?, ?, ?)""",
        (filename, owner, primary_node, replica_node)
    )
    conn.commit()
    conn.close()


def get_file_metadata(filename, owner):
    """
    Look up where a file is stored.
    Returns a dict with primary_node and replica_node, or None.
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """SELECT primary_node, replica_node
           FROM files
           WHERE filename = ? AND owner = ?
           ORDER BY uploaded_at DESC LIMIT 1""",
        (filename, owner)
    )
    row = cursor.fetchone()
    conn.close()

    if row:
        return {"primary_node": row[0], "replica_node": row[1]}
    return None


def list_user_files(owner):
    """
    Return a list of all filenames uploaded by 'owner'.
    Each entry: (filename, primary_node, replica_node, uploaded_at)
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """SELECT filename, primary_node, replica_node, uploaded_at
           FROM files WHERE owner = ?
           ORDER BY uploaded_at DESC""",
        (owner,)
    )
    rows = cursor.fetchall()
    conn.close()
    return rows


def delete_file_metadata(filename, owner):
    """Remove a file's metadata record."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "DELETE FROM files WHERE filename = ? AND owner = ?",
        (filename, owner)
    )
    conn.commit()
    conn.close()


# ──────────────────────────────────────────────
# Quick test — run this file directly to verify
# ──────────────────────────────────────────────
if __name__ == "__main__":
    init_db()

    # Test user registration
    print(register_user("aayush", "pass123"))
    print(register_user("aayush", "pass123"))  # Should fail (duplicate)

    # Test login
    print("Login valid?", verify_user("aayush", "pass123"))
    print("Login valid?", verify_user("aayush", "wrongpass"))

    # Test file metadata
    save_file_metadata("resume.pdf", "aayush", 9001, 9002)
    print(get_file_metadata("resume.pdf", "aayush"))
    print(list_user_files("aayush"))
