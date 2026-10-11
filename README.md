# Mini GFS — Distributed File Sharing System
### DCT Project | Aayush Patwa, Bhumi Neema, Dhruv Goyal, Soumya Gangrade

---

## How to Run (4 terminals on one laptop)

Open **4 separate terminal windows** in the `mini_gfs/` folder:

```
Terminal 1:   python server.py
Terminal 2:   python node.py 9001
Terminal 3:   python node.py 9002
Terminal 4:   python node.py 9003
```

Then open a **5th terminal** (or double-click):
```
python client_gui.py
```

---

## Demo Script for Faculty

### 1. Login / Register
- Open GUI → Register a new user → Login

### 2. Upload + Replication
- Click Browse → pick any file → Upload
- After upload, check: `storage/node9001/` and `storage/node9002/`
- Same file appears in BOTH folders → **Replication working ✓**

### 3. Node Status Panel
- Right panel shows Node 9001, 9002, 9003 all GREEN (ALIVE)

### 4. Fault Tolerance Demo ← most impressive
- Upload a file (it goes to e.g. Node 9001, replica on 9002)
- **Kill Terminal 2** (Node 9001) — press Ctrl+C
- Node status turns RED for Node 9001
- Now click Download on that same file
- It still downloads! → **Served from replica Node 9002 ✓**
- Server log shows: "Primary node DOWN! Trying replica..."

### 5. Load Balancing
- Upload 3 different files
- See in server terminal: files go to Node 9001, 9002, 9003 in rotation
- → **Round-robin load balancing ✓**

---

## File Structure

```
mini_gfs/
├── server.py          Main coordinator (port 9000)
├── node.py            Storage node (run 3x on 9001, 9002, 9003)
├── client_gui.py      Tkinter GUI client
├── database.py        SQLite helper functions
├── gfs.db             Created automatically on first run
└── storage/
    ├── node9001/      Files stored by Node 1
    ├── node9002/      Files stored by Node 2
    └── node9003/      Files stored by Node 3
```

---

## Requirements
- Python 3.8+
- No pip installs needed — all standard library
