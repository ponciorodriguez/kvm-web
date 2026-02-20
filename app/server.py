from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO
import os, subprocess, time

FIFO = "/tmp/pico_cmd"

KVM_START = "/home/jefe/kvm/kvm_start.sh"
KVM_STOP  = "/home/jefe/kvm/kvm_stop.sh"

PROC_PATTERNS = [
    "pico_uart.py",
    "x11_mouse_grab_to_fifo.py",
    "evdev_mouse_to_fifo.py",
    "ffplay -f v4l2",
]

# IMPORTANTE: apuntamos a ../templates y ../static
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATES_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "templates"))
STATIC_DIR    = os.path.abspath(os.path.join(BASE_DIR, "..", "static"))

app = Flask(__name__, template_folder=TEMPLATES_DIR, static_folder=STATIC_DIR)
socketio = SocketIO(app, cors_allowed_origins="*")

def send_fifo(line: str):
    if os.path.exists(FIFO):
        with open(FIFO, "w") as f:
            f.write(line + "\n")

def _run_script(path: str):
    if not os.path.exists(path):
        return False, f"No existe: {path}"
    if not os.access(path, os.X_OK):
        return False, f"No ejecutable: {path} (chmod +x)"
    try:
        p = subprocess.run([path], capture_output=True, text=True, timeout=30)
        ok = (p.returncode == 0)
        msg = (p.stdout or p.stderr or f"rc={p.returncode}").strip()
        return ok, msg
    except subprocess.TimeoutExpired:
        return False, "Timeout ejecutando script"
    except Exception as e:
        return False, f"Error ejecutando script: {e}"

def _pgrep_any(pattern: str) -> bool:
    try:
        r = subprocess.run(["pgrep", "-af", pattern], capture_output=True, text=True)
        return r.returncode == 0
    except Exception:
        return False

def kvm_status():
    found = {p: _pgrep_any(p) for p in PROC_PATTERNS}
    running = any(found.values())
    return {"running": running, "procs": found}

@app.route("/")
def index():
    return render_template("index.html")

@app.get("/api/status")
def api_status():
    return jsonify(kvm_status())

@app.post("/api/start")
def api_start():
    ok, msg = _run_script(KVM_START)
    time.sleep(0.3)
    return jsonify({"ok": ok, "msg": msg, "status": kvm_status()}), (200 if ok else 500)

@app.post("/api/stop")
def api_stop():
    ok, msg = _run_script(KVM_STOP)
    time.sleep(0.2)
    return jsonify({"ok": ok, "msg": msg, "status": kvm_status()}), (200 if ok else 500)

@socketio.on("mouse")
def on_mouse(d):
    dx = int(d.get("dx", 0))
    dy = int(d.get("dy", 0))
    wheel = int(d.get("wheel", 0))
    btn = int(d.get("btn", 0))
    send_fifo(f"MD {dx} {dy} {wheel} {btn}")

@socketio.on("key")
def on_key(d):
    cmd = d.get("cmd")
    hid = d.get("hid")
    mod = d.get("mod", "00")
    if cmd in ("KD", "KU") and isinstance(hid, str):
        send_fifo(f"{cmd} {hid} {mod}")

if __name__ == "__main__":
    socketio.run(app, host="0.0.0.0", port=5000)
