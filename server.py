#!/usr/bin/env python3
from flask import Flask, jsonify, render_template, request, Response, stream_with_context
import subprocess
import os
import signal
from pathlib import Path
import threading
import time
import queue

import serial  # pip install pyserial

# =============================
# PATHS
# =============================
BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

KVM_LOG_PATH = LOG_DIR / "kvm.log"
FFMPEG_STREAM_ERR = LOG_DIR / "ffmpeg_stream.stderr.log"
INPUT_LOG_PATH = LOG_DIR / "input.log"

# =============================
# APP INIT
# =============================
app = Flask(__name__)

# =============================
# VIDEO CONFIG
# =============================
VIDEO_DEV = "/dev/video0"
VIDEO_SIZE = "1280x1024"
FPS = "30"
INPUT_FORMAT = "mjpeg"

# =============================
# SERIAL CONFIG (TU CASO: CP210x -> /dev/ttyUSB0)
# =============================
SERIAL_PORT = "/dev/ttyUSB0"
SERIAL_BAUD = 115200
SERIAL_TIMEOUT = 0

# =============================
# LOG
# =============================
def log(msg: str):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}\n"
    try:
        with open(KVM_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass
    print(line, end="", flush=True)

def log_input(msg: str):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}\n"
    try:
        with open(INPUT_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass

# =============================
# STREAM PROCESS
# =============================
_ffmpeg_stream_proc = None
_ffmpeg_lock = threading.Lock()

def _start_stream_proc():
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "warning",
        "-fflags", "nobuffer",
        "-flags", "low_delay",
        "-f", "v4l2",
        "-input_format", INPUT_FORMAT,
        "-video_size", VIDEO_SIZE,
        "-framerate", FPS,
        "-i", VIDEO_DEV,
        "-f", "mjpeg",
        "-q:v", "5",
        "pipe:1",
    ]

    errf = open(FFMPEG_STREAM_ERR, "ab", buffering=0)
    log(f"Starting stream ffmpeg: {' '.join(cmd)}")

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=errf,
        bufsize=0,
        start_new_session=True
    )
    return proc

def _ensure_stream_proc():
    global _ffmpeg_stream_proc
    with _ffmpeg_lock:
        if _ffmpeg_stream_proc and _ffmpeg_stream_proc.poll() is None:
            return _ffmpeg_stream_proc

        _ffmpeg_stream_proc = None
        proc = _start_stream_proc()

        time.sleep(0.4)
        if proc.poll() is not None:
            log("ERROR: stream ffmpeg died at start (see logs/ffmpeg_stream.stderr.log)")
            _ffmpeg_stream_proc = None
            return None

        _ffmpeg_stream_proc = proc
        log(f"stream ffmpeg running pid={proc.pid}")
        return _ffmpeg_stream_proc

def _stream_generator():
    proc = _ensure_stream_proc()
    if not proc or not proc.stdout:
        return

    boundary = b"--frame\r\n"
    header = b"Content-Type: image/jpeg\r\n\r\n"

    buf = b""
    while True:
        if proc.poll() is not None:
            break

        chunk = proc.stdout.read(4096)
        if not chunk:
            continue

        buf += chunk

        while True:
            soi = buf.find(b"\xff\xd8")
            if soi < 0:
                if len(buf) > 2_000_000:
                    buf = buf[-500_000:]
                break

            eoi = buf.find(b"\xff\xd9", soi)
            if eoi < 0:
                if soi > 0:
                    buf = buf[soi:]
                break

            jpg = buf[soi:eoi+2]
            buf = buf[eoi+2:]
            yield boundary + header + jpg + b"\r\n"

@app.route("/stream.mjpg")
def stream_mjpg():
    proc = _ensure_stream_proc()
    if not proc:
        return "STREAM OFF\n", 503

    return Response(
        stream_with_context(_stream_generator()),
        mimetype="multipart/x-mixed-replace; boundary=frame",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
        }
    )

# =============================
# SERIAL TX (cola + hilo)
# =============================
_ser = None
_ser_lock = threading.Lock()
_txq: "queue.Queue[bytes]" = queue.Queue(maxsize=8000)

# Estado para watchdog/keepalive
_state_lock = threading.Lock()
mouse_btnmask = 0          # bits: 1 left, 2 right, 4 middle (boot mouse)
any_key_down = False       # para mantener vivo si tecla pulsada

def _open_serial():
    global _ser
    with _ser_lock:
        if _ser and _ser.is_open:
            return _ser
        _ser = serial.Serial(SERIAL_PORT, SERIAL_BAUD, timeout=SERIAL_TIMEOUT)
        log(f"Serial open: {SERIAL_PORT} @ {SERIAL_BAUD}")
        return _ser

def _tx_worker():
    while True:
        data = _txq.get()
        try:
            s = _open_serial()
            s.write(data)
        except Exception as e:
            log(f"Serial write error: {e}")
            time.sleep(0.3)
        finally:
            _txq.task_done()

threading.Thread(target=_tx_worker, daemon=True).start()

def _send_line(line: str):
    if not line.endswith("\n"):
        line += "\n"
    b = line.encode("utf-8", errors="replace")
    try:
        _txq.put_nowait(b)
    except queue.Full:
        log("WARN: input queue full, dropping input")

def _keepalive_worker():
    # Evita que el watchdog de 250ms de la Pico suelte teclas/botones
    while True:
        time.sleep(0.12)  # ~8 Hz
        with _state_lock:
            btn = mouse_btnmask
            kd = any_key_down
        if btn != 0 or kd:
            _send_line(f"MD 0 0 0 {btn}")
threading.Thread(target=_keepalive_worker, daemon=True).start()

# =============================
# HID MAP (KeyboardEvent.code -> HID Usage)
# HID usage IDs (Keyboard/Keypad page)
# =============================
HID = {
    # Letters
    **{f"Key{chr(c)}": 0x04 + (c - ord('A')) for c in range(ord('A'), ord('Z')+1)},
    # Digits (top row)
    "Digit1": 0x1E, "Digit2": 0x1F, "Digit3": 0x20, "Digit4": 0x21, "Digit5": 0x22,
    "Digit6": 0x23, "Digit7": 0x24, "Digit8": 0x25, "Digit9": 0x26, "Digit0": 0x27,

    # Basic keys
    "Enter": 0x28,
    "Escape": 0x29,
    "Backspace": 0x2A,
    "Tab": 0x2B,
    "Space": 0x2C,

    # Symbols
    "Minus": 0x2D,
    "Equal": 0x2E,
    "BracketLeft": 0x2F,
    "BracketRight": 0x30,
    "Backslash": 0x31,
    "Semicolon": 0x33,
    "Quote": 0x34,
    "Backquote": 0x35,
    "Comma": 0x36,
    "Period": 0x37,
    "Slash": 0x38,

    # Locks
    "CapsLock": 0x39,

    # Function keys
    "F1": 0x3A, "F2": 0x3B, "F3": 0x3C, "F4": 0x3D, "F5": 0x3E, "F6": 0x3F,
    "F7": 0x40, "F8": 0x41, "F9": 0x42, "F10": 0x43, "F11": 0x44, "F12": 0x45,

    # Navigation
    "PrintScreen": 0x46,
    "ScrollLock": 0x47,
    "Pause": 0x48,
    "Insert": 0x49,
    "Home": 0x4A,
    "PageUp": 0x4B,
    "Delete": 0x4C,
    "End": 0x4D,
    "PageDown": 0x4E,
    "ArrowRight": 0x4F,
    "ArrowLeft": 0x50,
    "ArrowDown": 0x51,
    "ArrowUp": 0x52,

    # Keypad (por si lo usas)
    "NumpadEnter": 0x58,
    "NumpadAdd": 0x57,
    "NumpadSubtract": 0x56,
    "NumpadMultiply": 0x55,
    "NumpadDivide": 0x54,
    "NumpadDecimal": 0x63,
    "Numpad0": 0x62, "Numpad1": 0x59, "Numpad2": 0x5A, "Numpad3": 0x5B, "Numpad4": 0x5C,
    "Numpad5": 0x5D, "Numpad6": 0x5E, "Numpad7": 0x5F, "Numpad8": 0x60, "Numpad9": 0x61,
}

def mods_to_byte(mods: dict) -> int:
    # Usamos los modificadores "izquierdos" (suficiente)
    m = 0
    if mods.get("ctrl"):  m |= 0x01
    if mods.get("shift"): m |= 0x02
    if mods.get("alt"):   m |= 0x04
    if mods.get("meta"):  m |= 0x08
    return m

# =============================
# WEB
# =============================
@app.route("/")
def index():
    return render_template("index.html")

# =============================
# API STATUS / STREAM RESTART
# =============================
@app.route("/api/status")
def api_status():
    with _ffmpeg_lock:
        p = _ffmpeg_stream_proc
    running = bool(p and p.poll() is None)
    pid = p.pid if running else None
    return jsonify(ok=True, running=running, pid=pid, stream="ON" if running else "OFF")

@app.route("/api/stream_restart", methods=["POST"])
def api_stream_restart():
    global _ffmpeg_stream_proc
    with _ffmpeg_lock:
        p = _ffmpeg_stream_proc
        _ffmpeg_stream_proc = None
    if p and p.poll() is None:
        try:
            os.killpg(p.pid, signal.SIGTERM)
        except Exception:
            pass
    return jsonify(ok=True)

# =============================
# INPUT API -> TU PROTOCOLO (MD/KD/KU/CL)
# =============================

# Browser button -> boot mouse bits
# browser: 0 left, 1 middle, 2 right
BTN_MAP = {0: 0x01, 1: 0x04, 2: 0x02}

@app.route("/api/input/move", methods=["POST"])
def api_input_move():
    d = request.get_json(force=True, silent=True) or {}
    dx = int(d.get("dx", 0))
    dy = int(d.get("dy", 0))

    with _state_lock:
        btn = mouse_btnmask

    _send_line(f"MD {dx} {dy} 0 {btn}")
    log_input(f"MD {dx} {dy} 0 {btn}")
    return jsonify(ok=True)

@app.route("/api/input/button", methods=["POST"])
def api_input_button():
    global mouse_btnmask
    d = request.get_json(force=True, silent=True) or {}
    button = int(d.get("button", 0))
    state = int(d.get("state", 0))

    bit = BTN_MAP.get(button, 0)

    with _state_lock:
        if state:
            mouse_btnmask |= bit
        else:
            mouse_btnmask &= (~bit) & 0x07
        btn = mouse_btnmask

    # Enviamos un MD “cero” solo para actualizar botones
    _send_line(f"MD 0 0 0 {btn}")
    log_input(f"MD 0 0 0 {btn} (btn event b={button} s={state})")
    return jsonify(ok=True)

@app.route("/api/input/wheel", methods=["POST"])
def api_input_wheel():
    d = request.get_json(force=True, silent=True) or {}
    delta = int(d.get("delta", 0))  # +1/-1

    with _state_lock:
        btn = mouse_btnmask

    _send_line(f"MD 0 0 {delta} {btn}")
    log_input(f"MD 0 0 {delta} {btn} (wheel)")
    return jsonify(ok=True)

@app.route("/api/input/key", methods=["POST"])
def api_input_key():
    global any_key_down
    d = request.get_json(force=True, silent=True) or {}
    code = str(d.get("code", ""))
    state = int(d.get("state", 0))
    mods = d.get("mods", {}) or {}

    hid = HID.get(code, 0)
    modb = mods_to_byte(mods)

    # Si no reconocemos la tecla, ignoramos
    if hid == 0:
        log_input(f"KEY UNKNOWN {code} state={state} mods={mods}")
        return jsonify(ok=True)

    if state == 1:
        with _state_lock:
            any_key_down = True
        _send_line(f"KD {hid:02X} {modb:02X}")
        log_input(f"KD {hid:02X} {modb:02X} ({code})")
    else:
        with _state_lock:
            any_key_down = False
        # Tu parser exige 3 tokens siempre:
        _send_line("KU 00 00")
        log_input(f"KU 00 00 ({code})")

    return jsonify(ok=True)

@app.route("/api/input/clear", methods=["POST"])
def api_input_clear():
    global mouse_btnmask, any_key_down
    with _state_lock:
        mouse_btnmask = 0
        any_key_down = False
    _send_line("CL")
    log_input("CL")
    return jsonify(ok=True)

# =============================
# MAIN
# =============================
if __name__ == "__main__":
    print("Starting KVM Web Server...")
    app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False, threaded=True)
