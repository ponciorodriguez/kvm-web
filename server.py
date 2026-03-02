#!/usr/bin/env python3
from flask import Flask, jsonify, render_template, request, Response, stream_with_context
import subprocess
import os
import signal
from pathlib import Path
import threading
import time
import queue
import json

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
# VIDEO CONFIG (defaults)
# =============================
VIDEO_DEV = "/dev/video0"
VIDEO_SIZE = "1280x720"
FPS = "12"
INPUT_FORMAT = "mjpeg"
DEFAULT_QV = 5

# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# HARD LOCK: FPS FIJO SIEMPRE
# Pon None si algún día quieres permitir cambios
LOCK_FPS = 12
# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

# Runtime-editable config file
VIDEO_CFG_PATH = BASE_DIR / "config" / "video.json"
VIDEO_CFG_PATH.parent.mkdir(exist_ok=True)

_video_cfg_lock = threading.Lock()
_video_cfg_cache = None
_video_cfg_mtime = 0.0

# =============================
# SERIAL CONFIG (CP210x -> /dev/ttyUSB0)
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

def invalidate_video_cfg_cache():
    global _video_cfg_cache, _video_cfg_mtime
    with _video_cfg_lock:
        _video_cfg_cache = None
        _video_cfg_mtime = 0.0

def _sanitize_cfg(size: str, fps: int, qv: int, fmt: str):
    # sane limits
    if fps < 5: fps = 5
    if fps > 60: fps = 60
    if qv < 2: qv = 2
    if qv > 15: qv = 15
    fmt = (fmt or "mjpeg").lower()
    if fmt not in ("mjpeg", "mjpg"):
        fmt = "mjpeg"
    # FPS lock
    if LOCK_FPS is not None:
        fps = int(LOCK_FPS)
    return size, fps, qv, "mjpeg"

def _write_video_cfg(size: str, fps: int, qv: int, fmt: str = "mjpeg"):
    VIDEO_CFG_PATH.parent.mkdir(exist_ok=True)
    with open(VIDEO_CFG_PATH, "w", encoding="utf-8") as f:
        json.dump({"size": size, "fps": fps, "qv": qv, "format": "mjpeg"}, f, indent=2)

def load_video_cfg():
    """
    Lee config de vídeo desde config/video.json con cache por mtime.
    Devuelve dict: {size, fps, qv, format}
    """
    global _video_cfg_cache, _video_cfg_mtime

    defaults = {
        "size": VIDEO_SIZE,
        "fps": int(FPS),
        "qv": int(DEFAULT_QV),
        "format": INPUT_FORMAT,
    }

    # Si hay LOCK_FPS, que el default también lo respete
    if LOCK_FPS is not None:
        defaults["fps"] = int(LOCK_FPS)

    try:
        st = VIDEO_CFG_PATH.stat()
        mtime = st.st_mtime
    except FileNotFoundError:
        # no existe config aún
        size, fps, qv, fmt = _sanitize_cfg(defaults["size"], defaults["fps"], defaults["qv"], defaults["format"])
        return {"size": size, "fps": fps, "qv": qv, "format": fmt}

    with _video_cfg_lock:
        if _video_cfg_cache is not None and mtime == _video_cfg_mtime:
            return _video_cfg_cache

        try:
            with open(VIDEO_CFG_PATH, "r", encoding="utf-8") as f:
                cfg = json.load(f) or {}
        except Exception as e:
            log(f"WARN: cannot read {VIDEO_CFG_PATH}: {e}")
            size, fps, qv, fmt = _sanitize_cfg(defaults["size"], defaults["fps"], defaults["qv"], defaults["format"])
            return {"size": size, "fps": fps, "qv": qv, "format": fmt}

        size = str(cfg.get("size", defaults["size"]))
        fps = int(cfg.get("fps", defaults["fps"]))
        qv = int(cfg.get("qv", defaults["qv"]))
        fmt = str(cfg.get("format", defaults["format"])).lower()

        size, fps, qv, fmt = _sanitize_cfg(size, fps, qv, fmt)

        # Si el JSON tenía otro fps y hay LOCK_FPS, lo corregimos en disco (una sola vez)
        if LOCK_FPS is not None:
            try:
                if int(cfg.get("fps", -1)) != int(LOCK_FPS):
                    log(f"INFO: forcing fps to {LOCK_FPS} (was {cfg.get('fps')}); rewriting {VIDEO_CFG_PATH}")
                    _write_video_cfg(size, fps, qv, fmt)
                    # refrescar mtime
                    mtime = VIDEO_CFG_PATH.stat().st_mtime
            except Exception as e:
                log(f"WARN: cannot rewrite locked fps cfg: {e}")

        _video_cfg_cache = {"size": size, "fps": fps, "qv": qv, "format": fmt}
        _video_cfg_mtime = mtime
        return _video_cfg_cache


# =============================
# STREAM PROCESS + BROADCASTER
# =============================
_ffmpeg_stream_proc = None
_ffmpeg_errf = None
_ffmpeg_lock = threading.Lock()

class FrameHub:
    def __init__(self):
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._frame = None
        self._seq = 0
        self._running = False
        self._thread = None

    def start(self):
        with self._lock:
            if self._running:
                return
            self._running = True
            self._thread = threading.Thread(target=self._worker, daemon=True)
            self._thread.start()

    def update_frame(self, jpg: bytes):
        with self._lock:
            self._frame = jpg
            self._seq += 1
            self._cond.notify_all()

    def get_next(self, last_seq: int, timeout: float = 2.0):
        with self._lock:
            if not self._running:
                return None, last_seq
            end = time.time() + timeout
            while self._seq == last_seq and self._running:
                remaining = end - time.time()
                if remaining <= 0:
                    break
                self._cond.wait(timeout=remaining)
            return self._frame, self._seq

    def _worker(self):
        backoff = 0.5
        buf = b""
        while True:
            with self._lock:
                if not self._running:
                    return

            proc = ensure_stream_proc()
            if not proc or not proc.stdout:
                time.sleep(backoff)
                backoff = min(backoff * 1.5, 3.0)
                continue

            backoff = 0.5
            try:
                chunk = proc.stdout.read(4096)
                if not chunk:
                    time.sleep(0.01)
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
                    self.update_frame(jpg)

            except Exception as e:
                log(f"WARN: frame worker read error: {e}")
                stream_restart_internal()
                time.sleep(backoff)
                backoff = min(backoff * 1.5, 3.0)

framehub = FrameHub()
framehub.start()

def start_stream_proc():
    cfg = load_video_cfg()

    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "warning",
        "-fflags", "nobuffer",
        "-flags", "low_delay",
        "-f", "v4l2",
        "-input_format", cfg["format"],
        "-video_size", cfg["size"],
        "-framerate", str(cfg["fps"]),
        "-i", VIDEO_DEV,
        "-f", "mjpeg",
        "-q:v", str(cfg["qv"]),
        "pipe:1",
    ]

    global _ffmpeg_errf
    _ffmpeg_errf = open(FFMPEG_STREAM_ERR, "ab", buffering=0)
    log(f"Starting stream ffmpeg: {' '.join(cmd)}")

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=_ffmpeg_errf,
        bufsize=0,
        start_new_session=True
    )
    return proc

def stop_stream_proc(proc):
    if not proc:
        return
    try:
        if proc.poll() is None:
            try:
                os.killpg(proc.pid, signal.SIGTERM)
            except Exception:
                pass
            for _ in range(20):
                if proc.poll() is not None:
                    break
                time.sleep(0.05)
            if proc.poll() is None:
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except Exception:
                    pass
    finally:
        try:
            if proc.stdout:
                proc.stdout.close()
        except Exception:
            pass

def ensure_stream_proc():
    global _ffmpeg_stream_proc
    with _ffmpeg_lock:
        if _ffmpeg_stream_proc and _ffmpeg_stream_proc.poll() is None:
            return _ffmpeg_stream_proc

        if _ffmpeg_stream_proc:
            stop_stream_proc(_ffmpeg_stream_proc)
            _ffmpeg_stream_proc = None

        proc = start_stream_proc()
        time.sleep(0.25)
        if proc.poll() is not None:
            log("ERROR: stream ffmpeg died at start (see logs/ffmpeg_stream.stderr.log)")
            stop_stream_proc(proc)
            _ffmpeg_stream_proc = None
            return None

        _ffmpeg_stream_proc = proc
        log(f"stream ffmpeg running pid={proc.pid}")
        return _ffmpeg_stream_proc

def stream_restart_internal():
    global _ffmpeg_stream_proc
    with _ffmpeg_lock:
        p = _ffmpeg_stream_proc
        _ffmpeg_stream_proc = None
    if p:
        stop_stream_proc(p)

@app.route("/stream.mjpg")
def stream_mjpg():
    proc = ensure_stream_proc()
    if not proc:
        return "STREAM OFF\n", 503

    boundary = b"--frame\r\n"
    header = b"Content-Type: image/jpeg\r\n\r\n"

    def gen():
        last = 0
        while True:
            jpg, seq = framehub.get_next(last, timeout=2.0)
            last = seq
            if jpg:
                yield boundary + header + jpg + b"\r\n"
            else:
                yield boundary + b"Content-Type: text/plain\r\n\r\n\r\n"

    return Response(
        stream_with_context(gen()),
        mimetype="multipart/x-mixed-replace; boundary=frame",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
        }
    )

# =============================
# SERIAL TX (queue + thread)
# =============================
_ser = None
_ser_lock = threading.Lock()
_txq: "queue.Queue[bytes]" = queue.Queue(maxsize=8000)

_state_lock = threading.Lock()
mouse_btnmask = 0
any_key_down = False

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
    while True:
        time.sleep(0.12)
        with _state_lock:
            btn = mouse_btnmask
            kd = any_key_down
        if btn != 0 or kd:
            _send_line(f"MD 0 0 0 {btn}")

threading.Thread(target=_keepalive_worker, daemon=True).start()

# =============================
# HID MAP
# =============================
HID = {
    **{f"Key{chr(c)}": 0x04 + (c - ord('A')) for c in range(ord('A'), ord('Z')+1)},
    "Digit1": 0x1E, "Digit2": 0x1F, "Digit3": 0x20, "Digit4": 0x21, "Digit5": 0x22,
    "Digit6": 0x23, "Digit7": 0x24, "Digit8": 0x25, "Digit9": 0x26, "Digit0": 0x27,
    "Enter": 0x28, "Escape": 0x29, "Backspace": 0x2A, "Tab": 0x2B, "Space": 0x2C,
    "Minus": 0x2D, "Equal": 0x2E, "BracketLeft": 0x2F, "BracketRight": 0x30,
    "Backslash": 0x31, "Semicolon": 0x33, "Quote": 0x34, "Backquote": 0x35,
    "Comma": 0x36, "Period": 0x37, "Slash": 0x38,
    "CapsLock": 0x39,
    "F1": 0x3A, "F2": 0x3B, "F3": 0x3C, "F4": 0x3D, "F5": 0x3E, "F6": 0x3F,
    "F7": 0x40, "F8": 0x41, "F9": 0x42, "F10": 0x43, "F11": 0x44, "F12": 0x45,
    "PrintScreen": 0x46, "ScrollLock": 0x47, "Pause": 0x48,
    "Insert": 0x49, "Home": 0x4A, "PageUp": 0x4B, "Delete": 0x4C, "End": 0x4D,
    "PageDown": 0x4E, "ArrowRight": 0x4F, "ArrowLeft": 0x50, "ArrowDown": 0x51, "ArrowUp": 0x52,
    "NumpadEnter": 0x58, "NumpadAdd": 0x57, "NumpadSubtract": 0x56, "NumpadMultiply": 0x55,
    "NumpadDivide": 0x54, "NumpadDecimal": 0x63,
    "Numpad0": 0x62, "Numpad1": 0x59, "Numpad2": 0x5A, "Numpad3": 0x5B, "Numpad4": 0x5C,
    "Numpad5": 0x5D, "Numpad6": 0x5E, "Numpad7": 0x5F, "Numpad8": 0x60, "Numpad9": 0x61,
}

def mods_to_byte(mods: dict) -> int:
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
    cfg = load_video_cfg()
    return jsonify(ok=True, running=running, pid=pid, stream="ON" if running else "OFF", cfg=cfg)

@app.route("/api/stream_restart", methods=["POST"])
def api_stream_restart():
    stream_restart_internal()
    invalidate_video_cfg_cache()
    return jsonify(ok=True)
@app.route("/api/restart_server", methods=["POST"])
def api_restart_server():
    try:
        r = subprocess.run(
            ["sudo", "-n", "/bin/systemctl", "restart", "kvm-web"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        if r.returncode != 0:
            log(f"ERROR restart kvm-web failed rc={r.returncode} stderr={r.stderr.strip()} stdout={r.stdout.strip()}")
            return jsonify(ok=False, error="restart failed", rc=r.returncode, stderr=r.stderr, stdout=r.stdout), 500
        log("INFO restart kvm-web accepted")
        return jsonify(ok=True)
    except Exception as e:
        log(f"ERROR restart exception: {e}")
        return jsonify(ok=False, error=str(e)), 500


@app.route("/api/reboot", methods=["POST"])
def api_reboot():
    try:
        r = subprocess.run(
            ["sudo", "-n", "/bin/systemctl", "reboot"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        if r.returncode != 0:
            log(f"ERROR reboot failed rc={r.returncode} stderr={r.stderr.strip()} stdout={r.stdout.strip()}")
            return jsonify(ok=False, error="reboot failed", rc=r.returncode, stderr=r.returncode, stdout=r.stdout), 500
        log("INFO reboot accepted")
        return jsonify(ok=True)
    except Exception as e:
        log(f"ERROR reboot exception: {e}")
        return jsonify(ok=False, error=str(e)), 500
# =============================
# VIDEO CFG API
# =============================
@app.route("/api/video_cfg", methods=["GET"])
def api_video_cfg_get():
    return jsonify(ok=True, cfg=load_video_cfg())

@app.route("/api/video_cfg", methods=["POST"])
def api_video_cfg_set():
    d = request.get_json(force=True, silent=True) or {}
    current = load_video_cfg()

    allowed_sizes = {
        "1920x1080", "2560x1440", "1360x768",
        "1280x1024", "1280x960", "1280x720",
        "1024x768", "800x600",
        "720x576", "720x480", "640x480"
    }

    size = str(d.get("size", current["size"]))
    qv   = int(d.get("qv", current["qv"]))
    fmt  = str(d.get("format", current["format"])).lower()

    if size not in allowed_sizes:
        return jsonify(ok=False, error="size not allowed"), 400
    if not (2 <= qv <= 15):
        return jsonify(ok=False, error="qv out of range"), 400
    if fmt not in ("mjpeg", "mjpg"):
        return jsonify(ok=False, error="format not allowed"), 400

    # FPS SIEMPRE BLOQUEADO
    fps = int(LOCK_FPS) if LOCK_FPS is not None else current["fps"]

    _write_video_cfg(size, fps, qv, "mjpeg")
    invalidate_video_cfg_cache()
    stream_restart_internal()

    log(f"/api/video_cfg POST: {d} -> SAVED size={size} fps={fps} qv={qv}")
    return jsonify(ok=True, cfg=load_video_cfg())

# =============================
# INPUT API -> TU PROTOCOLO (MD/KD/KU/CL)
# =============================
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

    _send_line(f"MD 0 0 0 {btn}")
    log_input(f"MD 0 0 0 {btn} (btn event b={button} s={state})")
    return jsonify(ok=True)

@app.route("/api/input/wheel", methods=["POST"])
def api_input_wheel():
    d = request.get_json(force=True, silent=True) or {}
    delta = int(d.get("delta", 0))

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
    log("Starting KVM Web Server...")

    # crea/corrige config por defecto (útil para clonados) con FPS bloqueado
    try:
        if not VIDEO_CFG_PATH.exists():
            _write_video_cfg(VIDEO_SIZE, int(LOCK_FPS) if LOCK_FPS is not None else int(FPS), int(DEFAULT_QV), "mjpeg")
        else:
            # si existe y hay LOCK_FPS, lo normalizamos
            cfg = load_video_cfg()
            if LOCK_FPS is not None:
                _write_video_cfg(cfg["size"], int(LOCK_FPS), cfg["qv"], "mjpeg")
                invalidate_video_cfg_cache()
    except Exception as e:
        log(f"WARN: cannot init video cfg: {e}")

    app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False, threaded=True)
