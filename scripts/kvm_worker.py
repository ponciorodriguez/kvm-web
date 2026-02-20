#!/usr/bin/env python3
import time, signal, sys, subprocess, os
from datetime import datetime

RUN = True
ffmpeg_proc = None

VIDEO_DEV = "/dev/video0"
VIDEO_SIZE = "1280x1024"
FPS = "30"

def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

def handle_stop(signum, frame):
    global RUN
    RUN = False

signal.signal(signal.SIGTERM, handle_stop)
signal.signal(signal.SIGINT, handle_stop)

def start_ffmpeg():
    global ffmpeg_proc

    # mpjpeg a stdout (pipe:1)
    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "warning",
        "-f", "v4l2",
        "-input_format", "mjpeg",
        "-video_size", VIDEO_SIZE,
        "-framerate", FPS,
        "-i", VIDEO_DEV,
        "-f", "mpjpeg",
        "-q:v", "5",
        "pipe:1"
    ]

    log(f"Starting ffmpeg mpjpeg to stdout from {VIDEO_DEV} {VIDEO_SIZE}@{FPS}")
    ffmpeg_proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=0
    )

    time.sleep(1.5)
    if ffmpeg_proc.poll() is not None:
        err = (ffmpeg_proc.stderr.read() or b"").decode(errors="replace")[-2000:]
        raise RuntimeError(f"ffmpeg murió al arrancar. stderr:\n{err}")

    log(f"ffmpeg running pid={ffmpeg_proc.pid}")

def stop_ffmpeg():
    global ffmpeg_proc
    if not ffmpeg_proc:
        return
    log("Stopping ffmpeg...")
    try:
        ffmpeg_proc.terminate()
        try:
            ffmpeg_proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            ffmpeg_proc.kill()
    finally:
        ffmpeg_proc = None
    log("ffmpeg stopped")

def main():
    log("KVM worker STARTED")
    try:
        start_ffmpeg()
    except Exception as e:
        log(f"ERROR starting video: {e}")
        log("KVM worker STOPPED (video start failed)")
        return 1

    # Mantener vivo y recoger salida (evita zombies)
    while RUN:
        if ffmpeg_proc and ffmpeg_proc.poll() is not None:
            err = (ffmpeg_proc.stderr.read() or b"").decode(errors="replace")[-2000:]
            log("WARNING: ffmpeg died while running. stderr:")
            for line in err.splitlines()[-20:]:
                log(line)
            break
        time.sleep(0.2)

    log("KVM worker STOPPING")
    stop_ffmpeg()
    log("KVM worker STOPPED")
    return 0

if __name__ == "__main__":
    sys.exit(main())
