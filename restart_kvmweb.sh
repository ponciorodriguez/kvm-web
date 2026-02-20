#!/bin/bash
set -u

APP_DIR="$HOME/kvm-web"
PY="$APP_DIR/server.py"
VPY="$APP_DIR/venv/bin/python3"
LOG="$APP_DIR/logs/server.out.log"

mkdir -p "$APP_DIR/logs"

echo "🔄 Restarting KVM Web..."
echo "APP_DIR=$APP_DIR"
echo "PY=$PY"
echo "VPY=$VPY"
echo "LOG=$LOG"

# Mata servidor anterior
pkill -f "$PY" 2>/dev/null || true

# Mata ffmpeg colgados (opcional, pero lo dejamos)
pkill -9 ffmpeg 2>/dev/null || true

# Comprueba venv python
if [ ! -x "$VPY" ]; then
  echo "❌ No existe $VPY (¿venv roto o ruta incorrecta?)"
  exit 1
fi

cd "$APP_DIR" || exit 1

# Arranca en segundo plano y deja log
echo "🚀 Starting server..."
nohup "$VPY" "$PY" >> "$LOG" 2>&1 &

PID=$!
sleep 0.6

# Comprueba si está escuchando en 5000
if ss -lnt | grep -q ":5000"; then
  echo "✅ KVM Web arrancado. PID=$PID"
else
  echo "❌ No está escuchando en :5000. Últimas líneas del log:"
  tail -n 80 "$LOG" || true
  exit 2
fi
