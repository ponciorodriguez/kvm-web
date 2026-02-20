#!/bin/bash

APP_DIR="$HOME/kvm-web"
PY="$APP_DIR/server.py"

echo "🛑 Stopping KVM Web..."

# Mata servidor Flask
pkill -f "$PY" 2>/dev/null || true

# Mata ffmpeg si queda algo colgado
pkill -9 ffmpeg 2>/dev/null || true

sleep 0.5

# Verifica
if ss -lnt | grep -q ":5000"; then
  echo "⚠️  El puerto 5000 sigue abierto"
else
  echo "✅ KVM Web detenido correctamente"
fi
