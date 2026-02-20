# kvm-web

KVM Web (Flask) con streaming MJPEG (`/stream.mjpg`) + inyección de teclado/ratón a través de Raspberry Pico (UART -> USB HID).

## Requisitos
- Debian
- ffmpeg
- Python 3
- venv
- pyserial
- Capturadora V4L2

## Instalación rápida
```bash
sudo apt update
sudo apt install -y ffmpeg python3-venv
python3 -m venv venv
source venv/bin/activate
pip install flask pyserial
python3 server.py
