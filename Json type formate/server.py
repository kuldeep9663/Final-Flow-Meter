import socket
import threading
import json
from datetime import datetime
import os

# ================= CONFIG =================
HOST = "0.0.0.0"
PORT = 5003
BUFFER_SIZE = 4096

DATA_FILE = "modem_data.jsonl"
DEVICE_FILE = "devices.json"

file_lock = threading.Lock()
device_lock = threading.Lock()

# Cache for reconnects (IP → IMEI)
ip_imei_cache = {}

print("🐧 NB-IoT Modem TCP Server (Linux)")
print("🔐 Device verification enabled")
print("🚀 Ready")

# ================= LOAD DEVICES =================
def load_devices():
    if not os.path.exists(DEVICE_FILE):
        return {}
    with device_lock, open(DEVICE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def save_devices(devices):
    with device_lock, open(DEVICE_FILE, "w", encoding="utf-8") as f:
        json.dump(devices, f, indent=2)

# ================= LOGGER =================
def log_json(protocol, addr, imei, raw_bytes):
    record = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "protocol": protocol,
        "ip": addr[0],
        "port": addr[1],
        "imei": imei,
        "data_text": raw_bytes.decode(errors="ignore"),
        "data_hex": raw_bytes.hex()
    }

    print(record)

    with file_lock:
        with open(DATA_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

# ================= TCP KEEPALIVE =================
def enable_keepalive(sock):
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 120)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 30)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 5)

# ================= IMEI PARSER =================
def extract_imei(msg):
    if msg.startswith("IMEI:"):
        return msg.replace("IMEI:", "").strip()

    for part in msg.replace("\n", ",").split(","):
        if part.isdigit() and len(part) == 15:
            return part

    return None

# ================= TCP HANDLER =================
def handle_tcp(conn, addr):
    print(f"🔗 TCP connected: {addr}")

    enable_keepalive(conn)
    conn.settimeout(300)

    imei = None
    imei_verified = False

    try:
        while True:
            try:
                data = conn.recv(BUFFER_SIZE)
                if not data:
                    print(f"🔌 Modem disconnected: {addr}")
                    break

                msg = data.decode(errors="ignore")
                detected_imei = extract_imei(msg)

                devices = load_devices()
                now = datetime.now().isoformat()

                # ----- IMEI VERIFICATION -----
                if detected_imei:
                    device = devices.get(detected_imei)

                    if device and device.get("activated") is True:
                        imei = detected_imei
                        imei_verified = True
                        ip_imei_cache[addr[0]] = imei

                        if not device.get("first_seen"):
                            device["first_seen"] = now
                        device["last_seen"] = now
                        devices[imei] = device
                        save_devices(devices)

                        print(f"✅ IMEI verified: {imei}")
                    else:
                        print(f"⛔ IMEI not registered / not activated: {detected_imei}")
                        continue  # ❌ DO NOT LOG DATA

                elif addr[0] in ip_imei_cache:
                    cached = ip_imei_cache[addr[0]]
                    device = devices.get(cached)

                    if device and device.get("activated"):
                        imei = cached
                        imei_verified = True
                        device["last_seen"] = now
                        devices[imei] = device
                        save_devices(devices)

                # ----- LOG DATA ONLY IF VERIFIED -----
                if imei_verified:
                    log_json("TCP", addr, imei, data)
                else:
                    print(f"🚫 Data ignored (IMEI not verified) from {addr}")

            except socket.timeout:
                continue

    except Exception as e:
        print(f"⚠ TCP error {addr}: {e}")

    finally:
        conn.close()
        print(f"❌ TCP session closed: {addr}")

# ================= TCP SERVER =================
def tcp_server():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((HOST, PORT))
    sock.listen(200)

    print(f"🚀 TCP listening on {HOST}:{PORT}")

    while True:
        conn, addr = sock.accept()
        threading.Thread(
            target=handle_tcp,
            args=(conn, addr),
            daemon=True
        ).start()

# ================= MAIN =================
if __name__ == "__main__":
    tcp_server()