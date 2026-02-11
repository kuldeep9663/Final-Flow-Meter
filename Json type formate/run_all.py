import subprocess
import sys
import os
import signal
import time

# ================= FILES TO RUN =================
SCRIPTS = [
    "server.py",
    "app.py",
    "Decription-test.py",
    "mqtty.py"
]

processes = []

def start_scripts():
    print("🚀 Starting all services...\n")

    for script in SCRIPTS:
        if not os.path.exists(script):
            print(f"❌ File not found: {script}")
            continue

        print(f"▶ Starting {script}")

        p = subprocess.Popen(
            [sys.executable, script],
            stdout=None,   # ✅ let OS handle stdout (NO BLOCKING)
            stderr=None,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
        )

        processes.append((script, p))

    print("\n✅ All services started successfully")
    print("🛑 Press CTRL+C to stop everything\n")

def stop_scripts():
    print("\n🛑 Stopping all services...")

    for name, p in processes:
        try:
            if os.name == "nt":
                p.send_signal(signal.CTRL_BREAK_EVENT)
            else:
                p.terminate()
            print(f"✔ Stopped {name}")
        except Exception as e:
            print(f"⚠ Failed to stop {name}: {e}")

    time.sleep(2)
    print("✅ Shutdown complete")
    sys.exit(0)

def monitor_processes():
    """Restart or warn if a process dies"""
    for name, p in processes:
        if p.poll() is not None:
            print(f"❌ {name} crashed (PID {p.pid})")

if __name__ == "__main__":
    try:
        start_scripts()
        while True:
            monitor_processes()
            time.sleep(2)   # ✅ very low CPU usage
    except KeyboardInterrupt:
        stop_scripts()
