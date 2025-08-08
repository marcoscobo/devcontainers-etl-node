import subprocess
import sys
import signal

SCRIPTS = [
    "postgres_loader.py",
    "kafka_loader.py",
    "minio_loader.py",
]
PROCESSES = []

def terminate_all():
    print("\n❌ Terminating all processes...")
    for p in PROCESSES:
        if p.poll() is None:
            p.terminate()
    for p in PROCESSES:
        try:
            p.wait(timeout=5)
        except Exception:
            p.kill()

def main():
    try:
        for script in SCRIPTS:
            proc = subprocess.Popen(
                [sys.executable, script],
                cwd="/workspace/src/data_chargers"
            )
            PROCESSES.append(proc)
        print("🚀 Processes started in parallel. Press Ctrl+C to stop.")
        for p in PROCESSES:
            p.wait()
    except KeyboardInterrupt:
        terminate_all()

if __name__ == "__main__":
    # Handle signals for clean termination
    signal.signal(signal.SIGINT, lambda sig, frame: terminate_all() or sys.exit(0))
    main()
