#!/usr/bin/env python3
"""Start/stop the tabletop rules browser (Dash app on port 8000).

Usage:
    python scripts/tabletop_rules/browser.py start
    python scripts/tabletop_rules/browser.py stop
    python scripts/tabletop_rules/browser.py log [N]
"""
import subprocess
import sys
import time

CONTAINER = "lakehouse-workspace"
SCRIPT = "/workspace/dashapp/tabletop_browser.py"
LOG = "/tmp/tabletop_browser.log"
PORT = 8000


def _docker(*args):
    env = {**subprocess.os.environ, "MSYS_NO_PATHCONV": "1"}
    return subprocess.run(
        ["docker", "exec", CONTAINER] + list(args),
        capture_output=True, text=True, env=env,
    )


PID_FILE = "/tmp/tabletop_browser.pid"


def _kill():
    # Kill by saved PID, then also by port
    for cmd in [
        f"cat {PID_FILE} 2>/dev/null | xargs -r kill -9",
        f"fuser -k {PORT}/tcp 2>/dev/null",
    ]:
        _docker("bash", "-c", cmd)
    time.sleep(1)


def cmd_start():
    _kill()
    _docker("bash", "-c",
        f"cd /workspace && python {SCRIPT} > {LOG} 2>&1 & echo $! > {PID_FILE}")
    time.sleep(3)
    result = _docker("bash", "-c",
        f"curl -s -o /dev/null -w '%{{http_code}}' http://localhost:{PORT}/")
    if result.stdout.strip() == "200":
        print(f"Running at http://localhost:{PORT}")
    else:
        print("Failed to start:")
        cmd_log(10)


def cmd_stop():
    killed = _kill()
    print(f"Stopped ({killed} process(es))" if killed else "Not running")


def cmd_log(n=20):
    result = _docker("tail", f"-{n}", LOG)
    print(result.stdout)


if __name__ == "__main__":
    args = sys.argv[1:]
    if not args or args[0] == "help":
        print(__doc__)
    elif args[0] == "start":
        cmd_start()
    elif args[0] == "stop":
        cmd_stop()
    elif args[0] == "log":
        cmd_log(int(args[1]) if len(args) > 1 else 20)
    else:
        print(f"Unknown: {args[0]}")
        print(__doc__)
