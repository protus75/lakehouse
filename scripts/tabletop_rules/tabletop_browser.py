#!/usr/bin/env python3
"""Start/stop the tabletop rules browser (Dash app on port 8000).

Usage:
    python scripts/tabletop_rules/browser.py start
    python scripts/tabletop_rules/browser.py stop
    python scripts/tabletop_rules/browser.py reset
    python scripts/tabletop_rules/browser.py log [N]
"""
import subprocess
import sys
import time
from pathlib import Path

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
    # Kill by saved PID, then find all via /proc (container has no ps/pgrep)
    _docker("bash", "-c", f"cat {PID_FILE} 2>/dev/null | xargs -r kill -9")
    kill_cmd = (
        "for p in /proc/[0-9]*/cmdline; do "
        "if strings \"$p\" 2>/dev/null | grep -q tabletop_browser; then "
        "pid=\"$(echo $p | cut -d/ -f3)\"; kill -9 \"$pid\" 2>/dev/null; fi; done"
    )
    _docker("bash", "-c", kill_cmd)
    time.sleep(1)
    _docker("bash", "-c", kill_cmd)


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


def cmd_reset():
    """Kill browser, clear pycache (host + container), restart."""
    _kill()
    # Clear host pycache
    import shutil
    project_root = Path(__file__).resolve().parent.parent.parent
    count = 0
    for p in project_root.rglob("__pycache__"):
        if p.is_dir():
            shutil.rmtree(p, ignore_errors=True)
            count += 1
    print(f"Cleared {count} host __pycache__ dirs")
    # Clear container pycache
    _docker("bash", "-c", "find /workspace -name __pycache__ -type d -exec rm -rf {} + 2>/dev/null")
    print("Cleared container __pycache__")
    cmd_start()


if __name__ == "__main__":
    args = sys.argv[1:]
    if not args or args[0] == "help":
        print(__doc__)
    elif args[0] == "start":
        cmd_start()
    elif args[0] == "stop":
        cmd_stop()
    elif args[0] == "reset":
        cmd_reset()
    elif args[0] == "log":
        cmd_log(int(args[1]) if len(args) > 1 else 20)
    else:
        print(f"Unknown: {args[0]}")
        print(__doc__)
