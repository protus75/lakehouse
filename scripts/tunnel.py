#!/usr/bin/env python3
"""Manage Cloudflare Tunnel for gamerules.ai.

Usage:
    python scripts/tunnel.py start       Start Dash app + tunnel (foreground)
    python scripts/tunnel.py start --bg  Start Dash app + tunnel (background)
    python scripts/tunnel.py stop        Stop tunnel and Dash app
    python scripts/tunnel.py status      Check if tunnel and Dash app are running
    python scripts/tunnel.py dash        Start only the Dash app (no tunnel)
"""

import os
import signal
import subprocess
import sys
import time

import yaml

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
CONFIG_PATH = os.path.join(PROJECT_ROOT, "config", "lakehouse.yaml")
PID_DIR = os.path.join(PROJECT_ROOT, ".tunnel")


def load_config():
    with open(CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)
    return cfg["tunnel"], cfg["dashapp"]


def pid_file(name):
    os.makedirs(PID_DIR, exist_ok=True)
    return os.path.join(PID_DIR, f"{name}.pid")


def save_pid(name, pid):
    with open(pid_file(name), "w") as f:
        f.write(str(pid))


def read_pid(name):
    pf = pid_file(name)
    if os.path.exists(pf):
        with open(pf) as f:
            return int(f.read().strip())
    return None


def clear_pid(name):
    pf = pid_file(name)
    if os.path.exists(pf):
        os.remove(pf)


def is_running(pid):
    """Check if a process with given PID is running."""
    if pid is None:
        return False
    try:
        os.kill(pid, 0)
        return True
    except (OSError, ProcessLookupError):
        return False


CONTAINER = "lakehouse-workspace"


def _docker_env():
    """Return env dict that prevents Git Bash from mangling Linux paths."""
    env = os.environ.copy()
    env["MSYS_NO_PATHCONV"] = "1"
    return env


def _is_dash_running():
    """Check if Dash app is running inside the container."""
    result = subprocess.run(
        ["docker", "exec", CONTAINER, "python", "-c",
         "import socket; s=socket.socket(); s.settimeout(1); s.connect(('localhost',8000)); s.close(); print('up')"],
        capture_output=True, text=True, env=_docker_env(),
    )
    return result.returncode == 0


def start_dash(dashapp_cfg, background=False):
    """Start the Dash browser app inside the workspace container."""
    if _is_dash_running():
        print(f"Dash app already running in {CONTAINER}")
        return None

    cmd = [
        "docker", "exec", "-d" if background else "", CONTAINER,
        "python", "-u", "/workspace/dashapp/tabletop_browser.py",
    ]
    cmd = [c for c in cmd if c]
    env = _docker_env()

    if background:
        subprocess.run(cmd, check=True, env=env)
        print(f"Dash app started in {CONTAINER} — http://localhost:{dashapp_cfg['port']}")
        return None
    else:
        proc = subprocess.Popen(cmd, env=env)
        print(f"Dash app started in {CONTAINER} — http://localhost:{dashapp_cfg['port']}")
        return proc


def start_tunnel(tunnel_cfg, dashapp_cfg, background=False):
    """Start cloudflared tunnel."""
    tunnel_pid = read_pid("tunnel")
    if is_running(tunnel_pid):
        print(f"Tunnel already running (PID {tunnel_pid})")
        return tunnel_pid

    creds = os.path.expanduser(tunnel_cfg["credentials_file"])
    cmd = [
        "cloudflared", "tunnel",
        "--url", f"http://localhost:{dashapp_cfg['port']}",
        "--credentials-file", creds,
        "run", tunnel_cfg["name"],
    ]

    if background:
        proc = subprocess.Popen(
            cmd,
            stdout=open(os.path.join(PID_DIR, "tunnel.log"), "w"),
            stderr=subprocess.STDOUT,
            creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
        )
        save_pid("tunnel", proc.pid)
        print(f"Tunnel started (PID {proc.pid}) — https://{tunnel_cfg['domain']}")
        return proc.pid
    else:
        proc = subprocess.Popen(cmd)
        save_pid("tunnel", proc.pid)
        print(f"Tunnel started (PID {proc.pid}) — https://{tunnel_cfg['domain']}")
        return proc


def cmd_start(background=False):
    tunnel_cfg, dashapp_cfg = load_config()

    dash_result = start_dash(dashapp_cfg, background=True)
    time.sleep(2)  # let Dash app bind the port

    if background:
        start_tunnel(tunnel_cfg, dashapp_cfg, background=True)
        print(f"\nBoth running in background. Use 'python scripts/tunnel.py status' to check.")
        print(f"Logs: {PID_DIR}/dash.log, {PID_DIR}/tunnel.log")
    else:
        print(f"\nTunnel running in foreground. Ctrl+C to stop.")
        print(f"  Local:  http://localhost:{dashapp_cfg['port']}")
        print(f"  Public: https://{tunnel_cfg['domain']}")
        try:
            tunnel_proc = start_tunnel(tunnel_cfg, dashapp_cfg, background=False)
            tunnel_proc.wait()
        except KeyboardInterrupt:
            print("\nShutting down...")
            cmd_stop()


def cmd_stop():
    # Stop tunnel (local process)
    tunnel_pid = read_pid("tunnel")
    if is_running(tunnel_pid):
        try:
            if sys.platform == "win32":
                subprocess.run(["taskkill", "/F", "/PID", str(tunnel_pid)],
                               capture_output=True)
            else:
                os.kill(tunnel_pid, signal.SIGTERM)
            print(f"Stopped tunnel (PID {tunnel_pid})")
        except Exception as e:
            print(f"Failed to stop tunnel (PID {tunnel_pid}): {e}")
    else:
        print("Tunnel not running")
    clear_pid("tunnel")

    # Stop Dash app (inside Docker container) via /proc since pkill not installed
    env = _docker_env()
    result = subprocess.run(
        ["docker", "exec", CONTAINER, "python", "-c",
         "import os,signal\n"
         "for p in os.listdir('/proc'):\n"
         " if not p.isdigit(): continue\n"
         " try:\n"
         "  c=open(f'/proc/{p}/cmdline').read()\n"
         "  if 'tabletop_browser' in c and str(os.getpid()) != p:\n"
         "   os.kill(int(p),signal.SIGTERM); print(f'Killed {p}')\n"
         " except: pass\n"],
        capture_output=True, text=True, env=env,
    )
    if result.stdout.strip():
        print(f"Stopped Dash app in {CONTAINER}")
    else:
        print("Dash app not running")


def cmd_status():
    tunnel_cfg, dashapp_cfg = load_config()

    # Check Dash app inside Docker
    if _is_dash_running():
        print(f"  Dash app: RUNNING in {CONTAINER} — http://localhost:{dashapp_cfg['port']}")
    else:
        print("  Dash app: STOPPED")

    # Check tunnel (local process)
    tunnel_pid = read_pid("tunnel")
    if is_running(tunnel_pid):
        print(f"  Tunnel:   RUNNING (PID {tunnel_pid}) — https://{tunnel_cfg['domain']}")
    else:
        print("  Tunnel:   STOPPED")
        clear_pid("tunnel")


def cmd_dash():
    _, dashapp_cfg = load_config()
    print(f"Starting Dash app in {CONTAINER} — http://localhost:{dashapp_cfg['port']}")
    start_dash(dashapp_cfg, background=True)
    print("Use 'python scripts/tunnel.py stop' to stop it.")


if __name__ == "__main__":
    args = sys.argv[1:]
    if not args or args[0] == "help":
        print(__doc__)
        sys.exit(0)

    cmd = args[0]
    if cmd == "start":
        cmd_start(background="--bg" in args)
    elif cmd == "stop":
        cmd_stop()
    elif cmd == "status":
        cmd_status()
    elif cmd == "dash":
        cmd_dash()
    else:
        print(f"Unknown command: {cmd}")
        print(__doc__)
        sys.exit(1)
