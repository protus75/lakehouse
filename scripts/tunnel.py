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


def start_dash(dashapp_cfg, background=False):
    """Start the Dash browser app."""
    dash_pid = read_pid("dash")
    if is_running(dash_pid):
        print(f"Dash app already running (PID {dash_pid})")
        return dash_pid

    env = os.environ.copy()
    env["PYTHONPATH"] = PROJECT_ROOT

    cmd = [
        sys.executable, "-u",
        os.path.join(PROJECT_ROOT, "dashapp", "tabletop_browser.py"),
    ]

    if background:
        proc = subprocess.Popen(
            cmd, env=env,
            stdout=open(os.path.join(PID_DIR, "dash.log"), "w"),
            stderr=subprocess.STDOUT,
            creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
        )
        save_pid("dash", proc.pid)
        print(f"Dash app started (PID {proc.pid}) — http://localhost:{dashapp_cfg['port']}")
        return proc.pid
    else:
        proc = subprocess.Popen(cmd, env=env)
        save_pid("dash", proc.pid)
        print(f"Dash app started (PID {proc.pid}) — http://localhost:{dashapp_cfg['port']}")
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
    for name in ["tunnel", "dash"]:
        pid = read_pid(name)
        if is_running(pid):
            try:
                if sys.platform == "win32":
                    subprocess.run(["taskkill", "/F", "/PID", str(pid)],
                                   capture_output=True)
                else:
                    os.kill(pid, signal.SIGTERM)
                print(f"Stopped {name} (PID {pid})")
            except Exception as e:
                print(f"Failed to stop {name} (PID {pid}): {e}")
        else:
            print(f"{name.capitalize()} not running")
        clear_pid(name)


def cmd_status():
    tunnel_cfg, dashapp_cfg = load_config()
    for name, label, url in [
        ("dash", "Dash app", f"http://localhost:{dashapp_cfg['port']}"),
        ("tunnel", "Tunnel", f"https://{tunnel_cfg['domain']}"),
    ]:
        pid = read_pid(name)
        if is_running(pid):
            print(f"  {label}: RUNNING (PID {pid}) — {url}")
        else:
            print(f"  {label}: STOPPED")
            clear_pid(name)


def cmd_dash():
    _, dashapp_cfg = load_config()
    print(f"Starting Dash app — http://localhost:{dashapp_cfg['port']}")
    print("Ctrl+C to stop.")
    try:
        proc = start_dash(dashapp_cfg, background=False)
        proc.wait()
    except KeyboardInterrupt:
        print("\nStopped.")
        cmd_stop()


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
