#!/usr/bin/env python3
"""Convenience wrapper for Dagster operations via GraphQL API.

Usage:
    python scripts/dagster.py launch <job_name> [--force]
    python scripts/dagster.py run <job_name> [--force]   Launch + poll until done
    python scripts/dagster.py status <run_id>
    python scripts/dagster.py state <run_id>     Structured state: per-step + dbt model progress
    python scripts/dagster.py watch <run_id>     Adaptive live polling until terminal state
    python scripts/dagster.py logs <run_id> [N]
    python scripts/dagster.py cancel <run_id>
    python scripts/dagster.py errors <run_id>    Show compute stderr for failed steps
    python scripts/dagster.py dbt-logs [N]       Tail dbt's own log file
    python scripts/dagster.py dbt-results [run_id]  dbt error results (run_id-filtered)
    python scripts/dagster.py dbt-clean          Force-clean dbt target/ + parse cache
    python scripts/dagster.py catalog            List all catalog tables + check data on disk
    python scripts/dagster.py catalog clean      Drop catalog entries with no data on disk
    python scripts/dagster.py query <sql>         Run a SQL query via DuckDB reader
    python scripts/dagster.py unload              Unload all Ollama models from GPU
    python scripts/dagster.py preflight           Check all prerequisites before pipeline run
    python scripts/dagster.py reset              Clear caches + restart Dagster
    python scripts/dagster.py reload
    python scripts/dagster.py jobs
    python scripts/dagster.py runs [N]
    python scripts/dagster.py verify             Check row counts and data integrity
"""

import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

CONTAINER = "lakehouse-dagster-webserver"
WORKSPACE = "lakehouse-workspace"
GQL_URL = "http://localhost:3000/graphql"
LOCATION = "assets.py"
REPO = "__repository__"


def gql(query: str) -> dict:
    """Run a GraphQL query inside the Dagster webserver container."""
    payload = json.dumps({"query": query})
    result = subprocess.run(
        ["docker", "exec", CONTAINER, "curl", "-s", "-X", "POST",
         GQL_URL, "-H", "Content-Type: application/json", "-d", payload],
        capture_output=True, text=True, check=True,
    )
    return json.loads(result.stdout)


def _docker_py(code: str) -> str:
    """Run Python code in the workspace container, return stdout."""
    env = {**subprocess.os.environ, "MSYS_NO_PATHCONV": "1"}
    result = subprocess.run(
        ["docker", "exec", WORKSPACE, "python", "-c", code],
        capture_output=True, text=True, env=env,
    )
    if result.returncode != 0:
        return f"ERROR: {result.stderr.strip()}"
    return result.stdout.strip()


def cmd_launch(job: str, force: bool = False) -> str | None:
    run_config_str = ""
    if force:
        rc = json.dumps({"ops": {"bronze_tabletop": {"config": {"force": True}}}})
        run_config_str = ', runConfigData: "' + rc.replace('"', '\\"') + '"'
    q = f'''mutation {{
        launchRun(executionParams: {{
            selector: {{
                repositoryLocationName: "{LOCATION}",
                repositoryName: "{REPO}",
                jobName: "{job}"
            }},
            mode: "default"
            {run_config_str}
        }}) {{
            __typename
            ... on LaunchRunSuccess {{ run {{ runId status }} }}
            ... on PythonError {{ message }}
        }}
    }}'''
    d = gql(q)["data"]["launchRun"]
    if d["__typename"] == "LaunchRunSuccess":
        run_id = d['run']['runId']
        print(f"Launched: {run_id}")
        return run_id
    else:
        print(f"Error: {d.get('message', d['__typename'])}")
        return None


def cmd_poll(run_id: str) -> str:
    """Poll a run until it completes. Returns final status."""
    terminal = {"SUCCESS", "FAILURE", "CANCELED"}
    last_log_count = 0
    while True:
        q = f'{{ runOrError(runId: "{run_id}") {{ ... on Run {{ status }} }} }}'
        status = gql(q)["data"]["runOrError"]["status"]

        # Print new log messages since last poll
        q2 = f'''{{ logsForRun(runId: "{run_id}", afterCursor: null, limit: 500) {{
            ... on EventConnection {{ events {{ ... on MessageEvent {{ message }} }} }}
        }} }}'''
        events = gql(q2)["data"]["logsForRun"]["events"]
        msgs = [e["message"] for e in events if e.get("message")]
        for m in msgs[last_log_count:]:
            print(f"  {m}")
        last_log_count = len(msgs)

        if status in terminal:
            print(f"\nRun {run_id[:12]} finished: {status}")
            return status
        time.sleep(10)


def cmd_run(job: str, force: bool = False):
    """Launch a job and poll until it completes, then verify."""
    run_id = cmd_launch(job, force=force)
    if not run_id:
        sys.exit(1)
    status = cmd_poll(run_id)
    print("\n" + "=" * 50)
    print("POST-RUN VERIFICATION")
    print("=" * 50)
    cmd_verify()
    if status != "SUCCESS":
        sys.exit(1)


import re

# ANSI escape stripper for dbt log lines (they have color codes)
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m|\[\d+m")
# dbt model progress: "13 of 16 START sql external model silver_tabletop.silver_files ..."
_DBT_MODEL_RE = re.compile(
    r"(\d+) of (\d+) (START|OK created|ERROR(?:[^\s]+)?|SKIP)\s+"
    r"(?:python\s+|sql\s+)?(?:external\s+|table\s+|view\s+|incremental\s+)?"
    r"(?:table\s+model|view\s+model|model|relation)?\s*"
    r"([\w.]+)"
)


def _strip_ansi(s: str) -> str:
    return _ANSI_RE.sub("", s)


def _fmt_duration(seconds: float) -> str:
    if seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    if seconds < 60:
        return f"{seconds:.1f}s"
    m, s = divmod(int(seconds), 60)
    if m < 60:
        return f"{m}m{s:02d}s"
    h, m = divmod(m, 60)
    return f"{h}h{m:02d}m"


def _fetch_run_state(run_id: str) -> dict:
    """One-shot fetch of run state: top-level status, per-step stats, and the
    recent log events parsed for dbt model progress. No log scraping for
    timing — uses dagster's structured stepStats.
    """
    q = f'''{{
      runOrError(runId: "{run_id}") {{
        ... on Run {{
          status
          startTime
          endTime
          stepStats {{
            stepKey
            status
            startTime
            endTime
          }}
        }}
      }}
    }}'''
    run = gql(q)["data"]["runOrError"]
    if not run or "status" not in run:
        return {"error": "run not found"}

    # Get recent message events for dbt model progress (only place where we
    # need to parse text — dbt's structured event for model progress is the
    # log line itself).
    q2 = f'''{{
      logsForRun(runId: "{run_id}", afterCursor: null, limit: 500) {{
        ... on EventConnection {{
          events {{
            ... on MessageEvent {{ message timestamp }}
          }}
        }}
      }}
    }}'''
    log_data = gql(q2).get("data", {}).get("logsForRun") or {}
    events = log_data.get("events", []) or []

    # Track dbt model state. Parse model lines as we go in chronological order.
    dbt_models: dict[str, dict] = {}
    dbt_total = 0
    for e in events:
        msg = _strip_ansi(e.get("message", "") or "")
        if not msg:
            continue
        for m in _DBT_MODEL_RE.finditer(msg):
            idx = int(m.group(1))
            total = int(m.group(2))
            action = m.group(3)
            name = m.group(4).strip()
            dbt_total = total
            entry = dbt_models.setdefault(name, {"index": idx, "status": "PENDING",
                                                  "start_ts": None, "end_ts": None})
            ts = float(e.get("timestamp", 0)) / 1000.0 if e.get("timestamp") else None
            if action == "START":
                entry["status"] = "RUN"
                entry["start_ts"] = ts
            elif action.startswith("OK"):
                entry["status"] = "OK"
                entry["end_ts"] = ts
            elif action.startswith("ERROR"):
                entry["status"] = "ERROR"
                entry["end_ts"] = ts
            elif action == "SKIP":
                entry["status"] = "SKIP"
                entry["end_ts"] = ts

    return {
        "status": run["status"],
        "start_time": run.get("startTime"),
        "end_time": run.get("endTime"),
        "step_stats": run.get("stepStats", []) or [],
        "dbt_total": dbt_total,
        "dbt_models": dbt_models,
    }


def cmd_state(run_id: str):
    """Print structured state of a run: top status, per-step timeline,
    and dbt model progress within dbt_build. Single GraphQL roundtrip,
    no log message text scraping for timing data.
    """
    run_id = _resolve_run_id(run_id)
    state = _fetch_run_state(run_id)
    if state.get("error"):
        print(state["error"])
        return

    import time as _time
    now = _time.time()
    age = ""
    if state.get("start_time"):
        elapsed = (state.get("end_time") or now) - state["start_time"]
        age = f" for {_fmt_duration(elapsed)}"

    print(f"[{state['status']}{age}] {run_id[:12]}")

    # Per-step timeline
    for ss in state["step_stats"]:
        dur = ""
        if ss.get("startTime"):
            end = ss.get("endTime") or now
            dur = _fmt_duration(end - ss["startTime"])
        print(f"  {ss['stepKey']:24s} {ss['status']:10s} {dur}")

    # dbt model progress (only meaningful while dbt_build is running or done)
    if state["dbt_models"]:
        total = state["dbt_total"]
        ok = sum(1 for m in state["dbt_models"].values() if m["status"] == "OK")
        err = sum(1 for m in state["dbt_models"].values() if m["status"] == "ERROR")
        run_now = [(name, m) for name, m in state["dbt_models"].items()
                   if m["status"] == "RUN"]
        print(f"  dbt: {ok}/{total} ok, {err} error, {len(run_now)} running")
        for name, m in sorted(run_now, key=lambda x: x[1]["index"]):
            run_for = ""
            if m.get("start_ts"):
                run_for = f" ({_fmt_duration(now - m['start_ts'])})"
            print(f"    {m['index']:3d}/{total} RUN  {name}{run_for}")


def cmd_dbt_results(run_id: str | None = None):
    """Dump dbt's run_results.json error rows for the most recent dbt run.

    With a run_id, also prints the dbt invocation_id from that run's logs
    so you can verify the results file matches. If they don't match,
    refuses to print stale results.
    """
    env = {**subprocess.os.environ, "MSYS_NO_PATHCONV": "1"}
    expected_invocation = None
    if run_id:
        run_id = _resolve_run_id(run_id)
        # Find the dbt invocation id from the dagster log stream — dbt prints
        # "Running with dbt=1.11.5" then later structured events include the
        # invocation_id. Easier path: scrape "invocation_id" from log messages.
        q = f'''{{ logsForRun(runId: "{run_id}", afterCursor: null, limit: 500) {{
            ... on EventConnection {{ events {{
                ... on MessageEvent {{ message }}
            }} }}
        }} }}'''
        events = (gql(q).get("data", {}).get("logsForRun") or {}).get("events", []) or []
        for e in events:
            msg = _strip_ansi(e.get("message", "") or "")
            m = re.search(r'invocation_id["\'\s:=]+([0-9a-f-]{36})', msg)
            if m:
                expected_invocation = m.group(1)
                break

    py = (
        "import json, sys; "
        "d = json.load(open('/workspace/dbt/lakehouse_mvp/target/run_results.json')); "
        "inv = d.get('metadata', {}).get('invocation_id', '?'); "
        f"expected = {expected_invocation!r}; "
        "print(f'dbt invocation: {inv}'); "
        "sys.exit(2) if expected and inv != expected else None; "
        "[print(r['unique_id'], r['status'], (r.get('message') or '').replace('\\n', ' | ')) "
        "for r in d['results'] if r['status'] != 'success']"
    )
    result = subprocess.run(
        ["docker", "exec", WORKSPACE, "python", "-c", py],
        capture_output=True, text=True, env=env,
    )
    sys.stdout.write(result.stdout)
    if result.returncode == 2:
        print("STALE: run_results.json invocation_id does not match the requested run.")
        print("       Either dbt hasn't run yet for this run, or this run was older.")
        sys.exit(0)
    if result.returncode != 0:
        sys.stderr.write(result.stderr)


def cmd_watch(run_id: str):
    """Live-poll a run, printing state changes as they happen.

    Adaptive cadence: 30s polling while bronze is running (Marker is slow),
    5s while dbt or other Python steps are running. Stops when the run
    reaches a terminal state. Prints dbt model transitions individually.
    """
    import time as _time
    run_id = _resolve_run_id(run_id)
    seen_step_states: dict[str, str] = {}
    seen_model_states: dict[str, str] = {}
    last_print_time = 0.0
    start_wall = _time.time()

    def _stamp() -> str:
        elapsed = int(_time.time() - start_wall)
        m, s = divmod(elapsed, 60)
        return f"{m:02d}:{s:02d}"

    while True:
        state = _fetch_run_state(run_id)
        if state.get("error"):
            print(state["error"])
            return
        status = state["status"]

        # Detect step transitions
        active_step = None
        for ss in state["step_stats"]:
            key = ss["stepKey"]
            cur = ss["status"]
            prev = seen_step_states.get(key)
            if prev != cur:
                # Compute duration if terminal
                dur = ""
                if cur in ("SUCCESS", "FAILURE", "SKIPPED") and ss.get("startTime"):
                    end = ss.get("endTime") or _time.time()
                    dur = " " + _fmt_duration(end - ss["startTime"])
                print(f"{_stamp()}  {key:24s} {cur}{dur}")
                seen_step_states[key] = cur
            if cur in ("STARTED", "IN_PROGRESS"):
                active_step = key

        # dbt model transitions
        if state["dbt_models"]:
            for name in sorted(state["dbt_models"], key=lambda n: state["dbt_models"][n]["index"]):
                m = state["dbt_models"][name]
                cur = m["status"]
                prev = seen_model_states.get(name)
                if prev != cur:
                    dur = ""
                    if cur in ("OK", "ERROR", "SKIP") and m.get("start_ts") and m.get("end_ts"):
                        dur = " " + _fmt_duration(m["end_ts"] - m["start_ts"])
                    total = state["dbt_total"]
                    print(f"{_stamp()}  dbt: {m['index']:3d}/{total} {cur:5s} {name}{dur}")
                    seen_model_states[name] = cur

        # Terminal status?
        if status in ("SUCCESS", "FAILURE", "CANCELED"):
            print(f"{_stamp()}  RUN {status}")
            if status == "FAILURE":
                print()
                print("=== dbt errors (filtered to this run) ===")
                cmd_dbt_results(run_id)
            return

        # Adaptive sleep
        sleep_s = 30 if active_step == "bronze_tabletop" else 5
        _time.sleep(sleep_s)


def cmd_status(run_id: str):
    run_id = _resolve_run_id(run_id)
    q = f'{{ runOrError(runId: "{run_id}") {{ ... on Run {{ status startTime endTime }} }} }}'
    d = gql(q)["data"]["runOrError"]
    print(f"Status: {d['status']}")


def cmd_logs(run_id: str, n: int = 20):
    run_id = _resolve_run_id(run_id)
    q = f'''{{ logsForRun(runId: "{run_id}", afterCursor: null, limit: {n}) {{
        ... on EventConnection {{ events {{
            __typename
            ... on MessageEvent {{ message }}
            ... on ExecutionStepFailureEvent {{ stepKey error {{
                message stack
                cause {{ message stack
                    cause {{ message stack }}
                }}
            }} }}
        }} }}
    }} }}'''
    events = gql(q)["data"]["logsForRun"]["events"]
    msgs = []
    errors = []

    def _format_err(err, indent=""):
        out = [f"{indent}{err['message'].rstrip()}"]
        for line in (err.get("stack") or []):
            out.append(f"{indent}  {line.rstrip()}")
        if err.get("cause"):
            out.append(f"{indent}--- caused by ---")
            out.extend(_format_err(err["cause"], indent))
        return out

    for e in events:
        if e.get("message"):
            msgs.append(e["message"])
        if e.get("error"):
            step = e.get("stepKey", "?")
            errors.append(f"\n!! STEP FAILED: {step}")
            errors.extend(_format_err(e["error"]))
    for m in msgs[-n:]:
        print(m)
    for e in errors:
        print(e)


def _resolve_run_id(partial: str) -> str:
    """Resolve a partial run ID to a full one."""
    q = '{ runsOrError(limit: 20) { ... on Runs { results { runId status } } } }'
    runs = gql(q)["data"]["runsOrError"]["results"]
    matches = [r["runId"] for r in runs if r["runId"].startswith(partial)]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        print(f"Ambiguous partial ID '{partial}', matches: {matches}")
        sys.exit(1)
    return partial  # try as-is


def cmd_cancel(run_id: str):
    run_id = _resolve_run_id(run_id)
    q = f'''mutation {{ terminateRun(runId: "{run_id}", terminatePolicy: MARK_AS_CANCELED_IMMEDIATELY) {{
        __typename
        ... on TerminateRunSuccess {{ run {{ status }} }}
        ... on PythonError {{ message }}
    }} }}'''
    d = gql(q)["data"]["terminateRun"]
    if d["__typename"] == "TerminateRunSuccess":
        print(f"Cancelled {run_id[:12]}: {d['run']['status']}")
    else:
        print(f"Error: {d.get('message', d['__typename'])}")


def cmd_cancel_all():
    """Cancel all running/queued pipeline runs."""
    q = '{ runsOrError(limit: 20) { ... on Runs { results { runId status } } } }'
    runs = gql(q)["data"]["runsOrError"]["results"]
    active = [r for r in runs if r["status"] in ("STARTED", "QUEUED", "STARTING")]
    if not active:
        print("No active runs to cancel")
        return
    for r in active:
        cmd_cancel(r["runId"])


def cmd_errors(run_id: str):
    """Show compute stderr for all failed steps in a run."""
    run_id = _resolve_run_id(run_id)
    q = f'''{{ runOrError(runId: "{run_id}") {{
        ... on Run {{
            stepStats {{
                stepKey
                status
            }}
        }}
    }} }}'''
    stats = gql(q)["data"]["runOrError"]["stepStats"]
    failed = [s["stepKey"] for s in stats if s["status"] == "FAILURE"]
    if not failed:
        print("No failed steps")
        return

    for step in failed:
        q2 = f'''{{ runOrError(runId: "{run_id}") {{
            ... on Run {{
                capturedLogs(fileKey: "{step}") {{
                    stderr
                }}
            }}
        }} }}'''
        try:
            data = gql(q2)["data"]["runOrError"]["capturedLogs"]
            stderr = data.get("stderr") if data else None
        except Exception:
            stderr = None

        if not stderr:
            # Fall back to reading compute log files from the container.
            # Search by step key and run_id prefix; dump both stdout and stderr.
            env = {**subprocess.os.environ, "MSYS_NO_PATHCONV": "1"}
            result = subprocess.run(
                ["docker", "exec", CONTAINER, "bash", "-c",
                 f"find /workspace/dagster -name '*{step}*' -name '*.err' "
                 f"-newer /tmp/.dockerenv -exec cat {{}} \\; 2>/dev/null; "
                 f"echo ---OUT---; "
                 f"find /workspace/dagster -name '*{step}*' -name '*.out' "
                 f"-newer /tmp/.dockerenv -exec tail -200 {{}} \\; 2>/dev/null"],
                capture_output=True, text=True, env=env,
            )
            stderr = result.stdout.strip()

        print(f"\n{'='*60}")
        print(f"STEP FAILED: {step}")
        print(f"{'='*60}")
        if stderr:
            print(stderr)
        else:
            print("(no stderr captured)")


def cmd_dbt_logs(n: int = 100):
    """Dump the last N lines of /workspace/dbt/lakehouse_mvp/logs/dbt.log."""
    env = {**subprocess.os.environ, "MSYS_NO_PATHCONV": "1"}
    result = subprocess.run(
        ["docker", "exec", WORKSPACE, "bash", "-c",
         f"tail -n {n} /workspace/dbt/lakehouse_mvp/logs/dbt.log 2>&1"],
        capture_output=True, text=True, env=env,
    )
    sys.stdout.write(result.stdout)
    if result.returncode != 0:
        sys.stderr.write(result.stderr)


def cmd_dbt_internals(what: str = "list", path: str = ""):
    """Inspect installed dbt-duckdb internals.

    Subcommands (passed as the first arg):
        list                       — list dbt.adapters.duckdb package files
        plugins                    — list dbt.adapters.duckdb.plugins files
        macros                     — list bundled materialization macros (.sql)
        cat-pkg <relative/path>    — cat a file under dbt.adapters.duckdb
        cat-inc <relative/path>    — cat a file under dbt.include.duckdb
        find <pattern>             — grep -rln pattern under both trees
    """
    env = {**subprocess.os.environ, "MSYS_NO_PATHCONV": "1"}
    pkg = "/usr/local/lib/python3.11/site-packages/dbt/adapters/duckdb"
    inc = "/usr/local/lib/python3.11/site-packages/dbt/include/duckdb"
    if what == "list":
        cmd = f"ls -la {pkg}"
    elif what == "plugins":
        cmd = f"ls -la {pkg}/plugins"
    elif what == "macros":
        cmd = f"find {inc} -name '*.sql' | sort"
    elif what == "cat-pkg":
        cmd = f"cat {pkg}/{path}"
    elif what == "cat-inc":
        cmd = f"cat {inc}/{path}"
    elif what == "cat":  # backwards compat with earlier usage
        cmd = f"cat {pkg}/{path}"
    elif what == "find":
        cmd = f"grep -rln {path!r} {pkg} {inc} 2>/dev/null"
    else:
        print(cmd_dbt_internals.__doc__)
        return
    result = subprocess.run(
        ["docker", "exec", WORKSPACE, "bash", "-c", cmd],
        capture_output=True, text=True, env=env,
    )
    sys.stdout.write(result.stdout)
    if result.returncode != 0:
        sys.stderr.write(result.stderr)


def cmd_dbt_clean():
    """Force-clean dbt's target/ and partial-parsing cache so the next run does
    a full parse. Use after editing dbt sources/schema yml files."""
    env = {**subprocess.os.environ, "MSYS_NO_PATHCONV": "1"}
    result = subprocess.run(
        ["docker", "exec", WORKSPACE, "bash", "-c",
         "rm -rf /workspace/dbt/lakehouse_mvp/target "
         "/workspace/dbt/lakehouse_mvp/partial_parse.msgpack 2>&1; "
         "echo cleaned"],
        capture_output=True, text=True, env=env,
    )
    sys.stdout.write(result.stdout)
    if result.returncode != 0:
        sys.stderr.write(result.stderr)


def cmd_query(sql: str):
    """Run a SQL query via the DuckDB reader."""
    code = f'''
import sys, json
sys.path.insert(0, "/workspace")
from dlt.lib.duckdb_reader import get_reader
conn = get_reader()
result = conn.execute({sql!r}).fetchall()
cols = [d[0] for d in conn.description]
types = [str(d[1]) for d in conn.description]
print(json.dumps({{"columns": cols, "types": types, "rows": [[str(c)[:200] for c in r] for r in result[:20]]}}))
'''
    raw = _docker_py(code)
    if raw.startswith("ERROR:"):
        print(raw)
        return
    try:
        data = json.loads(raw)
    except Exception:
        print(raw)
        return
    print(f"Columns: {data['columns']}")
    print(f"Types:   {data['types']}")
    print(f"Rows ({len(data['rows'])}):")
    for r in data["rows"]:
        print(f"  {r}")


def cmd_unload():
    """Unload all Ollama models from GPU memory."""
    from urllib.request import urlopen, Request
    from urllib.error import URLError

    try:
        resp = urlopen("http://localhost:11434/api/tags", timeout=5)
        models = [m["name"] for m in json.loads(resp.read()).get("models", [])]
    except (URLError, OSError) as e:
        print(f"Ollama not reachable: {e}")
        return

    for model in models:
        try:
            req = Request(
                "http://localhost:11434/api/generate",
                data=json.dumps({"model": model, "keep_alive": 0}).encode(),
                headers={"Content-Type": "application/json"},
            )
            urlopen(req, timeout=10)
            print(f"  Unloaded {model}")
        except Exception as e:
            print(f"  Failed to unload {model}: {e}")

    print("All models unloaded from GPU")


def cmd_preflight():
    """Check all prerequisites before a pipeline run."""
    from urllib.request import urlopen, Request
    from urllib.error import URLError
    problems = []

    # 1. Docker containers running
    print("Checking Docker containers...")
    for c in ["lakehouse-dagster-webserver", "lakehouse-dagster-daemon",
              "lakehouse-workspace", "lakehouse-postgres"]:
        result = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Running}}", c],
            capture_output=True, text=True,
        )
        running = result.stdout.strip() == "true"
        status = "OK" if running else "DOWN"
        marker = "  " if running else "!!"
        print(f"  {marker} {c:40s} {status}")
        if not running:
            problems.append(f"{c} is not running")

    # 2. Ollama API
    print("Checking Ollama...")
    try:
        resp = urlopen("http://localhost:11434/api/tags", timeout=5)
        models = [m["name"] for m in json.loads(resp.read()).get("models", [])]
        print(f"     Ollama running, {len(models)} models loaded")
        # Check required models from config
        required = _docker_py('''
import sys, json; sys.path.insert(0, "/workspace")
import yaml
with open("/workspace/config/lakehouse.yaml") as f:
    cfg = yaml.safe_load(f)
print(json.dumps(cfg.get("models", {}).get("ollama", {}).get("models", [])))
''')
        try:
            required_models = json.loads(required)
        except Exception:
            required_models = []
        for rm in required_models:
            found = any(rm in m for m in models)
            marker = "  " if found else "!!"
            status = "OK" if found else "MISSING"
            print(f"  {marker} {rm:40s} {status}")
            if not found:
                problems.append(f"Ollama model {rm} not loaded")
    except (URLError, OSError) as e:
        print(f"  !! Ollama not reachable: {e}")
        problems.append("Ollama is not running")

    # 3. Warehouse directory
    print("Checking warehouse...")
    warehouse = _docker_py('''
import sys, json; sys.path.insert(0, "/workspace")
import yaml; from pathlib import Path
with open("/workspace/config/lakehouse.yaml") as f:
    cfg = yaml.safe_load(f)
w = Path(cfg["catalog"]["warehouse"])
print(json.dumps({"path": str(w), "exists": w.exists()}))
''')
    try:
        wh = json.loads(warehouse)
        marker = "  " if wh["exists"] else "!!"
        status = "OK" if wh["exists"] else "MISSING"
        print(f"  {marker} {wh['path']:40s} {status}")
        if not wh["exists"]:
            problems.append(f"Warehouse directory {wh['path']} does not exist")
    except Exception:
        print(f"  !! Could not check warehouse: {warehouse}")

    # 4. PostgreSQL / catalog
    print("Checking catalog...")
    cat_check = _docker_py('''
import sys; sys.path.insert(0, "/workspace")
try:
    from dlt.lib.iceberg_catalog import get_catalog
    c = get_catalog()
    ns = c.list_namespaces()
    print(f"OK ({len(ns)} namespaces)")
except Exception as e:
    print(f"ERROR: {e}")
''')
    if cat_check.startswith("OK"):
        print(f"     {cat_check}")
    else:
        print(f"  !! {cat_check}")
        problems.append(f"Catalog: {cat_check}")

    # Summary
    print()
    if problems:
        print(f"{len(problems)} PROBLEM(S):")
        for p in problems:
            print(f"  !! {p}")
        return False
    else:
        print("All checks passed — ready to run pipeline")
        return True


def cmd_catalog(clean: bool = False):
    """List catalog tables and optionally drop stale entries."""
    code = f'''
import sys, json
sys.path.insert(0, "/workspace")
from pathlib import Path
from dlt.lib.iceberg_catalog import get_catalog, list_all_tables, _load_config

cfg = _load_config()
warehouse = Path(cfg["catalog"]["warehouse"])
catalog = get_catalog()

results = []
for ns, tables in list_all_tables().items():
    for tname in tables:
        table_dir = warehouse / ns / tname
        has_data = table_dir.exists() and any(table_dir.rglob("*.parquet"))
        has_metadata = table_dir.exists() and any(table_dir.rglob("*.metadata.json"))
        results.append({{
            "ns": ns, "table": tname,
            "has_data": has_data, "has_metadata": has_metadata,
        }})

clean = {clean}
dropped = []
if clean:
    for r in results:
        if not r["has_metadata"]:
            full = f"{{r['ns']}}.{{r['table']}}"
            try:
                catalog.drop_table(full)
                dropped.append(full)
            except Exception as e:
                dropped.append(f"{{full}} (error: {{e}})")

print(json.dumps({{"tables": results, "dropped": dropped}}))
'''
    raw = _docker_py(code)
    if raw.startswith("ERROR:"):
        print(raw)
        return

    data = json.loads(raw)
    current_ns = None
    stale = 0
    for t in data["tables"]:
        if t["ns"] != current_ns:
            current_ns = t["ns"]
            print(f"\n  {current_ns}")
        status = "OK" if t["has_metadata"] else "STALE (no metadata on disk)"
        marker = "  " if t["has_metadata"] else "!!"
        if not t["has_metadata"]:
            stale += 1
        print(f"  {marker} {t['table']:40s} {status}")

    if data["dropped"]:
        print(f"\nDropped {len(data['dropped'])} stale entries:")
        for d in data["dropped"]:
            print(f"  - {d}")
    elif stale > 0 and not clean:
        print(f"\n{stale} stale entries. Run 'python scripts/dagster.py catalog clean' to drop them.")
    elif stale == 0:
        print("\nAll catalog entries have data on disk.")


def cmd_reset():
    """Clear all caches and restart Dagster containers."""
    project_root = Path(__file__).resolve().parent.parent
    containers = ["lakehouse-dagster-daemon", "lakehouse-dagster-webserver"]

    # Clear host pycache
    count = 0
    for p in project_root.rglob("__pycache__"):
        if p.is_dir():
            import shutil
            shutil.rmtree(p, ignore_errors=True)
            count += 1
    print(f"Cleared {count} host __pycache__ dirs")

    # Clear container pycache
    for c in containers:
        subprocess.run(
            ["docker", "exec", c, "bash", "-c",
             "find /workspace -name __pycache__ -type d -exec rm -rf {} + 2>/dev/null"],
            capture_output=True, env={**subprocess.os.environ, "MSYS_NO_PATHCONV": "1"},
        )
    print("Cleared container __pycache__")

    # Restart containers
    subprocess.run(["docker", "restart"] + containers, capture_output=True)
    print("Restarted Dagster containers, waiting 15s for grpc...")
    time.sleep(15)
    print("Ready")


def cmd_reload():
    q = f'''mutation {{ reloadRepositoryLocation(repositoryLocationName: "{LOCATION}") {{
        __typename
        ... on WorkspaceLocationEntry {{ name loadStatus }}
        ... on PythonError {{ message }}
    }} }}'''
    d = gql(q)["data"]["reloadRepositoryLocation"]
    if d["__typename"] == "WorkspaceLocationEntry":
        print(f"Reloaded: {d['name']} ({d['loadStatus']})")
    else:
        print(f"Error: {d.get('message', d['__typename'])}")


def cmd_jobs():
    q = "{ repositoriesOrError { ... on RepositoryConnection { nodes { pipelines { name } } } } }"
    nodes = gql(q)["data"]["repositoriesOrError"]["nodes"]
    for node in nodes:
        for p in node["pipelines"]:
            if p["name"] != "__ASSET_JOB":
                print(p["name"])


def cmd_runs(n: int = 5):
    q = f'''{{ runsOrError(limit: {n}) {{
        ... on Runs {{ results {{ runId status jobName startTime endTime }} }}
    }} }}'''
    runs = gql(q)["data"]["runsOrError"]["results"]
    for r in runs:
        start = datetime.fromtimestamp(r["startTime"]).strftime("%H:%M:%S") if r.get("startTime") else "?"
        end = datetime.fromtimestamp(r["endTime"]).strftime("%H:%M:%S") if r.get("endTime") else "running"
        print(f"{r['runId'][:12]}  {r['status']:10}  {r['jobName']:30}  {start} -> {end}")


def cmd_verify():
    """Check row counts across all pipeline layers and flag problems."""
    code = '''
import sys, json
sys.path.insert(0, "/workspace")
from pathlib import Path

errors = []

# Build expected tables from pipeline definitions
from dlt.bronze_tabletop_rules import BRONZE_TABLES, NAMESPACE as BRONZE_NS
from dlt.publish_to_iceberg import PUBLISH_MAP, ENRICHMENT_TABLES, META_TABLES

expected = {BRONZE_NS: BRONZE_TABLES}
for registry in [PUBLISH_MAP, ENRICHMENT_TABLES, META_TABLES]:
    for ns, tables in registry.items():
        expected.setdefault(ns, []).extend(tables)

# Connect to all namespaces
from dlt.lib.duckdb_reader import get_reader
all_ns = list(expected.keys())
try:
    conn = get_reader(namespaces=all_ns)
except Exception as e:
    print(json.dumps({"error": str(e)}))
    sys.exit(0)

# Row counts for all expected tables
counts = {}
for ns, tables in expected.items():
    for tname in tables:
        full = f"{ns}.{tname}"
        try:
            row = conn.execute(f"SELECT count(*) as n FROM {full}").fetchone()
            counts[full] = row[0]
        except Exception:
            counts[full] = -1

checks = {}

# Entry type distribution
try:
    rows = conn.execute(
        "SELECT entry_type, count(*) as n FROM gold_tabletop.gold_entry_index GROUP BY entry_type"
    ).fetchall()
    checks["entry_types"] = {r[0]: r[1] for r in rows}
except Exception as e:
    errors.append(f"entry_type check: {e}")

# Summary coverage per entry type (gold_ai_summaries is owned by enrichment)
try:
    rows = conn.execute("""
        SELECT
            i.entry_type,
            count(DISTINCT i.entry_id) as total,
            count(DISTINCT s.entry_id) as with_summary
        FROM gold_tabletop.gold_entry_index i
        LEFT JOIN gold_tabletop.gold_ai_summaries s
            ON i.entry_id = s.entry_id
        GROUP BY i.entry_type
    """).fetchall()
    checks["summary_by_type"] = {r[0]: {"total": r[1], "with_summary": r[2],
                                         "missing": r[1] - r[2]} for r in rows}
except Exception:
    # Table doesn't exist yet — enrichment hasn't run
    checks["summary_by_type"] = "not yet created"

# Annotation coverage per entry type
try:
    rows = conn.execute("""
        SELECT
            i.entry_type,
            count(DISTINCT i.entry_id) as total,
            count(DISTINCT a.entry_id) as with_annotation
        FROM gold_tabletop.gold_entry_index i
        LEFT JOIN gold_tabletop.gold_ai_annotations a
            ON i.entry_id = a.entry_id
        GROUP BY i.entry_type
    """).fetchall()
    checks["annotation_by_type"] = {r[0]: {"total": r[1], "with_annotation": r[2],
                                            "missing": r[1] - r[2]} for r in rows}
except Exception:
    checks["annotation_by_type"] = "not yet created"

# Read configured entry types for summaries/annotations from config
import yaml
with open("/workspace/documents/tabletop_rules/configs/_default.yaml") as f:
    cfg = yaml.safe_load(f) or {}
gold_cfg = cfg.get("gold", {})
checks["summary_entry_types"] = gold_cfg.get("summary_entry_types", [])
checks["annotation_entry_types"] = gold_cfg.get("annotation_entry_types", [])

# dbt test results
try:
    rows = conn.execute(
        "SELECT status, count(*) as n FROM meta.dbt_test_results GROUP BY status"
    ).fetchall()
    checks["dbt_tests"] = {r[0]: r[1] for r in rows}
except Exception:
    checks["dbt_tests"] = "no results"

conn.close()
print(json.dumps({"counts": counts, "checks": checks, "errors": errors}))
'''
    raw = _docker_py(code)
    if raw.startswith("ERROR:"):
        print(raw)
        return

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        print(f"Unexpected output: {raw}")
        return

    if "error" in data:
        print(f"Cannot connect: {data['error']}")
        return

    counts = data["counts"]
    checks = data["checks"]
    errors = data["errors"]
    summary_types = checks.get("summary_entry_types", [])
    annotation_types = checks.get("annotation_entry_types", [])

    # Print table counts
    print("-- Row counts ----------------------------------")
    current_ns = None
    problems = []
    for table, n in counts.items():
        ns = table.rsplit(".", 1)[0]
        if ns != current_ns:
            current_ns = ns
            print(f"\n  {ns}")
        short_name = table.rsplit(".", 1)[1]
        status = "OK" if n > 0 else "EMPTY" if n == 0 else "MISSING"
        marker = "  " if n > 0 else "!!"
        print(f"  {marker} {short_name:45s} {n:>6}")
        if n <= 0:
            problems.append(f"{table} is {status}")

    # Print checks
    print("\n-- Data checks ---------------------------------")

    if isinstance(checks.get("entry_types"), dict):
        print(f"\n  Entry types: {checks['entry_types']}")
        if list(checks["entry_types"].keys()) == ["rule"]:
            problems.append("All entries are type 'rule' -- entry_type classification missing")

    sbt = checks.get("summary_by_type")
    if isinstance(sbt, dict):
        label = ", ".join(summary_types) if summary_types else "all"
        print(f"  Summary coverage (configured: {label}):")
        for etype, sc in sbt.items():
            pct = f"{sc['with_summary']*100//sc['total']}%" if sc['total'] else "n/a"
            configured = etype in summary_types
            marker = "  " if (sc["missing"] == 0 or not configured) else "!!"
            tag = " *" if configured else ""
            print(f"    {marker} {etype:20s} {sc['with_summary']:>4}/{sc['total']:<4} ({pct}){tag}")
            if configured and sc["missing"] > 0:
                problems.append(f"{etype} summaries: {sc['with_summary']}/{sc['total']} ({sc['missing']} missing)")
    else:
        print(f"  Summary coverage: {sbt}")

    abt = checks.get("annotation_by_type")
    if isinstance(abt, dict):
        label = ", ".join(annotation_types) if annotation_types else "all"
        print(f"  Annotation coverage (configured: {label}):")
        for etype, sc in abt.items():
            pct = f"{sc['with_annotation']*100//sc['total']}%" if sc['total'] else "n/a"
            configured = etype in annotation_types
            marker = "  " if (sc["missing"] == 0 or not configured) else "!!"
            tag = " *" if configured else ""
            print(f"    {marker} {etype:20s} {sc['with_annotation']:>4}/{sc['total']:<4} ({pct}){tag}")
            if configured and sc["missing"] > 0:
                problems.append(f"{etype} annotations: {sc['with_annotation']}/{sc['total']} ({sc['missing']} missing)")
    else:
        print(f"  Annotation coverage: {abt}")

    if isinstance(checks.get("dbt_tests"), dict):
        dt = checks["dbt_tests"]
        total = sum(dt.values())
        failed = dt.get("fail", 0) + dt.get("error", 0)
        print(f"  dbt tests: {dt}")
        if failed > 0:
            problems.append(f"{failed}/{total} dbt tests failing")
    else:
        print(f"  dbt tests: {checks.get('dbt_tests', 'unknown')}")

    for e in errors:
        problems.append(e)

    # Summary
    print("\n-- Result --------------------------------------")
    if problems:
        print(f"  {len(problems)} PROBLEM(S):")
        for p in problems:
            print(f"  !! {p}")
    else:
        print("  All checks passed")


if __name__ == "__main__":
    args = sys.argv[1:]
    if not args or args[0] == "help":
        print(__doc__)
        sys.exit(0)

    cmd = args[0]
    try:
        if cmd == "launch":
            force = "--force" in args
            job_args = [a for a in args[1:] if not a.startswith("--")]
            cmd_launch(job_args[0], force=force)
        elif cmd == "run":
            force = "--force" in args
            job_args = [a for a in args[1:] if not a.startswith("--")]
            cmd_run(job_args[0], force=force)
        elif cmd == "status":
            cmd_status(args[1])
        elif cmd == "state":
            cmd_state(args[1])
        elif cmd == "watch":
            cmd_watch(args[1])
        elif cmd == "logs":
            cmd_logs(args[1], int(args[2]) if len(args) > 2 else 20)
        elif cmd == "cancel":
            if len(args) > 1 and args[1] == "all":
                cmd_cancel_all()
            else:
                cmd_cancel(args[1])
        elif cmd == "errors":
            cmd_errors(args[1])
        elif cmd == "dbt-logs":
            cmd_dbt_logs(int(args[1]) if len(args) > 1 else 100)
        elif cmd == "dbt-results":
            cmd_dbt_results(args[1] if len(args) > 1 else None)
        elif cmd == "dbt-clean":
            cmd_dbt_clean()
        elif cmd == "dbt-internals":
            sub = args[1] if len(args) > 1 else "list"
            arg = args[2] if len(args) > 2 else ""
            cmd_dbt_internals(sub, arg)
        elif cmd == "catalog":
            cmd_catalog(clean="clean" in args[1:])
        elif cmd == "query":
            cmd_query(args[1])
        elif cmd == "unload":
            cmd_unload()
        elif cmd == "preflight":
            if not cmd_preflight():
                sys.exit(1)
        elif cmd == "reset":
            cmd_reset()
        elif cmd == "reload":
            cmd_reload()
        elif cmd == "jobs":
            cmd_jobs()
        elif cmd == "runs":
            cmd_runs(int(args[1]) if len(args) > 1 else 5)
        elif cmd == "verify":
            cmd_verify()
        else:
            print(f"Unknown command: {cmd}")
            print(__doc__)
            sys.exit(1)
    except IndexError:
        print(f"Missing argument for '{cmd}'")
        print(__doc__)
        sys.exit(1)
