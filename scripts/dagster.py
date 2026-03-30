#!/usr/bin/env python3
"""Convenience wrapper for Dagster operations via GraphQL API.

Usage:
    python scripts/dagster.py launch <job_name> [--force]
    python scripts/dagster.py run <job_name> [--force]   Launch + poll until done
    python scripts/dagster.py status <run_id>
    python scripts/dagster.py logs <run_id> [N]
    python scripts/dagster.py cancel <run_id>
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


def cmd_status(run_id: str):
    q = f'{{ runOrError(runId: "{run_id}") {{ ... on Run {{ status startTime endTime }} }} }}'
    d = gql(q)["data"]["runOrError"]
    print(f"Status: {d['status']}")


def cmd_logs(run_id: str, n: int = 20):
    q = f'''{{ logsForRun(runId: "{run_id}", afterCursor: null, limit: 200) {{
        ... on EventConnection {{ events {{ ... on MessageEvent {{ message }} }} }}
    }} }}'''
    events = gql(q)["data"]["logsForRun"]["events"]
    msgs = [e["message"] for e in events if e.get("message")]
    for m in msgs[-n:]:
        print(m)


def cmd_cancel(run_id: str):
    q = f'''mutation {{ terminateRun(runId: "{run_id}", terminatePolicy: MARK_AS_CANCELED_IMMEDIATELY) {{
        __typename
        ... on TerminateRunSuccess {{ run {{ status }} }}
        ... on PythonError {{ message }}
    }} }}'''
    d = gql(q)["data"]["terminateRun"]
    if d["__typename"] == "TerminateRunSuccess":
        print(f"Cancelled: {d['run']['status']}")
    else:
        print(f"Error: {d.get('message', d['__typename'])}")


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
        elif cmd == "logs":
            cmd_logs(args[1], int(args[2]) if len(args) > 2 else 20)
        elif cmd == "cancel":
            cmd_cancel(args[1])
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
