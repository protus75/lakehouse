#!/usr/bin/env python3
"""Convenience wrapper for Dagster operations via GraphQL API.

Usage:
    python scripts/dagster.py launch <job_name> [--force]
    python scripts/dagster.py status <run_id>
    python scripts/dagster.py logs <run_id> [N]
    python scripts/dagster.py cancel <run_id>
    python scripts/dagster.py reload
    python scripts/dagster.py jobs
    python scripts/dagster.py runs [N]
"""

import json
import subprocess
import sys
from datetime import datetime

CONTAINER = "lakehouse-dagster-webserver"
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


def cmd_launch(job: str, force: bool = False):
    run_config = ""
    if force:
        config_json = json.dumps({"ops": {"bronze_tabletop": {"config": {"force": True}}}})
        run_config = f', runConfigData: "{config_json.replace(chr(34), chr(92)+chr(34))}"'
    q = f'''mutation {{
        launchRun(executionParams: {{
            selector: {{
                repositoryLocationName: "{LOCATION}",
                repositoryName: "{REPO}",
                jobName: "{job}"
            }},
            mode: "default"
            {run_config}
        }}) {{
            __typename
            ... on LaunchRunSuccess {{ run {{ runId status }} }}
            ... on PythonError {{ message }}
        }}
    }}'''
    d = gql(q)["data"]["launchRun"]
    if d["__typename"] == "LaunchRunSuccess":
        print(f"Launched: {d['run']['runId']}")
    else:
        print(f"Error: {d.get('message', d['__typename'])}")


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
        elif cmd == "status":
            cmd_status(args[1])
        elif cmd == "logs":
            cmd_logs(args[1], int(args[2]) if len(args) > 2 else 20)
        elif cmd == "cancel":
            cmd_cancel(args[1])
        elif cmd == "reload":
            cmd_reload()
        elif cmd == "jobs":
            cmd_jobs()
        elif cmd == "runs":
            cmd_runs(int(args[1]) if len(args) > 1 else 5)
        else:
            print(f"Unknown command: {cmd}")
            print(__doc__)
            sys.exit(1)
    except IndexError:
        print(f"Missing argument for '{cmd}'")
        print(__doc__)
        sys.exit(1)
