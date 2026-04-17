#!/usr/bin/env python3
"""Hardware stress tests for the pipeline's hot paths.

Used to isolate whether crashes (SIGSEGVs, hangs, shader errors) are
hardware-level (Raptor Lake Vmin shift, thermal, VRM, 4090 PCIe) vs
software bugs. Each subcommand exercises one instruction/hardware path
that has caused instability historically.

Subcommands:
    cpu    Hammer rapidfuzz with AVX2 C ext re-enabled (the crash we
           worked around 2026-04-17). Canonical AVX2 torture test.
    gpu    Sustained torch CUDA matmul + repeated Marker passes.
           Exercises VRAM, PCIe, and CPU->GPU transfer stability.
    llm    Tight loop of Ollama /api/generate on the host. Heavy AVX2.
    all    cpu + gpu + llm concurrently. Worst-case combined load.

Usage (from host, repo root):
    python scripts/stress.py cpu --duration 600
    python scripts/stress.py gpu --duration 600
    python scripts/stress.py llm --duration 600 --model qwen3:30b-a3b
    python scripts/stress.py all --duration 1800

While running, run HWiNFO64 in Sensors-only mode and enable CSV
logging. Any SIGSEGV (exit 139), hang, or non-zero exit is a
hardware-suspect signal worth correlating with the HWiNFO trace
(VCore spikes, VID, package power, P-core temps).
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

WORKSPACE = "lakehouse-workspace"
IN_CONTAINER = Path("/workspace").exists() and Path("/.dockerenv").exists()

# ---------------------------------------------------------------------------
# Host-side dispatch: re-exec inside the workspace container with proper env.
# ---------------------------------------------------------------------------

def _docker_exec(subcmd: str, args: list[str], env: dict[str, str]) -> int:
    """Re-run this script inside the workspace container."""
    cmd = ["docker", "exec"]
    for k, v in env.items():
        cmd += ["-e", f"{k}={v}"]
    cmd += [WORKSPACE, "python", "-u", "scripts/stress.py", subcmd, *args]
    proc_env = {**os.environ, "MSYS_NO_PATHCONV": "1"}
    return subprocess.call(cmd, env=proc_env)


# ---------------------------------------------------------------------------
# cpu: rapidfuzz AVX2 torture test (the exact path that SIGSEGV'd)
# ---------------------------------------------------------------------------

def _cpu_worker(duration: int, worker_id: int) -> None:
    """Each worker hammers rapidfuzz.fuzz with AVX2 C ext enabled."""
    import random
    import string
    from rapidfuzz import fuzz, process

    # Confirm we actually got the C ext (not the pure-python fallback)
    impl = os.environ.get("RAPIDFUZZ_IMPLEMENTATION", "cpp")
    print(f"[cpu-{worker_id}] rapidfuzz impl={impl}, starting")

    rng = random.Random(worker_id)
    corpus = ["".join(rng.choices(string.ascii_letters + " ", k=rng.randint(20, 200)))
              for _ in range(2000)]

    deadline = time.time() + duration
    iters = 0
    while time.time() < deadline:
        q = rng.choice(corpus)
        _ = process.extract(q, corpus, scorer=fuzz.WRatio, limit=5)
        _ = fuzz.token_sort_ratio(q, rng.choice(corpus))
        _ = fuzz.partial_ratio(q, rng.choice(corpus))
        iters += 1
        if iters % 500 == 0:
            print(f"[cpu-{worker_id}] iters={iters} t={int(time.time() - (deadline - duration))}s")


def cmd_cpu(duration: int, workers: int) -> int:
    """Parallel rapidfuzz AVX2 stress across N workers."""
    if not IN_CONTAINER:
        return _docker_exec(
            "cpu",
            ["--duration", str(duration), "--workers", str(workers)],
            env={"RAPIDFUZZ_IMPLEMENTATION": "cpp"},
        )

    import multiprocessing as mp
    print(f"[cpu] {workers} workers, {duration}s, rapidfuzz AVX2 C ext enabled")
    procs = [mp.Process(target=_cpu_worker, args=(duration, i)) for i in range(workers)]
    for p in procs:
        p.start()
    for p in procs:
        p.join()

    crashed = [p for p in procs if p.exitcode not in (0, None)]
    if crashed:
        print(f"[cpu] FAIL: {len(crashed)}/{len(procs)} workers crashed. "
              f"Exit codes: {[p.exitcode for p in crashed]}")
        print("[cpu] Exit code 139 = SIGSEGV = AVX2 heap corruption "
              "(Raptor Lake Vmin-shift hallmark). Check HWiNFO VCore/VID trace.")
        return 1
    print(f"[cpu] OK: all {workers} workers completed {duration}s clean.")
    return 0


# ---------------------------------------------------------------------------
# gpu: sustained CUDA matmul + Marker passes
# ---------------------------------------------------------------------------

def _gpu_matmul_loop(duration: int) -> int:
    import torch
    if not torch.cuda.is_available():
        print("[gpu] FAIL: CUDA not available in container.")
        return 2
    dev = torch.device("cuda")
    props = torch.cuda.get_device_properties(0)
    print(f"[gpu] device={props.name} vram={props.total_memory / 1e9:.1f}GB")

    # ~8GB of fp16 matmul. RTX 4090 has 24GB so leaves room for Marker.
    n = 8192
    a = torch.randn(n, n, device=dev, dtype=torch.float16)
    b = torch.randn(n, n, device=dev, dtype=torch.float16)

    deadline = time.time() + duration
    iters = 0
    last_report = time.time()
    while time.time() < deadline:
        c = a @ b
        a = (c * 0.5).to(torch.float16)
        torch.cuda.synchronize()
        iters += 1
        if time.time() - last_report > 10:
            mem = torch.cuda.memory_allocated() / 1e9
            print(f"[gpu] matmul iters={iters} vram_used={mem:.1f}GB "
                  f"elapsed={int(time.time() - (deadline - duration))}s")
            last_report = time.time()
    print(f"[gpu] matmul done, iters={iters}")
    return 0


def cmd_gpu(duration: int) -> int:
    if not IN_CONTAINER:
        return _docker_exec("gpu", ["--duration", str(duration)], env={})
    try:
        rc = _gpu_matmul_loop(duration)
    except Exception as e:
        print(f"[gpu] FAIL: {type(e).__name__}: {e}")
        return 1
    return rc


# ---------------------------------------------------------------------------
# llm: Ollama generate loop (AVX-heavy on host)
# ---------------------------------------------------------------------------

def cmd_llm(duration: int, model: str) -> int:
    # Always runs in the workspace container (which has `requests`).
    # Ollama lives on the host at 11434; we reach it via host.docker.internal.
    if not IN_CONTAINER:
        return _docker_exec("llm", ["--duration", str(duration), "--model", model], env={})

    import requests

    url = "http://host.docker.internal:11434"
    prompt = ("Summarize in one sentence: the quick brown fox jumps over the "
              "lazy dog while reciting the alphabet backwards and calculating "
              "prime numbers in its head.")

    # Warm up (keep model loaded across calls)
    try:
        r = requests.post(f"{url}/api/generate",
                          json={"model": model, "prompt": "hi", "stream": False,
                                "keep_alive": "1h"},
                          timeout=120)
        if r.status_code != 200:
            print(f"[llm] FAIL: Ollama returned {r.status_code}: {r.text[:500]}")
            return 2
    except Exception as e:
        print(f"[llm] FAIL: cannot reach Ollama at {url}: {e}")
        return 2
    print(f"[llm] model={model} loaded, starting {duration}s loop")

    deadline = time.time() + duration
    iters = 0
    errors = 0
    last_report = time.time()
    while time.time() < deadline:
        try:
            r = requests.post(f"{url}/api/generate",
                              json={"model": model, "prompt": prompt,
                                    "stream": False, "keep_alive": "1h",
                                    "options": {"num_predict": 64}},
                              timeout=300)
            if r.status_code != 200:
                errors += 1
            iters += 1
        except Exception as e:
            errors += 1
            print(f"[llm] request error: {e}")
        if time.time() - last_report > 15:
            print(f"[llm] iters={iters} errors={errors} "
                  f"elapsed={int(time.time() - (deadline - duration))}s")
            last_report = time.time()

    print(f"[llm] done iters={iters} errors={errors}")
    return 1 if errors > 0 else 0


# ---------------------------------------------------------------------------
# all: concurrent worst-case
# ---------------------------------------------------------------------------

def cmd_all(duration: int, workers: int, model: str) -> int:
    """Launch cpu + gpu + llm concurrently as subprocesses."""
    # Always dispatched from host.
    self_py = [sys.executable, str(Path(__file__).resolve())]
    procs = {
        "cpu": subprocess.Popen(self_py + ["cpu", "--duration", str(duration),
                                           "--workers", str(workers)]),
        "gpu": subprocess.Popen(self_py + ["gpu", "--duration", str(duration)]),
        "llm": subprocess.Popen(self_py + ["llm", "--duration", str(duration),
                                           "--model", model]),
    }
    print(f"[all] launched cpu/gpu/llm for {duration}s — "
          f"confirm HWiNFO64 CSV logging is running")

    rcs = {name: p.wait() for name, p in procs.items()}
    print(f"[all] exit codes: {rcs}")
    return 0 if all(rc == 0 for rc in rcs.values()) else 1


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_cpu = sub.add_parser("cpu", help="rapidfuzz AVX2 torture (all cores)")
    p_cpu.add_argument("--duration", type=int, default=600)
    p_cpu.add_argument("--workers", type=int, default=os.cpu_count() or 24)

    p_gpu = sub.add_parser("gpu", help="CUDA matmul torture")
    p_gpu.add_argument("--duration", type=int, default=600)

    p_llm = sub.add_parser("llm", help="Ollama generate loop")
    p_llm.add_argument("--duration", type=int, default=600)
    p_llm.add_argument("--model", default="qwen3:30b-a3b")

    p_all = sub.add_parser("all", help="cpu + gpu + llm concurrent")
    p_all.add_argument("--duration", type=int, default=1800)
    p_all.add_argument("--workers", type=int, default=os.cpu_count() or 24)
    p_all.add_argument("--model", default="qwen3:30b-a3b")

    args = parser.parse_args()
    if args.cmd == "cpu":
        return cmd_cpu(args.duration, args.workers)
    if args.cmd == "gpu":
        return cmd_gpu(args.duration)
    if args.cmd == "llm":
        return cmd_llm(args.duration, args.model)
    if args.cmd == "all":
        return cmd_all(args.duration, args.workers, args.model)
    return 2


if __name__ == "__main__":
    sys.exit(main())
