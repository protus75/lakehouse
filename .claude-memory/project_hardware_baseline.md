---
name: Hardware baseline — verified healthy 2026-04-17
description: 13900K + RTX 4090 passed full AVX2 + GPU + LLM stress. Hardware is not a suspect for crashes. Baseline numbers captured for future comparison.
type: project
---

## Verdict (2026-04-17)

Hardware passed comprehensive stress testing. **If you hit a pipeline crash, look at software first (library versions, Ollama state, Dagster process lifecycle) — do not assume Raptor Lake degradation without fresh evidence.**

**Why:** Intel 13th/14th-gen Vmin shift was a legitimate concern given the rapidfuzz AVX2 SIGSEGV fingerprint. A full stress-test + HWiNFO trace cleared the hardware, so that hypothesis is retired unless new symptoms emerge.

**How to apply:** on future weird crashes (SIGSEGV, WHEA, driver errors), start with the software stack. Re-run the stress battery only if you see a *new* signal like WHEA errors in Event Viewer, PCIe replays/errors, or fresh AVX instability.

## Test methodology

Stress runner: `scripts/stress.py` — subcommands `cpu` (rapidfuzz with AVX2 C ext forcibly enabled), `gpu` (torch CUDA matmul), `llm` (Ollama loop, llama3:70b preferred — exercises real CPU+GPU hybrid load), `all` (combined).

Monitoring: HWiNFO64 in Sensors-only mode with CSV logging. Analyze with a small Python script that filters to active-load rows (CPU >80W or GPU >100W) and reports per-column min/avg/max plus event counts for throttle/PROCHOT/EDP/PCIe-error columns.

## Healthy baseline numbers (2026-04-17)

Record for comparison — if a future run shows significant deviation, that's a hardware signal.

**Under real llama3:70b inference (10 min sustained):**
- Peak Core Max Temp: 94°C (zero TjMax hits)
- Peak CPU Pkg Power: 205 W (under 253W PL1)
- Peak P-core VID: 1.51 V transient, 1.46 V sustained
- Peak VR Current: 166 A (under 307A IccMax)
- Coolant temp peak: 37.5°C (comfortable)
- GPU Hot Spot peak: 68°C, VRAM 94% full (model split CPU/GPU)
- System RAM peak: 40.6 GB
- Zero PROCHOT, zero PCIe errors, zero thermal throttle at TjMax

**Under synthetic worst-case (32 rapidfuzz workers + CUDA matmul + Ollama, ~25 min sustained):**
- Peak Core Max: 100°C (9 samples at TjMax out of 1509)
- 26% of samples >=95°C
- Coolant peak: 44°C (H150i near its ceiling)
- Peak CPU Pkg Power: 248.5 W
- Still zero PROCHOT, zero PCIe errors, all 32 rapidfuzz workers exited clean

## Thermal headroom notes

- H150i (360mm) is adequate; 4000D Airflow case caps radiator at 360mm, so no larger AIO is possible without a new case.
- Under real pipeline load (not synthetic), CPU is well within thermals — 66°C average is fine.
- If real-pipeline thermals ever worsen: check **iCUE** first (Corsair AIO is controlled via iCUE over internal USB, not BIOS). Set pump profile to `Extreme` for free headroom.
- Fall-back lever (no hardware change): lower BIOS PL1 from 253W to ~200W. For pipeline workloads (mostly memory-bound LLM inference) the perf impact is negligible.
