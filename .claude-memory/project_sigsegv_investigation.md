---
name: SIGSEGV in silver_entries — RESOLVED 2026-04-17
description: rapidfuzz AVX2 C extension crashes in build_entries_from_pages. Fixed with RAPIDFUZZ_IMPLEMENTATION=python env var.
type: project
---

## Status: RESOLVED (2026-04-17)

**Root cause:** rapidfuzz 3.14.5's AVX2 C extension (`rapidfuzz.fuzz_cpp_avx2`)
corrupts the heap on this hardware, causing SIGSEGV in unrelated pure-Python
code downstream in `build_entries_from_pages`.

**Fix:** Set `RAPIDFUZZ_IMPLEMENTATION=python` env var. Applied to webserver,
daemon, and workspace services in `docker/docker-compose.yml`.

## How to detect if this reappears
- Exit code 139 (= 128 + SIGSEGV 11) from silver_entries
- faulthandler trace points at `tabletop_cleanup.py` pure Python code
- Heisenbug: adding prints/logging near crash site makes it go away
- Hardware is fine (memtest clean, no WHEA errors)

## Why: rapidfuzz is pulled in transitively by marker-pdf. It's used in 4 places
in `tabletop_cleanup.py` for fuzzy title matching. The AVX2 variant was
auto-selected based on CPU support (`platform.machine() == 'x86_64'`), and
manifested as heap corruption on this specific hardware/OS combination.

## How to apply
If you see similar SIGSEGVs in other services, add the env var to that
service's `environment:` block in docker-compose.yml and restart. The env var
forces rapidfuzz to use its pure-Python fallback. Performance impact is
negligible for our use case (hundreds of fuzzy matches, not millions).

## Scripts created during investigation (can be removed)
- `scripts/test_sigsegv.py`
- `scripts/test_sigsegv_nofitz.py`
- `scripts/test_sigsegv_isolate.py`
- `scripts/test_sigsegv_noarrow.py`
- `scripts/test_sigsegv_bisect.py`
- `dagster/silver_entries_crash.log`
- `dagster/sigsegv_*.log`

## Related changes during investigation (keep or revert?)
- `dagster/lakehouse_assets/assets.py`: added `in_process_executor` to all jobs.
  Keep — multiprocess executor had its own SIGSEGV issues with DuckDB iceberg
  extension documented in earlier memory.
- `dlt/silver_tabletop/entries.py`: replaced DuckDB reader with PyIceberg reads.
  Keep — cleaner architecture, eliminates DuckDB iceberg extension from the
  silver_entries process (which had been flagged as unstable).
