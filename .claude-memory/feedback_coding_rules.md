---
name: Coding rules and standards
description: Generic coding rules — no hardcoded values, config-first, no regex, no warn hacks, fix at ingestion, follow architecture, no one-off scripts, never discard data
type: feedback
---

## No hardcoded values — CRITICAL (repeated multiple times)
NEVER hardcode thresholds, field names, patterns, pixel values, magic numbers, or string literals in code. Everything goes in YAML config. This includes metadata fields, length thresholds, chunk sizes, dedup lengths, sub-heading limits, model names, URLs, prompts.

**Why:** User has corrected this many times. Hardcoded values break across different inputs/contexts.

## Config-first — CRITICAL
Write YAML config FIRST, then code that reads it. Never hardcode then "migrate later."

**How to apply:** Before writing any new function: (1) add config keys to default config, (2) add context-specific overrides, (3) write code to read from config. Code should never work without config.

## No regex — use string ops or LLM
STOP using regex for content parsing. Regex has caused catastrophic backtracking (52s hang), unmaintainable patterns, hours of debugging.

**Use instead:** String operations (split, startswith, find, `in`), ML-detected headings, LLM for complex classification. Only use regex for truly atomic patterns (extracting a number, fixed format).

## Never set test severity to warn
Fix actual failures. Zero validation errors means zero — not "zero errors but some warnings." Never suppress with severity='warn'.

## Fix at ingestion, not export
Data quality issues belong in ingestion/parsing code, not export layer. Use config-driven patterns so fixes apply to all inputs.

## Follow lakehouse architecture — CRITICAL
S3 + Parquet + query engine. Don't shortcut with quick hacks. Check established patterns. Ask before writing code on wrong foundation.

## No one-off scripts
All functionality in bronze (`dlt/`), silver/gold (`dbt/`). Results in proper tables. Config-driven. Runnable via standard pipeline.

## Never discard data during extraction — CRITICAL
Capture EVERY field from ANY source. Store raw in bronze with ALL columns. Never reduce or simplify during extraction — that's for silver/gold. If unsure, KEEP IT and ask.
