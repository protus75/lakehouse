---
name: feedback_follow_architecture
description: CRITICAL - Follow lakehouse architecture patterns, don't shortcut with quick hacks that need rework later
type: feedback
---

Follow the actual architecture. This is a lakehouse — that means S3 + Parquet + query engine, not dumping everything into a proprietary database file because it's easier. Don't take shortcuts that create rework.

**Why:** User had to repeatedly correct fundamental architecture violations. Building on DuckDB proprietary storage when the design called for S3 + Parquet meant all the work had to be redone. The user explicitly said "this is a fucking lakehouse, follow the design."

**How to apply:** Before implementing ANY data storage: (1) check what storage layer the project uses, (2) follow the established patterns, (3) if unsure, ask BEFORE writing code — not after building an entire feature on the wrong foundation. Never choose the "quick" approach over the architecturally correct one.
