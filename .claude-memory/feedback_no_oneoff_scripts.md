---
name: No one-off scripts — integrate into lakehouse layers
description: All new functionality must be placed in the proper lakehouse layer (bronze/silver/gold), never as standalone scripts
type: feedback
---

STOP creating random one-off scripts. This is a lakehouse project with a medallion architecture.

**Why:** One-off scripts bypass the pipeline, create maintenance burden, and don't store results in the lakehouse. The whole point of the architecture is that data flows through layers with validation at each step.

**How to apply:** Any new functionality must be:
1. A function in the appropriate layer's module (bronze = `dlt/`, silver/gold = `dbt/`)
2. Results stored in a proper table (bronze_tabletop.*, silver_tabletop.*, gold_tabletop.*)
3. Config-driven via the YAML configs
4. Runnable as part of the standard pipeline commands

Never create files in `scripts/` for things that belong in the pipeline.
