---
name: project_model_seeding
description: Model downloads must go through dedicated seed pipeline only, never during normal runs
type: project
---

No pipeline should ever download model files. There must be a dedicated seeding pipeline that alone is allowed to download models. All other pipelines must run offline and fail if models are missing from cache.

**Why:** Metered network. Marker model is 1.34GB and was downloading on every Dagster run because daemon container was missing HF_HOME env var and datalab cache volume mount.

**How to apply:**
1. Daemon container has `TRANSFORMERS_OFFLINE=1` and `HF_HUB_OFFLINE=1` env vars
2. Cache volumes: `cache/huggingface` (HF_HOME), `cache/datalab` (/root/.cache/datalab), `cache/marker` (Marker output)
3. Need to create a dedicated `seed_models` Dagster asset/job that downloads models (the only one allowed online)
4. Bronze/silver/gold pipelines must verify cache exists before running Marker
