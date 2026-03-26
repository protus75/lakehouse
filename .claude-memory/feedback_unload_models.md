---
name: feedback_unload_models
description: Always unload Ollama models between passes to free RAM/VRAM before loading next model
type: feedback
---

When switching between Ollama models (e.g. bronze 8b scan → silver 70b review), explicitly unload the previous model before loading the next one via `POST /api/generate {"model":"<name>","keep_alive":0}`.

**Why:** The 70b model uses ~42GB (21GB VRAM + 21GB RAM). If left loaded while the 8b runs, it wastes resources. Ollama won't auto-unload until its timeout expires.

**How to apply:** Add unload calls in any pipeline code that switches models. Also applies to enrichment scripts that use different models for different tasks.
