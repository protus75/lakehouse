---
name: Ollama setup and model location
description: Where Ollama models are stored, how to access from docker, model names
type: reference
---

## Ollama Setup

- Runs on Windows host (not in Docker)
- Installed at: `C:\Users\richard\AppData\Local\Programs\Ollama\ollama.exe`
- Models stored at: `D:\ollama\models` (via `OLLAMA_MODELS` env var)
- Original default location was `C:\Users\richard\.ollama\models` — files were copied to D:
- API endpoint: `http://localhost:11434` (from host) or `http://host.docker.internal:11434` (from container)

## Available Models
- `llama3:70b` — used for AI enrichment (summaries, annotations, OCR checking)
- `minicpm-v:latest` — vision model

## From Docker Container
```python
ollama_url = "http://host.docker.internal:11434"
ollama_model = "llama3:70b"
```
