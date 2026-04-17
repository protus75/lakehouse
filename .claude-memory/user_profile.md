---
name: user_profile
description: User prefers direct PowerShell/CLI over Jupyter. Windows workstation with i9-13900K, RTX 4090 24GB, 64GB DDR5, 4x1TB NVMe.
type: user
---

Prefers PowerShell and direct CLI commands over Jupyter notebooks. Don't assume familiarity with Jupyter — give explicit step-by-step instructions or provide alternatives that run from the terminal instead.

## Workstation specs

- **CPU:** Intel Core i9-13900K — 24 cores (8 P-cores + 16 E-cores), up to 5.8 GHz, 36M cache
- **GPU:** MSI GeForce RTX 4090, 24GB GDDR6X (Ada Lovelace) — used by Marker OCR and available for CUDA workloads
- **RAM:** 64GB DDR5-6000 (2x32GB G.SKILL Trident Z5 RGB, CL32, XMP 3.0)
- **Storage:** 4x Samsung 990 PRO 1TB NVMe (PCIe 4.0 M.2)
- **Motherboard:** MSI MAG Z790 Tomahawk MAX WiFi
- **PSU:** Corsair HX1200 (1200W, fully modular)
- **Cooling:** Corsair iCUE H150i RGB Elite AIO liquid cooler

**How to apply:** sizing decisions should take advantage of this headroom — GPU batch sizes, Ollama model selection (large models fit in 24GB VRAM), parallel Docker workers, large in-memory DuckDB operations. No need to conserve RAM or suggest lighter-weight alternatives unless the task specifically warrants it.
