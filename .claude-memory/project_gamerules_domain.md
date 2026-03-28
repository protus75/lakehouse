---
name: gamerules.ai domain and Cloudflare Tunnel
description: Domain gamerules.ai registered at Unstoppable Domains, served via Cloudflare Tunnel to local Dash app on port 8000
type: project
---

## Domain: gamerules.ai
- Registered at Unstoppable Domains
- Nameservers pointed to Cloudflare: ashton.ns.cloudflare.com, nataly.ns.cloudflare.com
- Cloudflare free tier, DNS-only management

## Cloudflare Tunnel
- Tunnel name: `gamerules`, ID: `f60f5fc0-d2a9-4344-92ce-bb7760bde999`
- Credentials: `C:\Users\richard\.cloudflared\f60f5fc0-d2a9-4344-92ce-bb7760bde999.json`
- Routes `gamerules.ai` → `localhost:8000` (Dash app)
- Installed via `winget install Cloudflare.cloudflared` (v2025.8.1)

**Why:** User wants the Dash tabletop rules browser publicly accessible.

**How to apply:** Use `python scripts/tunnel.py` to manage. Config in `config/lakehouse.yaml` under `tunnel:` and `dashapp:`.
