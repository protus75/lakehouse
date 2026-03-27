"""Deterministic integer keys from natural key columns.

Produces the same int64 ID for the same entity across rebuilds.
Uses SHA-256 hash truncated to 63-bit positive integer (fits int64,
compatible with DuckDB, Iceberg, and Arrow).
"""

import hashlib

import yaml
from pathlib import Path


_CONFIG_PATH = Path("/workspace/config/lakehouse.yaml")
_key_defs: dict | None = None


def _load_key_defs() -> dict:
    global _key_defs
    if _key_defs is None:
        with open(_CONFIG_PATH) as f:
            cfg = yaml.safe_load(f) or {}
        _key_defs = cfg.get("stable_keys", {})
    return _key_defs


def stable_hash(*values) -> int:
    """Hash arbitrary values into a stable positive int64.

    Concatenates string representations with null separator,
    SHA-256 hashes, takes first 8 bytes as unsigned int,
    masks to 63 bits to stay positive in signed int64.
    """
    raw = "\x00".join(str(v) if v is not None else "" for v in values)
    digest = hashlib.sha256(raw.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") & 0x7FFFFFFFFFFFFFFF


def make_id(key_type: str, row: dict) -> int:
    """Generate a stable integer ID for a row using the configured key columns.

    Args:
        key_type: Key definition name from config (e.g. "entry_id", "toc_id")
        row: Dict with column values

    Returns:
        Stable positive int64
    """
    defs = _load_key_defs()
    if key_type not in defs:
        raise ValueError(f"Unknown key type '{key_type}'. Configured: {list(defs.keys())}")
    columns = defs[key_type]["columns"]
    values = [row.get(col) for col in columns]
    return stable_hash(*values)
