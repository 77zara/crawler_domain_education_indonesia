"""Site strategy manifest loader (sites.yaml).

PRD v2.0: strategi domain (CSS/script) harus bisa di-update tanpa ubah kode.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


def load_sites_yaml(path: Path) -> dict[str, dict[str, Any]]:
    """Load sites.yaml safely.

    Returns empty dict if file is missing or invalid.
    """
    try:
        if not path.exists():
            return {}
        raw = path.read_text(encoding="utf-8")
        data = yaml.safe_load(raw) or {}
        if not isinstance(data, dict):
            return {}
        out: dict[str, dict[str, Any]] = {}
        for k, v in data.items():
            if not isinstance(k, str) or not isinstance(v, dict):
                continue
            out[k.strip().lower()] = v
        return out
    except Exception:
        return {}


def match_site_config(hostname: str, sites: dict[str, dict[str, Any]]) -> tuple[str, dict[str, Any]] | None:
    """Find the best matching site config for a hostname.

    - exact match wins
    - otherwise longest suffix match wins (e.g., edukasi.kompas.com -> kompas.com)
    """
    host = (hostname or "").strip().lower()
    if not host:
        return None

    if host in sites:
        return host, sites[host]

    best_key = ""
    best_val: dict[str, Any] | None = None
    for key, val in sites.items():
        if not key:
            continue
        if host == key or host.endswith("." + key):
            if len(key) > len(best_key):
                best_key = key
                best_val = val

    if best_val is None:
        return None
    return best_key, best_val


def get_allow_list(site_cfg: dict[str, Any] | None) -> list[str]:
    allow = []
    if isinstance(site_cfg, dict):
        raw = site_cfg.get("allow")
        if isinstance(raw, list):
            allow = [str(x) for x in raw if str(x).strip()]
    return allow


def get_sitemaps(site_cfg: dict[str, Any] | None) -> list[str]:
    seeds = []
    if isinstance(site_cfg, dict):
        raw = site_cfg.get("sitemaps")
        if isinstance(raw, list):
            seeds = [str(x) for x in raw if str(x).strip()]
    return seeds


def get_method(site_cfg: dict[str, Any] | None) -> str:
    if not isinstance(site_cfg, dict):
        return ""
    m = site_cfg.get("method")
    return (str(m) if m is not None else "").strip().lower()


def get_selectors(site_cfg: dict[str, Any] | None) -> dict[str, str]:
    if not isinstance(site_cfg, dict):
        return {}
    raw = site_cfg.get("selectors")
    if not isinstance(raw, dict):
        return {}
    out: dict[str, str] = {}
    for k, v in raw.items():
        if not isinstance(k, str):
            continue
        out[k.strip().lower()] = (str(v) if v is not None else "").strip()
    return out


def get_strip_selectors(site_cfg: dict[str, Any] | None) -> list[str]:
    if not isinstance(site_cfg, dict):
        return []
    raw = site_cfg.get("strip")
    if not isinstance(raw, list):
        return []
    return [str(x) for x in raw if str(x).strip()]
