"""Sitemap crawler (PRD v2.0).

- Support sitemap.xml and sitemap_index.xml (recursive)
- Extract <loc> links
- Optional allow-list filtering (substring match)
"""

from __future__ import annotations

import gzip
import logging
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Iterable

import aiohttp

logger = logging.getLogger(__name__)


@dataclass
class SitemapCrawlResult:
    pages: list[str]
    sitemaps_seen: int


def _maybe_gunzip(data: bytes) -> bytes:
    if not data:
        return b""
    # gzip magic header
    if len(data) >= 2 and data[0] == 0x1F and data[1] == 0x8B:
        try:
            return gzip.decompress(data)
        except Exception:
            return data
    return data


def _extract_loc_values(root: ET.Element) -> list[str]:
    locs: list[str] = []
    for el in root.findall(".//{*}loc"):
        try:
            if el.text:
                loc = el.text.strip()
                if loc:
                    locs.append(loc)
        except Exception:
            continue
    # Deduplicate preserving order
    return list(dict.fromkeys(locs))


def _parse_sitemap_xml(xml_bytes: bytes) -> tuple[list[str], list[str]]:
    """Return (child_sitemaps, page_urls)."""
    try:
        root = ET.fromstring(xml_bytes)
    except Exception:
        return [], []

    tag = (root.tag or "").lower()
    locs = _extract_loc_values(root)

    # heuristic: sitemapindex contains child sitemaps
    if tag.endswith("sitemapindex"):
        return locs, []

    # urlset contains pages
    if tag.endswith("urlset"):
        return [], locs

    # unknown: treat as pages (best-effort)
    return [], locs


def _allowed(url: str, allow_substrings: Iterable[str] | None) -> bool:
    if not allow_substrings:
        return True
    u = (url or "")
    return any((a or "") in u for a in allow_substrings)


async def crawl_sitemap_recursive(
    session: aiohttp.ClientSession,
    seed_urls: list[str],
    *,
    allow_substrings: list[str] | None = None,
    max_sitemaps: int = 200,
    max_pages: int = 5000,
    timeout_s: float = 30.0,
) -> SitemapCrawlResult:
    """Recursively crawl sitemap urls and return discovered page urls."""

    queue: list[str] = [u for u in seed_urls if (u or "").strip()]
    seen: set[str] = set()
    pages: list[str] = []

    while queue and len(seen) < max_sitemaps and len(pages) < max_pages:
        sitemap_url = queue.pop(0)
        sitemap_url = (sitemap_url or "").strip()
        if not sitemap_url or sitemap_url in seen:
            continue
        seen.add(sitemap_url)

        try:
            async with session.get(sitemap_url, timeout=timeout_s) as resp:
                if resp.status >= 400:
                    logger.debug("Sitemap HTTP %d: %s", resp.status, sitemap_url)
                    continue
                data = await resp.read()
        except Exception:
            continue

        data = _maybe_gunzip(data)
        child_sitemaps, page_urls = _parse_sitemap_xml(data)

        # enqueue child sitemaps
        for sm in child_sitemaps:
            if sm not in seen:
                queue.append(sm)

        # collect page urls
        for page in page_urls:
            if len(pages) >= max_pages:
                break
            if not _allowed(page, allow_substrings):
                continue
            pages.append(page)

    # Deduplicate pages preserving order
    pages = list(dict.fromkeys(pages))

    return SitemapCrawlResult(pages=pages, sitemaps_seen=len(seen))
