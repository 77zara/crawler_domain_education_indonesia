#!/usr/bin/env python3
"""Test selectors and script-variable extraction for a single URL.

Usage: python tools/selector_test.py <url>
"""
import asyncio
import sys
import pathlib
# Ensure project root is on sys.path when run from tools/
ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from config import Settings
from core.crawler import CrawlEngine
import aiohttp

async def fetch(url: str) -> str:
    async with aiohttp.ClientSession() as sess:
        async with sess.get(url, timeout=30) as r:
            return await r.text()

async def main(url: str):
    settings = Settings()
    engine = CrawlEngine(settings, asyncio.Event())
    html = await fetch(url)

    script_text, script_source = engine._extract_from_script_vars(html, url)
    if script_text:
        q = engine._estimate_extraction_quality(script_text)
        print("--- Script-var extraction (source=", script_source, ") ---")
        print(f"quality={q:.3f}\n")
        print(script_text[:2000])
    else:
        print("No script-variable content found.")

    # Full-page Trafilatura
    try:
        if engine and html:
            print('\n--- Full-page trafilatura extract ---')
            if engine and getattr(engine, 'settings', None):
                # call the same fallback used by crawler
                try:
                    from trafilatura import extract as trafilatura_extract
                except Exception:
                    trafilatura_extract = None
                if trafilatura_extract is not None:
                    try:
                        extracted = trafilatura_extract(html, url=url)
                    except Exception:
                        extracted = trafilatura_extract(html)
                    if extracted:
                        q2 = engine._estimate_extraction_quality(extracted)
                        print(f"quality={q2:.3f}\n")
                        print(extracted[:2000])
                    else:
                        print("Trafilatura returned no content.")
                else:
                    print("Trafilatura not available in this environment.")
    except Exception as e:
        print("Error running trafilatura extract:", e)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: selector_test.py <url>")
        sys.exit(1)
    url = sys.argv[1]
    asyncio.run(main(url))
