"""Utility script: crawl + cleaning experiment (single/few URLs).

Ini BUKAN crawler utama dataset.
Untuk crawling dataset endless + output data/raw, pakai:
  python main.py --production
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
from pathlib import Path

# Ensure project root is importable when running: python src/cleaner/clean_crawler.py
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

try:
    from bs4 import BeautifulSoup
except Exception:  # pragma: no cover
    BeautifulSoup = None

try:
    from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
except Exception:  # pragma: no cover
    AsyncWebCrawler = None
    BrowserConfig = None
    CrawlerRunConfig = None
    CacheMode = None

try:
    from trafilatura import extract as trafilatura_extract
except Exception:  # pragma: no cover
    trafilatura_extract = None

from config import Settings
from utils.processor import (
    clean_markdown,
    fuzzy_science_relevance,
    is_indonesian_text,
    relevance_score,
)

class CleanDataCrawler:
    def __init__(self):
        self.settings = Settings()

        if AsyncWebCrawler is None or BrowserConfig is None or CrawlerRunConfig is None:
            raise RuntimeError(
                "crawl4ai belum ter-install di environment ini. Jalankan: pip install -r requirements.txt"
            )
        if BeautifulSoup is None:
            raise RuntimeError(
                "beautifulsoup4 belum ter-install di environment ini. Jalankan: pip install -r requirements.txt"
            )

        # Konfigurasi Browser (Headless, User Agent realistis)
        self.browser_config = BrowserConfig(
            headless=self.settings.HEADLESS,
            verbose=True,
            # Tambahkan args jika perlu bypass deteksi bot sederhana
            extra_args=["--disable-blink-features=AutomationControlled"],
        )

        # Optional: if Crawl4AI provides RsTrafilaturaStrategy in this version
        extraction_strategy = None
        try:
            from crawl4ai.extraction_strategy import RsTrafilaturaStrategy

            strategy = RsTrafilaturaStrategy()
            if hasattr(strategy, "output_markdown"):
                setattr(strategy, "output_markdown", True)
            if hasattr(strategy, "favor_precision"):
                setattr(strategy, "favor_precision", True)
            extraction_strategy = strategy
        except Exception:
            extraction_strategy = None

        # Konfigurasi Crawling
        self.crawler_config = CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            page_timeout=self.settings.PAGE_TIMEOUT,
            extraction_strategy=extraction_strategy,
            # Wait for CSS selector umum untuk konten utama (bisa disesuaikan per situs)
            wait_for="article, main, .content, body",
            delay_before_return_html=2.0,  # Beri waktu 2 detik setelah load untuk JS render
            exclude_external_links=True,
            process_iframes=False,  # Seringkali iframe adalah iklan
        )

    def clean_html(self, html: str) -> str:
        """
        Membersihkan HTML dari tag navigasi, script, style, dan boilerplate lainnya
        sebelum diekstraksi menjadi teks.
        """
        soup = BeautifulSoup(html, 'lxml')

        # Hapus tag yang tidak diinginkan
        for tag in soup(['script', 'style', 'nav', 'footer', 'header', 'aside', 'form', 'noscript']):
            tag.decompose()

        # Hapus atribut yang tidak perlu (onclick, onmouseover, dll)
        for tag in soup.find_all(True):
            attrs = dict(tag.attrs)
            for attr in attrs:
                if attr.startswith('on') or attr in ['id', 'class']: # Opsional: hapus class/id jika terlalu kotor
                     # Kita simpan class/id jika diperlukan untuk struktur, tapi bisa dihapus jika ingin polos
                    pass 
        
        return str(soup)

    def extract_main_content(self, html: str, url: str) -> str:
        """Ekstrak konten utama menggunakan Trafilatura (jika tersedia)."""
        if trafilatura_extract is None or not html:
            return ""

        try:
            text = trafilatura_extract(
                html,
                url=url,
                include_tables=False,
                include_comments=False,
            )
        except TypeError:
            text = trafilatura_extract(html, url=url)
        except Exception:
            text = None

        return text.strip() if isinstance(text, str) and text.strip() else ""

    def normalize_text(self, text: str) -> str:
        """
        Normalisasi teks: hapus spasi berlebih, karakter aneh, dll.
        """
        if not text:
            return ""
        
        # Hapus spasi berlebih
        text = re.sub(r'\s+', ' ', text).strip()
        
        # Hapus karakter kontrol non-printable
        text = ''.join(char for char in text if char.isprintable() or char in ['\n', '\t'])
        
        return text

    def _estimate_extraction_quality(self, content: str, *, title: str = "") -> float:
        if not content:
            return 0.0

        text = " ".join(content.split())
        wc = len(text.split())

        if wc >= 400:
            q = 0.95
        elif wc >= 250:
            q = 0.90
        elif wc >= 150:
            q = 0.82
        elif wc >= 110:
            q = 0.78
        else:
            q = 0.60

        link_hits = text.lower().count("http")
        if link_hits > max(3, wc // 80):
            q -= 0.05

        t = (title or "").strip().lower()
        if len(t) >= 8 and t in text.lower():
            q += 0.02

        return 0.0 if q < 0.0 else 1.0 if q > 1.0 else q

    async def crawl_and_process(self, url: str):
        print(f"🕷️  Mulai crawling: {url}")

        async with AsyncWebCrawler(config=self.browser_config) as crawler:
            result = await crawler.arun(url=url, config=self.crawler_config)

            if not result.success:
                print(f"❌ Gagal crawling {url}: {result.error_message}")
                return None

            title = result.metadata.get("title", "") if result.metadata else ""

            # Prefer extraction_strategy output if present
            content_md = ""
            extraction_quality = None

            extracted_payload = getattr(result, "extracted_content", None)
            if isinstance(extracted_payload, str) and extracted_payload.strip():
                try:
                    data = json.loads(extracted_payload)
                    item = None
                    if isinstance(data, list) and data and all(isinstance(x, dict) for x in data):
                        item = max(
                            data,
                            key=lambda x: float(x.get("extraction_quality", 0.0) or 0.0),
                        )
                    elif isinstance(data, dict):
                        item = data

                    if isinstance(item, dict):
                        q = item.get("extraction_quality")
                        if isinstance(q, (int, float)):
                            extraction_quality = float(q)

                        raw = (
                            item.get("content_markdown")
                            or item.get("markdown")
                            or item.get("main_content")
                            or item.get("content")
                            or ""
                        )
                        if isinstance(raw, str):
                            content_md = raw.strip()
                except Exception:
                    content_md = ""

            # Trafilatura fallback
            if not content_md:
                cleaned_html = self.clean_html(result.html or "")
                content_md = self.extract_main_content(cleaned_html, url)

            # Crawl4AI markdown fallback
            if not content_md:
                md = getattr(result, "markdown", None)
                if isinstance(md, str):
                    content_md = md.strip()
                elif md is not None:
                    content_md = (
                        getattr(md, "fit_markdown", None)
                        or getattr(md, "raw_markdown", "")
                        or ""
                    ).strip()

            content_md = clean_markdown(content_md)

            if len(content_md) < 100:
                print(f"⚠️  Konten terlalu pendek untuk {url}, mungkin hanya boilerplate.")
                return None

            if extraction_quality is None:
                extraction_quality = self._estimate_extraction_quality(content_md, title=title)

            if extraction_quality < self.settings.MIN_EXTRACTION_QUALITY:
                print(
                    f"⚠️  Extraction quality rendah ({extraction_quality:.2f}) untuk {url}, skip."
                )
                return None

            # Indonesian + relevance / fuzzy science gate
            is_english_subject = any(
                x in url.lower() for x in ["bahasa-inggris", "english", "b-inggris"]
            )
            if not is_english_subject and not is_indonesian_text(content_md):
                print(f"⚠️  Non-Indonesian content untuk {url}, skip.")
                return None

            combined_text = f"{title} {content_md}"
            exact_score = relevance_score(combined_text)

            fuzzy_score = 0
            fuzzy_hits = []
            if exact_score < self.settings.MIN_RELEVANCE_SCORE:
                fuzzy_score, fuzzy_hits = fuzzy_science_relevance(
                    combined_text,
                    threshold=self.settings.FUZZY_SCIENCE_THRESHOLD,
                    min_hits=self.settings.FUZZY_SCIENCE_MIN_HITS,
                )

            passed = (
                exact_score >= self.settings.MIN_RELEVANCE_SCORE
                or (
                    fuzzy_score >= self.settings.FUZZY_SCIENCE_THRESHOLD
                    and len(fuzzy_hits) >= self.settings.FUZZY_SCIENCE_MIN_HITS
                )
            )
            if not passed:
                print(
                    f"⚠️  Low relevance untuk {url} (exact={exact_score}, fuzzy={fuzzy_score}), skip."
                )
                return None

            final_text = self.normalize_text(content_md)

            print(f"✅ Berhasil memproses {url} (Panjang: {len(final_text)} karakter)")

            return {
                "url": url,
                "title": title,
                "content": final_text,
                "word_count": len(final_text.split()),
                "metadata": {
                    "relevance_score": exact_score,
                    "fuzzy_science_score": fuzzy_score,
                    "fuzzy_science_hits": fuzzy_hits,
                    "extraction_quality": extraction_quality,
                },
            }

def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(add_help=True)
    p.add_argument("--url", action="append", default=[], help="URL untuk diuji (boleh diulang)")
    p.add_argument(
        "--urls-file",
        type=str,
        default="",
        help="File txt berisi daftar URL (1 per baris)",
    )
    p.add_argument(
        "--out",
        type=str,
        default="data/raw/dataset_llm.jsonl",
        help="Output JSONL (default: data/raw/dataset_llm.jsonl)",
    )
    return p.parse_args(argv)


async def run_cli(args: argparse.Namespace) -> int:
    urls: list[str] = []

    if args.urls_file:
        path = Path(args.urls_file)
        if path.exists():
            for line in path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line and not line.startswith("#"):
                    urls.append(line)

    if args.url:
        urls.extend([u.strip() for u in args.url if u and u.strip()])

    if not urls:
        print(
            "Ini script utilitas untuk test ekstraksi/cleaning.\n"
            "Untuk crawling dataset utama, pakai: python main.py --production\n\n"
            "Contoh penggunaan tool ini:\n"
            "  python src/cleaner/clean_crawler.py --url https://contoh.com/artikel\n"
        )
        return 0

    crawler = CleanDataCrawler()
    dataset: list[dict] = []

    for url in urls:
        data = await crawler.crawl_and_process(url)
        if data:
            dataset.append(data)

    output_path = Path(args.out)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        for item in dataset:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")

    print(f"\nSelesai. Disimpan di {output_path}")
    print(f"Total dokumen bersih: {len(dataset)}")
    return 0


if __name__ == "__main__":
    _args = _parse_args()
    try:
        raise SystemExit(asyncio.run(run_cli(_args)))
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(1)
