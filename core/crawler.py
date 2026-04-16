"""
CrawlEngine – endless queue-based crawler.

Arsitektur:
1. DiscoveryWorker: Jalankan search engine queries → masukkan URL ke queue
2. CrawlWorker(s): Ambil URL dari queue → crawl dengan Crawl4AI →
   simpan konten → ekstrak link dari page → masukkan ke queue
3. Semua berjalan sampai stop_event di-set oleh /stop command.
"""

from __future__ import annotations

import asyncio
import collections
import json
import hashlib
import logging
import re
import time
from typing import Optional, Callable, Coroutine, Any
from urllib.parse import urlparse, urlsplit, urlunsplit, parse_qsl, urlencode

import aiosqlite

from crawl4ai import (
    AsyncWebCrawler,
    BrowserConfig,
    CrawlerRunConfig,
    CacheMode,
)
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
from crawl4ai.content_filter_strategy import PruningContentFilter

try:
    from trafilatura import extract as trafilatura_extract
except Exception:  # pragma: no cover
    trafilatura_extract = None

try:
    from bs4 import BeautifulSoup
except Exception:  # pragma: no cover
    BeautifulSoup = None

from config import Settings, TARGET_KEYWORDS, SCIENCE_VOCAB_ID
from utils.processor import (
    build_record,
    build_cpt_record,
    append_jsonl,
    classify_level,
    clean_markdown,
    extract_keywords_found,
    is_indonesian_text,
    relevance_score,
    fuzzy_science_relevance,
)
from utils.discovery import (
    DiscoveryEngine,
    extract_links_from_page,
    is_valid_crawl_url,
    is_indonesian_education_url,
)

logger = logging.getLogger(__name__)


class CrawlStats:
    """Thread-safe crawl statistics."""

    def __init__(self) -> None:
        self.urls_discovered: int = 0
        self.urls_crawled: int = 0
        self.urls_success: int = 0
        self.urls_failed: int = 0
        self.urls_skipped: int = 0
        self.search_queries_done: int = 0
        self.links_extracted: int = 0
        self.tokens_total: int = 0
        self.start_time: float = time.time()
        self._lock = asyncio.Lock()

    async def incr(self, field: str, amount: int = 1) -> None:
        async with self._lock:
            setattr(self, field, getattr(self, field) + amount)

    @property
    def elapsed(self) -> str:
        secs = int(time.time() - self.start_time)
        h, remainder = divmod(secs, 3600)
        m, s = divmod(remainder, 60)
        return f"{h:02d}:{m:02d}:{s:02d}"

    def summary(self) -> str:
        return (
            f"⏱ Uptime: {self.elapsed}\n"
            f"🔍 Search queries: {self.search_queries_done}\n"
            f"📡 URL discovered: {self.urls_discovered}\n"
            f"🌐 URL crawled: {self.urls_crawled}\n"
            f"✅ Success: {self.urls_success}\n"
            f"❌ Failed: {self.urls_failed}\n"
            f"⏭ Skipped: {self.urls_skipped}\n"
            f"🔗 Links extracted: {self.links_extracted}\n"
            f"🧮 Tokens (Qwen): {self.tokens_total}"
        )


class CrawlEngine:
    """Endless search-engine-driven educational crawler."""

    def __init__(self, settings: Settings, stop_event: asyncio.Event) -> None:
        self.settings = settings
        self.stop_event = stop_event

        # URL management
        self.url_queue: asyncio.Queue[str] = asyncio.Queue()
        self.visited: set[str] = set()
        self._visited_lock = asyncio.Lock()

        # Instance ID for multi-worker isolation (no folder splitting; keep everything under data/raw)
        self.instance_id = self._sanitize_instance_id(settings.INSTANCE_ID)
        self.data_dir = settings.DATA_DIR
        self.data_dir.mkdir(parents=True, exist_ok=True)

        suffix = f"_{self.instance_id}" if self.instance_id else ""

        # Data files (always write crawl outputs into data/raw/)
        self.raw_dir = self.data_dir / "raw"
        self.raw_dir.mkdir(parents=True, exist_ok=True)

        # Persistent dedupe (across restarts): visited URLs + content hashes
        # Prefer DB next to raw outputs so your pipeline stays: data/raw -> processed -> gold
        raw_db_path = (self.raw_dir / f"dedupe{suffix}.sqlite3")

        # Backward-compatible fallback ONLY for default instance
        legacy_db_path = (self.data_dir / "dedupe.sqlite3")
        default_raw_db_path = (self.raw_dir / "dedupe.sqlite3")
        if not suffix and legacy_db_path.exists() and not default_raw_db_path.exists():
            self._dedupe_db_path = legacy_db_path
        else:
            self._dedupe_db_path = raw_db_path

        self._db: aiosqlite.Connection | None = None
        self._db_lock = asyncio.Lock()

        # Per-domain cap: max pages saved per domain to ensure diversity
        self.domain_counter: dict[str, int] = collections.defaultdict(int)
        self._domain_lock = asyncio.Lock()
        self.MAX_PER_DOMAIN = 50  # cap per domain (Wikipedia etc.)

        # Browser restart signal
        self._browser_restart_needed = asyncio.Event()
        self._pages_since_restart: int = 0
        self.RESTART_BROWSER_EVERY: int = 500  # restart browser every N crawled pages

        # Stats
        self.stats = CrawlStats()
        self.is_running: bool = False

        # Discovery
        self.discovery = DiscoveryEngine(
            max_pages_per_query=3,
            shard_index=settings.DISCOVERY_SHARD_INDEX,
            shard_count=settings.DISCOVERY_SHARD_COUNT,
        )

        # Notify callback (set by bot)
        self._notify_callback: Optional[Callable[[str], Coroutine[Any, Any, None]]] = None

        # Output naming: raw first; processed stages later (not "CPT" yet)
        self.output_file = self.raw_dir / f"dataset_raw{suffix}.jsonl"  # raw crawl output + metadata
        self.output_processed_1_file = self.raw_dir / f"dataset_raw_processed_1{suffix}.jsonl"  # stage-1 cleaned text

        # Tokenizer (lazy init)
        self._hf_tokenizer = None
        self._hf_tokenizer_failed = False

    def set_notify_callback(self, callback) -> None:
        """Set callback untuk notifikasi Telegram."""
        self._notify_callback = callback

    async def _notify(self, message: str) -> None:
        """Kirim notifikasi ke Telegram jika callback tersedia."""
        if self._notify_callback:
            try:
                await self._notify_callback(message)
            except Exception:
                pass

    def _sanitize_instance_id(self, instance_id: str) -> str:
        """Sanitize instance id to a safe directory name (prevents path traversal)."""
        raw = (instance_id or "").strip()
        if not raw:
            return ""

        safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", raw)
        safe = safe.strip("._-")
        return safe[:64] if safe else ""

    # ------------------------------------------------------------------
    # Persistent dedupe helpers
    # ------------------------------------------------------------------

    def _normalize_url(self, url: str) -> str:
        """Normalize URL for stable dedupe (remove fragments + tracking params)."""
        url = (url or "").strip()
        if not url:
            return ""

        # Strip fragment early
        url = url.split("#")[0]

        try:
            parts = urlsplit(url)
        except Exception:
            return ""

        # Normalize scheme + netloc
        scheme = (parts.scheme or "").lower()
        netloc = (parts.netloc or "").lower()
        path = parts.path or ""

        # Remove common tracking params
        blocked_params = {
            "fbclid",
            "gclid",
            "yclid",
            "igshid",
            "mc_cid",
            "mc_eid",
            "ref",
            "ref_src",
        }
        query_items = []
        for k, v in parse_qsl(parts.query, keep_blank_values=False):
            k_lower = k.lower()
            if k_lower.startswith("utm_"):
                continue
            if k_lower in blocked_params:
                continue
            query_items.append((k, v))
        query_items.sort()
        query = urlencode(query_items, doseq=True)

        # Normalize trailing slash
        if path.endswith("/") and path != "/":
            path = path.rstrip("/")

        return urlunsplit((scheme, netloc, path, query, ""))

    async def _open_dedupe_db(self) -> None:
        if self._db is not None:
            return

        self._dedupe_db_path.parent.mkdir(parents=True, exist_ok=True)
        self._db = await aiosqlite.connect(self._dedupe_db_path, timeout=30)
        await self._db.execute("PRAGMA journal_mode=WAL;")
        await self._db.execute("PRAGMA synchronous=NORMAL;")
        await self._db.execute("PRAGMA busy_timeout=5000;")
        await self._db.execute(
            "CREATE TABLE IF NOT EXISTS visited_urls (url TEXT PRIMARY KEY)"
        )
        await self._db.execute(
            "CREATE TABLE IF NOT EXISTS content_hashes (hash TEXT PRIMARY KEY)"
        )
        await self._db.commit()

    async def _close_dedupe_db(self) -> None:
        if self._db is None:
            return
        try:
            await self._db.close()
        finally:
            self._db = None

    async def _dedupe_insert(self, table: str, column: str, value: str) -> bool:
        """Return True if value is new (inserted), False if already existed."""
        if self._db is None:
            return True

        allowed = {
            ("visited_urls", "url"),
            ("content_hashes", "hash"),
        }
        if (table, column) not in allowed:
            raise ValueError("Invalid dedupe target")

        async with self._db_lock:
            cur = await self._db.execute(
                f"INSERT OR IGNORE INTO {table}({column}) VALUES (?)",
                (value,),
            )
            changes_cur = await self._db.execute("SELECT changes()")
            try:
                row = await changes_cur.fetchone()
                await self._db.commit()
                return bool(row and row[0] == 1)
            finally:
                await cur.close()
                await changes_cur.close()

    async def _is_new_content(self, content: str) -> bool:
        """Content-level dedupe using SHA-256 of normalized cleaned text."""
        if not content:
            return False
        normalized = " ".join(content.lower().split())
        digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
        return await self._dedupe_insert("content_hashes", "hash", digest)

    def _estimate_extraction_quality(self, content: str, *, title: str = "") -> float:
        """Heuristik quality score 0..1 untuk gating ekstraksi.

        Tujuan: menolak halaman dangkal/boilerplate tanpa terlalu kompleks.
        """
        if not content:
            return 0.0

        text = " ".join(content.split())
        words = text.split()
        wc = len(words)

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

        # Penalize link-heavy pages
        link_hits = text.lower().count("http")
        if link_hits > max(3, wc // 80):
            q -= 0.05

        # Slight boost if title appears in content
        t = (title or "").strip().lower()
        if len(t) >= 8 and t in text.lower():
            q += 0.02

        if q < 0.0:
            return 0.0
        if q > 1.0:
            return 1.0
        return q

    def _extract_main_content_css(self, html: str) -> str:
        """Best-effort main-article extraction using CSS selectors.

        Returns plain text (paragraphs joined by blank lines) or empty string.
        """
        if not html or BeautifulSoup is None:
            return ""

        # Prefer lxml if available; fallback to html.parser
        try:
            soup = BeautifulSoup(html, "lxml")
        except Exception:
            soup = BeautifulSoup(html, "html.parser")

        # Drop obvious boilerplate early
        for tag in soup(["script", "style", "nav", "footer", "header", "aside", "noscript", "form"]):
            try:
                tag.decompose()
            except Exception:
                pass

        selectors = [
            "article",
            "main article",
            "main",
            "[role='main']",
            ".post-content",
            ".entry-content",
            ".article-body",
            ".article-content",
            ".content-area",
            ".td-post-content",
            ".post-body",
            "#content",
        ]

        root = None
        for sel in selectors:
            try:
                node = soup.select_one(sel)
            except Exception:
                node = None
            if node is not None:
                root = node
                break

        if root is None:
            root = soup.body or soup

        paras: list[str] = []
        for p in root.find_all("p"):
            txt = p.get_text(" ", strip=True)
            if txt:
                paras.append(txt)

        # Fallback: if no <p>, grab text blocks
        if not paras:
            txt = root.get_text("\n", strip=True)
            paras = [x.strip() for x in txt.splitlines() if x.strip()]

        text = "\n\n".join(paras)
        return text.strip()

    def _select_main_paragraphs(self, text: str) -> str:
        """Keep only good paragraphs (avoid headings/minimal/repetitive lines)."""
        if not text:
            return ""

        raw = text.replace("\r\n", "\n").replace("\r", "\n")

        # Prefer paragraph boundaries; fallback to line split
        parts = [p.strip() for p in re.split(r"\n{2,}", raw) if p.strip()]
        if len(parts) <= 1:
            parts = [p.strip() for p in raw.split("\n") if p.strip()]

        # User intent: paragraph inti biasanya > 30 kata; jangan ketat untuk panjang maksimal
        min_words = 15
        min_chars = 50
        max_chars = 0  # 0 = no max

        kept: list[str] = []
        seen: set[str] = set()

        for p in parts:
            # Strip obvious markdown headings/bullets
            p = re.sub(r"^#{1,6}\s+", "", p)
            p = re.sub(r"^[-*•]+\s+", "", p)
            p = " ".join(p.split())

            if len(p) < min_chars:
                continue
            if max_chars and len(p) > max_chars:
                continue
            if len(p.split()) < min_words:
                continue

            key = re.sub(r"\W+", "", p.lower())
            if not key or key in seen:
                continue
            seen.add(key)
            kept.append(p)

        out = "\n\n".join(kept).strip()
        if not out:
            return ""
        return out + "\n"

    def _count_tokens_qwen(self, text: str) -> int:
        """Count tokens using a Qwen tokenizer (best-effort).

        Uses HuggingFace `transformers` AutoTokenizer if available; otherwise falls back
        to a cheap approximation (word count). This should never crash the crawler.
        """
        if not text:
            return 0

        # Keep it bounded for performance
        sample = text if len(text) <= 200000 else text[:200000]

        model_id = (getattr(self.settings, "TOKENIZER_MODEL_ID", "") or "").strip()
        if not model_id:
            return 0

        if self._hf_tokenizer_failed:
            return len(sample.split())

        if self._hf_tokenizer is None:
            try:
                import importlib

                transformers = importlib.import_module("transformers")
                AutoTokenizer = getattr(transformers, "AutoTokenizer")

                self._hf_tokenizer = AutoTokenizer.from_pretrained(
                    model_id,
                    use_fast=True,
                    trust_remote_code=bool(
                        getattr(self.settings, "TOKENIZER_TRUST_REMOTE_CODE", False)
                    ),
                )
            except Exception as exc:
                self._hf_tokenizer_failed = True
                logger.warning("Tokenizer init failed (%s): %s", model_id, str(exc)[:200])
                return len(sample.split())

        try:
            # `encode` is supported for both fast/slow tokenizers
            return len(self._hf_tokenizer.encode(sample, add_special_tokens=False))
        except Exception:
            return len(sample.split())

    # ------------------------------------------------------------------
    # Browser & run config builders
    # ------------------------------------------------------------------

    def _build_browser_config(self) -> BrowserConfig:
        return BrowserConfig(
            headless=self.settings.HEADLESS,
            text_mode=True,
            light_mode=True,
            browser_type="chromium",
            ignore_https_errors=True,
            java_script_enabled=True,
        )

    def _build_run_config(self) -> CrawlerRunConfig:
        cache_map = {
            "enabled": CacheMode.ENABLED,
            "disabled": CacheMode.DISABLED,
            "bypass": CacheMode.BYPASS,
        }
        cache = cache_map.get(self.settings.CACHE_MODE.lower(), CacheMode.ENABLED)

        content_filter = PruningContentFilter(
            threshold=0.48,
            threshold_type="fixed",
            min_word_threshold=30,
        )
        md_generator = DefaultMarkdownGenerator(
            content_filter=content_filter,
            options={
                "ignore_links": True,
                "ignore_images": True,
                "body_width": 0,
            },
        )

        # Optional: if Crawl4AI provides RsTrafilaturaStrategy in this version,
        # use it to extract higher-precision markdown.
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

        return CrawlerRunConfig(
            markdown_generator=md_generator,
            extraction_strategy=extraction_strategy,
            cache_mode=cache,
            word_count_threshold=50,
            page_timeout=self.settings.PAGE_TIMEOUT,
            wait_until="domcontentloaded",
            verbose=False,
            # --- Content selection: only main article content ---
            excluded_tags=["nav", "footer", "header", "aside", "noscript"],
            excluded_selector=(
                "#ads, .ads, .ad-banner, .advertisement, "
                ".sidebar, .widget, .widget-area, "
                ".breadcrumb, .breadcrumbs, "
                ".social-share, .share-buttons, .social-media, "
                ".related-posts, .related-articles, "
                ".comment, .comments, #comments, .comment-form, "
                ".author-box, .author-bio, "
                ".cookie-banner, .cookie-notice, "
                ".newsletter, .subscribe-form, .popup, .modal, "
                ".menu, .nav-menu, .navigation, "
                ".login, .signup, .register, "
                ".search-form, .search-box, "
                ".pagination, .pager, "
                ".tag-cloud, .tags, "
                ".footer-widgets, .site-footer, "
                ".site-header, .top-bar, .toolbar"
            ),
            remove_forms=True,
            remove_overlay_elements=True,
            exclude_external_links=True,
        )

    # ------------------------------------------------------------------
    # URL management
    # ------------------------------------------------------------------

    async def _try_enqueue(self, url: str) -> bool:
        """Add URL to queue if not visited. Returns True if added."""
        url = self._normalize_url(url)
        if not url:
            return False

        # Fast in-run dedupe
        async with self._visited_lock:
            if url in self.visited:
                return False
            self.visited.add(url)

        # Persistent dedupe across restarts
        if not await self._dedupe_insert("visited_urls", "url", url):
            return False

        await self.url_queue.put(url)
        await self.stats.incr("urls_discovered")
        return True

    # ------------------------------------------------------------------
    # Discovery worker
    # ------------------------------------------------------------------

    async def _discovery_worker(self, crawler: AsyncWebCrawler) -> None:
        """Search engine discovery loop."""
        logger.info("🔍 Discovery worker started")
        await self._notify("🔍 Discovery worker dimulai — mencari URL via search engine...")

        delay = self.settings.SEARCH_DELAY
        cycle = 0

        while not self.stop_event.is_set():
            cycle += 1
            logger.info("🔄 Discovery cycle %d", cycle)

            batch = self.discovery.get_next_batch(batch_size=4)
            if not batch:
                logger.info("♻ Semua search queries sudah diproses, memulai cycle baru")
                await self._notify(f"♻ Discovery cycle {cycle}: reset queries, mulai ulang")
                batch = self.discovery.get_next_batch(batch_size=4)
                if not batch:
                    await asyncio.sleep(10)
                    continue

            for search_url, engine in batch:
                if self.stop_event.is_set():
                    break

                found_urls = await self.discovery.search_one(search_url, engine, crawler)
                await self.stats.incr("search_queries_done")

                added = 0
                for url in found_urls:
                    if await self._try_enqueue(url):
                        added += 1

                if added > 0:
                    logger.info(
                        "🆕 %d URL baru ditambahkan ke queue (total queue: %d)",
                        added, self.url_queue.qsize(),
                    )

                # Rate limiting untuk search engine
                await asyncio.sleep(delay)

            # Periodic status update
            if cycle % 5 == 0:
                await self._notify(
                    f"📊 Discovery update:\n{self.stats.summary()}\n"
                    f"📋 Queue size: {self.url_queue.qsize()}"
                )

        logger.info("🛑 Discovery worker stopped")

    # ------------------------------------------------------------------
    # Crawl worker
    # ------------------------------------------------------------------

    async def _crawl_worker(
        self,
        worker_id: int,
        crawler: AsyncWebCrawler,
        run_cfg: CrawlerRunConfig,
    ) -> None:
        """Crawl worker: take URLs from queue, crawl, save, extract links."""
        logger.info("🕷 Crawl worker %d started", worker_id)
        consecutive_errors = 0

        while not self.stop_event.is_set():
            try:
                url = await asyncio.wait_for(
                    self.url_queue.get(), timeout=5.0
                )
            except asyncio.TimeoutError:
                continue

            await self.stats.incr("urls_crawled")

            # Per-domain cap BEFORE crawling — avoid wasting resources
            try:
                save_domain = urlparse(url).netloc.lower()
            except Exception:
                save_domain = "unknown"
            async with self._domain_lock:
                if self.domain_counter[save_domain] >= self.MAX_PER_DOMAIN:
                    await self.stats.incr("urls_skipped")
                    logger.info(
                        "⏭ Domain cap reached (%d) for %s, skip: %s",
                        self.MAX_PER_DOMAIN, save_domain, url,
                    )
                    self.url_queue.task_done()
                    continue

            # Check if browser restart is needed (proactive)
            self._pages_since_restart += 1
            if self._pages_since_restart >= self.RESTART_BROWSER_EVERY:
                logger.info(
                    "🔄 Worker %d: %d pages reached, signaling browser restart",
                    worker_id, self._pages_since_restart,
                )
                self._browser_restart_needed.set()
                # Put URL back so it's not lost
                await self.url_queue.put(url)
                self.url_queue.task_done()
                break  # Exit worker loop — run() will restart browser

            # Crawl the page
            try:
                result = await crawler.arun(url=url, config=run_cfg)
                consecutive_errors = 0  # reset on success
            except Exception as exc:
                await self.stats.incr("urls_failed")
                consecutive_errors += 1
                err_msg = str(exc)
                logger.error("💥 Worker %d exception: %s → %s", worker_id, url, err_msg)
                self.url_queue.task_done()

                # Browser mungkin crash — detect & trigger restart
                _browser_dead_signals = [
                    "Failed to open a new tab",
                    "Target.createTarget",
                    "Target page, context or browser has been closed",
                    "browser has been closed",
                    "Browser closed",
                    "Connection closed",
                    "Protocol error",
                    "Target closed",
                    "Session closed",
                    "Execution context was destroyed",
                ]
                if any(sig.lower() in err_msg.lower() for sig in _browser_dead_signals):
                    logger.warning(
                        "🔧 Worker %d: browser dead/exhausted, triggering restart...",
                        worker_id,
                    )
                    self._browser_restart_needed.set()
                    # Don't sleep — just break and let run() restart
                    break

                if consecutive_errors >= 10:
                    logger.warning(
                        "⚠ Worker %d: %d consecutive errors, sleeping 60s...",
                        worker_id, consecutive_errors,
                    )
                    await asyncio.sleep(60)
                    consecutive_errors = 0
                continue

            if not result.success:
                await self.stats.incr("urls_failed")
                logger.warning(
                    "❌ Worker %d: fail %s → %s",
                    worker_id, url, result.error_message,
                )
                self.url_queue.task_done()
                continue

            title = result.metadata.get("title", "") if result.metadata else ""

            # Extract main content (prefer extraction_strategy → CSS selector → Trafilatura → markdown)
            content = ""
            extraction_quality: float | None = None
            extraction_quality_source = "none"
            extraction_source = "none"

            html = result.html or ""

            # 1) If an extraction_strategy produced extracted_content, prefer it
            extracted_payload = getattr(result, "extracted_content", None)
            if isinstance(extracted_payload, str) and extracted_payload.strip():
                try:
                    data = json.loads(extracted_payload)
                    item: dict | None = None

                    if isinstance(data, list) and data and all(isinstance(x, dict) for x in data):
                        item = max(
                            data,
                            key=lambda x: float(x.get("extraction_quality", 0.0) or 0.0),
                        )
                    elif isinstance(data, dict):
                        item = data

                    if item is not None:
                        q = item.get("extraction_quality")
                        if isinstance(q, (int, float)):
                            extraction_quality = float(q)
                            extraction_quality_source = "strategy"

                        raw = (
                            item.get("content_markdown")
                            or item.get("markdown")
                            or item.get("main_content")
                            or item.get("content")
                            or ""
                        )
                        if isinstance(raw, str) and raw.strip():
                            content = raw.strip()
                            extraction_source = "extraction_strategy"
                except Exception:
                    content = ""

            # 2) CSS selector extraction (more selective for main article)
            if not content and html:
                css_text = self._extract_main_content_css(html)
                if css_text:
                    content = css_text.strip()
                    extraction_source = "css_selector"

            # 3) Trafilatura fallback
            if not content and trafilatura_extract is not None and html:
                try:
                    extracted = trafilatura_extract(
                        html,
                        url=url,
                        include_comments=False,
                        include_tables=False,
                    )
                except TypeError:
                    # Signature differs across Trafilatura versions
                    extracted = trafilatura_extract(html, url=url)
                except Exception:
                    extracted = None

                if extracted:
                    content = extracted.strip()
                    extraction_source = "trafilatura"

            # 4) Markdown fallback
            if not content:
                md = getattr(result, "markdown", None)
                if isinstance(md, str):
                    content = md.strip()
                    extraction_source = "crawl4ai_markdown"
                elif md is not None:
                    content = (
                        getattr(md, "fit_markdown", None)
                        or getattr(md, "raw_markdown", "")
                    ).strip()
                    extraction_source = "crawl4ai_markdown"
                else:
                    content = ""

            if len(content.strip()) < 100:
                await self.stats.incr("urls_skipped")
                self.url_queue.task_done()
                continue

            # --- CONTENT CLEANING: strip UI noise (nav, buttons, etc.) ---
            content = clean_markdown(content)
            if len(content.strip()) < 100:
                await self.stats.incr("urls_skipped")
                self.url_queue.task_done()
                continue

            # --- MAIN PARAGRAPH SELECTION: drop headings/minimal/repetitive lines ---
            content = self._select_main_paragraphs(content)
            if len(content.strip()) < 250:
                await self.stats.incr("urls_skipped")
                self.url_queue.task_done()
                continue

            # --- EXTRACTION QUALITY GATE: skip low-quality extraction ---
            if extraction_quality is None:
                extraction_quality = self._estimate_extraction_quality(content, title=title)
                extraction_quality_source = "heuristic"

            if extraction_quality < self.settings.MIN_EXTRACTION_QUALITY:
                await self.stats.incr("urls_skipped")
                logger.debug(
                    "⏭ Low extraction quality (%.2f/%.2f), skip: %s",
                    extraction_quality,
                    self.settings.MIN_EXTRACTION_QUALITY,
                    url,
                )
                self.url_queue.task_done()
                continue

            # --- CONTENT DEDUPE: skip duplicate articles across runs ---
            if not await self._is_new_content(content):
                await self.stats.incr("urls_skipped")
                self.url_queue.task_done()
                continue

            # --- ADVERTISEMENT GATE: tolak konten promosi/iklan ---
            content_lower = content.lower()
            _AD_SIGNALS = [
                "daftar sekarang", "berlangganan", "langganan",
                "beli paket", "harga paket", "gratis trial",
                "download aplikasi", "unduh aplikasi",
                "promo ", "diskon ", "cashback",
                "hubungi kami", "hubungi sales",
                "free trial", "start free", "sign up",
                "testimoni", "gabung sekarang",
                "coba gratis", "mulai belajar gratis",
                "paket belajar", "fitur premium",
            ]
            ad_hit = sum(1 for sig in _AD_SIGNALS if sig in content_lower)
            if ad_hit >= 3:
                await self.stats.incr("urls_skipped")
                logger.debug("⏭ Advertisement content (%d signals), skip: %s", ad_hit, url)
                self.url_queue.task_done()
                continue

            # --- RELEVANCE GATE: tolak konten asing / tidak relevan ---
            combined_text = f"{title} {content}"

            # Cek bahasa Indonesia (kecuali URL bahasa Inggris)
            is_english_subject = any(x in url.lower() for x in [
                "bahasa-inggris", "english", "b-inggris",
            ])
            if not is_english_subject and not is_indonesian_text(content):
                await self.stats.incr("urls_skipped")
                logger.debug("⏭ Non-Indonesian content, skip: %s", url)
                self.url_queue.task_done()
                continue

            # Cek relevance score berbasis frasa (>= 2 kata) — lebih selektif untuk STEM/humaniora
            phrase_keywords = [kw for kw in TARGET_KEYWORDS if len(kw.split()) >= 2]
            exact_score = relevance_score(combined_text, phrase_keywords)
            min_score = getattr(self.settings, "MIN_PHRASE_RELEVANCE_SCORE", 1)

            fuzzy_score = 0
            fuzzy_hits: list[dict] = []
            if exact_score < min_score:
                # Fuzzy hanya pakai term berbasis frasa (>= 2 kata) untuk konteks lebih kuat
                fuzzy_vocab = [
                    t for t in (SCIENCE_VOCAB_ID + phrase_keywords)
                    if len((t or "").split()) >= 2
                ]
                fuzzy_score, fuzzy_hits = fuzzy_science_relevance(
                    combined_text,
                    threshold=self.settings.FUZZY_SCIENCE_THRESHOLD,
                    min_hits=self.settings.FUZZY_SCIENCE_MIN_HITS,
                    vocab=fuzzy_vocab,
                )

            passed_exact = exact_score >= min_score
            passed_fuzzy = (
                fuzzy_score >= self.settings.FUZZY_SCIENCE_THRESHOLD
                and len(fuzzy_hits) >= self.settings.FUZZY_SCIENCE_MIN_HITS
            )

            if not (passed_exact or passed_fuzzy):
                await self.stats.incr("urls_skipped")
                logger.debug(
                    "⏭ Low relevance (exact=%d/%d, fuzzy=%d/%d), skip: %s",
                    exact_score,
                    min_score,
                    fuzzy_score,
                    self.settings.FUZZY_SCIENCE_THRESHOLD,
                    url,
                )
                self.url_queue.task_done()
                continue

            # Increment domain counter (cap check was done before crawling)
            async with self._domain_lock:
                self.domain_counter[save_domain] += 1

            # Build and save record
            record = build_record(url, title, content)
            level = classify_level(content, url)
            kw = extract_keywords_found(content)
            record["metadata"]["level"] = level
            record["metadata"]["keywords_found"] = kw
            record["metadata"]["relevance_score"] = exact_score
            record["metadata"]["fuzzy_science_score"] = fuzzy_score
            record["metadata"]["fuzzy_science_hits"] = fuzzy_hits
            record["metadata"]["extraction_quality"] = extraction_quality
            record["metadata"]["extraction_quality_source"] = extraction_quality_source
            record["metadata"]["extraction_source"] = extraction_source

            append_jsonl(self.output_file, record)

            # Also save stage-1 processed text (not "CPT" yet)
            processed_1_record = build_cpt_record(
                url,
                title,
                content,
                level=level,
                source_domain=save_domain,
            )
            saved_processed_1 = False
            if processed_1_record is not None and processed_1_record["word_count"] >= 75:
                append_jsonl(self.output_processed_1_file, processed_1_record)
                saved_processed_1 = True
            else:
                logger.debug("⏭ Processed-1 quality gate rejected: %s", url)

            # Token counting (Qwen) for milestone reporting
            token_text = ""
            if saved_processed_1 and processed_1_record is not None:
                token_text = processed_1_record.get("text", "") or ""
            if not token_text:
                token_text = content

            tokens = self._count_tokens_qwen(token_text)
            if tokens > 0:
                await self.stats.incr("tokens_total", tokens)

            await self.stats.incr("urls_success")

            logger.info(
                "✅ Worker %d: %s [%s] (%d chars)",
                worker_id, url, level, len(content),
            )

            # Extract links for spidering — LIMIT Wikipedia spidering
            html = result.html or ""
            if html:
                try:
                    is_wikipedia = "wikipedia.org" in url.lower()
                    links = extract_links_from_page(html, url)
                    added = 0
                    max_spider_links = 5 if is_wikipedia else 50
                    for link in links:
                        if added >= max_spider_links:
                            break
                        if is_indonesian_education_url(link) and await self._try_enqueue(link):
                            added += 1
                    if added > 0:
                        await self.stats.incr("links_extracted", added)
                        logger.debug("🔗 %d new links from %s", added, url)
                except Exception:
                    pass

            # Notify every N successes
            if (
                self.stats.urls_success > 0
                and self.stats.urls_success % self.settings.NOTIFY_EVERY == 0
            ):
                await self._notify(
                    f"📈 Milestone: {self.stats.urls_success} halaman berhasil!\n"
                    f"{self.stats.summary()}"
                )

            self.url_queue.task_done()

        logger.info("🛑 Crawl worker %d stopped", worker_id)

    # ------------------------------------------------------------------
    # Main run
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Jalankan endless crawl loop dengan periodic browser restart."""
        logger.info("🚀 CrawlEngine starting — endless mode")
        self.stats = CrawlStats()
        self.visited.clear()
        self.domain_counter.clear()
        self._pages_since_restart = 0
        self.is_running = True

        await self._open_dedupe_db()

        await self._notify(
            "🚀 *Crawler dimulai — Endless Mode*\n"
            f"Workers: {self.settings.MAX_CONCURRENCY}\n"
            f"Output (data/raw):\n"
            f"  • raw/{self.output_file.name} (raw crawl output)\n"
            f"  • raw/{self.output_processed_1_file.name} (stage-1 processed text)\n"
            f"Browser restart setiap: {self.RESTART_BROWSER_EVERY} halaman\n"
            "Gunakan /stop untuk menghentikan."
        )

        # Add seed URLs if any in urls.txt
        seed_file = self.settings.BASE_DIR / "urls.txt"
        if seed_file.exists():
            seeds_added = 0
            for line in seed_file.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line and not line.startswith("#"):
                    if is_valid_crawl_url(line):
                        if await self._try_enqueue(line):
                            seeds_added += 1
            if seeds_added:
                logger.info("🌱 %d seed URLs loaded from urls.txt", seeds_added)

        browser_cfg = self._build_browser_config()
        run_cfg = self._build_run_config()
        session_num = 0

        try:
            while not self.stop_event.is_set():
                session_num += 1
                self._browser_restart_needed.clear()
                self._pages_since_restart = 0

                logger.info("🔄 Browser session #%d starting...", session_num)
                if session_num > 1:
                    await self._notify(
                        f"🔄 Browser restart (session #{session_num}) — "
                        f"membersihkan memory Chromium...\n"
                        f"{self.stats.summary()}"
                    )

                try:
                    async with AsyncWebCrawler(config=browser_cfg) as crawler:
                        tasks: list[asyncio.Task] = []

                        # Discovery worker (1 instance)
                        tasks.append(
                            asyncio.create_task(
                                self._discovery_worker(crawler),
                                name="discovery",
                            )
                        )

                        # Crawl workers
                        for i in range(self.settings.MAX_CONCURRENCY):
                            tasks.append(
                                asyncio.create_task(
                                    self._crawl_worker(i, crawler, run_cfg),
                                    name=f"crawl-{i}",
                                )
                            )

                        if session_num == 1:
                            logger.info(
                                "⚡ %d workers launched (1 discovery + %d crawl)",
                                len(tasks), self.settings.MAX_CONCURRENCY,
                            )

                        # Wait for either stop_event or browser_restart_needed
                        restart_task = asyncio.create_task(
                            self._browser_restart_needed.wait()
                        )
                        stop_task = asyncio.create_task(
                            self.stop_event.wait()
                        )

                        done, pending = await asyncio.wait(
                            [restart_task, stop_task],
                            return_when=asyncio.FIRST_COMPLETED,
                        )

                        # Cancel whichever didn't fire
                        for p in pending:
                            p.cancel()

                        # Cancel all worker tasks with timeout
                        for task in tasks:
                            task.cancel()
                        try:
                            await asyncio.wait_for(
                                asyncio.gather(*tasks, return_exceptions=True),
                                timeout=15.0,
                            )
                        except asyncio.TimeoutError:
                            logger.warning(
                                "⚠ Worker tasks did not cancel within 15s, forcing..."
                            )

                        if self.stop_event.is_set():
                            logger.info("🛑 Stop event received, shutting down...")
                            await self._notify(
                                "🛑 Stop command diterima, menghentikan workers..."
                            )
                            break

                        # Browser restart needed
                        logger.info(
                            "🔄 Browser session #%d ended, restarting...",
                            session_num,
                        )
                        # Brief pause before restarting browser
                        await asyncio.sleep(5)

                except Exception as exc:
                    logger.exception(
                        "💥 Browser session #%d crashed: %s", session_num, exc
                    )
                    await self._notify(
                        f"💥 Browser crash pada session #{session_num}, "
                        f"auto-restart dalam 10 detik...\n"
                        f"Error: {str(exc)[:200]}"
                    )
                    await asyncio.sleep(10)
                    # Continue the while loop → restart browser
                    continue

        except Exception as exc:
            logger.exception("Fatal error pada crawl engine: %s", exc)
        finally:
            self.is_running = False

        # Final report
        report = (
            "📊 *Crawl Selesai — Final Report*\n"
            f"{self.stats.summary()}\n"
            f"📋 Remaining in queue: {self.url_queue.qsize()}\n"
            f"🗃 Total visited URLs: {len(self.visited)}\n"
            f"🔄 Browser sessions: {session_num}"
        )
        logger.info(report)
        await self._notify(report)
        await self._close_dedupe_db()
