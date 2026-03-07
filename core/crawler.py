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
import logging
import time
from typing import Optional, Callable, Coroutine, Any
from urllib.parse import urlparse

from crawl4ai import (
    AsyncWebCrawler,
    BrowserConfig,
    CrawlerRunConfig,
    CacheMode,
)
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
from crawl4ai.content_filter_strategy import PruningContentFilter

from config import Settings
from config import MIN_RELEVANCE_KEYWORDS
from utils.processor import (
    build_record,
    build_cpt_record,
    append_jsonl,
    classify_level,
    clean_markdown,
    extract_keywords_found,
    is_indonesian_text,
    relevance_score,
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
            f"🔗 Links extracted: {self.links_extracted}"
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
        self.discovery = DiscoveryEngine(max_pages_per_query=3)

        # Notify callback (set by bot)
        self._notify_callback: Optional[Callable[[str], Coroutine[Any, Any, None]]] = None

        # Data files
        self.output_file = settings.DATA_DIR / "dataset.jsonl"        # raw markdown + full metadata
        self.output_cpt_file = settings.DATA_DIR / "dataset_cpt.jsonl"  # clean plain text for CPT

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
        return CrawlerRunConfig(
            markdown_generator=md_generator,
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
        # Normalize
        url = url.split("#")[0].rstrip("/")
        if not url:
            return False

        async with self._visited_lock:
            if url in self.visited:
                return False
            self.visited.add(url)

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

            # Extract markdown content
            md = getattr(result, "markdown", None)
            if isinstance(md, str):
                content = md
            elif md is not None:
                content = (
                    getattr(md, "fit_markdown", None)
                    or getattr(md, "raw_markdown", "")
                )
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
            combined_text = f"{content}"
            title = result.metadata.get("title", "") if result.metadata else ""
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

            # Cek relevance score (minimal N keyword match)
            score = relevance_score(combined_text)
            min_score = self.settings.MIN_RELEVANCE_SCORE
            if score < min_score:
                await self.stats.incr("urls_skipped")
                logger.debug(
                    "⏭ Low relevance (%d/%d keywords), skip: %s",
                    score, min_score, url,
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
            record["metadata"]["relevance_score"] = score

            append_jsonl(self.output_file, record)

            # Also save CPT-ready version (clean plain text)
            cpt_record = build_cpt_record(
                url, title, content,
                level=level,
                source_domain=save_domain,
            )
            if cpt_record is not None and cpt_record["word_count"] >= 75:
                append_jsonl(self.output_cpt_file, cpt_record)
            else:
                logger.debug("⏭ CPT quality gate rejected: %s", url)

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

        await self._notify(
            "🚀 *Crawler dimulai — Endless Mode*\n"
            f"Workers: {self.settings.MAX_CONCURRENCY}\n"
            f"Output:\n"
            f"  • dataset.jsonl (raw markdown)\n"
            f"  • dataset\_cpt.jsonl (clean text for CPT)\n"
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
