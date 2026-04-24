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
import random
import ast
import html as _html
from typing import Optional, Callable, Coroutine, Any
from urllib.parse import urlparse, urlsplit, urlunsplit, parse_qsl, urlencode

from datetime import datetime, timezone

import aiosqlite

import aiohttp

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
from utils.site_config import (
    load_sites_yaml,
    match_site_config,
    get_allow_list,
    get_method,
    get_selectors,
    get_strip_selectors,
    get_sitemaps,
)
from utils.sitemap import crawl_sitemap_recursive

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

        # Site strategies (sites.yaml)
        try:
            sites_path = settings.BASE_DIR / (getattr(settings, "SITES_YAML_PATH", "sites.yaml") or "sites.yaml")
        except Exception:
            sites_path = None
        self._sites_path = sites_path
        self._sites = load_sites_yaml(sites_path) if sites_path else {}

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
        if getattr(settings, "DOMAIN_WHITELIST", None):
            # Focused crawl mode: allow many pages from the whitelisted domain(s)
            self.MAX_PER_DOMAIN = max(self.MAX_PER_DOMAIN, 5000)

        # Browser restart signal
        self._browser_restart_needed = asyncio.Event()
        self._pages_since_restart: int = 0
        self.RESTART_BROWSER_EVERY: int = 500  # restart browser every N crawled pages

        # Stats
        self.stats = CrawlStats()
        self.is_running: bool = False

        # Discovery
        focus_domain: str | None = None
        try:
            wl = [d for d in (getattr(settings, "DOMAIN_WHITELIST", None) or []) if (d or "").strip()]
            if len(wl) == 1:
                focus_domain = wl[0]
        except Exception:
            focus_domain = None

        self.discovery = DiscoveryEngine(
            max_pages_per_query=1 if focus_domain else 3,
            shard_index=settings.DISCOVERY_SHARD_INDEX,
            shard_count=settings.DISCOVERY_SHARD_COUNT,
            only_domain=focus_domain,
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

    def _match_site(self, url: str) -> tuple[str, dict[str, Any]] | None:
        """Return (site_key, site_cfg) for URL hostname based on sites.yaml."""
        try:
            host = urlparse(url).netloc.lower()
        except Exception:
            host = ""
        m = match_site_config(host, self._sites)
        return m

    async def _open_dedupe_db(self) -> None:
        async with self._db_lock:
            if self._db is not None:
                return

            self._dedupe_db_path.parent.mkdir(parents=True, exist_ok=True)
            self._db = await aiosqlite.connect(self._dedupe_db_path, timeout=30)
            self._db.row_factory = aiosqlite.Row
            await self._db.execute("PRAGMA journal_mode=WAL;")
            await self._db.execute("PRAGMA synchronous=NORMAL;")
            await self._db.execute("PRAGMA busy_timeout=5000;")

        # State machine table (PRD v2.0): pending/processing/completed/failed/ignored
        await self._db.execute(
            """
            CREATE TABLE IF NOT EXISTS url_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url_hash TEXT UNIQUE,
                url TEXT,
                domain TEXT,
                status TEXT,
                title TEXT,
                content_markdown TEXT,
                metadata_json TEXT,
                created_at TEXT,
                updated_at TEXT,
                attempts INTEGER DEFAULT 0,
                next_retry_at TEXT,
                last_error TEXT
            )
            """
        )
        await self._db.execute("CREATE INDEX IF NOT EXISTS idx_url_jobs_status ON url_jobs(status)")
        await self._db.execute("CREATE INDEX IF NOT EXISTS idx_url_jobs_domain ON url_jobs(domain)")
        await self._db.execute("CREATE INDEX IF NOT EXISTS idx_url_jobs_retry ON url_jobs(next_retry_at)")

        # Schema migration: add `priority` for URL prioritization (e.g., /edu/ first)
        try:
            cur_cols = await self._db.execute("PRAGMA table_info(url_jobs)")
            rows = await cur_cols.fetchall()
            await cur_cols.close()
            col_names = {str(r["name"]) for r in (rows or []) if r and ("name" in r.keys())}

            if "priority" not in col_names:
                await self._db.execute(
                    "ALTER TABLE url_jobs ADD COLUMN priority INTEGER DEFAULT 10"
                )

            await self._db.execute(
                "CREATE INDEX IF NOT EXISTS idx_url_jobs_priority ON url_jobs(priority)"
            )

            # Best-effort backfill (idempotent enough): prioritize Kompas EDU URLs
            await self._db.execute(
                """
                UPDATE url_jobs
                SET priority = CASE
                    WHEN url LIKE 'https://www.kompas.com/edu/%' THEN 0
                    WHEN url LIKE 'https://www.kompas.com/skola/%' THEN 0
                    WHEN url LIKE 'https://edukasi.kompas.com/%' THEN 1
                    WHEN domain LIKE '%kompas.com' THEN 2
                    ELSE 10
                END
                WHERE priority IS NULL OR priority IN (2, 10)
                """
            )
        except Exception:
            pass

        await self._db.execute(
            "CREATE TABLE IF NOT EXISTS visited_urls (url TEXT PRIMARY KEY)"
        )
        await self._db.execute(
            "CREATE TABLE IF NOT EXISTS content_hashes (hash TEXT PRIMARY KEY)"
        )
        await self._db.execute(
            "CREATE TABLE IF NOT EXISTS domain_selectors (domain TEXT PRIMARY KEY, selector TEXT, last_seen INTEGER, quality REAL)"
        )

        # Resume safety: return stuck processing jobs back to pending after a crash.
        # (Best-effort; avoids permanently stuck rows.)
        await self._db.execute(
            "UPDATE url_jobs SET status='pending' WHERE status='processing'"
        )
        await self._db.commit()

    def _url_priority(self, url: str) -> int:
        """Lower number = higher priority."""
        u = (url or "").strip()
        if not u:
            return 10
        try:
            p = urlparse(u)
        except Exception:
            return 10

        host = (p.netloc or "").lower()
        path = (p.path or "").lower()

        if host == "www.kompas.com" and (path.startswith("/edu/") or path.startswith("/skola/")):
            return 0
        if host == "edukasi.kompas.com":
            return 1
        if host.endswith("kompas.com"):
            return 2

        # New domains (zara_adjust.md) — slightly lower priority than kompas
        if host.endswith("detik.com") and "/edu/" in path:
            return 3
        if host.endswith("ruangguru.com") and "/blog/" in path:
            return 4
        if host.endswith("liputan6.com") and "/read/" in path:
            return 5
        if host.endswith("detik.com") or host.endswith("ruangguru.com") or host.endswith("liputan6.com"):
            return 6

        # Batch 2 domains (zara_adjust_1.md)
        if host.endswith("republika.co.id") and "/berita/" in path:
            return 7
        if host.endswith("quipper.com") and "/blog/" in path:
            return 8
        if host.endswith("zenius.net") and "/blog/" in path:
            return 9
        if host.endswith("republika.co.id") or host.endswith("quipper.com") or host.endswith("zenius.net"):
            return 9

        return 10

    def _utcnow_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _url_hash(self, url: str) -> str:
        """Stable SHA-256 hash for URL dedupe/state entry."""
        return hashlib.sha256((url or "").encode("utf-8")).hexdigest()

    async def _job_insert_pending(
        self,
        url: str,
        *,
        source: str = "discovery",
        parent_url: str | None = None,
    ) -> bool:
        """Insert a discovered URL into url_jobs with status='pending'.

        Returns True if inserted (new), False if already exists.
        """
        if self._db is None:
            return False

        url = self._normalize_url(url)
        if not url or not is_valid_crawl_url(url):
            return False

        # Respect domain whitelist
        try:
            wl = getattr(self.settings, "DOMAIN_WHITELIST", None) or []
            if wl:
                net = urlparse(url).netloc.lower()
                allowed = any(net == w or net.endswith("." + w) for w in wl)
                if not allowed:
                    return False
        except Exception:
            return False

        domain = urlparse(url).netloc.lower()
        now = self._utcnow_iso()
        priority = self._url_priority(url)
        payload = {
            "source": (source or "discovery"),
            "parent_url": parent_url,
        }
        meta_json = json.dumps(payload, ensure_ascii=False)
        url_hash = self._url_hash(url)

        async with self._db_lock:
            cur = await self._db.execute(
                """
                INSERT OR IGNORE INTO url_jobs(
                    url_hash, url, domain, status, metadata_json, created_at, updated_at, priority
                ) VALUES(?, ?, ?, 'pending', ?, ?, ?, ?)
                """,
                (url_hash, url, domain, meta_json, now, now, priority),
            )
            changes_cur = await self._db.execute("SELECT changes()")
            try:
                row = await changes_cur.fetchone()
                await self._db.commit()
                inserted = bool(row and row[0] == 1)
            finally:
                await cur.close()
                await changes_cur.close()

        if inserted:
            await self.stats.incr("urls_discovered")
        return inserted

    async def _claim_next_job(self) -> aiosqlite.Row | None:
        """Atomically claim the next pending job and mark it as processing."""
        if self._db is None:
            return None

        now = self._utcnow_iso()

        # Build optional domain filter from DOMAIN_WHITELIST
        wl = getattr(self.settings, "DOMAIN_WHITELIST", None) or []
        domain_filter_sql = ""
        domain_filter_params: list[str] = []
        if wl:
            placeholders = ",".join("?" for _ in wl)
            # Match exact domain or subdomain (e.g. 'detik.com' matches 'www.detik.com')
            conditions: list[str] = []
            for w in wl:
                conditions.append("domain = ?")
                domain_filter_params.append(w)
                conditions.append("domain LIKE ?")
                domain_filter_params.append(f"%.{w}")
            domain_filter_sql = " AND (" + " OR ".join(conditions) + ")"

        async with self._db_lock:
            try:
                await self._db.execute("BEGIN IMMEDIATE")
                query = f"""
                    SELECT id, url, domain, attempts, metadata_json
                    FROM url_jobs
                    WHERE status='pending'
                      AND (next_retry_at IS NULL OR next_retry_at <= ?)
                      {domain_filter_sql}
                    ORDER BY priority ASC, created_at
                    LIMIT 1
                """
                cur = await self._db.execute(
                    query,
                    (now, *domain_filter_params),
                )
                row = await cur.fetchone()
                await cur.close()
                if row is None:
                    await self._db.execute("COMMIT")
                    return None

                await self._db.execute(
                    "UPDATE url_jobs SET status='processing', updated_at=? WHERE id=?",
                    (now, row["id"]),
                )
                await self._db.execute("COMMIT")
                return row
            except Exception:
                try:
                    await self._db.execute("ROLLBACK")
                except Exception:
                    pass
                return None

    async def _mark_job_completed(
        self,
        job_id: int,
        *,
        title: str,
        content_markdown: str,
        metadata_update: dict[str, Any] | None = None,
    ) -> None:
        if self._db is None:
            return
        now = self._utcnow_iso()

        async with self._db_lock:
            cur = await self._db.execute(
                "SELECT metadata_json FROM url_jobs WHERE id=?",
                (job_id,),
            )
            row = await cur.fetchone()
            await cur.close()

            meta: dict[str, Any] = {}
            if row and row[0]:
                try:
                    meta = json.loads(row[0]) or {}
                except Exception:
                    meta = {}
            if metadata_update:
                try:
                    meta.update(metadata_update)
                except Exception:
                    pass

            meta_json = json.dumps(meta, ensure_ascii=False)
            await self._db.execute(
                """
                UPDATE url_jobs
                SET status='completed', title=?, content_markdown=?, metadata_json=?, updated_at=?, last_error=NULL
                WHERE id=?
                """,
                (title, content_markdown, meta_json, now, job_id),
            )
            await self._db.commit()

    async def _mark_job_ignored(self, job_id: int, *, reason: str) -> None:
        if self._db is None:
            return
        now = self._utcnow_iso()
        async with self._db_lock:
            await self._db.execute(
                "UPDATE url_jobs SET status='ignored', last_error=?, updated_at=? WHERE id=?",
                ((reason or "")[:500], now, job_id),
            )
            await self._db.commit()

    async def _mark_job_failed(self, job_id: int, *, error: str) -> None:
        """Mark failed; retry with exponential backoff up to RETRY_MAX_ATTEMPTS."""
        if self._db is None:
            return

        now = self._utcnow_iso()
        max_attempts = int(getattr(self.settings, "RETRY_MAX_ATTEMPTS", 3) or 3)
        base = float(getattr(self.settings, "RETRY_BASE_SECONDS", 10.0) or 10.0)

        # Load current attempts
        async with self._db_lock:
            cur = await self._db.execute(
                "SELECT attempts FROM url_jobs WHERE id=?",
                (job_id,),
            )
            row = await cur.fetchone()
            await cur.close()
            attempts = int(row[0]) if row and row[0] is not None else 0
            attempts += 1

            if attempts >= max_attempts:
                await self._db.execute(
                    """
                    UPDATE url_jobs
                    SET status='failed', attempts=?, last_error=?, next_retry_at=NULL, updated_at=?
                    WHERE id=?
                    """,
                    (attempts, (error or "")[:500], now, job_id),
                )
                await self._db.commit()
                return

            delay = base * (2 ** max(0, attempts - 1))
            delay = delay + random.uniform(0.0, min(5.0, delay * 0.25))
            retry_at = datetime.now(timezone.utc).timestamp() + delay
            # store as ISO for simplicity
            retry_iso = datetime.fromtimestamp(retry_at, tz=timezone.utc).isoformat()

            await self._db.execute(
                """
                UPDATE url_jobs
                SET status='pending', attempts=?, last_error=?, next_retry_at=?, updated_at=?
                WHERE id=?
                """,
                (attempts, (error or "")[:500], retry_iso, now, job_id),
            )
            await self._db.commit()

    async def get_job_counts(self) -> dict[str, int]:
        """Return counts of url_jobs by status.

        Includes a derived `pending_ready` count (pending rows whose `next_retry_at`
        is NULL or already due).
        """
        if self._db is None:
            await self._open_dedupe_db()
        if self._db is None:
            return {}

        now = self._utcnow_iso()
        counts: dict[str, int] = {}
        async with self._db_lock:
            cur = await self._db.execute(
                "SELECT status, COUNT(*) AS c FROM url_jobs GROUP BY status"
            )
            rows = await cur.fetchall()
            await cur.close()
            for r in rows:
                try:
                    k = str(r["status"])
                    counts[k] = int(r["c"])  # type: ignore[call-arg]
                except Exception:
                    continue

            cur2 = await self._db.execute(
                """
                SELECT COUNT(*) AS c
                FROM url_jobs
                WHERE status='pending'
                  AND (next_retry_at IS NULL OR next_retry_at <= ?)
                """,
                (now,),
            )
            row2 = await cur2.fetchone()
            await cur2.close()

        total = sum(counts.values())
        counts["total"] = total
        counts["pending_ready"] = int(row2[0] if row2 else 0)
        return counts

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

    async def _get_persisted_selector(self, domain: str) -> str | None:
        if self._db is None:
            return None
        async with self._db_lock:
            cur = await self._db.execute(
                "SELECT selector FROM domain_selectors WHERE domain = ?",
                (domain,),
            )
            row = await cur.fetchone()
            await cur.close()
            if row:
                return row[0]
            return None

    async def _save_persisted_selector(self, domain: str, selector: str, quality: float | None = None) -> None:
        if self._db is None:
            return
        ts = int(time.time())
        async with self._db_lock:
            await self._db.execute(
                "INSERT OR REPLACE INTO domain_selectors(domain, selector, last_seen, quality) VALUES(?,?,?,?)",
                (domain, selector, ts, quality if quality is not None else 0.0),
            )
            await self._db.commit()

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

    async def _extract_main_content_css(self, html: str, url: str | None = None) -> tuple[str, str | None]:
        """Best-effort main-article extraction using CSS selectors.

        Returns (plain_text, selector_used). selector_used is None if no selector matched.
        """
        if not html or BeautifulSoup is None:
            return "", None

        # Prefer lxml if available; fallback to html.parser
        try:
            soup = BeautifulSoup(html, "lxml")
        except Exception:
            soup = BeautifulSoup(html, "html.parser")

        def _drop_boilerplate(root) -> None:
            try:
                for tag in root.find_all(
                    [
                        "script",
                        "style",
                        "nav",
                        "footer",
                        "header",
                        "aside",
                        "noscript",
                        "form",
                    ]
                ):
                    try:
                        tag.decompose()
                    except Exception:
                        pass
            except Exception:
                pass

        _drop_boilerplate(soup)

        # Try domain-specific persisted selector first
        if url and self._db is not None:
            try:
                domain = urlparse(url).netloc.lower()
            except Exception:
                domain = ""

            if domain:
                try:
                    persisted = await self._get_persisted_selector(domain)
                except Exception:
                    persisted = None

                if persisted:
                    try:
                        el = soup.select_one(persisted)
                    except Exception:
                        el = None
                    if el is not None:
                        _drop_boilerplate(el)
                        txt = el.get_text("\n", strip=True)
                        if txt and len(txt.split()) >= 40:
                            return txt.strip(), persisted

        selectors = [
            "article",
            "main article",
            "main",
            "[role='main']",
            ".read__content",
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
        used: str | None = None
        for sel in selectors:
            try:
                el = soup.select_one(sel)
            except Exception:
                el = None

            if el is None:
                continue

            _drop_boilerplate(el)
            txt = el.get_text("\n", strip=True)
            if txt and len(txt.split()) >= 40:
                root = el
                used = sel
                break

        if root is None:
            # Fallback: choose the largest block by word count (bounded for perf)
            best_el = None
            best_wc = 0
            scanned = 0
            try:
                for el in soup.find_all(["article", "main", "div", "section"]):
                    scanned += 1
                    if scanned > 800:
                        break
                    try:
                        t = el.get_text(" ", strip=True)
                    except Exception:
                        continue
                    wc = len(t.split())
                    if wc > best_wc:
                        best_wc = wc
                        best_el = el
            except Exception:
                best_el = None

            if best_el is None or best_wc < 80:
                return "", None

            root = best_el
            used = "largest_block"
            _drop_boilerplate(root)

        # Prefer <p> blocks; otherwise grab text lines
        paras: list[str] = []
        try:
            for p in root.find_all("p"):
                txt = p.get_text(" ", strip=True)
                if txt:
                    paras.append(txt)
        except Exception:
            paras = []

        if not paras:
            try:
                txt = root.get_text("\n", strip=True)
            except Exception:
                txt = ""
            paras = [x.strip() for x in txt.splitlines() if x.strip()]

        text = "\n\n".join(paras).strip()
        if not text:
            return "", None
        return text, used

    def _select_main_paragraphs(self, content: str) -> str:
        """Select the most useful paragraphs from extracted markdown.

        This is a lightweight post-processing step to drop very short UI crumbs
        and duplicated blocks that sometimes slip through extraction.
        """
        if not content:
            return ""

        text = content.replace("\r\n", "\n").replace("\r", "\n").strip()
        if not text:
            return ""

        # Prefer paragraph split; fallback to per-line
        paras = [p.strip() for p in re.split(r"\n\s*\n+", text) if p.strip()]
        if not paras:
            paras = [p.strip() for p in text.split("\n") if p.strip()]

        kept: list[str] = []
        seen: set[str] = set()

        for p in paras:
            p = p.strip()
            if not p:
                continue

            key = re.sub(r"\W+", "", p.lower())
            if not key or key in seen:
                continue
            seen.add(key)

            # Keep headings
            if p.startswith("#"):
                kept.append(p)
                continue

            words = p.split()
            # Drop very short fragments (often leftover nav crumbs)
            if len(words) < 6 and len(p) < 80:
                continue

            kept.append(p)

        if not kept:
            out = "\n\n".join(paras).strip()
            return out + ("\n" if out else "")

        out = "\n\n".join(kept).strip()
        return out + ("\n" if out else "")

    def _extract_from_script_vars(self, html: str, url: str) -> tuple[str | None, str | None]:
        """Try to extract article HTML/text from known JS variables in <script> tags.

        Returns (extracted_text, source_tag) or (None, None).
        """
        if not html:
            return None, None

        if BeautifulSoup is not None:
            try:
                soup = BeautifulSoup(html, "lxml")
            except Exception:
                soup = BeautifulSoup(html, "html.parser")
        else:
            soup = None

        candidates = []

        # If we have soup, iterate script tags; otherwise fallback to raw regex search
        scripts = []
        if soup is not None:
            for s in soup.find_all("script"):
                txt = s.string or ""
                if txt and "keywordBrandSafety" in txt:
                    scripts.append(txt)
        else:
            # simple raw search
            for m in re.finditer(r"<script[^>]*>(.*?)</script>", html, re.S | re.I):
                txt = m.group(1)
                if "keywordBrandSafety" in txt:
                    scripts.append(txt)

        for script_text in scripts:
            # Try to find JS var assignment like: var keywordBrandSafety = ...;
            m = re.search(r"var\s+keywordBrandSafety\s*=\s*(\{.*?\}|\[.*?\]|\"(?:\\.|[^\"])*\"|'(?:\\.|[^'])*')\s*;",
                          script_text,
                          re.S)
            if not m:
                continue
            raw = m.group(1).strip()
            content_html = None

            # String literal
            try:
                if raw and raw[0] in ('\"', "'"):
                    # Unescape JS-like string using ast.literal_eval on a Python literal
                    content_html = ast.literal_eval(raw)
                else:
                    # Try JSON parsing
                    try:
                        obj = json.loads(raw)
                    except Exception:
                        obj = None
                    if isinstance(obj, dict):
                        for k in ("content", "html", "body", "article", "text"):
                            if k in obj and isinstance(obj[k], str) and obj[k].strip():
                                content_html = obj[k]
                                break
                    elif isinstance(obj, str):
                        content_html = obj
            except Exception:
                content_html = None

            if not content_html:
                # Try to unescape common escapes
                try:
                    stripped = raw.strip()
                    if stripped.startswith('"') and stripped.endswith('"'):
                        stripped = stripped[1:-1]
                    content_html = _html.unescape(stripped)
                except Exception:
                    content_html = None

            if not content_html:
                continue

            # Try Trafilatura extraction if available
            extracted_text = None
            if trafilatura_extract is not None:
                try:
                    extracted_text = trafilatura_extract(content_html, url=url)
                except Exception:
                    try:
                        extracted_text = trafilatura_extract(content_html)
                    except Exception:
                        extracted_text = None

            if not extracted_text and BeautifulSoup is not None:
                try:
                    sub = BeautifulSoup(content_html, "lxml").get_text("\n", strip=True)
                    extracted_text = sub
                except Exception:
                    extracted_text = None

            if extracted_text and isinstance(extracted_text, str) and extracted_text.strip():
                return extracted_text.strip(), "script:keywordBrandSafety"

        return None, None

    async def _render_with_playwright(self, url: str) -> str | None:
        """Render the URL using Playwright (async) and return the rendered HTML.

        This is optional and only attempted if Playwright is installed. Any
        exceptions are caught and return None so the crawler can continue.
        """
        try:
            from playwright.async_api import async_playwright
        except Exception:
            return None

        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=bool(self.settings.HEADLESS))
                page = await browser.new_page()
                try:
                    await page.goto(url, timeout=max(1000, int(self.settings.PAGE_TIMEOUT)))
                    # Wait for network idle to let client-rendered content settle
                    try:
                        await page.wait_for_load_state("networkidle", timeout=5000)
                    except Exception:
                        pass
                    content = await page.content()
                finally:
                    try:
                        await page.close()
                    except Exception:
                        pass
                try:
                    await browser.close()
                except Exception:
                    pass
                return content
        except Exception:
            return None

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
    # Custom domain extraction methods (zara_adjust.md)
    # ------------------------------------------------------------------

    def _extract_detik(self, soup, strip_selectors: list[str]) -> str:
        """Custom extraction for detik.com/edu articles.

        Strategy (from zara_adjust.md):
        1. Locate div.detail__body-text.itp_bodycontent
        2. Strip noise selectors (.clearfix, table.linksisip, etc.)
        3. Remove dateline <strong> from first paragraph
        4. Extract direct child <p>, <ul>/<ol> > <li> only
        """
        if soup is None or BeautifulSoup is None:
            return ""

        wrapper = soup.select_one("div.detail__body-text.itp_bodycontent")
        if not wrapper:
            # Fallback: try broader selector
            wrapper = soup.select_one("div.detail__body-text")
        if not wrapper:
            return ""

        # Pre-clean: strip noise selectors from manifest + detik-specific
        detik_noise = [
            ".clearfix",
            "table.linksisip",
            "div.sisip_artikel",
            "div.video-20detik",
            "div.embed-video",
        ]
        for sel in list(strip_selectors) + detik_noise:
            if not sel:
                continue
            try:
                if sel.isidentifier():
                    for tag in wrapper.find_all(sel):
                        try:
                            tag.decompose()
                        except Exception:
                            pass
                else:
                    for el in wrapper.select(sel):
                        try:
                            el.decompose()
                        except Exception:
                            pass
            except Exception:
                pass

        # Remove dateline <strong> from first paragraph
        try:
            first_p = wrapper.select_one("p:first-of-type")
            if first_p:
                strong = first_p.select_one("strong:first-child")
                if strong:
                    strong.decompose()
                # Trim leftover leading hyphen/dash
                if first_p.string:
                    first_p.string = first_p.string.lstrip(" -\u2014\u2013")
        except Exception:
            pass

        clean_content: list[str] = []

        # Extract direct child <p> tags (using > combinator logic)
        try:
            for p in wrapper.find_all("p", recursive=False):
                text = p.get_text(" ", strip=True)
                if not text:
                    continue
                # Skip "Baca juga" / "Baca Juga" lines
                if "Baca Juga" in text or "Baca juga" in text:
                    continue
                clean_content.append(text)
        except Exception:
            pass

        # Extract list items
        try:
            for ul_or_ol in wrapper.find_all(["ul", "ol"], recursive=False):
                for li in ul_or_ol.find_all("li"):
                    text = li.get_text(" ", strip=True)
                    if text:
                        clean_content.append(f"- {text}")
        except Exception:
            pass

        return "\n\n".join(clean_content).strip()

    def _extract_ruangguru(self, soup, strip_selectors: list[str]) -> str:
        """Custom extraction for ruangguru.com/blog articles.

        Strategy (from zara_adjust.md):
        1. Locate div.content-body
        2. Pre-clean: destroy images, image captions, "Baca Juga" blocks
        3. Iterate children sequentially (h2, h3, p, ol, ul)
        4. Filter visual dividers, empty spacers, promotional intros
        """
        if soup is None or BeautifulSoup is None:
            return ""

        wrapper = soup.select_one("div.content-body")
        if not wrapper:
            return ""

        # Pre-clean: strip manifest noise selectors
        for sel in strip_selectors:
            if not sel:
                continue
            try:
                if sel.isidentifier():
                    for tag in wrapper.find_all(sel):
                        try:
                            tag.decompose()
                        except Exception:
                            pass
                else:
                    for el in wrapper.select(sel):
                        try:
                            el.decompose()
                        except Exception:
                            pass
            except Exception:
                pass

        # Pre-clean: destroy all images (prevents alt text/URL bleeding)
        try:
            for img in wrapper.find_all("img"):
                img.decompose()
        except Exception:
            pass

        clean_content: list[str] = []

        # Iterate through children sequentially
        try:
            for element in wrapper.find_all(["h2", "h3", "p", "ol", "ul"], recursive=False):
                text = element.get_text(strip=True)

                # Skip empty tags, visual spacers (\xa0), em-dashes
                if not text or text == "\u2014" or text == "&#8212;" or text == "\xa0":
                    continue

                # Filter "Baca Juga", image captions, promotional intros
                if "Baca Juga:" in text or "(Sumber:" in text or "Yuk simak" in text:
                    continue

                # Format by element type
                if element.name == "h2":
                    clean_content.append(f"## {text}")
                elif element.name == "h3":
                    clean_content.append(f"### {text}")
                elif element.name == "ol":
                    for i, li in enumerate(element.find_all("li"), start=1):
                        li_text = li.get_text(strip=True)
                        if li_text:
                            clean_content.append(f"{i}. {li_text}")
                elif element.name == "ul":
                    for li in element.find_all("li"):
                        li_text = li.get_text(strip=True)
                        if li_text:
                            clean_content.append(f"- {li_text}")
                elif element.name == "p":
                    clean_content.append(text)
        except Exception:
            pass

        return "\n\n".join(clean_content).strip()

    def _extract_liputan6(self, soup, strip_selectors: list[str]) -> str:
        """Custom extraction for liputan6.com articles.

        Strategy (from zara_adjust.md):
        1. Locate div.article-content-body__item-content (or fallback)
        2. Pre-clean: destroy ads, "BACA JUGA" blocks, tag snippets
        3. Extract remaining <p> tags, filter "Advertisement" text
        """
        if soup is None or BeautifulSoup is None:
            return ""

        wrapper = soup.select_one("div.article-content-body__item-content")
        if not wrapper:
            wrapper = soup.select_one("div.article-content-body")
        if not wrapper:
            return ""

        # Pre-clean: strip manifest noise + liputan6-specific selectors
        liputan6_noise = [
            "div.baca-juga-collections",
            "div.advertisement-placeholder",
            "div.article-ad",
            "[id*='gpt-ad']",
            "[id*='revive-ad']",
            "div.tags--snippet",
            "div#preco",
        ]
        for sel in list(strip_selectors) + liputan6_noise:
            if not sel:
                continue
            try:
                if sel.isidentifier():
                    for tag in wrapper.find_all(sel):
                        try:
                            tag.decompose()
                        except Exception:
                            pass
                else:
                    for el in wrapper.select(sel):
                        try:
                            el.decompose()
                        except Exception:
                            pass
            except Exception:
                pass

        clean_content: list[str] = []

        # Extract remaining <p> tags
        try:
            for p in wrapper.find_all("p"):
                text = p.get_text(" ", strip=True)
                if not text:
                    continue
                # Double-check: skip stray "Advertisement" text
                if text.lower() == "advertisement":
                    continue
                # Skip "Baca Juga" that might survive DOM cleanup
                if "BACA JUGA:" in text or "Baca Juga:" in text:
                    continue
                clean_content.append(text)
        except Exception:
            pass

        return "\n\n".join(clean_content).strip()

    def _extract_republika(self, soup, strip_selectors: list[str]) -> str:
        """Custom extraction for republika.co.id articles.

        Strategy (from zara_adjust_1.md):
        1. Locate div.article-content
        2. Pre-clean: destroy scripts, ad wrappers, internal links
        3. Dateline Filter: Strip "REPUBLIKA.CO.ID, [CITY] - " using Regex
        4. Extract <p> tags
        """
        if soup is None or BeautifulSoup is None:
            return ""

        wrapper = soup.select_one("div.article-content")
        if not wrapper:
            return ""

        # Pre-clean: strip manifest noise + republika-specific
        republika_noise = ["script", "[id*='div-gpt-ad']", "div.baca-juga", "div.terkait"]
        for sel in list(strip_selectors) + republika_noise:
            if not sel:
                continue
            try:
                for el in wrapper.select(sel):
                    el.decompose()
            except Exception:
                pass

        clean_content: list[str] = []

        # Iterate & Extract
        try:
            for p in wrapper.find_all("p"):
                text = p.get_text(separator=" ", strip=True)
                if not text:
                    continue

                # Clean Dateline (Regex to catch "REPUBLIKA.CO.ID, [CITY] - ")
                if "REPUBLIKA.CO.ID" in text:
                    text = re.sub(
                        r"^REPUBLIKA\.CO\.ID,\s+[A-Z\s]+[-–—]+\s*",
                        "",
                        text,
                        flags=re.IGNORECASE,
                    )

                if text:
                    clean_content.append(text)
        except Exception:
            pass

        return "\n\n".join(clean_content).strip()

    def _extract_quipper(self, soup, strip_selectors: list[str]) -> str:
        """Custom extraction for quipper.com/id/blog articles.

        Strategy (from zara_adjust_1.md):
        1. Locate div#penci-post-entry-inner
        2. Pre-clean: destroy ToC, tags, pagination, image wrappers
        3. Format: h2, h3, p, ol, ul with Markdown prefixing
        """
        if soup is None or BeautifulSoup is None:
            return ""

        wrapper = soup.select_one("div#penci-post-entry-inner")
        if not wrapper:
            return ""

        # Pre-clean
        quipper_noise = [
            "div.lwptoc",
            "div.post-tags",
            "div.penci-single-link-pages",
            "i.penci-post-countview-number-check",
            "figure.wp-block-image",
            "div.wp-block-image",
            "hr.wp-block-separator",
            "img",
        ]
        for sel in list(strip_selectors) + quipper_noise:
            if not sel:
                continue
            try:
                for el in wrapper.select(sel):
                    el.decompose()
            except Exception:
                pass

        clean_content: list[str] = []

        # Iterate & Format
        try:
            for element in wrapper.find_all(["h2", "h3", "p", "ol", "ul"]):
                text = element.get_text(separator=" ", strip=True)
                if not text:
                    continue

                if element.name == "h2":
                    clean_content.append(f"## {text}")
                elif element.name == "h3":
                    clean_content.append(f"### {text}")
                elif element.name == "ol":
                    for i, li in enumerate(element.find_all("li"), start=1):
                        li_text = li.get_text(strip=True)
                        if li_text:
                            clean_content.append(f"{i}. {li_text}")
                elif element.name == "ul":
                    for li in element.find_all("li"):
                        li_text = li.get_text(strip=True)
                        if li_text:
                            clean_content.append(f"- {li_text}")
                elif element.name == "p":
                    clean_content.append(text)
        except Exception:
            pass

        return "\n\n".join(clean_content).strip()

    def _extract_zenius(self, soup, strip_selectors: list[str]) -> str:
        """Custom extraction for zenius.net/blog articles.

        Strategy (from zara_adjust_1.md):
        1. Parse JSON-LD for metadata (headline, description, etc.)
        2. Locate section.gh-content
        3. Pre-clean: destroy UI cards, comments, scripts
        4. Math Handling: Convert img with alt to [Formula: ...]
        5. Semantic formatting (h2, h3, blockquote, lists, etc.)
        """
        if soup is None or BeautifulSoup is None:
            return ""

        # Phase 1: Metadata extraction from JSON-LD (handled by caller if needed,
        # but we can try to extract headline as title fallback here)
        # Note: the engine currently handles title via separate selector,
        # but we can improve it here.

        wrapper = soup.select_one("section.gh-content")
        if not wrapper:
            return ""

        # Pre-clean
        zenius_noise = [
            "div.kg-button-card",
            "figure.kg-image-card",
            "div.gh-comments",
            "script",
            "hr",
            "div.ez-toc-container",
            "div#toc_container",
        ]
        for sel in list(strip_selectors) + zenius_noise:
            if not sel:
                continue
            try:
                for el in wrapper.select(sel):
                    el.decompose()
            except Exception:
                pass

        clean_content: list[str] = []

        # Iterate & Format
        try:
            for element in wrapper.find_all(
                ["h2", "h3", "p", "ol", "ul", "blockquote", "div"]
            ):
                # Math Image Handling
                for img in element.find_all("img"):
                    alt_text = img.get("alt", "")
                    if alt_text:
                        img.replace_with(f" [Formula: {alt_text}] ")
                    else:
                        img.decompose()

                # Ghost UI Handling (kg-callout-card)
                if element.name == "div":
                    if "kg-callout-card" in element.get("class", []):
                        text = element.get_text(separator="\n", strip=True)
                    else:
                        continue
                else:
                    text = element.get_text(separator=" ", strip=True)

                if not text:
                    continue

                lower_text = text.lower()
                if (
                    "baca juga:" in lower_text
                    or "🔗" in text
                    or "download aplikasi zenius" in lower_text
                ):
                    continue

                # Markdown Formatting
                if element.name == "h2":
                    clean_content.append(f"## {text}")
                elif element.name == "h3":
                    clean_content.append(f"### {text}")
                elif element.name == "blockquote":
                    clean_content.append(f"> {text}")
                elif element.name == "ol":
                    for i, li in enumerate(element.find_all("li"), start=1):
                        li_text = li.get_text(strip=True)
                        if li_text:
                            clean_content.append(f"{i}. {li_text}")
                elif element.name == "ul":
                    for li in element.find_all("li"):
                        li_text = li.get_text(strip=True)
                        if li_text:
                            clean_content.append(f"- {li_text}")
                elif element.name in ["p", "div"]:
                    clean_content.append(text)
        except Exception:
            pass

        return "\n\n".join(clean_content).strip()

    # ------------------------------------------------------------------
    # Browser & run config builders
    # ------------------------------------------------------------------

    # Note: we no longer use Crawl4AI/Playwright. HTTP fetching is performed
    # with aiohttp in `_crawl_worker` and discovery uses `utils.discovery`.

    # ------------------------------------------------------------------
    # URL management
    # ------------------------------------------------------------------

    async def _try_enqueue(
        self,
        url: str,
        *,
        source: str = "discovery",
        parent_url: str | None = None,
    ) -> bool:
        """State entry (PRD v2.0): insert discovered URL into SQLite as pending."""
        if self._db is None:
            await self._open_dedupe_db()
        return await self._job_insert_pending(url, source=source, parent_url=parent_url)

    # ------------------------------------------------------------------
    # Discovery worker
    # ------------------------------------------------------------------

    async def _sitemap_watch_worker(self) -> None:
        """Background sitemap rescanner (PRD v2.0 Watch Mode)."""
        if not bool(getattr(self.settings, "DISCOVERY_ENABLE_SITEMAP", True)):
            return

        logger.info("🗺 Sitemap watch worker started")
        await self._notify("🗺 Sitemap watch: mulai scan sitemap...")

        # Determine which domains to scan
        wl = getattr(self.settings, "DOMAIN_WHITELIST", None) or []
        target_domains: list[str] = [d.strip().lower() for d in wl if (d or "").strip()]
        if not target_domains:
            target_domains = list(self._sites.keys())

        if not target_domains:
            logger.info("🗺 No sitemap targets configured (sites.yaml empty + no whitelist)")
            return

        interval_hours = float(getattr(self.settings, "SITEMAP_RESCAN_HOURS", 6.0) or 6.0)
        interval_s = max(60.0, interval_hours * 3600.0)

        timeout = aiohttp.ClientTimeout(total=30)
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
            "Accept-Language": "id-ID,id;q=0.9,en;q=0.8",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }

        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            cycle = 0
            while not self.stop_event.is_set():
                cycle += 1
                total_new = 0

                for domain in target_domains:
                    if self.stop_event.is_set():
                        break

                    m = match_site_config(domain, self._sites)
                    site_cfg = m[1] if m else self._sites.get(domain)
                    allow = get_allow_list(site_cfg)
                    seeds = get_sitemaps(site_cfg)
                    if not seeds:
                        # Fallback standard seed
                        seeds = [f"https://{domain}/sitemap.xml"]

                    try:
                        res = await crawl_sitemap_recursive(
                            session,
                            seeds,
                            allow_substrings=allow or None,
                            max_sitemaps=int(getattr(self.settings, "SITEMAP_MAX_SITEMAPS", 200) or 200),
                            max_pages=int(getattr(self.settings, "SITEMAP_MAX_PAGES_PER_CYCLE", 5000) or 5000),
                        )
                    except Exception:
                        continue

                    added = 0
                    for u in res.pages:
                        if await self._try_enqueue(
                            u,
                            source="sitemap",
                            parent_url=(seeds[0] if seeds else None),
                        ):
                            added += 1
                    total_new += added
                    if added > 0:
                        logger.info("🗺 Sitemap %s: +%d URL (seen %d sitemaps)", domain, added, res.sitemaps_seen)

                if total_new > 0:
                    await self._notify(f"🗺 Sitemap cycle {cycle}: +{total_new} URL baru")

                # Sleep until next scan
                await asyncio.sleep(interval_s)

    async def _discovery_worker(self) -> None:
        """Search engine discovery loop."""
        if not bool(getattr(self.settings, "DISCOVERY_ENABLE_SEARCH", True)):
            return
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

                found_urls = await self.discovery.search_one(search_url, engine)
                await self.stats.incr("search_queries_done")

                added = 0
                for url in found_urls:
                    if await self._try_enqueue(url, source=f"search:{engine}", parent_url=search_url):
                        added += 1

                if added > 0:
                    counts = await self.get_job_counts()
                    logger.info(
                        "🆕 +%d URL baru (pending_ready=%d | pending=%d | total=%d)",
                        added,
                        counts.get("pending_ready", 0),
                        counts.get("pending", 0),
                        counts.get("total", 0),
                    )

                # Rate limiting untuk search engine
                await asyncio.sleep(delay)

            # Periodic status update
            if cycle % 5 == 0:
                counts = await self.get_job_counts()
                await self._notify(
                    f"📊 Discovery update:\n{self.stats.summary()}\n"
                    f"🗃 Jobs: pending_ready={counts.get('pending_ready', 0)} | pending={counts.get('pending', 0)} | "
                    f"processing={counts.get('processing', 0)} | completed={counts.get('completed', 0)} | "
                    f"failed={counts.get('failed', 0)} | ignored={counts.get('ignored', 0)}"
                )

        logger.info("🛑 Discovery worker stopped")

    # ------------------------------------------------------------------
    # Crawl worker
    # ------------------------------------------------------------------

    async def _crawl_worker(self, worker_id: int) -> None:
        """Crawl worker (PRD v2.0): claim pending jobs from SQLite, crawl, extract, persist."""
        logger.info("🕷 Crawl worker %d started", worker_id)
        consecutive_errors = 0

        # Create a persistent session for this worker
        timeout = aiohttp.ClientTimeout(total=max(10, int(self.settings.PAGE_TIMEOUT / 1000)))
        default_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
            "Accept-Language": "id-ID,id;q=0.9,en;q=0.8",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        session = aiohttp.ClientSession(timeout=timeout, headers=default_headers)

        try:
            while not self.stop_event.is_set():
                job = await self._claim_next_job()
                if job is None:
                    await asyncio.sleep(1.0)
                    continue

                job_id = int(job["id"])
                url = str(job["url"])
                try:
                    save_domain = (str(job["domain"] or "") or urlparse(url).netloc).lower()
                except Exception:
                    save_domain = "unknown"

                await self.stats.incr("urls_crawled")

                # Per-domain cap BEFORE crawling — avoid wasting resources
                async with self._domain_lock:
                    if self.domain_counter[save_domain] >= self.MAX_PER_DOMAIN:
                        await self.stats.incr("urls_skipped")
                        await self._mark_job_ignored(job_id, reason=f"domain_cap:{save_domain}")
                        continue

                job_finished = False

                try:
                    # Crawl the page using aiohttp
                    try:
                        async with session.get(url) as resp:
                            if resp.status >= 400:
                                await self.stats.incr("urls_failed")
                                await self._mark_job_failed(job_id, error=f"HTTP {resp.status}")
                                job_finished = True
                                continue
                            html = await resp.text(errors="ignore")
                    except Exception as exc:
                        await self.stats.incr("urls_failed")
                        consecutive_errors += 1
                        await self._mark_job_failed(job_id, error=f"fetch:{str(exc)[:200]}")
                        job_finished = True
                        if consecutive_errors >= 10:
                            logger.warning(
                                "⚠ Worker %d: %d consecutive errors, sleeping 60s...",
                                worker_id, consecutive_errors,
                            )
                            await asyncio.sleep(60)
                            consecutive_errors = 0
                        continue

                    consecutive_errors = 0

                    # Politeness jitter after each request
                    try:
                        dmin = float(getattr(self.settings, "REQUEST_DELAY_MIN", 2.0) or 2.0)
                        dmax = float(getattr(self.settings, "REQUEST_DELAY_MAX", 5.0) or 5.0)
                        if dmax < dmin:
                            dmin, dmax = dmax, dmin
                        await asyncio.sleep(max(0.0, random.uniform(dmin, dmax)))
                    except Exception:
                        pass

                    # Spidering: extract links from HTML to keep discovery alive
                    if html:
                        try:
                            links = extract_links_from_page(html, url)
                            added = 0
                            max_spider_links = 60

                            focused = bool(getattr(self.settings, "DOMAIN_WHITELIST", None))
                            site_match = self._match_site(url)
                            site_cfg = site_match[1] if site_match else None
                            allow_list = get_allow_list(site_cfg)

                            for link in links:
                                if added >= max_spider_links:
                                    break

                                if focused:
                                    try:
                                        path = (urlparse(link).path or "").lower()
                                    except Exception:
                                        path = ""

                                    if allow_list and not any(a in link for a in allow_list):
                                        continue

                                    if ("/read/" in path) or is_indonesian_education_url(link):
                                        if await self._try_enqueue(link, source="spider", parent_url=url):
                                            added += 1
                                else:
                                    if is_indonesian_education_url(link):
                                        if await self._try_enqueue(link, source="spider", parent_url=url):
                                            added += 1

                            if added > 0:
                                await self.stats.incr("links_extracted", added)
                        except Exception:
                            pass

                    # --- Strategy Pattern (PRD v2.0): manual -> fallback ---
                    site_match = self._match_site(url)
                    site_cfg = site_match[1] if site_match else None
                    method = get_method(site_cfg)
                    selectors = get_selectors(site_cfg)
                    strip_selectors = get_strip_selectors(site_cfg)

                    title = ""
                    content = ""
                    extraction_quality: float | None = None
                    extraction_quality_source = "none"
                    extraction_source = "none"
                    selector_used = None
                    selector_persisted = False
                    pagination_pages = 1

                    soup = None
                    if BeautifulSoup is not None and html:
                        try:
                            soup = BeautifulSoup(html, "lxml")
                        except Exception:
                            soup = BeautifulSoup(html, "html.parser")

                        # Strip boilerplate based on manifest
                        try:
                            for sel in strip_selectors:
                                if not sel:
                                    continue
                                if sel.isidentifier():
                                    for tag in soup.find_all(sel):
                                        try:
                                            tag.decompose()
                                        except Exception:
                                            pass
                                else:
                                    for el in soup.select(sel):
                                        try:
                                            el.decompose()
                                        except Exception:
                                            pass
                        except Exception:
                            pass

                        # Title selector from manifest
                        try:
                            title_sel = (selectors.get("title") or "").strip()
                            if title_sel:
                                t_el = soup.select_one(title_sel)
                                if t_el:
                                    title = (t_el.get_text(" ", strip=True) or "").strip()
                        except Exception:
                            title = ""

                        if not title:
                            try:
                                t = soup.title.string if soup.title and soup.title.string else ""
                                title = (t or "").strip()
                            except Exception:
                                title = ""

                    # Manual CSS strategy
                    if method in ("css", "") and soup is not None:
                        body_sel = (selectors.get("body") or "").strip()
                        if body_sel:
                            try:
                                el = soup.select_one(body_sel)
                                if el:
                                    txt = (el.get_text("\n", strip=True) or "").strip()
                                    if txt:
                                        content = txt
                                        extraction_source = f"manual_css:{body_sel}"
                                        selector_used = body_sel
                            except Exception:
                                pass

                            # Pagination (optional)
                            next_sel = (selectors.get("next_page") or "").strip()
                            if content and next_sel:
                                try:
                                    from urllib.parse import urljoin

                                    pages: list[str] = [content]
                                    seen_pages: set[str] = {url}
                                    next_url = None
                                    link_el = soup.select_one(next_sel)
                                    if link_el and link_el.get("href"):
                                        next_url = urljoin(url, str(link_el.get("href")))

                                    max_pages = int(getattr(self.settings, "MAX_PAGINATION_PAGES", 5) or 5)
                                    for _ in range(max(0, max_pages - 1)):
                                        if not next_url or next_url in seen_pages:
                                            break
                                        seen_pages.add(next_url)
                                        try:
                                            async with session.get(next_url) as resp2:
                                                if resp2.status >= 400:
                                                    break
                                                html2 = await resp2.text(errors="ignore")
                                        except Exception:
                                            break

                                        if not html2 or BeautifulSoup is None:
                                            break
                                        try:
                                            soup2 = BeautifulSoup(html2, "lxml")
                                        except Exception:
                                            soup2 = BeautifulSoup(html2, "html.parser")

                                        # strip again
                                        try:
                                            for sel2 in strip_selectors:
                                                if not sel2:
                                                    continue
                                                if sel2.isidentifier():
                                                    for tag in soup2.find_all(sel2):
                                                        try:
                                                            tag.decompose()
                                                        except Exception:
                                                            pass
                                                else:
                                                    for el2 in soup2.select(sel2):
                                                        try:
                                                            el2.decompose()
                                                        except Exception:
                                                            pass
                                        except Exception:
                                            pass

                                        try:
                                            el2 = soup2.select_one(body_sel)
                                            txt2 = (el2.get_text("\n", strip=True) if el2 else "")
                                            txt2 = (txt2 or "").strip()
                                        except Exception:
                                            txt2 = ""

                                        if txt2:
                                            pages.append(txt2)

                                        # find next
                                        try:
                                            link_el2 = soup2.select_one(next_sel)
                                            if link_el2 and link_el2.get("href"):
                                                next_url = urljoin(next_url, str(link_el2.get("href")))
                                            else:
                                                next_url = None
                                        except Exception:
                                            next_url = None

                                    if len(pages) > 1:
                                        content = "\n\n".join(pages)
                                        pagination_pages = len(pages)
                                        extraction_source = f"{extraction_source}+pagination:{pagination_pages}"
                                except Exception:
                                    pass

                    # Custom domain extraction methods (zara_adjust.md)
                    # These elif branches ONLY fire for new domain methods;
                    # the existing css/"" path above is untouched.
                    if not content and method == "custom_detik" and soup is not None:
                        try:
                            content = self._extract_detik(soup, strip_selectors)
                            if content:
                                extraction_source = "custom_detik"
                                selector_used = "custom_detik"
                        except Exception:
                            pass

                    if not content and method == "custom_ruangguru" and soup is not None:
                        try:
                            content = self._extract_ruangguru(soup, strip_selectors)
                            if content:
                                extraction_source = "custom_ruangguru"
                                selector_used = "custom_ruangguru"
                        except Exception:
                            pass

                    if not content and method == "custom_liputan6" and soup is not None:
                        try:
                            content = self._extract_liputan6(soup, strip_selectors)
                            if content:
                                extraction_source = "custom_liputan6"
                                selector_used = "custom_liputan6"
                        except Exception:
                            pass

                    # BATCH 2 Dispatch (zara_adjust_1.md)
                    if not content and method == "custom_republika" and soup is not None:
                        try:
                            content = self._extract_republika(soup, strip_selectors)
                            if content:
                                extraction_source = "custom_republika"
                                selector_used = "custom_republika"
                        except Exception:
                            pass

                    if not content and method == "custom_quipper" and soup is not None:
                        try:
                            content = self._extract_quipper(soup, strip_selectors)
                            if content:
                                extraction_source = "custom_quipper"
                                selector_used = "custom_quipper"
                        except Exception:
                            pass

                    if not content and method == "custom_zenius" and soup is not None:
                        try:
                            # Note: zenius extraction returns a dict with metadata + content
                            # but we currently only store 'content' in the main flow.
                            # We'll extract content for now to maintain compatibility.
                            doc = self._extract_zenius(soup, strip_selectors)
                            if isinstance(doc, dict):
                                content = doc.get("content", "")
                            else:
                                content = doc
                            if content:
                                extraction_source = "custom_zenius"
                                selector_used = "custom_zenius"
                        except Exception:
                            pass

                    # Heuristic CSS selector fallback (persisted selector + generic list)
                    if not content and html:
                        try:
                            css_text, css_selector = await self._extract_main_content_css(html, url)
                        except Exception:
                            css_text, css_selector = "", None
                        if css_text:
                            content = css_text.strip()
                            extraction_source = f"css_selector:{css_selector or 'auto'}"
                            selector_used = css_selector

                            # Persist selector when good
                            try:
                                quality = self._estimate_extraction_quality(content, title=title)
                                if quality >= self.settings.MIN_EXTRACTION_QUALITY and css_selector and url:
                                    domain = urlparse(url).netloc.lower()
                                    await self._save_persisted_selector(domain, css_selector, quality)
                                    selector_persisted = True
                            except Exception:
                                pass

                    # Script-variable extraction fallback (Kompas JS embedding)
                    if not content and html:
                        try:
                            script_text, script_source = self._extract_from_script_vars(html, url)
                            if script_text:
                                content = script_text.strip()
                                extraction_source = script_source
                                selector_used = script_source
                        except Exception:
                            pass

                    # Trafilatura fallback
                    if not content and trafilatura_extract is not None and html:
                        try:
                            extracted = trafilatura_extract(
                                html,
                                url=url,
                                include_comments=False,
                                include_tables=False,
                            )
                        except TypeError:
                            extracted = trafilatura_extract(html, url=url)
                        except Exception:
                            extracted = None

                        if extracted:
                            content = extracted.strip()
                            extraction_source = "trafilatura"

                    if not content:
                        await self.stats.incr("urls_skipped")
                        await self._mark_job_ignored(job_id, reason="no_content")
                        job_finished = True
                        continue

                    if len(content.strip()) < 100:
                        await self.stats.incr("urls_skipped")
                        await self._mark_job_ignored(job_id, reason="too_short_raw")
                        job_finished = True
                        continue

                    # --- CONTENT CLEANING ---
                    content = clean_markdown(content)
                    if len(content.strip()) < 100:
                        await self.stats.incr("urls_skipped")
                        await self._mark_job_ignored(job_id, reason="too_short_clean")
                        job_finished = True
                        continue

                    # --- MAIN PARAGRAPH SELECTION ---
                    content = self._select_main_paragraphs(content)
                    if len(content.strip()) < 250:
                        await self.stats.incr("urls_skipped")
                        await self._mark_job_ignored(job_id, reason="too_short_main")
                        job_finished = True
                        continue

                    # --- EXTRACTION QUALITY GATE ---
                    if extraction_quality is None:
                        extraction_quality = self._estimate_extraction_quality(content, title=title)
                        extraction_quality_source = "heuristic"

                    # Playwright-on-demand fallback (optional)
                    if extraction_quality < self.settings.MIN_EXTRACTION_QUALITY:
                        try:
                            rendered_html = await self._render_with_playwright(url)
                        except Exception:
                            rendered_html = None

                        if rendered_html:
                            try:
                                css_text2, css_selector2 = await self._extract_main_content_css(rendered_html, url)
                            except Exception:
                                css_text2, css_selector2 = "", None

                            if css_text2:
                                content = css_text2.strip()
                                extraction_source = f"playwright:css_selector:{css_selector2 or 'auto'}"
                                selector_used = css_selector2
                                extraction_quality = self._estimate_extraction_quality(content, title=title)
                                extraction_quality_source = "playwright"
                                try:
                                    if extraction_quality >= self.settings.MIN_EXTRACTION_QUALITY and css_selector2 and save_domain:
                                        await self._save_persisted_selector(save_domain, css_selector2, extraction_quality)
                                        selector_persisted = True
                                except Exception:
                                    pass
                            else:
                                try:
                                    script_text2, script_source2 = self._extract_from_script_vars(rendered_html, url)
                                    if script_text2:
                                        content = script_text2.strip()
                                        extraction_source = f"playwright:{script_source2}"
                                        selector_used = script_source2
                                        extraction_quality = self._estimate_extraction_quality(content, title=title)
                                        extraction_quality_source = "playwright"
                                except Exception:
                                    pass

                            if not content and trafilatura_extract is not None:
                                try:
                                    extracted2 = trafilatura_extract(rendered_html, url=url)
                                except TypeError:
                                    extracted2 = trafilatura_extract(rendered_html)
                                except Exception:
                                    extracted2 = None
                                if extracted2:
                                    content = extracted2.strip()
                                    extraction_source = "playwright:trafilatura"
                                    extraction_quality = self._estimate_extraction_quality(content, title=title)
                                    extraction_quality_source = "playwright"

                    if extraction_quality < self.settings.MIN_EXTRACTION_QUALITY:
                        await self.stats.incr("urls_skipped")
                        await self._mark_job_ignored(job_id, reason="low_extraction_quality")
                        job_finished = True
                        continue

                    # --- CONTENT DEDUPE ---
                    if not await self._is_new_content(content):
                        await self.stats.incr("urls_skipped")
                        await self._mark_job_ignored(job_id, reason="duplicate_content")
                        job_finished = True
                        continue

                    # --- ADVERTISEMENT GATE ---
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
                        await self._mark_job_ignored(job_id, reason="advertisement")
                        job_finished = True
                        continue

                    # --- RELEVANCE GATE ---
                    combined_text = f"{title} {content}"

                    is_english_subject = any(x in url.lower() for x in [
                        "bahasa-inggris", "english", "b-inggris",
                    ])
                    if not is_english_subject and not is_indonesian_text(content):
                        await self.stats.incr("urls_skipped")
                        await self._mark_job_ignored(job_id, reason="non_indonesian")
                        job_finished = True
                        continue

                    # Require explicit keyword hits (no fuzzy-only passes).
                    base_min = int(getattr(self.settings, "MIN_RELEVANCE_SCORE", 1) or 1)
                    base_min = max(1, base_min)

                    is_priority_section_url = (
                        url.startswith("https://www.kompas.com/edu/")
                        or url.startswith("https://www.kompas.com/skola/")
                        or url.startswith("https://edukasi.kompas.com/")
                    )
                    # EDU gets a small leniency: if base_min=2, require 1 keyword.
                    min_required = max(1, base_min - 1) if is_priority_section_url else base_min

                    kw_hits = extract_keywords_found(combined_text)
                    exact_score = len(kw_hits)
                    fuzzy_score = 0
                    fuzzy_hits: list[dict] = []

                    if exact_score < min_required:
                        await self.stats.incr("urls_skipped")
                        await self._mark_job_ignored(job_id, reason="low_relevance")
                        job_finished = True
                        continue

                    # Increment domain counter
                    async with self._domain_lock:
                        self.domain_counter[save_domain] += 1

                    # Build & save record (JSONL)
                    record = build_record(url, title, content)
                    level = classify_level(content, url)
                    kw = kw_hits

                    record["metadata"]["level"] = level
                    record["metadata"]["keywords_found"] = kw
                    record["metadata"]["relevance_score"] = exact_score
                    record["metadata"]["fuzzy_science_score"] = fuzzy_score
                    record["metadata"]["fuzzy_science_hits"] = fuzzy_hits
                    record["metadata"]["extraction_quality"] = extraction_quality
                    record["metadata"]["extraction_quality_source"] = extraction_quality_source
                    record["metadata"]["extraction_source"] = extraction_source
                    record["metadata"]["selector_used"] = selector_used
                    record["metadata"]["selector_persisted"] = selector_persisted
                    record["metadata"]["pagination_pages"] = pagination_pages

                    append_jsonl(self.output_file, record)

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

                    token_text = ""
                    if saved_processed_1 and processed_1_record is not None:
                        token_text = processed_1_record.get("text", "") or ""
                    if not token_text:
                        token_text = content

                    tokens = self._count_tokens_qwen(token_text)
                    if tokens > 0:
                        await self.stats.incr("tokens_total", tokens)

                    # Persist state to DB first (prevents stop-after-success racing DB writes)
                    await self._mark_job_completed(
                        job_id,
                        title=title,
                        content_markdown=content,
                        metadata_update={
                            "level": level,
                            "keywords_found": kw,
                            "relevance_score": exact_score,
                            "fuzzy_science_score": fuzzy_score,
                            "extraction_quality": extraction_quality,
                            "extraction_quality_source": extraction_quality_source,
                            "extraction_source": extraction_source,
                            "selector_used": selector_used,
                            "selector_persisted": selector_persisted,
                            "pagination_pages": pagination_pages,
                            "tokens": tokens,
                        },
                    )

                    await self.stats.incr("urls_success")

                    logger.info(
                        "✅ Worker %d: %s [%s] (%d chars)",
                        worker_id, url, level, len(content),
                    )

                    # Notify every N successes
                    if (
                        self.stats.urls_success > 0
                        and self.stats.urls_success % self.settings.NOTIFY_EVERY == 0
                    ):
                        await self._notify(
                            f"📈 Milestone: {self.stats.urls_success} halaman berhasil!\n"
                            f"{self.stats.summary()}"
                        )

                    job_finished = True
                except Exception as exc:
                    if not job_finished:
                        await self.stats.incr("urls_failed")
                        await self._mark_job_failed(job_id, error=f"exception:{str(exc)[:200]}")
        finally:
            try:
                await session.close()
            except Exception:
                pass

        logger.info("🛑 Crawl worker %d stopped", worker_id)

    # ------------------------------------------------------------------
    # Main run
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Run crawler using simple HTTP fetch workers and a discovery loop."""
        logger.info("🚀 CrawlEngine starting — PRD v2.0 (SQLite state machine)")
        self.stats = CrawlStats()
        self.domain_counter.clear()
        self.is_running = True

        await self._open_dedupe_db()

        await self._notify(
            "🚀 *Crawler dimulai — PRD v2.0 (SQLite state machine)*\n"
            f"Workers: {self.settings.MAX_CONCURRENCY}\n"
            f"Discovery: sitemap={getattr(self.settings, 'DISCOVERY_ENABLE_SITEMAP', True)} | "
            f"search={getattr(self.settings, 'DISCOVERY_ENABLE_SEARCH', True)}\n"
            f"Output (data/raw):\n"
            f"  • raw/{self.output_file.name} (raw crawl output)\n"
            f"  • raw/{self.output_processed_1_file.name} (stage-1 processed text)\n"
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
                        if await self._try_enqueue(line, source="seed:urls.txt"):
                            seeds_added += 1
            if seeds_added:
                logger.info("🌱 %d seed URLs loaded from urls.txt", seeds_added)

        # Launch discovery + crawl workers
        tasks: list[asyncio.Task] = []
        producers = 0
        if bool(getattr(self.settings, "DISCOVERY_ENABLE_SITEMAP", True)):
            tasks.append(asyncio.create_task(self._sitemap_watch_worker(), name="sitemap-watch"))
            producers += 1
        if bool(getattr(self.settings, "DISCOVERY_ENABLE_SEARCH", True)):
            tasks.append(asyncio.create_task(self._discovery_worker(), name="discovery"))
            producers += 1
        for i in range(self.settings.MAX_CONCURRENCY):
            tasks.append(asyncio.create_task(self._crawl_worker(i), name=f"crawl-{i}"))

        logger.info(
            "⚡ %d tasks launched (%d producers + %d crawl)",
            len(tasks),
            producers,
            self.settings.MAX_CONCURRENCY,
        )

        try:
            # Wait until stop_event is set
            await self.stop_event.wait()

        except Exception as exc:
            logger.exception("Fatal error pada crawl engine: %s", exc)
        finally:
            # Cancel tasks
            for t in tasks:
                t.cancel()
            try:
                await asyncio.gather(*tasks, return_exceptions=True)
            except Exception:
                pass
            self.is_running = False

        # Final report
        counts = await self.get_job_counts()
        report = (
            "📊 *Crawl Selesai — Final Report*\n"
            f"{self.stats.summary()}\n"
            f"🗃 Jobs: pending_ready={counts.get('pending_ready', 0)} | pending={counts.get('pending', 0)} | "
            f"processing={counts.get('processing', 0)} | completed={counts.get('completed', 0)} | "
            f"failed={counts.get('failed', 0)} | ignored={counts.get('ignored', 0)}"
        )
        logger.info(report)
        await self._notify(report)
        await self._close_dedupe_db()
