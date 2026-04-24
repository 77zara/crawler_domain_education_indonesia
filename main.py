"""
AITF SR-02 Crawler — Entry Point
Tim 2 Sekolah Rakyat (AITF 2026)

Mengumpulkan dataset berkualitas tinggi untuk Continued Pre-training (CPT)
yang berfokus pada kurikulum nasional Indonesia.

Usage:
    uv run main.py
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import re
import signal
import sys
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Event loop policy: gunakan uvloop di Linux/macOS, fallback di Windows
# ---------------------------------------------------------------------------
if sys.platform != "win32":
    try:
        import uvloop
        uvloop.install()
        print("✓ uvloop aktif (high-performance event loop)")
    except ImportError:
        pass  # Fallback ke default asyncio event loop

logger = logging.getLogger("main")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(add_help=True)

    run_mode = parser.add_mutually_exclusive_group()
    run_mode.add_argument(
        "--test",
        action="store_true",
        help="Jalankan crawler singkat untuk smoke test lalu exit.",
    )
    run_mode.add_argument(
        "--production",
        action="store_true",
        help="Jalankan crawler langsung (tanpa Telegram /run).",
    )

    run_state = parser.add_mutually_exclusive_group()
    run_state.add_argument(
        "--resume",
        action="store_true",
        help="Resume output+dedupe yang sudah ada (default).",
    )
    run_state.add_argument(
        "--restart",
        action="store_true",
        help="Mulai run baru dengan INSTANCE_ID baru (output+dedupe terpisah).",
    )

    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Override MAX_CONCURRENCY.",
    )
    parser.add_argument(
        "--instance-id",
        type=str,
        default=None,
        help="Override INSTANCE_ID (untuk multi-worker di server).",
    )
    parser.add_argument(
        "--only-domain",
        type=str,
        default=None,
        help="Limit crawl to a single domain (e.g. kompas.com).",
    )
    parser.add_argument(
        "--seed-url",
        type=str,
        default=None,
        help="Seed URL to enqueue at start (useful when only-domain set).",
    )

    # Test-only limits
    parser.add_argument(
        "--max-success",
        type=int,
        default=5,
        help="(test) Stop setelah N halaman sukses.",
    )
    parser.add_argument(
        "--max-seconds",
        type=int,
        default=120,
        help="(test) Stop setelah N detik.",
    )

    return parser.parse_args(argv)


async def main(args: argparse.Namespace) -> None:
    """Entry point utama — inisialisasi dan jalankan bot atau crawler."""
    from config import Settings, DATA_DIR
    from core.crawler import CrawlEngine
    from core.bot import TelegramController

    # Load settings (.env)
    settings = Settings()

    # Apply domain restriction if provided via CLI
    if getattr(args, "only_domain", None):
        settings.DOMAIN_WHITELIST = [args.only_domain]

    # Apply CLI overrides
    if args.restart:
        run_id = (args.instance_id or "").strip()
        if not run_id:
            ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            run_id = f"run_{ts}"
        settings.INSTANCE_ID = run_id
    elif args.instance_id:
        settings.INSTANCE_ID = args.instance_id.strip()

    if args.workers is not None:
        settings.MAX_CONCURRENCY = max(1, int(args.workers))
    elif args.production:
        settings.MAX_CONCURRENCY = max(settings.MAX_CONCURRENCY, 10)
    elif args.test:
        settings.MAX_CONCURRENCY = min(max(settings.MAX_CONCURRENCY, 1), 2)

    if args.production:
        settings.HEADLESS = True
        settings.CACHE_MODE = "enabled"
    if args.test:
        settings.HEADLESS = True
        settings.CACHE_MODE = "bypass"
        settings.NOTIFY_EVERY = 1

    # Logging setup (instance-scoped; safe for multi-process workers)
    raw_instance_id = (settings.INSTANCE_ID or "").strip()
    safe_instance_id = re.sub(r"[^A-Za-z0-9_.-]+", "_", raw_instance_id).strip("._-")[:64]
    log_dir = DATA_DIR / safe_instance_id if safe_instance_id else DATA_DIR
    log_dir.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_dir / "crawler.log", encoding="utf-8"),
        ],
        force=True,
    )

    stop_event = asyncio.Event()

    logger.info("=" * 60)
    logger.info("AITF SR-02 Crawler — Tim 2 Sekolah Rakyat")
    logger.info("Mode: PRD v2.0 (SQLite + Sitemap + Search)")
    logger.info("=" * 60)
    logger.info("Instance ID   : %s", safe_instance_id or "(default)")
    logger.info(
        "Discovery shard: %d/%d",
        settings.DISCOVERY_SHARD_INDEX,
        settings.DISCOVERY_SHARD_COUNT,
    )
    logger.info("Concurrency   : %d", settings.MAX_CONCURRENCY)
    logger.info("Cache mode    : %s", settings.CACHE_MODE)
    logger.info("Search delay  : %.1fs", settings.SEARCH_DELAY)
    logger.info("Notify every  : %d halaman", settings.NOTIFY_EVERY)
    logger.info("Telegram Bot  : %s", "Configured" if settings.TELEGRAM_BOT_TOKEN else "NOT SET")
    logger.info("=" * 60)

    engine = CrawlEngine(settings, stop_event)

    # Seed initial URL if provided
    if getattr(args, "seed_url", None):
        try:
            await engine._try_enqueue(args.seed_url)
        except Exception:
            pass
    else:
        # If focused on kompas.com, seed common listing/search pages to bootstrap discovery
        if getattr(args, "only_domain", None) and args.only_domain.lower().endswith("kompas.com"):
            seeds = [
                "https://www.kompas.com/edu",
                "https://www.kompas.com/edu/",
                "https://edukasi.kompas.com/",
                "https://www.kompas.com/",
            ]
            for s in seeds:
                try:
                    await engine._try_enqueue(s)
                except Exception:
                    pass

        # Seed: Detik.com/edu
        if getattr(args, "only_domain", None) and args.only_domain.lower().endswith("detik.com"):
            seeds = [
                "https://www.detik.com/edu",
                "https://www.detik.com/edu/",
                "https://www.detik.com/edu/sekolah",
                "https://www.detik.com/edu/perguruan-tinggi",
                "https://www.detik.com/edu/edutainment",
            ]
            for s in seeds:
                try:
                    await engine._try_enqueue(s)
                except Exception:
                    pass

        # Seed: Ruangguru Blog
        if getattr(args, "only_domain", None) and args.only_domain.lower().endswith("ruangguru.com"):
            seeds = [
                "https://www.ruangguru.com/blog",
                "https://www.ruangguru.com/blog/",
            ]
            for s in seeds:
                try:
                    await engine._try_enqueue(s)
                except Exception:
                    pass

        # Seed: Liputan6
        if getattr(args, "only_domain", None) and args.only_domain.lower().endswith("liputan6.com"):
            seeds = [
                "https://www.liputan6.com/news",
                "https://www.liputan6.com/education",
                "https://www.liputan6.com/",
            ]
            for s in seeds:
                try:
                    await engine._try_enqueue(s)
                except Exception:
                    pass

        # Seed: Republika.co.id
        if getattr(args, "only_domain", None) and args.only_domain.lower().endswith("republika.co.id"):
            seeds = [
                "https://news.republika.co.id/berita/nasional/pendidikan",
                "https://republika.co.id/berita/nasional/umum",
                "https://republika.co.id/",
            ]
            for s in seeds:
                try:
                    await engine._try_enqueue(s)
                except Exception:
                    pass

        # Seed: Quipper Blog
        if getattr(args, "only_domain", None) and args.only_domain.lower().endswith("quipper.com"):
            seeds = [
                "https://www.quipper.com/id/blog/",
                "https://www.quipper.com/id/blog/materi-belajar/",
                "https://www.quipper.com/id/blog/tips-trick/",
            ]
            for s in seeds:
                try:
                    await engine._try_enqueue(s)
                except Exception:
                    pass

        # Seed: Zenius Blog
        if getattr(args, "only_domain", None) and args.only_domain.lower().endswith("zenius.net"):
            seeds = [
                "https://www.zenius.net/blog/",
                "https://www.zenius.net/blog/category/materi-belajar/",
                "https://www.zenius.net/blog/category/tips-belajar/",
            ]
            for s in seeds:
                try:
                    await engine._try_enqueue(s)
                except Exception:
                    pass

    # Handle SIGINT/SIGTERM untuk graceful shutdown
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: asyncio.create_task(_shutdown(engine)))
        except NotImplementedError:
            pass

    run_direct = bool(args.production or args.test or not settings.TELEGRAM_BOT_TOKEN)
    if run_direct:
        if args.test:
            logger.info(
                "Test mode: stop setelah %d sukses atau %d detik",
                args.max_success,
                args.max_seconds,
            )

            async def stop_on_success() -> None:
                while not engine.stop_event.is_set():
                    if engine.stats.urls_success >= max(1, int(args.max_success)):
                        engine.stop_event.set()
                        break
                    await asyncio.sleep(1)

            async def stop_on_timeout() -> None:
                await asyncio.sleep(max(1, int(args.max_seconds)))
                engine.stop_event.set()

            t1 = asyncio.create_task(stop_on_success(), name="stop-on-success")
            t2 = asyncio.create_task(stop_on_timeout(), name="stop-on-timeout")
            try:
                await engine.run()
            finally:
                t1.cancel()
                t2.cancel()
        else:
            await engine.run()
        return

    # Telegram mode
    controller = TelegramController(settings, engine)
    await controller.start()


async def _shutdown(engine) -> None:
    """Graceful shutdown handler."""
    logger.info("Sinyal shutdown diterima — menghentikan crawler...")
    engine.stop_event.set()


if __name__ == "__main__":
    try:
        _args = parse_args()
        asyncio.run(main(_args))
    except KeyboardInterrupt:
        logger.info("Dihentikan oleh user (Ctrl+C).")
    except Exception as exc:
        logger.exception("Fatal error: %s", exc)
        sys.exit(1)
