"""
AITF SR-02 Crawler — Entry Point
Tim 2 Sekolah Rakyat (AITF 2026)

Mengumpulkan dataset berkualitas tinggi untuk Continued Pre-training (CPT)
yang berfokus pada kurikulum nasional Indonesia.

Usage:
    uv run main.py
"""

from __future__ import annotations

import asyncio
import logging
import signal
import sys

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

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("data/crawler.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("main")


async def main() -> None:
    """Entry point utama — inisialisasi dan jalankan bot + crawler."""
    from config import Settings, DATA_DIR
    from core.crawler import CrawlEngine
    from core.bot import TelegramController

    # Pastikan data directory ada
    DATA_DIR.mkdir(exist_ok=True)

    # Load settings
    settings = Settings()
    stop_event = asyncio.Event()

    logger.info("=" * 60)
    logger.info("AITF SR-02 Crawler — Tim 2 Sekolah Rakyat")
    logger.info("Mode: Endless Search Engine Crawler")
    logger.info("=" * 60)
    logger.info("Concurrency   : %d", settings.MAX_CONCURRENCY)
    logger.info("Cache mode    : %s", settings.CACHE_MODE)
    logger.info("Search delay  : %.1fs", settings.SEARCH_DELAY)
    logger.info("Notify every  : %d halaman", settings.NOTIFY_EVERY)
    logger.info("Telegram Bot  : %s", "Configured" if settings.TELEGRAM_BOT_TOKEN else "NOT SET")
    logger.info("=" * 60)

    # Inisialisasi engine & controller
    engine = CrawlEngine(settings, stop_event)
    controller = TelegramController(settings, engine)

    # Handle SIGINT/SIGTERM untuk graceful shutdown
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(
                sig,
                lambda: asyncio.create_task(_shutdown(engine)),
            )
        except NotImplementedError:
            # Windows tidak mendukung add_signal_handler untuk SIGINT
            pass

    # Start controller (Telegram bot polling, atau standalone jika token kosong)
    await controller.start()


async def _shutdown(engine) -> None:
    """Graceful shutdown handler."""
    logger.info("Sinyal shutdown diterima — menghentikan crawler...")
    engine.stop_event.set()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Dihentikan oleh user (Ctrl+C).")
    except Exception as exc:
        logger.exception("Fatal error: %s", exc)
        sys.exit(1)
