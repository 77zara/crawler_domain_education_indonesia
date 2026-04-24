"""
Telegram Bot — Remote Control untuk crawler.

Commands:
  /run    — Mulai endless crawling (search engine + spidering)
  /stop   — Graceful shutdown (simpan data terakhir)
  /status — Statistik: URL discovered, crawled, queue, RAM usage
  /help   — Daftar perintah
"""

from __future__ import annotations

import html
import asyncio
import logging
from pathlib import Path

import psutil
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

from config import Settings
from core.crawler import CrawlEngine

logger = logging.getLogger(__name__)


class TelegramController:
    """Mengelola Telegram Bot dan mengontrol CrawlEngine."""

    def __init__(self, settings: Settings, engine: CrawlEngine) -> None:
        self.settings = settings
        self.engine = engine
        self._crawl_task: asyncio.Task | None = None
        self._app: Application | None = None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _send_message(self, text: str) -> None:
        """Kirim pesan ke TELEGRAM_CHAT_ID via bot application."""
        if self._app and self.settings.TELEGRAM_CHAT_ID:
            try:
                await self._app.bot.send_message(
                    chat_id=self.settings.TELEGRAM_CHAT_ID,
                    text=text,
                    parse_mode="HTML",
                )
            except Exception as exc:
                logger.error("Gagal kirim pesan Telegram: %s", exc)

    def _get_ram_usage_mb(self) -> float:
        """Return RAM usage proses saat ini dalam MB."""
        process = psutil.Process()
        return process.memory_info().rss / (1024 * 1024)

    # ------------------------------------------------------------------
    # Command Handlers
    # ------------------------------------------------------------------

    async def cmd_run(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handler /run — memulai endless crawling."""
        if self.engine.is_running:
            await update.message.reply_text(
                "⚠️ Crawling sedang berjalan. Gunakan /stop untuk menghentikan."
            )
            return

        # Reset stop event
        self.engine.stop_event.clear()

        # Set notify callback
        self.engine.set_notify_callback(self._send_message)

        raw_name = self.engine.output_file.name
        processed_1_name = getattr(self.engine, "output_processed_1_file", self.engine.output_file).name

        await update.message.reply_text(
            f"🚀 <b>Endless Crawling dimulai!</b>\n"
            f"   Mode: <b>PRD v2.0</b> (Sitemap + Search + Spidering)\n"
            f"   Workers: <b>{self.settings.MAX_CONCURRENCY}</b>\n"
            f"   Search delay: <b>{self.settings.SEARCH_DELAY}s</b>\n"
            f"   Output (data/raw):\n"
            f"     • <code>data/raw/{raw_name}</code> (raw crawl output)\n"
            f"     • <code>data/raw/{processed_1_name}</code> (stage-1 processed text)\n\n"
            f"Crawler akan terus berjalan sampai /stop dikirim.",
            parse_mode="HTML",
        )

        # Jalankan crawl sebagai background task
        self._crawl_task = asyncio.create_task(self._run_with_auto_restart())

    async def _run_with_auto_restart(self) -> None:
        """Wrapper yang auto-restart engine.run() jika crash."""
        max_restarts = 50
        restart_count = 0
        while restart_count < max_restarts:
            try:
                await self.engine.run()
                # Normal exit (stop command)
                break
            except Exception as exc:
                restart_count += 1
                logger.exception(
                    "💥 Engine crashed (restart %d/%d): %s",
                    restart_count, max_restarts, exc,
                )
                await self._send_message(
                    f"💥 Engine crash #{restart_count}, auto-restart dalam 10 detik...\n"
                    f"Error: {str(exc)[:200]}"
                )
                # Reset state
                self.engine.is_running = False
                self.engine.stop_event.clear()
                await asyncio.sleep(10)
        
        if restart_count >= max_restarts:
            await self._send_message(
                f"🛑 Engine sudah crash {max_restarts} kali. Dihentikan permanen.\n"
                "Gunakan /run untuk coba lagi."
            )
            self.engine.is_running = False

    async def cmd_stop(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handler /stop — graceful shutdown."""
        if not self.engine.is_running:
            await update.message.reply_text("ℹ️ Tidak ada crawling yang sedang berjalan.")
            return

        self.engine.stop_event.set()
        await update.message.reply_text(
            "⏹ <b>Graceful shutdown initiated.</b>\n"
            "Workers akan berhenti setelah menyelesaikan URL yang sedang diproses.",
            parse_mode="HTML",
        )

    async def cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handler /status — menampilkan statistik crawling."""
        ram_mb = self._get_ram_usage_mb()
        output_path = self.engine.output_file
        file_size_mb = (
            output_path.stat().st_size / (1024 * 1024)
            if output_path.exists() else 0.0
        )
        total_lines = (
            sum(1 for _ in open(output_path, encoding="utf-8"))
            if output_path.exists() else 0
        )

        stats = self.engine.stats
        counts = await self.engine.get_job_counts()
        status_text = (
            f"📊 <b>Status Crawler</b>\n"
            f"{'─' * 28}\n"
            f"🔄 Running       : <b>{'Ya' if self.engine.is_running else 'Tidak'}</b>\n"
            f"⏱ Uptime        : <b>{stats.elapsed}</b>\n"
            f"🔍 Search done   : <b>{stats.search_queries_done}</b>\n"
            f"📡 URL discovered: <b>{stats.urls_discovered}</b>\n"
            f"🌐 URL crawled   : <b>{stats.urls_crawled}</b>\n"
            f"✅ Sukses        : <b>{stats.urls_success}</b>\n"
            f"❌ Gagal         : <b>{stats.urls_failed}</b>\n"
            f"⏭ Skipped       : <b>{stats.urls_skipped}</b>\n"
            f"🔗 Links found   : <b>{stats.links_extracted}</b>\n"
            f"🧮 Tokens (Qwen) : <b>{getattr(stats, 'tokens_total', 0)}</b>\n"
            f"🗃 Jobs total    : <b>{counts.get('total', 0)}</b>\n"
            f"   • pending_ready: <b>{counts.get('pending_ready', 0)}</b>\n"
            f"   • pending     : <b>{counts.get('pending', 0)}</b>\n"
            f"   • processing  : <b>{counts.get('processing', 0)}</b>\n"
            f"   • completed   : <b>{counts.get('completed', 0)}</b>\n"
            f"   • failed      : <b>{counts.get('failed', 0)}</b>\n"
            f"   • ignored     : <b>{counts.get('ignored', 0)}</b>\n"
            f"📁 Records saved : <b>{total_lines}</b>\n"
            f"💾 File size     : <b>{file_size_mb:.2f} MB</b>\n"
            f"🧠 RAM usage     : <b>{ram_mb:.1f} MB</b>\n"
        )
        await update.message.reply_text(status_text, parse_mode="HTML")

    async def cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handler /help — menampilkan daftar command."""
        help_text = (
            "🤖 <b>AITF SR-02 Crawler Bot</b>\n"
            "Tim 2 — Sekolah Rakyat\n\n"
            "<b>Mode:</b> PRD v2.0 (SQLite + Sitemap + Search)\n"
            "Bot akan men-scan sitemap dan/atau search engine,\n"
            "lalu crawl konten + spidering link secara terus-menerus.\n\n"
            "/run    — Mulai endless crawling\n"
            "/stop   — Graceful shutdown\n"
            "/status — Statistik crawling\n"
            "/help   — Tampilkan bantuan ini"
        )
        await update.message.reply_text(help_text, parse_mode="HTML")

    # ------------------------------------------------------------------
    # Bot lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Inisialisasi dan jalankan Telegram bot polling."""
        token = self.settings.TELEGRAM_BOT_TOKEN
        if not token:
            logger.error(
                "TELEGRAM_BOT_TOKEN belum diset! "
                "Tambahkan di .env file. Bot tidak akan berjalan."
            )
            logger.info("Running tanpa Telegram bot. Gunakan Ctrl+C untuk berhenti.")
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                pass
            return

        self._app = Application.builder().token(token).build()

        # Register handlers
        self._app.add_handler(CommandHandler("run", self.cmd_run))
        self._app.add_handler(CommandHandler("stop", self.cmd_stop))
        self._app.add_handler(CommandHandler("status", self.cmd_status))
        self._app.add_handler(CommandHandler("help", self.cmd_help))
        self._app.add_handler(CommandHandler("start", self.cmd_help))

        logger.info("Telegram bot dimulai. Menunggu perintah...")

        # Start polling
        await self._app.initialize()
        await self._app.start()
        await self._app.updater.start_polling(drop_pending_updates=True)

        # Kirim notifikasi startup
        if self.settings.TELEGRAM_CHAT_ID:
            await self._send_message(
                "🟢 <b>Bot AITF SR-02 Crawler online!</b>\n"
                "Mode: PRD v2.0 (SQLite + Sitemap + Search)\n"
                "Ketik /help untuk melihat daftar perintah."
            )

        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            pass
        finally:
            logger.info("Shutting down Telegram bot...")
            await self._app.updater.stop()
            await self._app.stop()
            await self._app.shutdown()
