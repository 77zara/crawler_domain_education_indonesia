"""
Konfigurasi terpusat untuk AITF SR-02 Crawler.
Menggunakan pydantic-settings untuk memuat variabel dari .env file.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import ClassVar

from pydantic_settings import BaseSettings, SettingsConfigDict

# ---------------------------------------------------------------------------
# Path constants
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

# ---------------------------------------------------------------------------
# Settings (loaded from .env)
# ---------------------------------------------------------------------------

class Settings(BaseSettings):
    """Konfigurasi aplikasi — nilai default bisa di-override lewat .env."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Path constants (non-env, computed)
    BASE_DIR: ClassVar[Path] = BASE_DIR
    DATA_DIR: ClassVar[Path] = DATA_DIR

    # Telegram
    TELEGRAM_BOT_TOKEN: str = ""
    TELEGRAM_CHAT_ID: str = ""

    # Crawler
    MAX_CONCURRENCY: int = 5
    NOTIFY_EVERY: int = 100
    CACHE_MODE: str = "enabled"  # enabled | bypass | disabled

    # Browser
    HEADLESS: bool = True
    PAGE_TIMEOUT: int = 60000  # ms

    # Search engine discovery
    SEARCH_DELAY: float = 3.0  # detik antar search query (rate limiting)
    MIN_RELEVANCE_SCORE: int = 2  # minimal keyword match untuk simpan konten


# ---------------------------------------------------------------------------
# Klasifikasi Level Pendidikan
# ---------------------------------------------------------------------------
CLASSIFICATION_RULES: list[tuple[str, re.Pattern[str]]] = [
    ("SD", re.compile(r"\b(sd|sekolah\s+dasar|tematik|kelas\s+[1-6])\b", re.IGNORECASE)),
    ("SMP", re.compile(r"\b(smp|sekolah\s+menengah\s+pertama|menengah\s+pertama|kelas\s+[7-9])\b", re.IGNORECASE)),
    ("SMA", re.compile(r"\b(sma|smk|sekolah\s+menengah\s+atas|menengah\s+atas|kelas\s+1[0-2])\b", re.IGNORECASE)),
]

# ---------------------------------------------------------------------------
# Target Keywords (Strategi Tim 2) — COMPREHENSIVE
# ---------------------------------------------------------------------------
TARGET_KEYWORDS: list[str] = [
    # --- Pendidikan Umum & Jenjang ---
    "Pendidikan Indonesia",
    "Sekolah Rakyat",
    "Sekolah Dasar",
    "Sekolah Menengah Pertama",
    "Sekolah Menengah Atas",
    "Sekolah Menengah Kejuruan",
    "Madrasah",
    # --- Kurikulum ---
    "Kurikulum Nasional",
    "Kurikulum Merdeka",
    "Kurikulum 2013",
    "Capaian Pembelajaran",
    "Alur Tujuan Pembelajaran",
    "Modul Ajar",
    "Silabus",
    "RPP",
    "Rencana Pelaksanaan Pembelajaran",
    # --- Kognitif & Asesmen ---
    "Kemampuan Kognitif",
    "Kognitif Siswa",
    "Ujian Nasional",
    "Asesmen Nasional",
    "Asesmen Kompetensi Minimum",
    "Asesmen Formatif",
    "Asesmen Sumatif",
    "Evaluasi Pembelajaran",
    "Soal Ujian",
    # --- STEM ---
    "Matematika",
    "Fisika",
    "Kimia",
    "Biologi",
    "Sains",
    "IPA",
    # --- Non-STEM ---
    "Ekonomi",
    "Sejarah",
    "Sosiologi",
    "Geografi",
    "IPS",
    "Ilmu Pengetahuan Sosial",
    # --- Mata Pelajaran Lokal ---
    "Pendidikan Kewarganegaraan",
    "PKN",
    "PPKN",
    "Sejarah Indonesia",
    "Bahasa Indonesia",
    "Bahasa Inggris",
    # --- Karakter & Nasional ---
    "Pancasila",
    "Profil Pelajar Pancasila",
    "Pendidikan Karakter",
    "Nasionalisme",
    # --- Kelembagaan ---
    "Kementerian Pendidikan",
    "Kemdikbud",
    "Kemendikbudristek",
    "Kemendikdasmen",
    "Kemensos Sekolah Rakyat",
    # --- Soal QnA & Level ---
    "Soal Mudah SD SMP SMA",
    "Soal Menengah SD SMP SMA",
    "Soal Sulit SD SMP SMA",
    "Olimpiade Sains Nasional (OSN) SD SMP SMA",
    "Latihan Soal",
    "Bank Soal",
    "Pembahasan Soal",
    "Kunci Jawaban",
    # --- Lainnya ---
    "Materi Pelajaran",
    "Buku Teks",
    "Bahan Ajar",
    "Pembelajaran",
    "Guru Penggerak",
    "Projek Penguatan Profil Pelajar Pancasila",
]

# ---------------------------------------------------------------------------
# Minimum Relevance — konten harus mengandung minimal N keyword untuk disimpan
# ---------------------------------------------------------------------------
MIN_RELEVANCE_KEYWORDS: int = 2
