"""
Utilitas pemrosesan data: klasifikasi level pendidikan, ekstraksi keyword,
dan streaming writer ke JSONL.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from config import CLASSIFICATION_RULES, TARGET_KEYWORDS, MIN_RELEVANCE_KEYWORDS

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Indonesian language detector (heuristik cepat)
# ---------------------------------------------------------------------------

# Kata-kata umum bahasa Indonesia yang jarang muncul di bahasa lain
_INDO_COMMON_WORDS: list[str] = [
    "dan", "yang", "untuk", "dengan", "pada", "dari", "adalah",
    "dalam", "ini", "itu", "akan", "tidak", "atau", "juga",
    "telah", "dapat", "oleh", "sebuah", "serta", "antara",
    "bahwa", "lebih", "karena", "seperti", "ada", "mereka",
    "harus", "bisa", "sudah", "tersebut", "secara", "maka",
    "tentang", "setiap", "sangat", "berdasarkan", "merupakan",
    "siswa", "pembelajaran", "pelajaran", "materi", "kurikulum",
]


def is_indonesian_text(text: str, threshold: float = 0.02) -> bool:
    """Deteksi apakah teks berbahasa Indonesia.

    Menghitung proporsi kata-kata umum bahasa Indonesia dalam teks.
    threshold=0.02 artinya minimal 2% kata adalah kata umum Indonesia.
    """
    words = text.lower().split()
    if len(words) < 30:
        return True  # Terlalu pendek untuk dinilai, loloskan
    indo_count = sum(1 for w in words if w in _INDO_COMMON_WORDS)
    ratio = indo_count / len(words)
    return ratio >= threshold


def relevance_score(text: str, keywords: list[str] | None = None) -> int:
    """Hitung jumlah keyword TARGET yang ditemukan dalam teks."""
    keywords = keywords or TARGET_KEYWORDS
    text_lower = text.lower()
    return sum(1 for kw in keywords if kw.lower() in text_lower)


# ---------------------------------------------------------------------------
# Klasifikasi Otomatis
# ---------------------------------------------------------------------------

def classify_level(text: str, url: str = "") -> str:
    """Menentukan level pendidikan (SD / SMP / SMA / Umum) berdasarkan teks & URL.

    Mengecek teks + URL terhadap CLASSIFICATION_RULES secara berurutan.
    Return level pertama yang cocok, atau 'Umum' jika tidak ada match.
    """
    combined = f"{text} {url}"
    for level, pattern in CLASSIFICATION_RULES:
        if pattern.search(combined):
            return level
    return "Umum"


# ---------------------------------------------------------------------------
# Keyword Extraction
# ---------------------------------------------------------------------------

def extract_keywords_found(text: str, keywords: list[str] | None = None) -> list[str]:
    """Return list keyword dari TARGET_KEYWORDS yang ditemukan dalam teks (case-insensitive)."""
    keywords = keywords or TARGET_KEYWORDS
    text_lower = text.lower()
    return [kw for kw in keywords if kw.lower() in text_lower]


# ---------------------------------------------------------------------------
# Record Builder
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Content cleaner — strip UI noise from markdown
# ---------------------------------------------------------------------------

import re as _re

# ===================================================================
# AGGRESSIVE CONTENT CLEANING FOR CPT QUALITY
# ===================================================================

# ---------------------------------------------------------------------------
# 1. Line-level noise patterns — lines matching these are ALWAYS removed
# ---------------------------------------------------------------------------
_UI_NOISE_PATTERNS: list[_re.Pattern] = [
    _re.compile(p, _re.IGNORECASE) for p in [
        # --- Navigation / menu items ---
        r"^\s*\*?\*?(masuk|login|log in|sign in|sign up|daftar|register)\s*\*?\*?\s*$",
        r"^\s*\*?\*?(beranda|home|menu|navigasi|navigation)\s*\*?\*?\s*$",
        r"^\s*\*?\*?(cari|search|telusuri)\s*\.{0,3}\s*\*?\*?\s*$",
        r"^\s*\*?\*?(bagikan|share|tweet|facebook|whatsapp|telegram|copy link)\s*\*?\*?\s*$",
        r"^\s*(skip to (?:content|main)|lewati ke konten|langsung ke konten)",
        r"^\s*(copyright|hak cipta|©|\(c\))\s",
        r"^\s*\*?\*?(baca juga|baca selengkapnya|read more|selengkapnya|lihat semua|see all)\s*:?\s*\*?\*?",
        r"^\s*\*?\*?(iklan|advertisement|sponsored|promoted|promo)\s*\*?\*?\s*$",
        r"^\s*(loading|memuat|please wait|tunggu)\.{0,3}\s*$",
        r"^\s*\*?\*?(kembali ke atas|back to top|↑|⬆)\s*\*?\*?\s*$",
        r"^\s*\*?\*?(hubungi kami|contact us|kontak)\s*\*?\*?\s*$",
        r"^\s*\*?\*?(tentang kami|about us|kebijakan privasi|privacy policy|syarat|terms)\s*\*?\*?\s*$",
        r"^\s*\*?\*?(download aplikasi|unduh aplikasi|get the app)\s*\*?\*?\s*$",
        r"^\s*\|.*\|.*\|.*\|\s*$",
        r"^\s*\*?\*?(subscribe|berlangganan|newsletter|whatsapp)\s*\*?\*?\s*$",
        r"^\s*follow us",
        r"^\s*ikuti kami",
        r"^\s*\*?\*?\d+\s*(komentar|comments?)\s*\*?\*?\s*$",
        r"^\s*\*?\*?(prev|next|sebelumnya|selanjutnya|older|newer)\s*(post|artikel)?\s*\*?\*?\s*$",

        # --- Material / icon names (from crawled results) ---
        r"^\s*_?(search|chevron_left|chevron_right|arrow_drop_down|arrow_back|arrow_forward|menu|close|expand_more|expand_less|check|star|favorite|thumb_up|thumb_down|visibility|edit|delete|add|remove|settings|info|warning|error|help|person|people|school|book|calendar|clock|phone|email|location|home|work)_?\s*$",
        r"^\s*_{1,2}[a-z_]+_{1,2}\s*$",  # generic _icon_name_ patterns

        # --- Slide / carousel indicators ---
        r"^\s*slide\s+\d+\s+(of|dari)\s+\d+",
        r"^\s*\d+\s*/\s*\d+\s*$",  # 1/5, 2/10 etc

        # --- 404 / error pages ---
        r"^\s*(page not found|404|halaman tidak ditemukan|oops|error|maaf.*tidak ditemukan)",
        r"^\s*(the page you|halaman yang anda)\s",

        # --- Breadcrumb patterns ---
        r"^\s*(beranda|home)\s*[>»›/]\s*",
        r"^(\s*\w+\s*[>»›/]){2,}",  # A > B > C > D breadcrumbs

        # --- Footer / legal junk ---
        r"^\s*nib[\s:]*\d",
        r"^\s*(all rights reserved|hak cipta dilindungi)",
        r"^\s*powered by\s",
        r"^\s*designed by\s",
        r"^\s*(ketentuan layanan|terms of service|terms & conditions)",

        # --- Social / app store ---
        r"^\s*(available on|tersedia di)\s*(google play|app store|play store)",
        r"^\s*(get it on|download on)\s",
        r"^\s*\*?\*?(facebook|twitter|instagram|youtube|linkedin|tiktok)\s*\*?\*?\s*$",

        # --- Cookie / GDPR ---
        r"^\s*(we use cookies|kami menggunakan cookie|this site uses cookies)",

        # --- Login/form prompts ---
        r"^\s*(masukkan|enter|input)\s*(email|password|kata sandi|nama|name|username)",
        r"^\s*(lupa password|forgot password|lupa kata sandi)",
        r"^\s*(belum punya akun|don't have an account|sudah punya akun|already have)",

        # --- View-more / CTA patterns ---
        r"^\s*\*?\*?(lihat|view|tampilkan|show)\s+(selengkapnya|semua|more|all|arsip|detail)\s*\*?\*?\s*$",
        r"^\s*\*?\*?(pelajari selengkapnya|learn more|read full|baca lengkap)\s*\*?\*?\s*$",
        r"^\s*\*?\*?(mulai sekarang|start now|coba sekarang|try now|daftar sekarang)\s*\*?\*?\s*$",
        r"^\s*\*?\*?(gabung|join)\s",

        # --- Repeated branding / taglines on every page ---
        r"^\s*(open main menu|toggle navigation|main menu|menu utama)",
    ]
]

# Words that are UI button/label noise when appearing as standalone short lines
_BUTTON_WORDS: set[str] = {
    "login", "masuk", "daftar", "register", "sign up", "sign in",
    "home", "beranda", "menu", "search", "cari", "telusuri",
    "share", "bagikan", "tweet", "print", "cetak", "salin",
    "prev", "next", "previous", "close", "tutup", "buka",
    "subscribe", "ok", "cancel", "batal", "submit", "filter",
    "kirim", "send", "reset", "clear", "hapus", "urutkan",
    "save", "simpan", "edit", "delete", "remove", "tambah",
    "profil", "profile", "akun", "account", "pengaturan", "settings",
    "unduh", "download", "upload", "unggah", "arsip", "archive",
    "kelas", "mapel", "materi", "modul", "topik", "kategori",
    "terbaru", "terlama", "populer", "trending", "semua",
    "sd", "smp", "sma", "smk", "paud", "tk",
    "overview", "ringkasan", "layanan", "service", "services",
    "berita", "artikel", "blog", "galeri", "gallery", "foto",
    "video", "livestream", "podcast", "webinar",
    "bantuan", "help", "faq", "panduan", "guide",
    "lainnya", "more", "lain", "other",
    "id", "en", "bahasa", "english", "indonesia",
}

# ---------------------------------------------------------------------------
# 2. Regex patterns to strip from text (inline removal, not line deletion)
# ---------------------------------------------------------------------------
_RE_URL = _re.compile(r"https?://\S+", _re.IGNORECASE)
_RE_EMAIL = _re.compile(r"\S+@\S+\.\S+")
_RE_PHONE = _re.compile(r"(?:(?:\+62|62|0)\s*[-.]?\s*\d{2,4}[-.\s]?\d{3,4}[-.\s]?\d{3,4})")
_RE_ICON = _re.compile(r"_{1,2}[a-z][a-z_]*_{1,2}")  # _search_, __menu__
_RE_LEADING_PREFIX = _re.compile(
    r"^(?:JAKARTA|BANDUNG|SURABAYA|YOGYAKARTA|MEDAN|SEMARANG|MAKASSAR|DENPASAR|SOLO|BOGOR)"
    r"[\s,]*(?:KOMPAS\.com|TEMPO\.CO|detik\.com|CNN Indonesia|ANTARA|REPUBLIKA\.co\.id|Bisnis\.com|JPNN\.com|Liputan6\.com|Okezone)"
    r"\s*[-–—]\s*",
    _re.IGNORECASE,
)
_RE_MULTI_SPACE = _re.compile(r"[ \t]{2,}")
_RE_MULTI_NEWLINE = _re.compile(r"\n{3,}")


def clean_markdown(text: str) -> str:
    """Remove UI / navigation noise lines from extracted markdown.

    Aggressively strips lines that are button labels, nav items, login prompts,
    share buttons, icon names, breadcrumbs, error pages, etc.
    """
    lines = text.split("\n")
    cleaned: list[str] = []
    consecutive_blank = 0

    for line in lines:
        stripped = line.strip()

        # Skip lines that match UI noise patterns
        if stripped and any(p.match(stripped) for p in _UI_NOISE_PATTERNS):
            continue

        # Skip standalone icon patterns like _search_ _chevron_right_
        plain = stripped.replace("*", "").strip()
        if plain and _RE_ICON.fullmatch(plain):
            continue

        # Skip very short standalone lines that look like button labels
        words = stripped.split()
        if stripped and len(words) <= 3 and not any(c in stripped for c in ".!?:;,()"):
            if not stripped.startswith("#"):
                lower = plain.lower()
                if lower in _BUTTON_WORDS:
                    continue
                # Also skip if it's just a single word with no punctuation
                if len(words) == 1 and len(lower) < 15 and lower.isalpha():
                    continue

        # Collapse excessive blank lines (max 2)
        if not stripped:
            consecutive_blank += 1
            if consecutive_blank > 2:
                continue
        else:
            consecutive_blank = 0

        cleaned.append(line)

    return "\n".join(cleaned).strip()


# ---------------------------------------------------------------------------
# Markdown → Plain text converter (for CPT training)
# ---------------------------------------------------------------------------

_MD_HEADING = _re.compile(r"^#{1,6}\s+")
_MD_BOLD_ITALIC = _re.compile(r"\*{1,3}([^*]+)\*{1,3}")
_MD_LINKS = _re.compile(r"\[([^\]]+)\]\([^)]+\)")
_MD_IMAGES = _re.compile(r"!\[([^\]]*)\]\([^)]+\)")
_MD_INLINE_CODE = _re.compile(r"`([^`]+)`")
_MD_HR = _re.compile(r"^[-*_]{3,}\s*$")
_MD_BLOCKQUOTE = _re.compile(r"^>+\s?")
_MD_LIST_BULLET = _re.compile(r"^\s*[-*+]\s+")
_MD_LIST_NUM = _re.compile(r"^\s*\d+\.\s+")
_MD_HTML_TAG = _re.compile(r"<[^>]+>")
_MD_TABLE_SEP = _re.compile(r"^\|?[-:|\s]+\|?$")
_MULTI_SPACE = _re.compile(r"  +")


def markdown_to_plain_text(md: str) -> str:
    """Convert markdown to clean plain text suitable for LLM CPT training.

    Strips all markdown formatting (headings, bold, links, images, code blocks,
    tables, blockquotes, HTML tags) and returns readable prose paragraphs.
    """
    lines = md.split("\n")
    out: list[str] = []
    in_code_block = False
    consecutive_blank = 0

    for line in lines:
        stripped = line.strip()

        # Handle fenced code blocks — skip entirely
        if stripped.startswith("```"):
            in_code_block = not in_code_block
            continue
        if in_code_block:
            continue

        # Skip table separator rows  |---|---|---|
        if _MD_TABLE_SEP.match(stripped):
            continue

        # Skip horizontal rules
        if _MD_HR.match(stripped):
            continue

        # Remove images (keep alt text if meaningful)
        stripped = _MD_IMAGES.sub(r"\1", stripped)

        # Remove links — keep link text
        stripped = _MD_LINKS.sub(r"\1", stripped)

        # Remove HTML tags
        stripped = _MD_HTML_TAG.sub("", stripped)

        # Remove heading markers  ## Title → Title
        stripped = _MD_HEADING.sub("", stripped)

        # Remove bold/italic markers
        stripped = _MD_BOLD_ITALIC.sub(r"\1", stripped)

        # Remove inline code backticks
        stripped = _MD_INLINE_CODE.sub(r"\1", stripped)

        # Remove blockquote markers
        stripped = _MD_BLOCKQUOTE.sub("", stripped)

        # Clean bullet/number list markers but keep text
        stripped = _MD_LIST_BULLET.sub("", stripped)
        stripped = _MD_LIST_NUM.sub("", stripped)

        # Remove table pipe characters
        if "|" in stripped:
            stripped = stripped.replace("|", " ")

        # Collapse multiple spaces
        stripped = _RE_MULTI_SPACE.sub(" ", stripped).strip()

        # Collapse blank lines (max 1)
        if not stripped:
            consecutive_blank += 1
            if consecutive_blank > 1:
                continue
            out.append("")
        else:
            consecutive_blank = 0
            out.append(stripped)

    return "\n".join(out).strip()


# ---------------------------------------------------------------------------
# Aggressive CPT text cleaner — produce training-quality plain text
# ---------------------------------------------------------------------------

# Minimum characters for a line to be considered "prose" (not nav/button)
_MIN_LINE_LEN = 25
# Minimum words for a line to be considered meaningful
_MIN_LINE_WORDS = 4

# Common navigation-style short texts that slip through
_NAV_LIKE_PATTERNS: list[_re.Pattern] = [
    _re.compile(p, _re.IGNORECASE) for p in [
        r"^kelas\s+\d",          # "Kelas 4", "Kelas 5"
        r"^(sd|smp|sma|smk)\s*$",
        r"^bab\s+\d",            # "Bab 1", "Bab 2"
        r"^(semester|sem)\s+\d",
        r"^(tema|subtema)\s+\d",
        r"^halaman\s+\d",        # "Halaman 1"
        r"^\d+\s*[-–]\s*\d+\s*$",  # "1 - 10" page ranges
        r"^\d+\s+(menit|jam|hari|minggu|bulan|tahun)\s*(lalu|yang lalu)?\s*$",
        r"^(oleh|by|penulis|author|editor|reporter|kontributor|foto|sumber)\s*:?\s*\w",
        r"^(tag|label|kategori|category)\s*:",
        r"^(dibaca|views?|dilihat)\s*:?\s*\d",
        r"^(updated?|diperbarui|dipublikasi|published|terbit)\s*:?\s",
        r"^(share|bagikan)\s*:",
        r"^\(\d+\)\s*$",         # (3) — notification badge
        r"^img\s+",              # leftover image alt text
        r"^gambar\s+\d",         # "Gambar 1"
        r"^(foto|image|ilustrasi|infografis)\s*:",
        r"^\[.*\]\s*$",          # leftover markdown link brackets
        # --- Wikipedia-specific noise ---
        r"^pindah ke bilah sisi",
        r"^(mode ulang tahun|baby globe)",
        r"^pelajari mode ulang tahun",
        r"^halaman ini selalu menggunakan",
        r"^the content is as wide as",
        r"^warna \(beta\)",
        r"^ikuti wikipedia",
        r"^gulingkan subbagian",
        r"^(dari wikipedia|from wikipedia)",
        r"^langsung ke konten",
        r"^(sembunyikan|tampilkan)\s+(subbagian|bagian|daftar)",
        r"^(artikel utama|main article)\s*:",
        r"^(lihat pula|see also)\s*$",
        r"^(pranala luar|external links)\s*$",
        r"^(referensi|references)\s*$",
        r"^(catatan kaki|footnotes)\s*$",
        r"^diperoleh dari\s",
        # --- Blog/article metadata noise ---
        r"^diposkan oleh\s",
        r"^posted by\s",
        r"^leave a comment",
        r"^posting komentar",
        r"^\d+ comment",
        r"^read more\s*[»›>]",
        r"^baca selengkapnya\s*[»›>]",
        r"^loading\s*\.{0,3}\s*$",
        # --- Concatenated nav detection (camelCase or words mashed together) ---
        r"^[A-Z][a-z]+[A-Z][a-z]+[A-Z]",  # FlashNewsNewsPlusDecodeHorizon
        # --- Social media noise ---
        r"^facebook\s+instagram\s+twitter",
        r"^follow us on",
        r"^share (this|article)",
        # --- Date-only lines ---
        r"^\w+,\s+\w+\s+\d{1,2},\s+\d{4}\s*$",  # Monday, March 3, 2026
        r"^\d{1,2}\s+\w+\s+\d{4}\s*$",  # 3 Maret 2026
        r"^\d{1,2}/\d{1,2}/\d{4}\s*$",  # 03/03/2026
        r"^\w+\s+\d{1,2},\s+\d{4}\s*$",  # Maret 3, 2026
        # --- Admin/author lines ---
        r"^admin\s+\d",
        r"^(no|nib|npwp)[\s.:]+\d",
        # --- Common remaining UI fragments ---
        r"^selamat datang",
        r"^masuk ke akun",
        r"^memulihkan kata sandi",
        r"^(sebuah )?kata sandi akan",
        r"^daftar keanggotaan",
        r"^dengan mendaftar.*anda dianggap",
        r"^headlines?\s*:?\s*$",
    ]
]

# Regex to detect concatenated navigation text (words without spaces)
_RE_CONCAT_NAV = _re.compile(
    r"^(?:[A-Z][a-z]+){4,}$"  # 4+ CamelCase words: "FlashNewsNewsPlusDecodeHorizon"
)

# Regex to detect lines that are just a list of short items separated by special chars
_RE_LIST_LINE = _re.compile(
    r"^(?:\w{2,20}\s*[|·•►▸▹▶→»›/\\,]\s*){3,}"  # "Berita | Artikel | Blog | Galeri"
)

# Repeated/duplicate line detector threshold
_MAX_DUPLICATE_RATIO = 0.3


def clean_text_for_cpt(text: str) -> str:
    """Aggressively clean plain text for CPT training quality.

    This applies AFTER markdown_to_plain_text() conversion.
    Removes URLs, emails, phone numbers, icon names, source prefixes,
    very short lines (nav/buttons), duplicate lines, Wikipedia UI noise,
    concatenated navigation, and verifies prose quality.

    Returns empty string if text fails quality checks.
    """
    if not text or not text.strip():
        return ""

    # --- Step 1: Inline removal of URLs, emails, phones, icons ---
    text = _RE_URL.sub("", text)
    text = _RE_EMAIL.sub("", text)
    text = _RE_PHONE.sub("", text)
    text = _RE_ICON.sub("", text)

    # --- Step 2: Strip source attribution prefixes ---
    text = _RE_LEADING_PREFIX.sub("", text)

    # --- Step 2b: Remove "Read more »" style suffixes ---
    text = _re.sub(r"\s*(Read more|Baca selengkapnya)\s*[»›>…]+\s*", " ", text, flags=_re.IGNORECASE)

    # --- Step 2c: Remove "Leave a Comment" and similar ---
    text = _re.sub(r"\s*Leave a Comment\s*", " ", text, flags=_re.IGNORECASE)
    text = _re.sub(r"\s*Posting Komentar\s*", " ", text, flags=_re.IGNORECASE)

    lines = text.split("\n")
    cleaned: list[str] = []
    seen_lines: set[str] = set()

    for line in lines:
        line = line.strip()

        # Skip empty lines (we'll add paragraph breaks later)
        if not line:
            if cleaned and cleaned[-1] != "":
                cleaned.append("")
            continue

        # Skip lines that match nav-like patterns
        if any(p.match(line) for p in _NAV_LIKE_PATTERNS):
            continue

        # Skip lines matching the main UI noise patterns (double-check)
        if any(p.match(line) for p in _UI_NOISE_PATTERNS):
            continue

        # Skip concatenated CamelCase navigation lines
        if _RE_CONCAT_NAV.match(line):
            continue

        # Skip list-separator lines (A | B | C | D)
        if _RE_LIST_LINE.match(line):
            continue

        # --- Step 3: Remove very short lines (likely nav/buttons) ---
        has_ending_punct = line[-1] in ".!?:;\"')"
        word_count = len(line.split())

        if len(line) < _MIN_LINE_LEN and not has_ending_punct:
            continue
        if word_count < _MIN_LINE_WORDS and not has_ending_punct:
            continue

        # --- Step 4: Remove duplicate/repeated lines ---
        line_key = line.lower().strip()
        if line_key in seen_lines:
            continue
        seen_lines.add(line_key)

        # Collapse multiple spaces
        line = _RE_MULTI_SPACE.sub(" ", line).strip()

        if line:
            cleaned.append(line)

    # Remove leading/trailing blank lines
    while cleaned and cleaned[0] == "":
        cleaned.pop(0)
    while cleaned and cleaned[-1] == "":
        cleaned.pop()

    result = "\n".join(cleaned).strip()

    # --- Step 5: Final quality checks ---
    final_lines = [l for l in result.split("\n") if l.strip()]

    if len(final_lines) < 2:
        # Need at least 2 non-empty lines for useful content
        return ""

    # Check prose density: what % of lines have >= 6 words?
    prose_lines = sum(1 for l in final_lines if len(l.split()) >= 6)
    prose_ratio = prose_lines / len(final_lines) if final_lines else 0

    if prose_ratio < 0.35:
        # Less than 35% of lines are prose-like → probably a menu/navigation page
        return ""

    # Check for 404/error pages in content
    result_lower = result.lower()
    error_signals = [
        "page not found", "404", "halaman tidak ditemukan",
        "the page you requested", "this page doesn't exist",
        "halaman yang anda cari tidak ditemukan",
    ]
    if any(sig in result_lower for sig in error_signals) and len(final_lines) < 10:
        return ""

    # Collapse multiple blank lines
    result = _RE_MULTI_NEWLINE.sub("\n\n", result)

    return result.strip()


def is_quality_content(text: str, min_words: int = 50) -> bool:
    """Check if cleaned text meets minimum quality bar for CPT training.

    Returns True if text has enough substance for training:
    - At least min_words words
    - Has actual sentence structure (periods, commas)
    - Not just a list of navigation items
    """
    if not text or not text.strip():
        return False

    words = text.split()
    if len(words) < min_words:
        return False

    # Check for sentence structure: should have some punctuation
    punct_count = sum(1 for c in text if c in ".!?,;:")
    if punct_count < 3:
        return False

    # Check average sentence length — very short avg means it's navigation/list
    sentences = [s.strip() for s in text.split(".") if s.strip()]
    if sentences:
        avg_sentence_words = sum(len(s.split()) for s in sentences) / len(sentences)
        if avg_sentence_words < 4:
            return False

    return True


# ---------------------------------------------------------------------------
# Record builders
# ---------------------------------------------------------------------------

def build_cpt_record(
    url: str,
    title: str,
    markdown_content: str,
    level: str = "Umum",
    source_domain: str = "",
) -> dict | None:
    """Build a HuggingFace CPT-ready record with aggressive cleaning.

    Returns None if content fails quality checks (too short, not prose,
    404 page, navigation-only page, etc.).

    Format follows the standard for Continued Pre-Training datasets:
    - Primary field ``text`` contains clean plain text (no markdown)
    - Minimal metadata kept for filtering/analysis

    Can be loaded directly with::

        from datasets import load_dataset
        ds = load_dataset("json", data_files="dataset_cpt.jsonl")
    """
    # Step 1: markdown → plain text (strip formatting)
    plain = markdown_to_plain_text(markdown_content)

    # Step 2: aggressive CPT cleaning (remove noise, short lines, duplicates)
    plain = clean_text_for_cpt(plain)

    # If cleaning returned empty, content is garbage — skip
    if not plain:
        return None

    # Prepend title as a natural opening line if it's not already in text
    if title and title.strip():
        clean_title = title.strip()
        # Don't add title if it's just a site name or very short
        if len(clean_title) > 10 and clean_title.lower() not in plain[:200].lower():
            plain = f"{clean_title}\n\n{plain}"

    # Final quality gate
    if not is_quality_content(plain, min_words=75):
        return None

    word_count = len(plain.split())

    return {
        "text": plain,
        "source": source_domain or urlparse(url).netloc,
        "url": url,
        "level": level,
        "word_count": word_count,
    }


def build_record(
    url: str,
    title: str,
    markdown_content: str,
    timestamp: str | None = None,
) -> dict:
    """Membuat satu record data untuk disimpan ke JSONL.

    Returns:
        dict dengan field: url, title, markdown_content, timestamp, metadata.
    """
    if timestamp is None:
        timestamp = datetime.now(timezone.utc).isoformat()

    combined_text = f"{title} {markdown_content}"
    domain = urlparse(url).netloc
    word_count = len(markdown_content.split())

    return {
        "url": url,
        "title": title,
        "markdown_content": markdown_content,
        "timestamp": timestamp,
        "metadata": {
            "level": classify_level(combined_text),
            "keywords_found": extract_keywords_found(combined_text),
            "source_domain": domain,
            "word_count": word_count,
        },
    }


# ---------------------------------------------------------------------------
# JSONL Streaming Writer
# ---------------------------------------------------------------------------

def append_jsonl(filepath: str | Path, record: dict) -> None:
    """Append satu record JSON ke file JSONL (satu baris per record).

    Menggunakan mode append agar tidak memakan RAM —
    setiap record langsung ditulis ke disk.
    """
    path = Path(filepath)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def load_urls(filepath: str) -> list[str]:
    """Baca file teks berisi URL (satu per baris). Skip baris kosong & komentar (#)."""
    path = Path(filepath)
    if not path.exists():
        logger.warning("File URL tidak ditemukan: %s", filepath)
        return []

    urls: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            urls.append(line)
    return urls
