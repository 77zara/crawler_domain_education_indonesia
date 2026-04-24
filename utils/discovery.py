"""
Search-engine URL discovery untuk crawler.

Menggunakan DuckDuckGo HTML dan Bing untuk menemukan URL relevan
berdasarkan keyword pendidikan Indonesia secara otomatis.
Juga mengekstrak link dari halaman yang sudah di-crawl (spidering).
"""

from __future__ import annotations

import base64
import logging
import re
from pathlib import Path
from urllib.parse import urlencode, urlparse, urljoin, parse_qs
from typing import AsyncIterator

import aiohttp
from aiohttp import ClientTimeout

from config import TARGET_KEYWORDS

logger = logging.getLogger(__name__)

_RE_SITE_SUFFIX = re.compile(r"\(\s*site:[^)]+\)\s*$", re.IGNORECASE)


def _repo_root() -> Path:
    # utils/ -> repo root
    return Path(__file__).resolve().parents[1]


def load_keyword_phrases_from_file(path: Path) -> list[str]:
    """Load keyword phrases from a simple text file.

    Supports lines like: `Kurikulum Merdeka(site:www.kompas.com)`.
    We strip the trailing `(site:...)` part and keep only the phrase.
    """
    try:
        raw = path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return []

    phrases: list[str] = []
    for line in raw.splitlines():
        s = (line or "").strip()
        if not s or s.startswith("#"):
            continue

        # Strip trailing (site:...) suffix
        s = _RE_SITE_SUFFIX.sub("", s).strip()

        # If written as: "foo site:bar" strip site part
        if "site:" in s.lower():
            s = re.split(r"\bsite:\S+", s, maxsplit=1, flags=re.IGNORECASE)[0].strip()

        s = re.sub(r"\s+", " ", s).strip()
        if s:
            phrases.append(s)

    # Deduplicate preserve order
    return list(dict.fromkeys(phrases))


def kompas_tag_slug(phrase: str) -> str:
    """Slugify phrase into Kompas tag path component."""
    s = (phrase or "").strip().lower()
    if not s:
        return ""

    # Keep letters/numbers, convert others to '-'
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = s.strip("-")
    return s[:80]


def is_kompas_article_url(url: str) -> bool:
    try:
        p = urlparse(url)
    except Exception:
        return False
    if not (p.scheme and p.netloc):
        return False
    if not p.netloc.lower().endswith("kompas.com"):
        return False
    return "/read/" in (p.path or "").lower()


def is_detik_article_url(url: str) -> bool:
    """Check if URL is a Detik.com /edu/ article page."""
    try:
        p = urlparse(url)
    except Exception:
        return False
    if not (p.scheme and p.netloc):
        return False
    if not p.netloc.lower().endswith("detik.com"):
        return False
    path = (p.path or "").lower()
    return "/edu/" in path and "/d-" in path


def is_liputan6_article_url(url: str) -> bool:
    """Check if URL is a Liputan6.com /read/ article page."""
    try:
        p = urlparse(url)
    except Exception:
        return False
    if not (p.scheme and p.netloc):
        return False
    if not p.netloc.lower().endswith("liputan6.com"):
        return False
    return "/read/" in (p.path or "").lower()


def is_ruangguru_blog_url(url: str) -> bool:
    """Check if URL is a Ruangguru.com /blog/ article page."""
    try:
        p = urlparse(url)
    except Exception:
        return False
    if not (p.scheme and p.netloc):
        return False
    if not p.netloc.lower().endswith("ruangguru.com"):
        return False
    path = (p.path or "").lower()
    # /blog/ must have a slug after it (not just /blog/ index)
    return path.startswith("/blog/") and len(path) > len("/blog/")


def is_republika_article_url(url: str) -> bool:
    """Check if URL is a Republika.co.id article page."""
    try:
        p = urlparse(url)
    except Exception:
        return False
    if not (p.scheme and p.netloc):
        return False
    if not p.netloc.lower().endswith("republika.co.id"):
        return False
    return "/berita/" in (p.path or "").lower()


def is_quipper_blog_url(url: str) -> bool:
    """Check if URL is a Quipper.com /blog/ article page."""
    try:
        p = urlparse(url)
    except Exception:
        return False
    if not (p.scheme and p.netloc):
        return False
    if not p.netloc.lower().endswith("quipper.com"):
        return False
    path = (p.path or "").lower()
    return "/blog/" in path and len(path) > len("/blog/")


def is_zenius_blog_url(url: str) -> bool:
    """Check if URL is a Zenius.net /blog/ article page."""
    try:
        p = urlparse(url)
    except Exception:
        return False
    if not (p.scheme and p.netloc):
        return False
    if not p.netloc.lower().endswith("zenius.net"):
        return False
    path = (p.path or "").lower()
    return "/blog/" in path and len(path) > len("/blog/")

# Headers ringan untuk mengurangi blokir/403 dari search engines.
_DEFAULT_SEARCH_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    ),
    "Accept-Language": "id-ID,id;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ---------------------------------------------------------------------------
# Search query generator
# ---------------------------------------------------------------------------

# Kombinasi query untuk search engine — COMPREHENSIVE
# Semua query sengaja dalam bahasa Indonesia agar hasil relevan
# Menggunakan site: operator untuk diversifikasi sumber
SEARCH_QUERIES: list[str] = [
    # ===== SUMBER: PORTAL PEMERINTAH (.go.id) - UPDATED FOR NEW MINISTRIES =====
    'site:go.id kurikulum merdeka materi pelajaran',
    'site:go.id modul ajar SD SMP SMA',
    'site:go.id capaian pembelajaran kurikulum',
    'site:go.id asesmen nasional AKM',
    'site:go.id ujian nasional soal',
    'site:go.id profil pelajar pancasila',
    'site:go.id guru penggerak kurikulum',
    'site:go.id silabus RPP kurikulum merdeka',
    'site:go.id pendidikan dasar menengah',
    'site:go.id buku teks pelajaran',
    'site:go.id peraturan kurikulum pendidikan',
    'site:kemdikdasmen.go.id sekolah rakyat kurikulum',
    'site:kemdiktisaintek.go.id pendidikan tinggi riset',
    'site:bskap.kemdikdasmen.go.id standar kurikulum asesmen',
    'site:ristekbrin.go.id penelitian pendidikan',
    
    # ===== SUMBER: UNIVERSITAS & AKADEMIK (.ac.id) =====
    'site:ac.id kurikulum merdeka penelitian',
    'site:ac.id jurnal pendidikan matematika',
    'site:ac.id jurnal pendidikan sains fisika kimia biologi',
    'site:ac.id jurnal pendidikan bahasa Indonesia',
    'site:ac.id jurnal pendidikan IPS ekonomi sejarah',
    'site:ac.id asesmen kognitif siswa',
    'site:ac.id modul ajar kurikulum',
    'site:ac.id pendidikan karakter pancasila',
    'site:ac.id evaluasi pembelajaran sekolah',
    'site:ac.id taksonomi Bloom HOTS pendidikan',
    
    # ===== SUMBER: SEKOLAH (.sch.id) =====
    'site:sch.id materi pelajaran kurikulum merdeka',
    'site:sch.id soal ujian pembahasan',
    'site:sch.id modul ajar guru',
    'site:sch.id RPP silabus kurikulum',
    
    # ===== SUMBER: PLATFORM EDUKASI INDONESIA =====
    'site:zenius.net materi pelajaran',
    'site:zenius.net soal pembahasan matematika fisika',
    'site:ruangguru.com materi pelajaran kurikulum',
    'site:ruangguru.com soal latihan pembahasan',
    'site:quipper.com materi pelajaran Indonesia',
    'site:kelaspintar.id materi kurikulum merdeka',
    'site:utbk.id soal pembahasan',
    
    # ===== SUMBER: NEWS ONLINE - PENDIDIKAN =====
    'site:kompas.com pendidikan kurikulum merdeka sekolah',
    'site:kompas.id pendidikan kurikulum Indonesia',
    'site:detik.com pendidikan kurikulum merdeka sekolah',
    'site:tempo.co pendidikan kurikulum Indonesia',
    'site:cnnindonesia.com education pendidikan',
    'site:cnbcindonesia.com education pendidikan',
    'site:republika.co.id pendidikan kurikulum sekolah',
    'site:tribunnews.com pendidikan sekolah kurikulum',
    'site:antaranews.com pendidikan sekolah Indonesia',
    'site:suara.com pendidikan sekolah kurikulum',
    'site:kontan.co.id pendidikan sekolah vokasi',
    'site:bisnis.com pendidikan sekolah Indonesia',
    'site:katadata.co.id pendidikan riset Indonesia',
    'site:nu.or.id pendidikan islam kurikulum',
    'site:muhammadiyah.or.id pendidikan sekolah',
    
    # ===== KURIKULUM & PENDIDIKAN UMUM (tanpa site:) =====
    '"Kurikulum Merdeka" materi pelajaran Indonesia',
    '"Kurikulum Merdeka" modul ajar SD Indonesia',
    '"Kurikulum Merdeka" modul ajar SMP Indonesia',
    '"Kurikulum Merdeka" modul ajar SMA Indonesia',
    '"Kurikulum Nasional" Indonesia pendidikan',
    '"Capaian Pembelajaran" kurikulum merdeka Indonesia',
    '"Alur Tujuan Pembelajaran" ATP kurikulum merdeka',
    '"Sekolah Rakyat" pendidikan Indonesia',
    'rencana pelaksanaan pembelajaran kurikulum merdeka',
    '"modul ajar" guru penggerak kurikulum merdeka',
    '"Kemdikdasmen" kurikulum pendidikan',
    '"Kemdiktisaintek" pendidikan tinggi',
    
    # ===== ASESMEN & UJIAN =====
    '"Ujian Nasional" soal pembahasan Indonesia',
    '"Asesmen Nasional" soal AKM Indonesia',
    '"Asesmen Kompetensi Minimum" AKM soal pembahasan',
    '"bank soal" ujian nasional Indonesia',
    'latihan soal ujian SD SMP SMA Indonesia',
    'soal dan pembahasan ujian nasional Indonesia',
    'soal AKM literasi numerasi pembahasan',
    
    # ===== STEM: MATEMATIKA =====
    'materi matematika SD kurikulum merdeka Indonesia',
    'materi matematika SMP kurikulum merdeka',
    'materi matematika SMA kurikulum merdeka',
    'soal matematika SMP pembahasan lengkap',
    'soal matematika SMA pembahasan lengkap',
    'olimpiade matematika Indonesia OSN soal pembahasan',
    'rumus matematika SMA lengkap Indonesia',
    
    # ===== STEM: FISIKA =====
    'materi fisika SMA kurikulum merdeka Indonesia',
    'soal fisika SMA pembahasan lengkap',
    'olimpiade fisika Indonesia soal pembahasan',
    'rumus fisika SMA lengkap',
    
    # ===== STEM: KIMIA =====
    'materi kimia SMA kurikulum merdeka Indonesia',
    'soal kimia SMA pembahasan lengkap',
    'olimpiade kimia Indonesia soal pembahasan',
    
    # ===== STEM: BIOLOGI =====
    'materi biologi SMA kurikulum merdeka Indonesia',
    'materi IPA biologi SMP kurikulum merdeka',
    'soal biologi SMA pembahasan lengkap',
    'olimpiade biologi Indonesia soal pembahasan',
    
    # ===== NON-STEM =====
    'materi ekonomi SMA kurikulum merdeka Indonesia',
    'soal ekonomi SMA pembahasan',
    'materi sejarah Indonesia SMA kurikulum merdeka',
    'soal sejarah Indonesia SMA pembahasan',
    'materi sosiologi SMA kurikulum merdeka Indonesia',
    'materi geografi SMA kurikulum merdeka Indonesia',
    'materi IPS SMP kurikulum merdeka Indonesia',
    
    # ===== PKN / PPKN =====
    'materi PPKN kurikulum merdeka Indonesia',
    'materi pendidikan kewarganegaraan SD SMP SMA',
    'soal PPKN Pancasila pembahasan',
    '"Pendidikan Pancasila" materi kurikulum merdeka',
    
    # ===== BAHASA INDONESIA =====
    'materi bahasa Indonesia SD kurikulum merdeka',
    'materi bahasa Indonesia SMP kurikulum merdeka',
    'materi bahasa Indonesia SMA kurikulum merdeka',
    'soal bahasa Indonesia pembahasan ujian',
    
    # ===== BAHASA INGGRIS =====
    'materi bahasa Inggris SMP kurikulum merdeka',
    'materi bahasa Inggris SMA kurikulum merdeka',
    'soal bahasa Inggris SMP SMA pembahasan',
    
    # ===== SOAL QNA PER LEVEL KESULITAN =====
    'soal pilihan ganda SD kelas 4 5 6 pembahasan Indonesia',
    'soal pilihan ganda SMP kelas 7 8 9 pembahasan Indonesia',
    'soal pilihan ganda SMA kelas 10 11 12 pembahasan',
    'soal olimpiade sains nasional OSN Indonesia',
    'soal HOTS matematika SMA pembahasan',
    'soal sulit olimpiade matematika fisika Indonesia',
    'contoh soal mudah menengah sulit SMA pembahasan',
    
    # ===== KOGNITIF & PEDAGOGIK =====
    'kemampuan kognitif siswa pendidikan Indonesia',
    'pembelajaran berdiferensiasi kurikulum merdeka',
    'pendidikan karakter nasionalisme siswa Indonesia',
    'projek penguatan profil pelajar Pancasila P5',
    
    # ===== JURNAL PENDIDIKAN & RISET =====
    'jurnal pendidikan Indonesia kurikulum merdeka',
    'jurnal penelitian pendidikan matematika Indonesia',
    'jurnal pendidikan sains Indonesia',
    'jurnal asesmen evaluasi pendidikan Indonesia',
    'garuda.kemdikbud.go.id jurnal pendidikan',
    'sinta.kemdikbud.go.id pendidikan',
    'neliti.com pendidikan Indonesia',
]

# Domain pendidikan Indonesia yang kita prioritaskan
PRIORITY_DOMAINS: set[str] = {
    # Pemerintah pendidikan (updated for new ministries)
    "kemdikbud.go.id",
    "kemdikbudristek.go.id",
    "kemendikbud.go.id",
    "kemendikdasmen.go.id",
    "kemdiktisaintek.go.id",
    "bskap.kemdikdasmen.go.id",
    "ristekbrin.go.id",
    "belajar.id",
    "guru.kemdikbud.go.id",
    "merdekamengajar.kemdikbud.go.id",
    "pusmendik.kemdikbud.go.id",
    "mediakeuangan.kemenkeu.go.id",
    # Platform edukasi
    "www.zenius.net",
    "www.ruangguru.com",
    "www.quipper.com",
    "www.kelaspintar.id",
    "www.pahamify.com",
    # Blog & portal guru
    "gurusiana.id",
    "indonesiana.id",
    "gurusd.web.id",
    "operatorsekolah.com",
    "sekolahdasar.net",
    "koranedukasi.id",
    "pintar.tanotofoundation.org",
    "inovasi.or.id",
    "pspk.id",
    # Berita pendidikan (news online)
    "edukasi.kompas.com",
    "www.kompas.id",
    "www.detik.com",
    "www.tempo.co",
    "www.cnnindonesia.com",
    "www.cnbcindonesia.com",
    "www.republika.co.id",
    "www.tribunnews.com",
    "www.antaranews.com",
    "pendidikan.suara.com",
    "www.kontan.co.id",
    "www.bisnis.com",
    "katadata.co.id",
    "www.nu.or.id",
    "www.muhammadiyah.or.id",
    # Jurnal & Repository
    "garuda.kemdikbud.go.id",
    "sinta.kemdikbud.go.id",
    "neliti.com",
    "repository.ui.ac.id",
    "repository.ugm.ac.id",
    "repository.itb.ac.id",
    # New domains (zara_adjust.md)
    "www.detik.com",
    "detik.com",
    "www.ruangguru.com",
    "ruangguru.com",
    "www.liputan6.com",
    "liputan6.com",
    # New domains (zara_adjust_1.md)
    "www.republika.co.id",
    "republika.co.id",
    "news.republika.co.id",
    "www.quipper.com",
    "quipper.com",
    "www.zenius.net",
    "zenius.net",
}

# Domain yang TIDAK boleh di-crawl (search engine, sosmed, e-commerce, berita asing)
BLOCKED_DOMAINS: set[str] = {
    # Search engines
    "google.com", "google.co.id", "www.google.com", "www.google.co.id",
    "bing.com", "www.bing.com",
    "duckduckgo.com", "html.duckduckgo.com",
    "yahoo.com", "search.yahoo.com",
    # Social media
    "youtube.com", "www.youtube.com", "m.youtube.com",
    "facebook.com", "www.facebook.com", "m.facebook.com",
    "twitter.com", "x.com",
    "instagram.com", "www.instagram.com",
    "tiktok.com", "www.tiktok.com",
    "linkedin.com", "www.linkedin.com",
    "pinterest.com", "reddit.com", "www.reddit.com",
    "quora.com", "www.quora.com",
    # App stores
    "play.google.com", "apps.apple.com",
    # E-commerce
    "amazon.com", "www.amazon.com",
    "shopee.co.id", "tokopedia.com", "bukalapak.com", "lazada.co.id",
    "blibli.com", "jd.id",
    # Non-Indonesian foreign sites
    "en.wikipedia.org", "en.wikibooks.org",
    "britannica.com", "www.britannica.com",
    "khanacademy.org", "www.khanacademy.org",
    "coursera.org", "www.coursera.org",
    "edx.org", "www.edx.org",
    "medium.com",
    "scribd.com", "www.scribd.com",
    "slideshare.net", "www.slideshare.net",
    "academia.edu", "www.academia.edu",
    "researchgate.net", "www.researchgate.net",
    # Non-Indonesian Wikipedia variants
    "ms.wikipedia.org", "min.wikipedia.org", "su.wikipedia.org",
    "jv.wikipedia.org", "ban.wikipedia.org", "ace.wikipedia.org",
    "bug.wikipedia.org", "map-bms.wikipedia.org",
    "de.wikipedia.org", "fr.wikipedia.org", "es.wikipedia.org",
    "ja.wikipedia.org", "zh.wikipedia.org", "pt.wikipedia.org",
    "ru.wikipedia.org", "ar.wikipedia.org", "hi.wikipedia.org",
    # Archive
    "web.archive.org",
}

# Pola URL yang harus di-blokir (junk pages + advertisement/commercial)
_BLOCKED_URL_PATTERNS: list[str] = [
    # Wikipedia non-article pages
    "/w/index.php",
    "action=edit",
    "veaction=edit",
    "action=history",
    "oldid=",
    "diff=",
    "/wiki/Special:",
    "/wiki/Kategori:",
    "/wiki/Category:",
    "/wiki/Pembicaraan:",
    "/wiki/Talk:",
    "/wiki/Pengguna:",
    "/wiki/User:",
    "/wiki/Wikipedia:",
    "/wiki/Portal:",
    "/wiki/Templat:",
    "/wiki/Template:",
    "/wiki/Berkas:",
    "/wiki/File:",
    "/wiki/Modul:",
    "/wiki/Module:",
    # Indonesian-language Wikipedia special pages
    "/wiki/Istimewa:",
    "/wiki/Halaman_pembicaraan:",
    "printable=yes",
    "mobileaction=",
    # Login/auth
    "/login",
    "/register",
    "/signup",
    "/auth/",
    # API endpoints
    "/api/",
    "/feed/",
    "/rss",
    "wp-json/",
    # ===== COMMERCIAL / ADVERTISEMENT / PRODUCT PAGES =====
    # Payment & pricing
    "bayar.",
    "/bayar",
    "/pricing",
    "/harga",
    "/paket",
    "/package/",
    "/checkout",
    "/payment",
    "/subscribe",
    "/subscription",
    "/langganan",
    "/promo",
    "/diskon",
    "/voucher",
    "voucher_serial=",
    # Landing / product pages (edu platforms)
    "/ruangbelajar",
    "/ruanguji",
    "/ruangles",
    "/privat",
    "/for-kids",
    "/skill-academy",
    "/roboguru",
    "pengajar.",
    # Referral & tracking
    "referralcookiesid=",
    "utm_source=",
    "utm_medium=",
    "utm_campaign=",
    # Career/hiring
    "/karir",
    "/career",
    "/jobs",
    "/lowongan",
    # About/contact (not educational content)
    "/about-us",
    "/tentang-kami",
    "/contact",
    "/hubungi",
    "/terms",
    "/privacy",
    "/kebijakan-privasi",
    "/syarat-ketentuan",
]

# Ekstensi file yang harus di-skip
SKIP_EXTENSIONS: set[str] = {
    ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
    ".zip", ".rar", ".7z", ".tar", ".gz",
    ".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp",
    ".mp3", ".mp4", ".avi", ".mov", ".wav",
    ".exe", ".msi", ".dmg", ".apk",
}


def is_valid_crawl_url(url: str) -> bool:
    """Cek apakah URL layak untuk di-crawl."""
    try:
        parsed = urlparse(url)
    except Exception:
        return False

    # Harus http/https
    if parsed.scheme not in ("http", "https"):
        return False

    # Harus punya hostname
    if not parsed.netloc:
        return False

    domain = parsed.netloc.lower()

    # Block domain terlarang
    for blocked in BLOCKED_DOMAINS:
        if domain == blocked or domain.endswith("." + blocked):
            return False

    # Skip file binary
    path_lower = parsed.path.lower()
    for ext in SKIP_EXTENSIONS:
        if path_lower.endswith(ext):
            return False

    # Block junk URL patterns (case-insensitive)
    url_lower = url.lower()
    for pattern in _BLOCKED_URL_PATTERNS:
        if pattern.lower() in url_lower:
            return False

    return True


# Indikator bahwa URL kemungkinan konten pendidikan Indonesia
# URL harus mengandung minimal satu indikator PENDIDIKAN
_URL_EDU_INDICATORS: list[str] = [
    # Pendidikan & kurikulum
    "kurikulum", "pendidikan", "pelajaran", "belajar", "modul",
    "sekolah", "guru", "siswa", "materi", "ajar", "ujian", "asesmen",
    "soal", "pembahasan", "latihan", "olimpiade", "bank-soal",
    "edukasi", "pembelajaran", "evaluasi", "kognitif", "pedagogik",
    # Jenjang
    "sekolah-dasar", "sekolah-menengah",
    # Mata pelajaran (spesifik)
    "matematika", "fisika", "kimia", "biologi", "ekonomi",
    "sosiologi", "geografi", "ppkn", "pkn", "kewarganegaraan",
    "bahasa-indonesia", "bahasa-inggris", "sains",
    # Domain Indonesia pendidikan
    "dikbud", "diknas", "dikdasmen", "dikti",
    ".sch.id",
    # Platform edukasi Indonesia
    "zenius", "ruangguru", "quipper", "pahamify", "roboguru",
    # Blog & portal guru / pendidikan Indonesia
    "gurusiana", "indonesiana", "gurusd", "operatorsekolah",
    "sekolahdasar", "ainamulyana", "gurune", "kangmartho",
    "sejutaguru", "informasiguru", "pak-anang", "kangdidik",
    "saling-sapa", "koranedukasi", "pintar.tanotofoundation",
    "inovasi.or.id", "pspk.id",
    # Kanal berita pendidikan
    "/edu", "edukasi.", "/pendidikan",
]

# Domain yang otomatis dianggap relevan (pemerintah pendidikan, universitas)
_AUTO_ACCEPT_DOMAIN_PARTS: list[str] = [
    ".go.id", ".ac.id", ".sch.id",
    "kemdikbud", "kemendikbud", "dikdasmen", "belajar.id",
    # Blog/portal guru & riset pendidikan
    "gurusiana.id", "indonesiana.id", "gurusd.web.id",
    "operatorsekolah.com", "sekolahdasar.net",
    "koranedukasi.id", "pspk.id", "inovasi.or.id",
    "pintar.tanotofoundation.org",
]


def is_indonesian_education_url(url: str) -> bool:
    """Heuristik ketat: apakah URL konten pendidikan Indonesia.

    Domain .go.id / .ac.id / .sch.id otomatis relevan.
    Lainnya harus mengandung indikator pendidikan di URL path.
    """
    url_lower = url.lower()

    # Auto-accept trusted Indonesian domains
    for part in _AUTO_ACCEPT_DOMAIN_PARTS:
        if part in url_lower:
            return True

    # Untuk domain lain, harus ada indikator pendidikan di URL
    return any(ind in url_lower for ind in _URL_EDU_INDICATORS)


# ---------------------------------------------------------------------------
# Search engine URL builders
# ---------------------------------------------------------------------------

def build_duckduckgo_url(query: str, page: int = 0) -> str:
    """Build DuckDuckGo HTML search URL."""
    params = {"q": query}
    if page > 0:
        params["s"] = str(page * 30)  # DDG pagination offset
        params["dc"] = str(page * 30 + 1)
    return "https://html.duckduckgo.com/html/?" + urlencode(params)


def build_bing_url(query: str, page: int = 0) -> str:
    """Build Bing search URL."""
    params = {"q": query}
    if page > 0:
        params["first"] = str(page * 10 + 1)
    return "https://www.bing.com/search?" + urlencode(params)


# ---------------------------------------------------------------------------
# URL extractor from search results
# ---------------------------------------------------------------------------

def extract_urls_from_duckduckgo(html: str) -> list[str]:
    """Extract result URLs from DuckDuckGo HTML response."""
    urls: list[str] = []

    # DDG result links: class="result__a" href="..."
    # or uddg= parameter in redirect URLs
    href_pattern = re.compile(r'href="([^"]*)"', re.IGNORECASE)
    uddg_pattern = re.compile(r'uddg=([^&"]+)', re.IGNORECASE)

    for match in href_pattern.finditer(html):
        href = match.group(1)
        # DDG uses redirect URLs with uddg= parameter
        uddg_match = uddg_pattern.search(href)
        if uddg_match:
            from urllib.parse import unquote
            real_url = unquote(uddg_match.group(1))
            if is_valid_crawl_url(real_url):
                urls.append(real_url)
        elif href.startswith("http") and "duckduckgo.com" not in href:
            if is_valid_crawl_url(href):
                urls.append(href)

    return list(dict.fromkeys(urls))  # Deduplicate, preserve order


def extract_urls_from_bing(html: str) -> list[str]:
    """Extract result URLs from Bing HTML response."""
    urls: list[str] = []
    # Bing result links: <a href="..." in <li class="b_algo">
    # Simpler: grab all <cite> or <a href="http..."> that are not bing.com
    href_pattern = re.compile(r'<a\s+href="(https?://[^"]+)"', re.IGNORECASE)

    def _decode_bing_redirect(href: str) -> str | None:
        """Decode Bing redirect URLs (ck/a) into the real target URL.

        Bing sering menyimpan URL target dalam query param `u` (base64-like),
        misal: u=a1aHR0cHM6Ly93d3cu...
        """
        try:
            parsed = urlparse(href)
            host = (parsed.netloc or "").lower()
            if "bing.com" not in host:
                return None

            qs = parse_qs(parsed.query)

            # Common: u=<encoded>
            u = (qs.get("u") or [""])[0]
            u = (u or "").strip()
            if not u:
                # Sometimes: url=<encoded>
                u = (qs.get("url") or [""])[0]
                u = (u or "").strip()
            if not u:
                return None

            # Query values may be percent-encoded
            try:
                from urllib.parse import unquote

                u = unquote(u)
            except Exception:
                pass

            # Some redirects store plain URL
            if u.startswith("http://") or u.startswith("https://"):
                return u

            # Common format: a1<base64(url)>
            if u.startswith("a1") and len(u) > 4:
                b64 = u[2:]
                pad = "=" * ((4 - (len(b64) % 4)) % 4)
                try:
                    raw = base64.urlsafe_b64decode((b64 + pad).encode("utf-8"))
                except Exception:
                    raw = base64.b64decode((b64 + pad).encode("utf-8"))
                txt = raw.decode("utf-8", errors="ignore")
                m = re.search(r"https?://[^\s\"'<>]+", txt)
                if m:
                    return m.group(0)
                if txt.startswith("http://") or txt.startswith("https://"):
                    return txt

        except Exception:
            return None

        return None

    for match in href_pattern.finditer(html):
        href = match.group(1)
        real = _decode_bing_redirect(href) or href
        if is_valid_crawl_url(real):
            urls.append(real)

    return list(dict.fromkeys(urls))


def extract_links_from_page(html: str, base_url: str) -> list[str]:
    """Extract all links from a crawled page for spidering."""
    urls: list[str] = []
    href_pattern = re.compile(r'href="([^"]*)"', re.IGNORECASE)

    for match in href_pattern.finditer(html):
        href = match.group(1)
        # Skip anchors, javascript, mailto
        if href.startswith(("#", "javascript:", "mailto:", "tel:")):
            continue
        # Resolve relative URLs
        try:
            full_url = urljoin(base_url, href)
        except Exception:
            continue
        if is_valid_crawl_url(full_url):
            urls.append(full_url)

    return list(dict.fromkeys(urls))


# ---------------------------------------------------------------------------
# Discovery Engine
# ---------------------------------------------------------------------------

class DiscoveryEngine:
    """Mesin pencarian URL menggunakan search engine."""

    def __init__(
        self,
        max_pages_per_query: int = 3,
        shard_index: int = 0,
        shard_count: int = 1,
        only_domain: str | None = None,
        keywords_file: str | None = None,
    ) -> None:
        self.max_pages_per_query = max_pages_per_query
        self.query_index: int = 0

        # Optional sharding for multi-worker setups.
        # Each worker processes only 1/N of generated search URLs.
        self.shard_count = max(1, int(shard_count))
        self.shard_index = int(shard_index) % self.shard_count

        # If set, generate site: queries to focus discovery.
        self.only_domain = self._normalize_domain(only_domain)

        # Optional keyword phrases (used for Kompas tag discovery)
        kw_path = None
        if keywords_file:
            try:
                kw_path = Path(keywords_file)
            except Exception:
                kw_path = None
        if kw_path is None:
            kw_path = _repo_root() / "keywords.txt"
        self.keyword_phrases: list[str] = []
        if kw_path and kw_path.exists():
            self.keyword_phrases = load_keyword_phrases_from_file(kw_path)

    def _normalize_domain(self, domain: str | None) -> str | None:
        raw = (domain or "").strip().lower()
        if not raw:
            return None
        raw = raw.replace("https://", "").replace("http://", "")
        raw = raw.split("/")[0]
        return raw or None

    def _build_site_queries(self, site_domain: str) -> list[str]:
        """Generate queries seperti: 'materi sma <keyword> site:kompas.com'."""
        site = self._normalize_domain(site_domain) or site_domain
        site_part = f"site:{site}"

        # Fokus pakai keyword berbasis frasa agar hasil lebih presisi
        phrase_keywords = [kw for kw in TARGET_KEYWORDS if len((kw or "").split()) >= 2]
        # Jika keyword list terlalu besar, cukup sebagian saja; sisanya akan tercakup di cycle berikutnya
        # lewat sharding pada search_urls.

        templates = [
            f"materi sma {{kw}} {site_part}",
            f"materi smp {{kw}} {site_part}",
            f"materi sd {{kw}} {site_part}",
            f"contoh soal {{kw}} {site_part}",
            f"bank soal {{kw}} {site_part}",
            f"kurikulum {{kw}} {site_part}",
            f"asesmen {{kw}} {site_part}",
        ]

        queries: list[str] = []
        for kw in phrase_keywords:
            kw = (kw or "").strip()
            if not kw:
                continue
            for tpl in templates:
                queries.append(tpl.format(kw=kw))
            queries.append(f"{kw} {site_part}")

        # Deduplicate preserve order
        queries = list(dict.fromkeys(queries))

        # Safety cap: avoid exploding query count (still enough for endless crawling)
        return queries[:400]

    def _get_kompas_tag_urls(self) -> list[tuple[str, str]]:
        """Generate Kompas tag pages from keyword phrases.

        These pages list related articles; the discovery worker will fetch the
        tag page and extract `/read/` links.
        """
        if not self.keyword_phrases:
            return []

        urls: list[tuple[str, str]] = []
        for phrase in self.keyword_phrases:
            slug = kompas_tag_slug(phrase)
            if not slug:
                continue
            urls.append((f"https://www.kompas.com/tag/{slug}", "kompas_tag"))

        # Deduplicate preserve order
        urls = list(dict.fromkeys(urls))
        return urls

    def _get_queries(self) -> list[str]:
        if self.only_domain:
            return self._build_site_queries(self.only_domain)
        return SEARCH_QUERIES

    def get_all_search_urls(self) -> list[tuple[str, str]]:
        """Generate semua search URL.

        Returns:
            List of (search_url, engine_name) tuples.
        """
        search_urls: list[tuple[str, str]] = []

        # Focused Kompas mode: prefer tag pages over external search engines
        if self.only_domain and self.only_domain.endswith("kompas.com"):
            search_urls = self._get_kompas_tag_urls()
        # Focused mode for new domains: use search engine queries with site: operator
        elif self.only_domain and (
            self.only_domain.endswith("detik.com")
            or self.only_domain.endswith("ruangguru.com")
            or self.only_domain.endswith("liputan6.com")
            or self.only_domain.endswith("republika.co.id")
            or self.only_domain.endswith("quipper.com")
            or self.only_domain.endswith("zenius.net")
        ):
            for query in self._get_queries():
                for page in range(self.max_pages_per_query):
                    search_urls.append((build_duckduckgo_url(query, page), "duckduckgo"))
                    search_urls.append((build_bing_url(query, page), "bing"))
        else:
            for query in self._get_queries():
                for page in range(self.max_pages_per_query):
                    search_urls.append((build_duckduckgo_url(query, page), "duckduckgo"))
                    search_urls.append((build_bing_url(query, page), "bing"))

        if self.shard_count > 1:
            search_urls = [
                item
                for idx, item in enumerate(search_urls)
                if (idx % self.shard_count) == self.shard_index
            ]

        return search_urls

    def get_next_batch(self, batch_size: int = 6) -> list[tuple[str, str]]:
        """Get next batch of search URLs to process.

        Returns empty list when all queries exhausted (then resets for next cycle).
        """
        all_urls = self.get_all_search_urls()

        if self.query_index >= len(all_urls):
            self.query_index = 0  # Reset untuk cycle berikutnya
            return []

        batch = all_urls[self.query_index : self.query_index + batch_size]
        self.query_index += batch_size
        return batch

    async def search_one(self, search_url: str, engine: str) -> list[str]:
        """Run one search query and return list of found URLs using HTTP fetch.

        Previously this used Crawl4AI to fetch search result pages; now we use
        a simple aiohttp GET and extract links from the HTML.
        """
        try:
            timeout = ClientTimeout(total=30)
            async with aiohttp.ClientSession(timeout=timeout, headers=_DEFAULT_SEARCH_HEADERS) as session:
                async with session.get(search_url) as resp:
                    if resp.status >= 400:
                        logger.warning("Search fetch failed (%s): HTTP %d", engine, resp.status)
                        return []
                    body = await resp.text(errors="ignore")

            if engine == "kompas_tag":
                # Extract only Kompas article links from tag pages
                links = extract_links_from_page(body, search_url)
                found = [u for u in links if is_kompas_article_url(u) and is_valid_crawl_url(u)]
                # Prioritize EDU/Skola section URLs first
                try:
                    found.sort(
                        key=lambda u: 0
                        if (
                            u.startswith("https://www.kompas.com/edu/")
                            or u.startswith("https://www.kompas.com/skola/")
                        )
                        else 1
                    )
                except Exception:
                    pass
            elif engine == "duckduckgo":
                found = extract_urls_from_duckduckgo(body)
            elif engine == "bing":
                found = extract_urls_from_bing(body)
            else:
                found = []

            logger.info("Search [%s] → %d URL ditemukan", engine, len(found))
            return found

        except Exception as exc:
            logger.error("Exception pada search (%s): %s", engine, exc)
            return []
