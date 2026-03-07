"""
Search-engine URL discovery untuk crawler.

Menggunakan DuckDuckGo HTML dan Bing untuk menemukan URL relevan
berdasarkan keyword pendidikan Indonesia secara otomatis.
Juga mengekstrak link dari halaman yang sudah di-crawl (spidering).
"""

from __future__ import annotations

import logging
import re
from urllib.parse import urlencode, urlparse, urljoin, parse_qs
from typing import AsyncIterator

from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode

from config import TARGET_KEYWORDS

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Search query generator
# ---------------------------------------------------------------------------

# Kombinasi query untuk search engine — COMPREHENSIVE
# Semua query sengaja dalam bahasa Indonesia agar hasil relevan
# Menggunakan site: operator untuk diversifikasi sumber
SEARCH_QUERIES: list[str] = [
    # ===== SUMBER: PORTAL PEMERINTAH (.go.id) =====
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
    'site:go.id kemendikdasmen sekolah rakyat',

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

    # ===== BERITA PENDIDIKAN =====
    'site:kompas.com pendidikan kurikulum merdeka',
    'site:tempo.co pendidikan kurikulum Indonesia',
    'site:detik.com pendidikan kurikulum merdeka sekolah',
    'site:republika.co.id pendidikan kurikulum sekolah',
    'site:jpnn.com pendidikan kurikulum Indonesia',

    # ===== JURNAL PENDIDIKAN =====
    'jurnal pendidikan Indonesia kurikulum merdeka',
    'jurnal penelitian pendidikan matematika Indonesia',
    'jurnal pendidikan sains Indonesia',
    'jurnal asesmen evaluasi pendidikan Indonesia',
]

# Domain pendidikan Indonesia yang kita prioritaskan
PRIORITY_DOMAINS: set[str] = {
    # Pemerintah pendidikan
    "kemdikbud.go.id",
    "kemdikbudristek.go.id",
    "kemendikbud.go.id",
    "kemendikdasmen.go.id",
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
    # Berita pendidikan
    "edukasi.kompas.com",
    "tirto.id",
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

    for match in href_pattern.finditer(html):
        href = match.group(1)
        if is_valid_crawl_url(href):
            urls.append(href)

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

    def __init__(self, max_pages_per_query: int = 3) -> None:
        self.max_pages_per_query = max_pages_per_query
        self.query_index: int = 0

    def get_all_search_urls(self) -> list[tuple[str, str]]:
        """Generate semua search URL.

        Returns:
            List of (search_url, engine_name) tuples.
        """
        search_urls: list[tuple[str, str]] = []

        for query in SEARCH_QUERIES:
            for page in range(self.max_pages_per_query):
                search_urls.append((build_duckduckgo_url(query, page), "duckduckgo"))
                search_urls.append((build_bing_url(query, page), "bing"))

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

    async def search_one(
        self,
        search_url: str,
        engine: str,
        crawler: AsyncWebCrawler,
    ) -> list[str]:
        """Jalankan satu search query dan kembalikan list URL yang ditemukan."""
        try:
            config = CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS,  # Selalu fresh dari search engine
                word_count_threshold=0,
                wait_until="domcontentloaded",
                page_timeout=30000,
                verbose=False,
            )
            result = await crawler.arun(url=search_url, config=config)

            if not result.success:
                logger.warning("Search gagal (%s): %s", engine, result.error_message)
                return []

            html = result.html or ""
            if engine == "duckduckgo":
                found = extract_urls_from_duckduckgo(html)
            elif engine == "bing":
                found = extract_urls_from_bing(html)
            else:
                found = []

            logger.info("Search [%s] → %d URL ditemukan", engine, len(found))
            return found

        except Exception as exc:
            logger.error("Exception pada search (%s): %s", engine, exc)
            return []
