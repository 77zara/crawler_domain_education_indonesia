"""
PDF Crawler & Extractor untuk Jurnal Ilmiah Indonesia.
Pipeline: Crawl Metadata/Link → Download PDF → Ekstrak Teks (pymupdf4llm) → Cleaning → JSONL
"""

import asyncio
import os
import re
import json
import logging
from pathlib import Path
from typing import Optional, List, Dict, Any
from urllib.parse import urlparse, urljoin

import aiohttp
import pymupdf4llm
from bs4 import BeautifulSoup

from config import Settings, DATA_DIR

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Konfigurasi PDF Crawler
# ---------------------------------------------------------------------------

JOURNAL_SOURCES = [
    # Repository Universitas
    "https://repository.ui.ac.id",
    "https://repository.ugm.ac.id",
    "https://repository.itb.ac.id",
    "https://repository.uny.ac.id",
    "https://eprints.uny.ac.id",
    # Portal Jurnal Nasional
    "https://garuda.kemdikbud.go.id",
    "https://sinta.kemdikbud.go.id",
    "https://neliti.com",
]

# Filter kata kunci untuk topik pendidikan
EDU_KEYWORDS = [
    "pendidikan", "pembelajaran", "kurikulum", "siswa", "guru",
    "sekolah", "mengajar", "asesmen", "evaluasi", "pedagogi",
    "didaktik", "literasi", "numerasi", "karakter", "pancasila"
]


class PDFCrawler:
    """Crawler khusus untuk mendownload dan mengekstrak PDF jurnal ilmiah."""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.session: Optional[aiohttp.ClientSession] = None
        self.output_file = DATA_DIR / "dataset_jurnal.jsonl"
        
    async def start(self):
        """Inisialisasi session aiohttp."""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7",
        }
        self.session = aiohttp.ClientSession(headers=headers)
        
    async def stop(self):
        """Tutup session aiohttp."""
        if self.session:
            await self.session.close()
    
    async def fetch_page(self, url: str) -> Optional[str]:
        """Ambil HTML dari URL."""
        try:
            async with self.session.get(url, timeout=30) as response:
                if response.status == 200:
                    return await response.text(encoding='utf-8')
                else:
                    logger.warning(f"⚠️  HTTP {response.status} untuk {url}")
                    return None
        except Exception as e:
            logger.error(f"❌ Error fetching {url}: {e}")
            return None
    
    async def download_pdf(self, pdf_url: str, save_path: Path) -> bool:
        """Download file PDF ke path tertentu."""
        try:
            async with self.session.get(pdf_url, timeout=60) as response:
                if response.status == 200:
                    content = await response.read()
                    save_path.write_bytes(content)
                    return True
                else:
                    logger.warning(f"⚠️  HTTP {response.status} untuk PDF {pdf_url}")
                    return False
        except Exception as e:
            logger.error(f"❌ Error downloading PDF {pdf_url}: {e}")
            return False
    
    def extract_pdf_links(self, html: str, base_url: str) -> List[str]:
        """Ekstrak semua link PDF dari HTML."""
        soup = BeautifulSoup(html, 'lxml')
        pdf_links = []
        
        # Cari tag <a> dengan href berakhiran .pdf
        for tag in soup.find_all('a', href=True):
            href = tag['href']
            if href.lower().endswith('.pdf'):
                full_url = urljoin(base_url, href)
                pdf_links.append(full_url)
        
        # Cari juga di embed/object tag
        for tag in soup.find_all(['embed', 'object'], src=True):
            src = tag.get('src', '')
            if src.lower().endswith('.pdf'):
                full_url = urljoin(base_url, src)
                pdf_links.append(full_url)
        
        return list(set(pdf_links))  # Deduplikasi
    
    def is_education_related(self, text: str, title: str = "") -> bool:
        """Cek apakah teks/judul terkait pendidikan."""
        combined = f"{title} {text}".lower()
        match_count = sum(1 for kw in EDU_KEYWORDS if kw in combined)
        return match_count >= 2  # Minimal 2 keyword match
    
    def extract_metadata_from_html(self, html: str, url: str) -> Dict[str, Any]:
        """Ekstrak metadata (judul, penulis, dll) dari halaman HTML jurnal."""
        soup = BeautifulSoup(html, 'lxml')
        
        metadata = {
            "title": "",
            "authors": [],
            "abstract": "",
            "source_url": url,
        }
        
        # Coba ekstrak judul (biasanya di h1, title tag, atau meta)
        title_tag = soup.find('h1') or soup.find('title')
        if title_tag:
            metadata["title"] = title_tag.get_text(strip=True)
        
        # Meta tag
        for meta in soup.find_all('meta'):
            name = meta.get('name', '').lower()
            content = meta.get('content', '')
            if 'citation_title' in name:
                metadata["title"] = content
            elif 'citation_author' in name:
                metadata["authors"].append(content)
            elif 'description' in name or 'dc.description' in name:
                metadata["abstract"] = content
        
        # Abstract biasanya di div/class tertentu
        abstract_div = soup.find(class_=re.compile(r'abstract|abstrak|summary', re.I))
        if abstract_div:
            metadata["abstract"] = abstract_div.get_text(strip=True)[:2000]  # Batasi panjang
        
        return metadata
    
    async def process_pdf(self, pdf_path: Path) -> Optional[str]:
        """Ekstrak teks dari PDF menggunakan pymupdf4llm (format Markdown)."""
        try:
            markdown_text = pymupdf4llm.to_markdown(str(pdf_path))
            return markdown_text
        except Exception as e:
            logger.error(f"❌ Error extracting PDF {pdf_path}: {e}")
            return None
    
    def clean_extracted_text(self, text: str) -> str:
        """Bersihkan teks hasil ekstraksi PDF."""
        if not text:
            return ""
        
        # Hapus header/footer berulang (biasanya ada nomor halaman, nama jurnal)
        lines = text.split('\n')
        cleaned_lines = []
        for line in lines:
            # Skip line yang terlalu pendek (kemungkinan nomor halaman)
            if len(line.strip()) < 3:
                continue
            # Skip line yang hanya angka
            if line.strip().isdigit():
                continue
            cleaned_lines.append(line)
        
        text = '\n'.join(cleaned_lines)
        
        # Normalisasi spasi
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r'\n{3,}', '\n\n', text)  # Maksimal 2 newline berturut-turut
        
        return text.strip()
    
    async def crawl_journal_page(self, url: str) -> List[Dict[str, Any]]:
        """Crawl satu halaman jurnal, download PDF, ekstrak, simpan ke JSONL."""
        results = []
        
        logger.info(f"📄 Crawling jurnal: {url}")
        html = await self.fetch_page(url)
        if not html:
            return results
        
        # Ekstrak metadata dari halaman
        metadata = self.extract_metadata_from_html(html, url)
        
        # Cek relevansi berdasarkan judul/abstrak
        if not self.is_education_related(metadata.get("abstract", ""), metadata.get("title", "")):
            logger.info(f"⏭  Tidak relevan (bukan pendidikan): {url}")
            return results
        
        # Ekstrak link PDF
        pdf_links = self.extract_pdf_links(html, url)
        if not pdf_links:
            logger.info(f"⚠️  Tidak ada PDF ditemukan di {url}")
            return results
        
        logger.info(f"🔗 Ditemukan {len(pdf_links)} PDF di {url}")
        
        # Proses setiap PDF
        for i, pdf_url in enumerate(pdf_links[:5]):  # Batasi 5 PDF per halaman
            logger.info(f"⬇️  Downloading PDF {i+1}/{len(pdf_links)}: {pdf_url}")
            
            # Generate filename unik
            parsed = urlparse(pdf_url)
            filename = Path(parsed.path).name or f"doc_{i}.pdf"
            temp_pdf_path = DATA_DIR / "temp_pdfs" / filename
            temp_pdf_path.parent.mkdir(exist_ok=True)
            
            # Download PDF
            success = await self.download_pdf(pdf_url, temp_pdf_path)
            if not success:
                continue
            
            # Ekstrak teks dari PDF
            markdown_text = await self.process_pdf(temp_pdf_path)
            if not markdown_text:
                temp_pdf_path.unlink(missing_ok=True)
                continue
            
            # Bersihkan teks
            cleaned_text = self.clean_extracted_text(markdown_text)
            
            if len(cleaned_text) < 500:  # Filter konten terlalu pendek
                logger.info(f"⚠️  Teks terlalu pendek: {pdf_url}")
                temp_pdf_path.unlink(missing_ok=True)
                continue
            
            # Buat record
            record = {
                "url": pdf_url,
                "source_page": url,
                "title": metadata.get("title", ""),
                "authors": metadata.get("authors", []),
                "abstract": metadata.get("abstract", ""),
                "content": cleaned_text,
                "word_count": len(cleaned_text.split()),
                "type": "jurnal_ilmiah",
                "level": "umum",  # Jurnal biasanya untuk guru/peneliti
            }
            
            results.append(record)
            
            # Simpan langsung ke JSONL (streaming)
            with open(self.output_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
            
            # Hapus PDF temp setelah diproses
            temp_pdf_path.unlink(missing_ok=True)
            
            logger.info(f"✅ Berhasil memproses PDF: {pdf_url} ({len(cleaned_text)} karakter)")
        
        return results
    
    async def run(self, urls: List[str]):
        """Jalankan crawling untuk daftar URL jurnal."""
        await self.start()
        
        all_results = []
        for url in urls:
            if not is_valid_journal_url(url):
                logger.warning(f"⏭  Skip URL bukan jurnal: {url}")
                continue
            
            results = await self.crawl_journal_page(url)
            all_results.extend(results)
            
            # Rate limiting
            await asyncio.sleep(2)
        
        await self.stop()
        
        logger.info(f"🎉 Selesai! Total {len(all_results)} dokumen jurnal diproses.")
        logger.info(f"📁 Output: {self.output_file}")
        
        return all_results


def is_valid_journal_url(url: str) -> bool:
    """Validasi apakah URL berasal dari sumber jurnal terpercaya."""
    parsed = urlparse(url)
    domain = parsed.netloc.lower()
    
    # Whitelist domain jurnal
    allowed_domains = [
        "garuda.kemdikbud.go.id",
        "sinta.kemdikbud.go.id",
        "neliti.com",
        "repository.ui.ac.id",
        "repository.ugm.ac.id",
        "repository.itb.ac.id",
        "repository.uny.ac.id",
        "eprints.uny.ac.id",
        "journal.uny.ac.id",
        "ejournal.ut.ac.id",
        "jurnal.upi.edu",
        "journal.um.ac.id",
    ]
    
    return any(domain == d or domain.endswith("." + d) for d in allowed_domains)


# ---------------------------------------------------------------------------
# Main Entry Point
# ---------------------------------------------------------------------------

async def main():
    """Contoh penggunaan PDFCrawler."""
    settings = Settings()
    crawler = PDFCrawler(settings)
    
    # Daftar URL jurnal untuk dicrawl (contoh)
    journal_urls = [
        "https://garuda.kemdikbud.go.id/documents?q=pendidikan+kurikulum",
        "https://repository.ui.ac.id/browse?type=title&starts_with=Pendidikan",
        "https://neliti.com/id/publications?search=pembelajaran+sains",
    ]
    
    results = await crawler.run(journal_urls)
    
    print(f"\n📊 Ringkasan:")
    print(f"   Total dokumen: {len(results)}")
    print(f"   Output file: {crawler.output_file}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    asyncio.run(main())
