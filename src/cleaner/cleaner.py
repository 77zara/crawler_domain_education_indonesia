import re
import json
from typing import List, Dict, Any
from bs4 import BeautifulSoup

class ContentCleaner:
    """
    Kelas untuk membersihkan konten HTML/Teks dari noise sebelum disimpan ke dataset LLM.
    Fokus: Menghilangkan caption gambar, metadata, dan teks tidak relevan.
    """
    
    # Pola regex untuk mendeteksi noise umum
    NOISE_PATTERNS = [
        r'^Foto\s*\d*\.?.*',             # "Foto 1.", "Foto bersama..."
        r'^Dok\..*',                      # "Dok. Kemendikbud"
        r'^\(.*\)$',                      # Teks dalam kurung tunggal di satu baris (caption)
        r'^_\(.*\)_$',                    # Teks miring dalam kurung (keterangan gambar)
        r'^Pemaparan Materi.*',           # Judul slide presentasi
        r'^From Data to Action.*',        # Judul bahasa Inggris yang sering muncul di footer/header
        r'^Jakarta, \d+ [A-Za-z]+ \d+.*', # Tanggal dan lokasi acara
        r'^The Second meeting.*',         # Judul acara internasional
        r'^.*assessments? are.*',         # Definisi umum yang sering terambil dari footer
        r'^Copyright ©.*',                # Copyright
        r'^Share this.*',                 # Tombol share
        r'^Baca juga.*',                  # Link artikel terkait
        r'^Tags:.*',                      # Tags
        r'^Kategori:.*',                  # Kategori
    ]

    # Tag HTML yang biasanya berisi konten inti (Priority Order)
    CONTENT_SELECTORS = [
        'article',
        '.post-content',
        '.entry-content',
        '.article-body',
        '.content-area',
        '.field--name-body',
        '.td-post-content',
        '.kt-entry-content',
        '#content',
        'main',
        '.post-detail',
        '.berita-detail', # Khusus site pemerintah/berita indo
        'body' # Fallback terakhir
    ]

    def __init__(self, min_word_count: int = 30, min_keyword_match: int = 1):
        self.min_word_count = min_word_count
        self.min_keyword_match = min_keyword_match
        
        # Keyword wajib untuk memastikan konteks pendidikan (bisa disesuaikan)
        self.edu_keywords = [
            "kurikulum", "sekolah", "siswa", "guru", "pembelajaran", "pendidikan",
            "asesmen", "murid", "kelas", "materi", "ujian", "sekolah rakyat",
            "kemendikdasmen", "kemdiktisaintek", "merdeka", "pelajar"
        ]

    def extract_main_content(self, html: str) -> str:
        """
        Mengekstrak konten utama dari HTML berdasarkan selector prioritas.
        """
        if not html:
            return ""
            
        soup = BeautifulSoup(html, 'html.parser')
        
        # Coba selector satu per satu
        for selector in self.CONTENT_SELECTORS:
            if selector.startswith('.'):
                elements = soup.select(selector)
            elif selector.startswith('#'):
                elements = [soup.find(id=selector[1:])]
            else:
                elements = soup.find_all(selector)
            
            for element in elements:
                text = element.get_text(separator=' ', strip=True)
                # Validasi awal: apakah teks ini cukup panjang?
                if len(text.split()) > self.min_word_count:
                    return str(element) # Kembalikan HTML dari bagian ini untuk diproses lebih lanjut
        
        # Jika tidak ditemukan, kembalikan body asli tapi sudah dibersihkan tag script/style
        for tag in soup(['script', 'style', 'nav', 'footer', 'header']):
            tag.decompose()
        return str(soup.body) if soup.body else html

    def clean_text_lines(self, text: str) -> List[str]:
        """
        Membersihkan teks baris per baris dari pola noise.
        """
        lines = text.split('\n')
        cleaned_lines = []
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # Cek apakah line匹配 dengan pola noise
            is_noise = False
            for pattern in self.NOISE_PATTERNS:
                if re.match(pattern, line, re.IGNORECASE):
                    is_noise = True
                    break
            
            if not is_noise:
                # Hapus karakter underscore berlebihan yang menandakan format italic markdown kotor
                line = re.sub(r'_+', '', line)
                # Hapus kurung jika isinya hanya keterangan gambar singkat
                if re.match(r'^\(.*\)$', line) and len(line) < 50:
                    continue
                cleaned_lines.append(line)
        
        return cleaned_lines

    def validate_content(self, text: str) -> bool:
        """
        Memvalidasi apakah konten memiliki relevansi pendidikan.
        """
        words = text.lower().split()
        if len(words) < self.min_word_count:
            return False
        
        # Hitung kemunculan keyword pendidikan
        matches = sum(1 for kw in self.edu_keywords if kw in text.lower())
        return matches >= self.min_keyword_match

    def process(self, raw_html: str, url: str) -> Dict[str, Any]:
        """
        Pipeline utama pemrosesan: Extract -> Clean -> Validate.
        Mengembalikan dictionary siap simpan atau None jika tidak valid.
        """
        # 1. Ekstrak konten utama dari HTML
        main_html = self.extract_main_content(raw_html)
        
        # Konversi ke teks bersih
        soup = BeautifulSoup(main_html, 'html.parser')
        # Hapus tag yang masih tersisa yang tidak diinginkan
        for tag in soup(['script', 'style', 'img', 'iframe', 'noscript']):
            tag.decompose()
        
        raw_text = soup.get_text(separator='\n', strip=True)
        
        # 2. Bersihkan baris per baris
        clean_lines = self.clean_text_lines(raw_text)
        final_text = '\n'.join(clean_lines)
        
        # 3. Validasi konten
        if not self.validate_content(final_text):
            return None
        
        # 4. Format output
        word_count = len(final_text.split())
        if word_count < self.min_word_count:
            return None

        return {
            "text": final_text,
            "source": url.split('/')[2],
            "url": url,
            "word_count": word_count,
            # Level bisa ditentukan nanti berdasarkan URL atau konten, default Umum
            "level": "Umum" 
        }

# Contoh penggunaan jika dijalankan langsung
if __name__ == "__main__":
    # Simulasi data kotor dari user
    sample_data = [
        {
            "html": """
            <html><body>
            <header>Menu Nav</header>
            <article class="post-content">
                <h1>Pusat Asesmen Pendidikan</h1>
                <p>(_Pemaparan Materi Pembuka pada Kunjungan Kerja oleh My Esti ..._)</p>
                <p>Dari Data ke Aksi: Memanfaatkan Asesmen Nasional dan Rapor Pendidikan</p>
                <p>Jakarta, 17 Oktober 2024 – Kementerian Pendidikan...</p>
                <p>Foto 1. Sambutan Kepala Badan Standar...</p>
                <p>TKA, sebagaimana tertuang dalam Peraturan Menteri Pendidikan Dasar dan Menengah, merupakan kegiatan pengukuran capaian akademik murid pada mata pelajaran tertentu. Dirancang untuk menjadi alat ukur yang objektif, TKA hadir untuk melengkapi ekosistem asesmen pendidikan, baik dalam konteks pembelajaran maupun perumusan kebijakan publik.</p>
                <p>Sesi utama webinar menghadirkan paparan dari Kepala Pusat Asesmen Pendidikan (Pusmendik), yang membahas secara komprehensif berbagai aspek dari kebijakan TKA.</p>
                <p>TKA memiliki manfaat dalam berbagai sektor pendidikan, mulai dari seleksi masuk jenjang pendidikan lebih lanjut, penyetaraan jalur pendidikan, pemetaan mutu antar daerah, hingga perbaikan proses pembelajaran secara langsung di tingkat kelas.</p>
            </article>
            <footer>Copyright 2024</footer>
            </body></html>
            """,
            "url": "https://pusmendik.kemdikbud.go.id/berita"
        }
    ]

    cleaner = ContentCleaner()
    
    print("=== Hasil Pembersihan ===")
    for item in sample_data:
        result = cleaner.process(item["html"], item["url"])
        if result:
            print(json.dumps(result, indent=2, ensure_ascii=False))
        else:
            print("Data ditolak karena tidak relevan atau terlalu sedikit.")
