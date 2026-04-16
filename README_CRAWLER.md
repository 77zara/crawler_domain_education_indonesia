# 📚 AITF Sekolah Rakyat Crawler - Panduan Lengkap

Crawler cerdas untuk mengumpulkan dataset pendidikan Indonesia berkualitas tinggi bagi LLM Sekolah Rakyat.

## 🎯 Fitur Utama

### 1. **Tools yang Digunakan**
- **Crawl4AI**: Framework crawling modern dengan dukungan JavaScript rendering
- **Playwright/Chromium**: Browser automation untuk situs dinamis
- **Trafilatura**: Ekstraksi konten utama dari HTML (menghilangkan boilerplate)
- **PyMuPDF4LLM**: Ekstraksi teks dari PDF jurnal ilmiah ke format Markdown
- **BeautifulSoup**: Parsing dan cleaning HTML

### 2. **Sumber Data**

#### A. Portal Pemerintah (Updated untuk Struktur Kementerian Baru)
- Kemdikdasmen (Kementerian Pendidikan Dasar dan Menengah)
- Kemdiktisaintek (Kementerian Pendidikan Tinggi, Sains, dan Teknologi)
- BSKAP (Badan Standar, Kurikulum, dan Asesmen Pendidikan)
- BRIN (Badan Riset dan Inovasi Nasional)

#### B. News Online (Domain Berita)
- Kompas, Detik, Tempo, CNN Indonesia, CNBC Indonesia
- Republika, Tribunnews, Antara, Suara
- Kontan, Bisnis, Katadata (untuk pendidikan vokasi)
- NU Online, Muhammadiyah (untuk nilai karakter & agama)

#### C. Jurnal Ilmiah & Repository
- Garuda, Sinta, Neliti
- Repository universitas (UI, UGM, ITB, UNY, dll.)

#### D. Platform Edukasi & Komunitas Guru
- Zenius, Ruangguru, Quipper
- Gurusiana, Indonesiana, dan blog guru lainnya

### 3. **Keyword Strategy (6 Kategori Utama)**

1. **Kurikulum & Standar Nasional**: Kurikulum Merdeka, Capaian Pembelajaran, ATP
2. **Materi Berbasis Lokal**: Kearifan lokal, cerita rakyat Nusantara
3. **Metodologi Pengajaran**: Pembelajaran berdiferensiasi, PjBL, asesmen formatif
4. **Pendidikan Karakter**: Pancasila, profil pelajar Pancasila, gotong royong
5. **Sumber Belajar Terbuka**: Rumah Belajar, buku teks Kemendikbud
6. **Isu Pendidikan**: Pendidikan inklusif, daerah 3T, literasi dasar

## 📁 File Konfigurasi

### `config.py`
- **TARGET_KEYWORDS**: Daftar kata kunci untuk filtering konten (sudah ditambahkan Kemdiktisaintek, BSKAP, dll.)
- **CLASSIFICATION_RULES**: Regex untuk klasifikasi level pendidikan (SD, SMP, SMA)
- **MIN_RELEVANCE_KEYWORDS**: Minimal keyword match agar konten disimpan

### `urls.txt`
Seed URLs yang akan di-crawl pertama kali, terbagi dalam kategori:
- Portal pemerintah (.go.id)
- News online (Kompas, Detik, Tempo, dll.)
- Jurnal & repository akademik
- Platform edukasi
- Blog guru

### `utils/discovery.py`
- **SEARCH_QUERIES**: Query otomatis untuk search engine (DuckDuckGo, Bing) dengan site: operator
- **PRIORITY_DOMAINS**: Domain yang diprioritaskan (sudah ditambahkan news online dan jurnal)
- **BLOCKED_DOMAINS**: Domain yang diblokir (sosmed, e-commerce, situs asing)

## 🚀 Cara Menggunakan

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

Jika ingin crawling PDF jurnal, install dependency PDF (opsional):

```bash
pip install pymupdf4llm
```

### 2. Crawling Website Umum

```bash
python src/cleaner/clean_crawler.py
```

Output: `dataset_llm.jsonl`

### 3. Crawling Jurnal Ilmiah (PDF)

```bash
python pdf_crawl/pdf_crawler.py
```

Output: `data/dataset_jurnal.jsonl`

### 4. Running Full Crawler (dengan Discovery Engine)

```bash
python main.py
```

Output:
- `data/dataset.jsonl`: Raw markdown dengan metadata lengkap
- `data/dataset_cpt.jsonl`: Clean plain text untuk Continuous Pre-Training

## 📊 Pipeline Pemrosesan Data

### Untuk Website HTML:
```
URL → Crawl (Crawl4AI + Playwright) 
    → Clean HTML (hapus nav, footer, ads) 
    → Extract Main Content (Trafilatura) 
    → Relevance Check (filter berdasarkan keyword) 
    → Normalize Text 
    → Save to JSONL
```

### Untuk Jurnal PDF:
```
URL → Crawl Metadata 
    → Download PDF 
    → Extract to Markdown (PyMuPDF4LLM) 
    → Clean Text (hapus header/footer berulang) 
    → Education Filter (minimal 2 keyword match) 
    → Save to JSONL dengan metadata (judul, penulis, abstrak)
```

## 🔧 Strategi Filtering Konten

### 1. **Relevansi Pendidikan**
Konten harus mengandung minimal 2 keyword dari TARGET_KEYWORDS

### 2. **Filter Iklan/Komersial**
Deteksi sinyal iklan seperti "daftar sekarang", "beli paket", "promo", dll.

### 3. **Filter Bahasa**
Prioritas konten bahasa Indonesia (kecuali URL khusus bahasa Inggris)

### 4. **Per-Domain Cap**
Maksimal 50 halaman per domain untuk memastikan diversitas

### 5. **Panjang Konten**
Minimal 100 kata setelah cleaning untuk menghindari konten terlalu pendek

## 📈 Tips untuk Dataset Berkualitas

1. **Gunakan Search Queries Spesifik**: Query dengan `site:go.id` atau `site:ac.id` memberikan hasil lebih relevan
2. **Prioritaskan Domain Terpercaya**: .go.id, .ac.id, dan media besar lebih reliable
3. **Filter Tanggal**: Untuk berita, prioritaskan konten 2-3 tahun terakhir
4. **Diversitas Sumber**: Jangan hanya dari satu jenis sumber (campur pemerintah, berita, jurnal, blog)
5. **Quality over Quantity**: Lebih baik sedikit konten berkualitas daripada banyak konten kotor

## 🛠 Troubleshooting

### Data Masih Kotor?
- Tingkatkan `MIN_RELEVANCE_KEYWORDS` di `config.py`
- Tambahkan pattern iklan baru di `_AD_SIGNALS` (core/crawler.py)
- Adjust threshold di `PruningContentFilter` (threshold=0.48 bisa dinaikkan)

### Banyak URL Tidak Relevan?
- Tambahkan domain ke `BLOCKED_DOMAINS` di `utils/discovery.py`
- Perketat `_URL_EDU_INDICATORS` 
- Gunakan lebih banyak query dengan `site:` operator

### PDF Gagal Diekstrak?
- Beberapa PDF mungkin scan image (OCR diperlukan)
- Cek log error untuk detail masalah
- Pastikan file PDF tidak corrupt

## 📝 Contoh Output Format

```json
{
  "url": "https://example.com/materi-matematika",
  "source_page": "https://example.com/blog",
  "title": "Materi Matematika SD Kelas 5",
  "authors": [],
  "abstract": "",
  "content": "Teks lengkap materi pembelajaran...",
  "word_count": 1500,
  "type": "jurnal_ilmiah",
  "level": "SD"
}
```

## 🎓 Kontribusi

Untuk menambahkan sumber baru:
1. Tambahkan URL ke `urls.txt`
2. Tambahkan domain ke `PRIORITY_DOMAINS` di `utils/discovery.py`
3. Tambahkan keyword terkait ke `TARGET_KEYWORDS` di `config.py`
4. Update `SEARCH_QUERIES` dengan query spesifik untuk sumber tersebut

## 📄 License

Proyek ini dibuat untuk tujuan pendidikan dalam rangka pengembangan LLM Sekolah Rakyat Indonesia.
