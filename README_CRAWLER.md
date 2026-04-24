# 📚 AITF Sekolah Rakyat Crawler - Panduan Lengkap (PRD v2.0)

Crawler otomatis cerdas (berarsitektur Queue + SQLite) untuk mengumpulkan dataset pendidikan Indonesia dari Google Search, Situs Pemerintah, Media Berita, dan repositori Jurnal.

## 🌟 Fitur Utama (v2.0)
- **Zero-Data Loss (Auto-Resume):** Menggunakan SQLite (`data/raw/dedupe.sqlite3`). Proses *stop/start* aman tanpa kehilangan *state* karena setiap URL dan hash konten terlacak di *Database*.
- **Async & Multi-worker:** Mendukung concurrency yang sangat tinggi (menggunakan Asyncio/UVloop).
- **Auto-Discovery:** Mencari page baru melalui link di dalam halaman (Sitemap & Page Extraction).
- **Deduplikasi Tingkat Lanjut:** Jika konten artikel sama dengan URL berbeda, tidak akan disimpan dua kali (SHA-256 Text Content Deduping).

---

## 🚀 Cara Menggunakan Crawler Utama

File utama penggerak (Engine) adalah `main.py`. Anda dapat memanggilnya murni menggunakan `python` atau `uv`.

### 1. Perintah Dasar
Menghidupkan crawler dengan mode default (akan otomatis melanjutkan progress sebelumnya jika ada):
```bash
python main.py
```
> **Catatan:** Mode default ini sepadan dengan flag `--resume`. Crawler membaca database `dedupe.sqlite3` dan meneruskan antrean URL berstatus 'pending'.

### 2. Membatasi Crawler Hanya untuk Satu Domain Spesifik
Jika Anda ingin fokus menambang 1 target domain saja tanpa menyebar ke *website* lain (misal: *kompas.com*):
```bash
python main.py --only-domain kompas.com
```

### 3. Memulai Ulang (Restart Clean State)
Jika ingin memulai sesesi crawling dari nol (tidak memakai database lama), gunakan instance id baru:
```bash
python main.py --restart --instance-id "sesi_baru_1"
```
Hasil dan DB untuk perintah ini akan diletakkan di `dataset_raw_sesi_baru_1.jsonl` dan terpisah dari data default.

### 4. Mode Produksi & Testing
- **Production Mode:** Kecepatan dimaksimalkan, concurrency diangkat.
  ```bash
  python main.py --production
  ```
- **Test Mode:** Sangat dibatasi (misal: hanya mengambil 5 *success pages* lalu engine tertutup secara otomatis). Cocok untuk uji coba.
  ```bash
  python main.py --test --max-success 10
  ```

---

## 🧩 Mengubah Konfigurasi & Strategi (Advanced)

### A. Mengubah Kata Kunci / Filter Topik (`config.py`)
Crawler ini otomatis mengecek *relevansi* teks yang didapat dengan *keyword* utama.
Buka `config.py` dan cari variabel `TARGET_KEYWORDS`.
```python
TARGET_KEYWORDS = [
    "kurikulum merdeka", "capaian pembelajaran", "modul ajar", 
    # Tambahkan di sini...
]
```
Jika kata kunci dalam badan halaman kurang dari batas (`MIN_RELEVANCE_KEYWORDS`), halaman itu dibuang.

### B. Mengatur Jumlah "Tab / Pekerja" Konkuren
Makin besar CPU/RAM Anda, makin banyak laman yang bisa disedot bersamaan:
Buka `config.py`:
```python
MAX_CONCURRENCY = 20 # Bawaan (Default)
```
Atau Anda bisa panggil dari terminal:
```bash
python main.py --workers 30
```

### C. Folder Output (Hasil Berada di Mana?)
- **File Teks Mentah (Raw Content):** `data/raw/dataset_raw.jsonl` (File berekstensi *jsonlines* hasil ekstraksi Markdown)
- **Database Status:** `data/raw/dedupe.sqlite3` (File pelacak tugas, progress, dan URL yang dikunjungi)
- **Log Engine:** `data/raw/crawler.log`

---

## 🔧 Keamanan / Menghentikan Crawler
- **Cara Menghentikan:** Tekan `Ctrl + C` di dalam terminal VS Code. Engine akan menerima "Sinyal Shutdown", membungkus semua file JSONL dengan rapi, dan menyimpan status pekerjaan ke SQLite sebelum akhirnya berhenti.
- Anda **tidak akan** kehilangan artikel/URL yang telah berhasil terproses di antrean.
