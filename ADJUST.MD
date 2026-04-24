# PRD: Sekolah Rakyat Scraping Engine (v2.0)

**Project Goal:** Build a high-fidelity, extensible, and "endless" web crawling system to extract clean Indonesian educational and news content for LLM Supervised Fine-Tuning (SFT).

---

## 1. System Architecture
The engine follows a **Producer-Consumer** pattern using a local SQLite database as the state coordinator. It is designed to be "monolithic-first"—simple to run on a single machine but structured for future scaling.



---

## 2. Functional Requirements

### FR1: Discovery Module (The Producer)
The system must discover URLs through two primary methods:
* **Sitemap Crawler:** Recursively parse `sitemap.xml` and `sitemap_index.xml`.
    * *Logic:* Extract all `<loc>` tags. Filter URLs based on "Allow Lists" (e.g., must contain `/berita/` or `/edu/`).
* **Search Dorker:** Automate queries like `"{keyword} site:{domain}"` using DuckDuckGo or a SERP API.
* **State Entry:** Every discovered URL must be hashed (MD5/SHA256) and inserted into the database with `status = 'pending'`.

### FR2: Extraction Module (The Consumer)
The system must process the `pending` queue using a **Strategy Pattern**:
* **Manual Strategy (Priority 1):** Check if the domain exists in `strategies/`.
    * Use CSS selectors (e.g., `div.read__content`) or Script-based extraction (e.g., parsing JSON from `<script>` tags) to find the "Main Content."
* **Heuristic Fallback (Priority 2):** Use `Trafilatura` or `Crawl4AI` to automatically detect content if no manual strategy is defined.
* **Cleaning:** Remove boilerplate (ads, navbars, social buttons) and convert the final output to clean **Markdown**.

### FR3: "Endless" & State Management
* **State Machine:** Track URL status: `pending`, `processing`, `completed`, `failed`, or `ignored`.
* **Pagination Handling:** If a "Next Page" selector is found in a manual strategy, the engine must fetch and append the content to the same record.
* **Watch Mode:** A background loop that re-scans sitemaps every X hours to find new content.

---

## 3. Technical Stack
* **Language:** Python 3.10+
* **HTTP Client:** `httpx` (Async support) or `curl_cffi` (to bypass basic TLS fingerprinting).
* **HTML Parsing:** `Selectolax` (High speed) or `BeautifulSoup4` (Complex navigation).
* **Database:** SQLite (using `SQLAlchemy` or `dataset` library).
* **Content Extraction:** `Trafilatura` (Fallback) + `custom_regex` (Manual).

---

## 4. Data Schema (SQLite)

| Column | Type | Description |
| :--- | :--- | :--- |
| `id` | INTEGER (PK) | Auto-increment ID. |
| `url_hash` | TEXT (UNIQUE) | Primary key for deduplication. |
| `url` | TEXT | The full source URL. |
| `domain` | TEXT | Extracted domain (e.g., `kompas.com`). |
| `title` | TEXT | Article headline. |
| `content_markdown` | TEXT | Cleaned text for LLM training. |
| `status` | TEXT | `pending`, `crawled`, `error`. |
| `metadata` | JSON | Store author, date, and original tags. |
| `created_at` | DATETIME | Timestamp of discovery. |

---

## 5. Strategy Manifest (The "Red Box" Logic)
Strategies are defined in a `sites.yaml` file to allow the Senior to update selectors without changing the Junior's code.

```yaml
# Example sites.yaml
kompas.com:
  method: "css"
  selectors:
    title: "h1.read__title"
    body: "div.read__content"
    next_page: "a.paging__link--next"
  strip: [".ads", "script", "aside", ".video-payload"]

kemdikbud.go.id:
  method: "script_json"
  target_var: "articleData" # Extract from <script> var articleData = {...}
```

---

## 6. Performance & Politeness
* **Concurrency:** Support `N` workers (default 3–5) to avoid IP bans.
* **Jitter:** Implement random delays (2.0s to 5.0s) between requests.
* **Retries:** Max 3 retries for `error` status with exponential backoff.
* **Memory:** Use `yield` generators when reading from the DB to keep RAM usage under 512MB.

---

## 7. Acceptance Criteria
1.  **High Signal:** Sample 50 records; >95% must contain purely the article text with no navigation menu junk.
2.  **Deduplication:** Running the Discovery module multiple times results in 0 duplicate `url_hash` entries.
3.  **Resilience:** If the script is killed, it resumes from the last `pending` URL upon restart.
4.  **Format:** Capability to export the SQLite table to `.jsonl` (JSON Lines) for immediate use in HuggingFace or training scripts.