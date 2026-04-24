Task: Republika.co.id Article Extraction (LLM Data Prep)
Objective: Extract continuous news text while stripping out Google Publisher Tag (GPT) advertisement scripts, handling malformed paragraph tags, and cleaning the standard Republika dateline.

Target Scope: https://news.republika.co.id/*
Base Wrapper Selector: div.article-content
Note: The actual text is often wrapped in an <article> tag inside this div, so the script must target elements inside div.article-content.

1. The Pre-Clean Phase (Script & Ad Removal)
Republika embeds advertisement logic directly into the HTML using <script> tags alongside ad container <div>s.

Remove All Scripts: Selector: script

Reason: The LLM does not need to read googletag.cmd.push(function()...).

Remove Ad Placeholders: Selector: [id*="div-gpt-ad"]

Reason: Destroys the hidden containers where ads render.

Remove Inline Promotions (BACA JUGA): Republika often uses bold text or specific divs for internal links.

Selector: div.baca-juga, div.terkait, or any strong tag containing "BACA JUGA".

2. The Dateline Filter (Regex Recommended)
Unlike Detik which just uses Jakarta -, Republika uses a very specific format: REPUBLIKA.CO.ID, JAKARTA - . The city changes depending on the article.

Action: The script must use Regular Expressions (Regex) to detect and strip this pattern from the very first paragraph so the LLM doesn't learn it as a repeated token.

3. Developer Implementation Guide (Python/BeautifulSoup)
Pass this logic to the junior programmer. It includes a specific Regex pattern to handle Republika's dynamic dateline.

Python
import re

# Assuming 'soup' is the parsed HTML of the test URL
article_wrapper = soup.select_one('div.article-content')
clean_content = []

if article_wrapper:
    # 1. PRE-CLEAN: Destroy scripts, ad wrappers, and internal links
    noise_selectors = [
        'script', 
        '[id*="div-gpt-ad"]', 
        'div.baca-juga',
        'div.terkait'
    ]
    
    for selector in noise_selectors:
        for noise in article_wrapper.select(selector):
            noise.decompose()
            
    # 2. ITERATE & EXTRACT
    # Targeting 'p' directly. If there are nested <p> tags, get_text() handles it safely.
    for p in article_wrapper.find_all('p'):
        # strip=True removes the &nbsp; and extra whitespace
        text = p.get_text(separator=' ', strip=True) 
        
        # Skip empty paragraphs created by malformed HTML or decomposed ads
        if not text:
            continue
            
        # 3. CLEAN DATELINE (Regex to catch "REPUBLIKA.CO.ID, [CITY] - ")
        # This targets "REPUBLIKA.CO.ID," followed by any uppercase city name and a hyphen
        if "REPUBLIKA.CO.ID" in text:
            text = re.sub(r'^REPUBLIKA\.CO\.ID,\s+[A-Z\s]+[-–—]+\s*', '', text, flags=re.IGNORECASE)
            
        clean_content.append(text)

# 4. JOIN WITH DOUBLE NEWLINES
llm_ready_text = "\n\n".join(clean_content)
QA Test Ticket: Republika Crawler Logic
Target Test URL: https://news.republika.co.id/berita/tdxzrb368/umj-perkuat-komitmen-kampus-hijau-lewat-sharing-session-pengelolaan-sampah

Instructions for Junior Programmer:
Run the extraction script against the URL above. Verify that the Google ad scripts and the Republika dateline are completely removed from the final output.

Pass/Fail Acceptance Criteria:

[ ] Pass if: The first sentence begins directly with the narrative content (e.g., "Universitas Muhammadiyah Jakarta (UMJ) menggelar sharing session...").

[ ] Fail if: The text starts with "REPUBLIKA.CO.ID, JAKARTA -". (This means the Regex dateline filter failed or the &nbsp; character broke the match).

[ ] Fail if: The output contains Javascript code like googletag.cmd.push. (This means the script pre-clean failed).

[ ] Pass if: The text flows continuously without any HTML tags (<p>, <div>, or `` comments) included in the final string.

Task: Quipper Blog Extraction (Updated Strategy)
Objective: Extract clean educational text from Quipper articles, effectively filtering out WordPress Table of Contents plugins, bottom-page SEO tags, and empty paragraph blocks.

Target Scope: https://www.quipper.com/id/blog/*
Base Wrapper Selector: div#penci-post-entry-inner (or the parent container holding the wp-block elements).

1. The Pre-Clean Phase (Crucial Updates)
Before iterating through the content, the junior programmer must add these new CSS selectors to the "Decompose / Destroy" list:

Remove Table of Contents:

Selector: div.lwptoc

Reason: Prevents the LLM from ingesting a duplicated, unformatted list of headers at the beginning of the text.

Remove SEO Tags:

Selector: div.post-tags

Reason: Strips out the comma-separated keyword lists at the bottom of the article.

Remove Pagination/Link Pages:

Selector: div.penci-single-link-pages

Maintain Existing Quipper Filters:

i.penci-post-countview-number-check (Hidden view counts).

figure.wp-block-image, img (Images and captions).

2. Developer Implementation Guide (Python/BeautifulSoup)
Here is the updated, robust logic to hand to your junior developer. It handles the h2/h3 tags, lists, and the new junk selectors.

Python
# Assuming 'article_wrapper' is div#penci-post-entry-inner
clean_content = []

if article_wrapper:
    # 1. PRE-CLEAN: Destroy ToC, Tags, Images, and Metrics
    noise_selectors = [
        'div.lwptoc',                          # Table of Contents plugin
        'div.post-tags',                       # Bottom SEO tags
        'div.penci-single-link-pages',         # Pagination blocks
        'i.penci-post-countview-number-check', # Hidden view counts
        'figure.wp-block-image',               # Image wrappers
        'div.wp-block-image',
        'hr.wp-block-separator',               # Visual dividers
        'img'                                  # Stray images
    ]
    
    for selector in noise_selectors:
        for noise in article_wrapper.select(selector):
            noise.decompose()
            
    # 2. ITERATE & FORMAT
    # Finding direct and nested content blocks
    for element in article_wrapper.find_all(['h2', 'h3', 'p', 'ol', 'ul']):
        text = element.get_text(separator=' ', strip=True)
        
        # Skip empty strings (Handles <p class="wp-block-paragraph"></p>)
        if not text:
            continue
            
        # 3. APPLY MARKDOWN FORMATTING
        if element.name == 'h2':
            # Clean embedded spans inside headers (like <span id="...">)
            clean_content.append(f"## {text}")
        elif element.name == 'h3':
            clean_content.append(f"### {text}")
        elif element.name == 'ol':
            list_items = element.find_all('li')
            for i, li in enumerate(list_items, start=1):
                clean_content.append(f"{i}. {li.get_text(strip=True)}")
        elif element.name == 'ul':
            list_items = element.find_all('li')
            for li in list_items:
                clean_content.append(f"- {li.get_text(strip=True)}")
        elif element.name == 'p':
            clean_content.append(text)

# 4. JOIN WITH DOUBLE NEWLINES
llm_ready_text = "\n\n".join(clean_content)
Acceptance Criteria for this Snippet
When the junior programmer runs this specific script against your HTML snippet, they must verify:

The text starts immediately with "Lagi semangat kuliah, ngejar impian..." and does not include the "Daftar Isi" (Table of Contents) text.

The heading "Apa Itu Beasiswa Djarum Foundation?" is successfully converted to ## Apa Itu Beasiswa Djarum Foundation?.

The list items under "Kenapa Beasiswa Ini Beda dari yang Lain?" are accurately numbered 1., 2., 3., 4..

The text successfully ends at the Disclaimer paragraph, and does not append the "beasiswa djarum foundation, info beasiswa, main news..." tags at the bottom.
sk: Zenius Full Article Extraction (LLM Data Prep)
Objective: Extract a fully structured JSON document from Zenius articles, capturing precise metadata (dates, keywords, headline) from Schema.org scripts, and compiling a clean, continuous Markdown-formatted body text that preserves math formulas while actively destroying promotional UI cards and internal links.

Target Scope: https://www.zenius.net/blog/*

1. Phase 1: Metadata Extraction (JSON-LD)
Before touching the HTML body, the script must parse the <script type="application/ld+json"> tags to extract the structural truth of the article.

Target Schema: Must verify the JSON object contains a "headline" key to ensure we aren't extracting breadcrumb or website metadata.

Target Fields: headline, description, url, datePublished, dateModified, keywords.

2. Phase 2: Body Text Pre-Cleaning (Ghost CMS Focus)
The junior developer must target section.gh-content as the primary wrapper and aggressively decompose the following elements before looping:

UI & Marketing Cards: div.kg-button-card, figure.kg-image-card

Comments & Scripts: div.gh-comments, script

Table of Contents (Fallback): div.ez-toc-container, div#toc_container

3. Phase 3: Semantic Extraction & Math Handling
When iterating through the remaining tags (h2, h3, p, ol, ul, blockquote, div.kg-callout-card), the script must:

Salvage Math: Check any remaining <img> tags inside paragraphs. If they have alt text, convert them to [Formula: <alt_text>] so equations are not lost.

Filter Interruption Text: Skip any text containing "Baca juga:", "🔗", or "download aplikasi zenius".

Apply Markdown: Prefix headers with ##, blockquotes with >, and format list items.

Developer Implementation Guide (Python/BeautifulSoup)
Please hand this combined, production-ready function to your junior programmer.

Python
import json
from bs4 import BeautifulSoup

def extract_zenius_article(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Initialize the structured output for the LLM dataset
    document = {
        "title": "",
        "summary": "",
        "url": "",
        "published_date": "",
        "modified_date": "",
        "keywords": "",
        "content": ""
    }

    # ==========================================
    # PHASE 1: METADATA EXTRACTION (JSON-LD)
    # ==========================================
    json_ld_scripts = soup.find_all('script', type='application/ld+json')
    for script in json_ld_scripts:
        try:
            schema_data = json.loads(script.string)
            # Verify this is the Article schema
            if isinstance(schema_data, dict) and 'headline' in schema_data:
                document["title"] = schema_data.get("headline", "").strip()
                document["summary"] = schema_data.get("description", "").strip()
                document["url"] = schema_data.get("url", "").strip()
                document["published_date"] = schema_data.get("datePublished", "")
                document["modified_date"] = schema_data.get("dateModified", "")
                document["keywords"] = schema_data.get("keywords", "")
                break # Found the main schema, stop searching
        except (json.JSONDecodeError, TypeError):
            continue

    # ==========================================
    # PHASE 2: BODY TEXT EXTRACTION
    # ==========================================
    article_wrapper = soup.select_one('section.gh-content')
    clean_content = []

    if article_wrapper:
        # 1. Pre-Clean: Destroy UI Cards, Comments, and Scripts
        noise_selectors = [
            'div.kg-button-card',  # CTA Buttons
            'figure.kg-image-card',# Promo Images and captions
            'div.gh-comments',     # Comments section
            'script',              # Extraneous scripts
            'hr',                  # Thematic breaks
            'div.ez-toc-container',# WP Table of Contents (Fallback)
            'div#toc_container'    # WP Table of Contents (Fallback)
        ]
        for selector in noise_selectors:
            for noise in article_wrapper.select(selector):
                noise.decompose()
                
        # 2. Iterate & Format
        for element in article_wrapper.find_all(['h2', 'h3', 'p', 'ol', 'ul', 'blockquote', 'div']):
            
            # --- Math Image Handling ---
            for img in element.find_all('img'):
                alt_text = img.get('alt', '')
                if alt_text:
                    img.replace_with(f" [Formula: {alt_text}] ")
                else:
                    img.decompose()

            # --- Text Extraction & Specific Ghost UI Handling ---
            if element.name == 'div':
                if 'kg-callout-card' in element.get('class', []):
                    text = element.get_text(separator='\n', strip=True)
                else:
                    continue # Ignore layout divs
            else:
                text = element.get_text(separator=' ', strip=True)
            
            # --- Noise Filtering ---
            if not text:
                continue
                
            lower_text = text.lower()
            if "baca juga:" in lower_text or "🔗" in text or "download aplikasi zenius" in lower_text:
                continue

            # --- Markdown Formatting ---
            if element.name == 'h2':
                clean_content.append(f"## {text}")
            elif element.name == 'h3':
                clean_content.append(f"### {text}")
            elif element.name == 'blockquote':
                clean_content.append(f"> {text}")
            elif element.name == 'ol':
                list_items = element.find_all('li')
                for i, li in enumerate(list_items, start=1):
                    clean_content.append(f"{i}. {li.get_text(strip=True)}")
            elif element.name == 'ul':
                list_items = element.find_all('li')
                for li in list_items:
                    clean_content.append(f"- {li.get_text(strip=True)}")
            elif element.name in ['p', 'div']:
                clean_content.append(text)

    # Compile the final document
    document["content"] = "\n\n".join(clean_content)
    return document
QA Test Ticket: Zenius Pipeline Validation
Instructions for Junior Programmer:
Pass a raw HTML string from a Zenius Ghost CMS article (e.g., the Gerak Parabola or UTBK test links) into the extract_zenius_article() function and print the resulting JSON dictionary.

Pass/Fail Acceptance Criteria:

[ ] Pass if: The returned object contains populated published_date and keywords extracted directly from the JSON-LD script.

[ ] Pass if: Math equations embedded as images inside paragraphs are translated cleanly into [Formula: ...] syntax.

[ ] Fail if: The content string contains any UI text like "Kunjungi Halaman Tryout Zenius" or "Baca juga: Strategi Belajar UTBK".

[ ] Fail if: The content string contains massive blocks of unformatted text from the "Daftar Isi" (Table of Contents).