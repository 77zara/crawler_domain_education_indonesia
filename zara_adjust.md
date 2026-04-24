PLEASE DONT CHANGE ANY LOGIC FROM OLD CODE, ADJUSTMENST JUST FOR THE ADD NEW DOMAIN AAND ITS OWN STRATEGY NOT FOR THE OLD ONE  


CSS Selector Mapping for Detik.com/edu
Target URL Pattern: https://www.detik.com/edu/*

Your script must use the following strict CSS selectors to separate the actual article narrative from the surrounding website noise. Do not extract the full text of the parent container; you must target the specific child nodes.

1. The Base Boundary (The Wrapper)
Use this selector only to locate the article body. Do not extract text directly from it, as it contains hidden elements and injected ads.

Selector: div.detail__body-text.itp_bodycontent

Action: Locate this element first. All subsequent selections must be made inside this specific wrapper.

2. The Target Content (What to Extract)
The actual sentences the LLM needs are housed inside paragraph tags.

Selector: div.detail__body-text.itp_bodycontent > p

Action: Extract the text from these elements.

Note on the > combinator: It is crucial to use > (direct child) rather than a space (descendant). This prevents the scraper from accidentally pulling <p> tags that might be hidden inside embedded widgets, blockquotes, or related article boxes nested deeper in the HTML.

3. The Exclusion Selectors (The "Drop" List)
Before extracting text from the <p> tags, the script must actively find and remove (decompose/destroy) elements matching these selectors from the HTML tree. These inject noise into the LLM context.

Selector: .clearfix

Reason: Empty layout dividers (<div class="clearfix"></div>).

Selector: table.linksisip

Reason: This targets the "Baca juga" (Read also) or internal link boxes that interrupt the article flow.

Selector: div.sisip_artikel

Reason: Targets embedded related articles or mid-content promotional blocks.

Selector: div.video-20detik, div.embed-video

Reason: Removes embedded video players and their captions.

Selector: style, script, 

Reason: Strips any inline CSS, JavaScript, or HTML comments (like ``).

4. The Dateline Selector (Specific Cleaning)
The first paragraph always contains a location dateline (e.g., <strong>Jakarta</strong> - ) which can confuse the LLM if appended to every article.

Selector: div.detail__body-text.itp_bodycontent > p:first-of-type > strong:first-child

Action: If this exact selector exists, remove the <strong> element entirely before extracting the text from that first <p> tag. Programmatically trim the leftover hyphen (-) that usually follows it.

5. List Items (Optional but Recommended)
Sometimes Detik articles use bullet points for lists. If you want to capture those:

Selector: div.detail__body-text.itp_bodycontent > ul > li, div.detail__body-text.itp_bodycontent > ol > li

Action: Extract these and format them with a dash (- ) so the LLM understands it is reading a list.


This is exactly the kind of "WYSIWYG (What You See Is What You Get) Editor" nightmare that ruins Large Language Model datasets. The text is fragmented, images are shoved inside paragraph tags, and formatting elements (`&nbsp;`, `&#8212;`) are used for visual spacing instead of actual CSS.

If your junior programmer just extracts all `<p>` tags, the LLM will ingest garbage like: `[Image link] —  Baca Juga: ...`

Here is the highly specific extraction strategy and pseudocode/Python logic to hand over, designed specifically to clean this Ruangguru structure.

***

### Task: Ruangguru Blog Extraction (Addressing CMS Fragmentation)

**Objective:** Reconstruct continuous, semantic text from highly fragmented WYSIWYG editor output within `div.content-body`, aggressively filtering out visual spacers, inline images, and embedded link blocks.

**Target Scope:** `div.content-body`

#### 1. The Pre-Clean Phase (Crucial for Ruangguru)
Before attempting to extract any text, the script must systematically destroy elements that create noise or leave behind empty wrapper tags.

* **Remove all Images:** `wrapper.select('img')` -> Decompose. (This prevents `alt` text or image URLs from bleeding into the paragraph text).
* **Remove Image Captions:** Locate and decompose any paragraph or span containing the word **"(Sumber:"**.
* **Remove "Baca Juga" Blocks:** Locate and decompose any paragraph where the text contains the exact substring **"Baca Juga:"**.

#### 2. The Extraction Loop & Element Mapping
Do not use a single `.text` call. The script must iterate through the children of `div.content-body` sequentially (`<h2>`, `<p>`, `<ol>`, `<ul>`) to preserve the chronological flow of the educational material.

Apply these specific element rules during the loop:

* **For `<h2>` and `<h3>` tags:**
    * Extract the text and prepend it with Markdown headers (e.g., `## ` for `<h2>`) to maintain the document hierarchy.
* **For `<ol>` (Ordered Lists) - *High Priority*:**
    * Ruangguru uses `<ol>` heavily for step-by-step concepts. The script must loop through the `<li>` tags inside the `<ol>` and extract them with numbers (e.g., `1. `, `2. `).
* **For `<p>` (Paragraphs) - *The Fragmentation Filter*:**
    * Extract the text.
    * **Filter 1 (Visual Dividers):** If the extracted string equals `—` (em-dash) or `&#8212;`, **skip it**.
    * **Filter 2 (Empty Spacers):** If the extracted string is completely empty or just contains a non-breaking space (`&nbsp;` or `\xa0`), **skip it**.
    * **Filter 3 (Promotional Intros):** If the paragraph contains "Yuk simak penjelasannya di artikel", **skip it**.

#### 3. Developer Implementation Guide (Python/BeautifulSoup)
Pass this logic directly to the junior programmer to handle the HTML structure you provided:

```python
article_wrapper = soup.select_one('div.content-body')
clean_content = []

if article_wrapper:
    # 1. PRE-CLEAN: Destroy images so they don't leave empty text artifacts
    for img in article_wrapper.find_all('img'):
        img.decompose()
        
    # 2. ITERATE & FILTER
    for element in article_wrapper.find_all(['h2', 'h3', 'p', 'ol', 'ul'], recursive=False):
        text = element.get_text(strip=True)
        
        # Skip empty tags, visual spacers (like &nbsp;), and em-dashes
        if not text or text == '—' or text == '&#8212;' or text == '\xa0':
            continue
            
        # Filter "Baca Juga" and Image Source Captions
        if "Baca Juga:" in text or "(Sumber:" in text or "Yuk simak" in text:
            continue

        # 3. FORMATTING
        if element.name == 'h2':
            clean_content.append(f"## {text}")
        elif element.name == 'h3':
            clean_content.append(f"### {text}")
        elif element.name == 'ol':
            # Handle ordered lists (like the Pancakarya programs)
            list_items = element.find_all('li')
            for i, li in enumerate(list_items, start=1):
                clean_content.append(f"{i}. {li.get_text(strip=True)}")
        elif element.name == 'ul':
            # Handle unordered lists
            list_items = element.find_all('li')
            for li in list_items:
                clean_content.append(f"- {li.get_text(strip=True)}")
        elif element.name == 'p':
            clean_content.append(text)

# 4. JOIN WITH DOUBLE NEWLINES
llm_ready_text = "\n\n".join(clean_content)
```

**Why this specific strategy works:**
By deleting the images first, those `<p><img ...></p>` tags become completely empty. When the loop reaches them, `element.get_text(strip=True)` returns an empty string, which triggers our `if not text:` filter and skips it entirely. This flawlessly stitches the separated paragraphs back together into a single, cohesive educational document.

test link = https://www.ruangguru.com/blog/cara-menggunakan-color-coding-yang-efektif-saat-mengajar-di-kelas


This snippet from Liputan6 is a classic example of a heavily monetized news site. The actual news narrative is constantly interrupted by advertisement placeholders (`div-gpt-ad...`) and injected reading recommendations (`baca-juga-collections`). 

If your junior programmer doesn't filter these out, the LLM will suddenly read "Advertisement" or "BACA JUGA: Filipina dan India..." right in the middle of a sentence about Donald Trump.

Here is the exact PRD-ready instruction set and logic for Liputan6.

***

### Task: Liputan6 Article Extraction (LLM Data Prep)

**Objective:** Extract continuous news text while aggressively stripping out programmatic ad placeholders, "BACA JUGA" injection blocks, and article metadata/tags at the bottom.

**Target Scope:** `https://www.liputan6.com/*`
**Base Wrapper Selector:** The junior programmer must target the primary article body wrapper. Based on Liputan6's data attributes, this is typically **`div.article-content-body`** or the direct parent of the `<p>` tags. 

#### 1. The Pre-Clean Phase (Heavy Ad & Link Filtering)
Liputan6 uses predictable CSS classes and IDs for its non-content injections. Before looping through the text, the script must search for and destroy (decompose) the following selectors:

* **Remove "BACA JUGA" Blocks:** * `Selector:` `div.baca-juga-collections`
  * `Reason:` Strips out embedded links that break the story's narrative flow.
* **Remove Ad Placeholders:** * `Selector:` `div.advertisement-placeholder`, `div.article-ad`, `[id*="gpt-ad"]`, `[id*="revive-ad"]`
  * `Reason:` Removes the invisible ad wrappers and the `<p>Advertisement</p>` text that often sneaks into scraping results.
* **Remove Tags and Topics:**
  * `Selector:` `div.tags--snippet`, `div#preco`
  * `Reason:` We only want the article narrative, not a comma-separated list of SEO tags at the bottom.

#### 2. The Extraction Loop
Once the noise is stripped from the DOM, extraction becomes very straightforward.

* **Target:** Extract text from all remaining `<p>` tags inside the base wrapper.
* **Filter:** Skip any `<p>` tag that returns an empty string after stripping whitespace. (Sometimes decomposing an ad leaves behind an empty `<p></p>`).

#### 3. Developer Implementation Guide (Python/BeautifulSoup)
Here is the exact logic to hand to the junior programmer to cleanly parse the Liputan6 snippet you provided:

```python
# Assuming 'article_wrapper' is the main container (e.g., div.article-content-body)
clean_content = []

if article_wrapper:
    # 1. PRE-CLEAN: Destroy ads, internal links, and tags
    noise_selectors = [
        'div.baca-juga-collections',  # Internal "Read Also" links
        'div.advertisement-placeholder', # "Advertisement" text blocks
        'div.article-ad',             # Bottom ad slots
        '[id*="gpt-ad"]',             # Google ad slots
        '[id*="revive-ad"]',          # Revive ad slots
        'div.tags--snippet',          # Bottom SEO tags
        'div#preco'                   # Tag metadata
    ]
    
    for selector in noise_selectors:
        for noise in article_wrapper.select(selector):
            noise.decompose()
            
    # 2. ITERATE & EXTRACT
    for p in article_wrapper.find_all('p', recursive=False):
        text = p.get_text(strip=True)
        
        # Double-check to ensure we don't grab stray "Advertisement" text
        if not text or text.lower() == "advertisement":
            continue
            
        clean_content.append(text)

# 3. JOIN WITH DOUBLE NEWLINES
llm_ready_text = "\n\n".join(clean_content)
```

**Why this works:**
Instead of trying to conditionally skip bad text during the loop, we use BeautifulSoup's `.decompose()` to literally delete the ad placeholders and "BACA JUGA" boxes from the HTML tree *before* we even look at the `<p>` tags. Once the tree is clean, grabbing the text is perfectly safe and continuous.


test link = https://www.liputan6.com/global/read/6322812/india-berang-atas-pernyataan-trump-soal-lubang-neraka