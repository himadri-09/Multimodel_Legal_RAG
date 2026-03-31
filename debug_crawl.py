"""
debug_crawl.py — Standalone markdown extraction debugger
=========================================================
Crawls a single URL using crawl4ai (for JS rendering),
then extracts clean content using BeautifulSoup + markdownify.

Saves output to:  debug_markdown/<slug>.md

Usage:
    python debug_crawl.py
    python debug_crawl.py --url https://docs.codepup.ai/introduction/plans-and-credits

Delete this file to remove the debug functionality entirely.
No changes needed to your main codebase.

Install deps if missing:
    pip install crawl4ai markdownify beautifulsoup4
"""

import asyncio
import argparse
import re
import sys
from pathlib import Path
from urllib.parse import urlparse

# ── Output directory ──────────────────────────────────────────────────────────
OUTPUT_DIR = Path("debug_markdown")
OUTPUT_DIR.mkdir(exist_ok=True)

# ── Default test URL ──────────────────────────────────────────────────────────
DEFAULT_URL = "https://docs.codepup.ai/introduction/plans-and-credits"

# ── JS: force all tab panels visible before extraction ───────────────────────
_TAB_CLICK_JS = """
(async () => {
    const delay = ms => new Promise(r => setTimeout(r, ms));

    // Click every tab
    const selectors = [
        '[role="tab"]', '[data-tab]', '[data-value]',
        '.tab', '.tabs li', '.tab-item', '.tab-button',
        'button[id*="tab"]', 'li[id*="tab"]', '[aria-selected]',
    ];
    const seen = new Set();
    for (const sel of selectors) {
        for (const el of document.querySelectorAll(sel)) {
            if (!seen.has(el)) {
                seen.add(el);
                try { el.click(); await delay(300); } catch(e) {}
            }
        }
    }

    // Force ALL tab panels visible simultaneously
    // (so HTML snapshot contains every panel, not just the last-clicked one)
    for (const el of document.querySelectorAll(
        '[role="tabpanel"], [data-tab-content], [data-panel], .tab-panel, .tabpanel'
    )) {
        el.style.display    = 'block';
        el.style.visibility = 'visible';
        el.style.opacity    = '1';
        el.removeAttribute('hidden');
        el.removeAttribute('aria-hidden');
    }

    // Open all accordions/details
    for (const el of document.querySelectorAll('details:not([open])')) {
        try { el.open = true; } catch(e) {}
    }

    await delay(600);
})();
"""


# ── BeautifulSoup + markdownify extraction ────────────────────────────────────

def html_to_clean_markdown(html: str, url: str) -> str:
    """
    Strip nav/sidebar/footer noise from HTML,
    then convert the main content area to clean markdown.
    """
    try:
        from bs4 import BeautifulSoup
        from markdownify import markdownify as md_convert
    except ImportError:
        print("ERROR: pip install markdownify beautifulsoup4")
        sys.exit(1)

    soup = BeautifulSoup(html, "html.parser")

    # ── Remove noise elements ─────────────────────────────────────────────────
    noise_selectors = [
        "nav", "header", "footer", "aside",
        "[role='navigation']", "[role='banner']", "[role='contentinfo']",
        ".sidebar", ".navbar", ".nav", ".toc", ".breadcrumb",
        "#sidebar", "#nav", "#header", "#footer",
        ".on-this-page", "[aria-label='On this page']",
        ".pagination", ".prev-next", ".edit-page",
        # Astro/Starlight specific (docs.codepup.ai uses Starlight)
        "starlight-menu-button", ".sl-sidebar", ".sl-nav",
        "[data-pagefind-ignore]",
    ]
    removed = 0
    for sel in noise_selectors:
        for tag in soup.select(sel):
            tag.decompose()
            removed += 1
    print(f"   🧹 Removed {removed} noise elements")

    # ── Find main content area ────────────────────────────────────────────────
    # Starlight (docs.codepup.ai) renders:
    #   <article class="card sl-flex">        ← card WRAPPER, not content
    #     <div class="sl-markdown-content">   ← actual content lives here
    #
    # Try most-specific selectors first, fall back broadly.
    def find_main(soup):
        # Starlight-specific
        el = soup.find(class_="sl-markdown-content")
        if el:
            return el, "sl-markdown-content"
        el = soup.find(class_="content-panel")
        if el:
            return el, "content-panel"
        # Generic docs patterns
        el = soup.find(class_="markdown-body")
        if el:
            return el, "markdown-body"
        el = soup.find(class_="prose")
        if el:
            return el, "prose"
        el = soup.find(id="content")
        if el:
            return el, "#content"
        el = soup.find("main")
        if el:
            return el, "main"
        el = soup.find(attrs={"role": "main"})
        if el:
            return el, "role=main"
        # Last resort: largest <article> by text length (avoids card wrappers)
        articles = soup.find_all("article")
        if articles:
            el = max(articles, key=lambda t: len(t.get_text()))
            return el, f"article[longest] class={el.get('class', [])}"
        return soup.body or soup, "body"

    main, selector_used = find_main(soup)
    print(f"   📍 Main content via   : {selector_used}")
    print(f"   📍 Element            : <{main.name}> class={main.get('class', [])}")
    print(f"   📏 Raw text length    : {len(main.get_text()):,} chars")

    # Strip script/style/svg/button inside selected element before converting
    for tag in main.find_all(["script", "style", "svg", "button", "noscript"]):
        tag.decompose()

    # ── Convert to markdown ───────────────────────────────────────────────────
    # markdownify does NOT allow strip= and convert= together — use strip= only.
    clean_md = md_convert(
        str(main),
        heading_style="ATX",
        bullets="-",
        strip=["script", "style", "svg", "button", "noscript", "img"],
    )

    # ── Post-process ──────────────────────────────────────────────────────────
    # Remove lines that are purely navigation links
    lines = clean_md.splitlines()
    filtered = []
    for line in lines:
        # Skip lines that are ONLY a markdown link (nav items)
        if re.match(r'^\s*\[([^\]]{1,80})\]\([^\)]+\)\s*$', line.strip()):
            continue
        filtered.append(line)

    clean_md = "\n".join(filtered)
    # Collapse 3+ blank lines into 2
    clean_md = re.sub(r'\n{3,}', '\n\n', clean_md)
    return clean_md.strip()


# ── Main crawler ──────────────────────────────────────────────────────────────

async def crawl_and_extract(url: str) -> dict:
    """Crawl URL with crawl4ai, extract markdown, return results dict."""
    try:
        from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
        from crawl4ai.extraction_strategy import NoExtractionStrategy
    except ImportError:
        print("ERROR: pip install crawl4ai")
        sys.exit(1)

    print(f"\n{'='*60}")
    print(f"🌐 Crawling: {url}")
    print(f"{'='*60}")

    browser_cfg = BrowserConfig(
        headless=True,
        verbose=False,
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        extra_args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
        ],
    )

    run_cfg = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        extraction_strategy=NoExtractionStrategy(),
        word_count_threshold=5,
        remove_overlay_elements=True,
        exclude_external_links=True,
        js_code=_TAB_CLICK_JS,
        wait_for="css:body",
        page_timeout=30000,
    )

    async with AsyncWebCrawler(config=browser_cfg) as crawler:
        result = await crawler.arun(url=url, config=run_cfg)

    if not result.success:
        print(f"❌ Crawl failed: {result.error_message}")
        return {}

    print(f"✅ Crawl succeeded")
    print(f"   HTML size     : {len(result.html or ''):,} chars")

    # crawl4ai >= 0.4: markdown is now an object with .raw_markdown / .fit_markdown
    # crawl4ai <  0.4: markdown is a plain string
    md_obj   = result.markdown
    if hasattr(md_obj, "fit_markdown"):
        fit_md = md_obj.fit_markdown or ""
        raw_md = md_obj.raw_markdown or str(md_obj) or ""
    else:
        # older API — markdown is just a string
        fit_md = str(md_obj) if md_obj else ""
        raw_md = fit_md

    print(f"   fit_markdown  : {len(fit_md):,} chars")
    print(f"   raw markdown  : {len(raw_md):,} chars")

    # ── Method 1: BeautifulSoup + markdownify (most reliable) ────────────────
    print(f"\n📝 Extracting with BeautifulSoup + markdownify...")
    bs4_md = html_to_clean_markdown(result.html or "", url)
    print(f"   Result        : {len(bs4_md):,} chars | {len(bs4_md.split()):,} words")

    return {
        "url":          url,
        "title":        result.metadata.get("title", ""),
        "html":         result.html or "",
        "bs4_markdown": bs4_md,
        "fit_markdown": fit_md,
        "raw_markdown": raw_md,
    }


# ── Save outputs ──────────────────────────────────────────────────────────────

def url_to_filename(url: str) -> str:
    parsed = urlparse(url)
    raw    = f"{parsed.netloc}{parsed.path}"
    slug   = re.sub(r"[^a-z0-9]+", "-", raw.lower()).strip("-")
    return slug or "page"


def save_outputs(data: dict):
    if not data:
        return

    slug = url_to_filename(data["url"])

    # ── Save BS4+markdownify result (primary) ─────────────────────────────────
    bs4_path = OUTPUT_DIR / f"{slug}__bs4.md"
    bs4_path.write_text(
        f"# {data['title']}\n"
        f"Source: {data['url']}\n"
        f"Method: BeautifulSoup + markdownify\n"
        f"Words: {len(data['bs4_markdown'].split())}\n\n"
        f"---\n\n"
        + data["bs4_markdown"],
        encoding="utf-8",
    )
    print(f"\n💾 Saved BS4 markdown  → {bs4_path}")

    # ── Save fit_markdown (for comparison) ────────────────────────────────────
    fit_path = OUTPUT_DIR / f"{slug}__fit.md"
    fit_path.write_text(
        f"# {data['title']}\n"
        f"Source: {data['url']}\n"
        f"Method: crawl4ai fit_markdown\n"
        f"Words: {len(data['fit_markdown'].split())}\n\n"
        f"---\n\n"
        + data["fit_markdown"],
        encoding="utf-8",
    )
    print(f"💾 Saved fit_markdown  → {fit_path}")

    # ── Save raw HTML (for debugging) ─────────────────────────────────────────
    html_path = OUTPUT_DIR / f"{slug}__raw.html"
    html_path.write_text(data["html"], encoding="utf-8")
    print(f"💾 Saved raw HTML      → {html_path}")

    # ── Print BS4 result to terminal ──────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"📄 BS4 MARKDOWN PREVIEW (first 3000 chars)")
    print(f"{'='*60}")
    print(data["bs4_markdown"][:3000])
    if len(data["bs4_markdown"]) > 3000:
        print(f"\n... [{len(data['bs4_markdown']) - 3000} more chars] ...")
    print(f"{'='*60}")

    # ── Word count comparison ─────────────────────────────────────────────────
    print(f"\n📊 EXTRACTION COMPARISON")
    print(f"{'Method':<30} {'Words':>8} {'Chars':>10}")
    print("-" * 50)
    print(f"{'BS4 + markdownify':<30} {len(data['bs4_markdown'].split()):>8,} {len(data['bs4_markdown']):>10,}")
    print(f"{'crawl4ai fit_markdown':<30} {len(data['fit_markdown'].split()):>8,} {len(data['fit_markdown']):>10,}")
    print(f"{'crawl4ai raw markdown':<30} {len(data['raw_markdown'].split()):>8,} {len(data['raw_markdown']):>10,}")


# ── Entry point ───────────────────────────────────────────────────────────────

async def main():
    parser = argparse.ArgumentParser(
        description="Debug crawler — extracts markdown from a URL and saves locally"
    )
    parser.add_argument(
        "--url",
        default=DEFAULT_URL,
        help=f"URL to crawl (default: {DEFAULT_URL})",
    )
    args = parser.parse_args()

    data = await crawl_and_extract(args.url)
    save_outputs(data)

    print(f"\n✅ Done. Files saved to: {OUTPUT_DIR}/")
    print(f"   Delete {OUTPUT_DIR}/ to remove all debug markdown files.")
    print(f"   Delete debug_crawl.py to remove this script entirely.")


if __name__ == "__main__":
    asyncio.run(main())