# utils/web_crawler.py
import asyncio
import re
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import List, Optional, Set
from urllib.parse import urljoin, urlparse

import aiohttp
from bs4 import BeautifulSoup
from markdownify import markdownify as md_convert
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai import DefaultTableExtraction
from crawl4ai.extraction_strategy import NoExtractionStrategy


@dataclass
class PageData:
    """Holds all extracted data for a single crawled page."""
    url: str
    title: str
    markdown: str
    image_urls: List[str] = field(default_factory=list)
    crawl_depth: int = 0


# JS: click every tab and force all panels visible simultaneously
# so the HTML snapshot contains ALL tab content, not just the last-clicked one
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


class WebCrawler:
    def __init__(
        self,
        max_pages: int = 100,
        max_depth: int = 5,
        concurrency: int = 5,
    ):
        self.max_pages = max_pages
        self.max_depth = max_depth
        self.concurrency = concurrency

    async def crawl(self, start_url: str, progress_callback=None) -> List[PageData]:

        start_url = start_url.rstrip("/")
        base_domain = self._base_domain(start_url)
        crawl_start = time.time()

        print(f"\n{'='*60}")
        print(f"🌐 CRAWL STARTED")
        print(f"   URL        : {start_url}")
        print(f"   Max pages  : {self.max_pages}")
        print(f"   Max depth  : {self.max_depth}")
        print(f"   Concurrency: {self.concurrency}")
        print(f"{'='*60}")

        visited: set[str] = set()
        results: List[PageData] = []
        failed_urls: List[str] = []
        depth_stats: dict[int, int] = {}

        queue: asyncio.Queue = asyncio.Queue()

        # Seed from sitemap first (catches pages not linked anywhere)
        sitemap_urls = await self._discover_from_sitemap(start_url, base_domain)
        if sitemap_urls:
            print(f"\n🗺️  SITEMAP  found {len(sitemap_urls)} URLs — seeding queue at depth 1")
            for u in sitemap_urls:
                norm = self._normalize_url(u)
                if self._is_allowed(norm, start_url, base_domain):
                    await queue.put((norm, 1))
        else:
            print(f"\n🗺️  SITEMAP  not found or empty — relying on link discovery only")

        # Always start from root at depth 0
        await queue.put((start_url, 0))

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

        table_strategy = DefaultTableExtraction(
            table_score_threshold=5,
            min_rows=2,
            min_cols=2,
            verbose=False,
        )

        run_cfg = CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            extraction_strategy=NoExtractionStrategy(),
            table_extraction=table_strategy,
            word_count_threshold=10,
            remove_overlay_elements=True,
            exclude_external_links=True,
            js_code=_TAB_CLICK_JS,
            wait_for="css:body",
            page_timeout=30000,
        )

        semaphore = asyncio.Semaphore(self.concurrency)

        async with AsyncWebCrawler(config=browser_cfg) as crawler:
            while not queue.empty() and len(results) < self.max_pages:
                # Drain a batch
                batch: List[tuple] = []
                while not queue.empty() and len(batch) < self.concurrency:
                    url, depth = await queue.get()
                    norm = self._normalize_url(url)
                    if norm in visited or not self._is_allowed(norm, start_url, base_domain):
                        continue
                    visited.add(norm)
                    batch.append((norm, depth))

                if not batch:
                    break

                print(f"\n📦 Batch {len(batch)} URLs  |  crawled={len(results)}  queued={queue.qsize()}")

                tasks = [
                    self._crawl_one(crawler, run_cfg, semaphore, url, depth)
                    for url, depth in batch
                ]
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)

                for (url, depth), result in zip(batch, batch_results):
                    if isinstance(result, Exception) or result is None:
                        reason = str(result) if isinstance(result, Exception) else "empty response"
                        print(f"   ❌ FAILED  [depth={depth}] {url}  → {reason}")
                        failed_urls.append(url)
                        continue

                    results.append(result)
                    depth_stats[depth] = depth_stats.get(depth, 0) + 1

                    words = len(result.markdown.split()) if result.markdown else 0
                    imgs  = len(result.image_urls)

                    print(
                        f"   ✅ [{len(results):>3}/{self.max_pages}]  "
                        f"depth={depth}  words={words:>5}  images={imgs:>3}  "
                        f"| {result.title[:40]!r}  {url}"
                    )

                    if progress_callback:
                        await progress_callback(len(results), len(visited))

                    # Enqueue discovered links
                    if depth < self.max_depth:
                        new_links = 0
                        for link in result._discovered_links:  # type: ignore[attr-defined]
                            norm_link = self._normalize_url(link)
                            if norm_link not in visited and self._is_allowed(norm_link, start_url, base_domain):
                                await queue.put((norm_link, depth + 1))
                                new_links += 1
                        if new_links:
                            print(f"         ↳ enqueued {new_links} links at depth {depth+1}")

        # Summary
        elapsed      = time.time() - crawl_start
        total_words  = sum(len(p.markdown.split()) for p in results if p.markdown)
        total_images = sum(len(p.image_urls) for p in results)

        print(f"\n{'='*60}")
        print(f"🏁 CRAWL COMPLETE  ({elapsed:.1f}s)")
        print(f"   Pages crawled  : {len(results)}")
        print(f"   Pages failed   : {len(failed_urls)}")
        print(f"   URLs visited   : {len(visited)}")
        print(f"   Total words    : {total_words:,}")
        print(f"   Total images   : {total_images}")
        print(f"   Avg per page   : {elapsed/max(len(results),1):.2f}s")
        print(f"\n   Pages by depth:")
        for d in sorted(depth_stats):
            bar = "█" * min(depth_stats[d], 40)
            print(f"     depth {d}: {depth_stats[d]:>4}  {bar}")
        if failed_urls:
            print(f"\n   Failed URLs ({len(failed_urls)}):")
            for u in failed_urls[:10]:
                print(f"     - {u}")
            if len(failed_urls) > 10:
                print(f"     ... and {len(failed_urls)-10} more")
        print(f"{'='*60}\n")

        return results

    # ------------------------------------------------------------------
    # Sitemap discovery
    # ------------------------------------------------------------------

    async def _discover_from_sitemap(self, start_url: str, base_domain: str) -> List[str]:
        candidates = [
            f"{base_domain}/sitemap.xml",
            f"{base_domain}/sitemap_index.xml",
            f"{base_domain}/sitemap-index.xml",
            f"{base_domain}/sitemap/sitemap.xml",
        ]

        found: List[str] = []

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10)
        ) as session:
            for sitemap_url in candidates:
                urls = await self._fetch_sitemap(session, sitemap_url, base_domain, depth=0)
                if urls:
                    found = urls
                    print(f"   ✅ sitemap: {sitemap_url}  ({len(found)} URLs)")
                    break
                else:
                    print(f"   ⚪ sitemap: {sitemap_url}  (not found)")

        return found

    async def _fetch_sitemap(
        self,
        session: aiohttp.ClientSession,
        sitemap_url: str,
        base_domain: str,
        depth: int,
    ) -> List[str]:
        if depth > 2:
            return []

        try:
            async with session.get(sitemap_url) as resp:
                if resp.status != 200:
                    return []
                text = await resp.text()
        except Exception:
            return []

        try:
            root = ET.fromstring(text)
        except ET.ParseError:
            return []

        ns_re = re.compile(r"\{[^}]+\}")
        tag = lambda el: ns_re.sub("", el.tag)

        urls: List[str] = []

        for child in root:
            child_tag = tag(child)

            if child_tag == "sitemap":
                for sub in child:
                    if tag(sub) == "loc" and sub.text:
                        sub_urls = await self._fetch_sitemap(
                            session, sub.text.strip(), base_domain, depth + 1
                        )
                        urls.extend(sub_urls)

            elif child_tag == "url":
                for sub in child:
                    if tag(sub) == "loc" and sub.text:
                        loc = sub.text.strip()
                        if loc.startswith(base_domain):
                            urls.append(loc)

        return urls

    # ------------------------------------------------------------------
    # Core page crawl — BS4 + markdownify extraction
    # ------------------------------------------------------------------

    async def _crawl_one(self, crawler, run_cfg, semaphore, url, depth) -> Optional[PageData]:
        async with semaphore:
            try:
                result = await crawler.arun(url=url, config=run_cfg)
                if not result.success:
                    return None

                title = result.metadata.get("title", "") or urlparse(url).path or url

                # ── PRIMARY: BS4 + markdownify ────────────────────────────────
                # Uses the fully-rendered HTML snapshot (after JS execution),
                # so tab panels forced visible by _TAB_CLICK_JS are included.
                markdown = self._html_to_clean_markdown(result.html or "")

                # ── FALLBACK: crawl4ai raw markdown ───────────────────────────
                # Used only if BS4 extraction yields too little content.
                if len(markdown.split()) < 30:
                    md_obj = result.markdown
                    if hasattr(md_obj, "raw_markdown"):
                        fallback = md_obj.raw_markdown or ""
                    else:
                        fallback = str(md_obj) if md_obj else ""
                    if fallback:
                        print(f"      ⚠️  BS4 low content ({len(markdown.split())} words), using crawl4ai fallback for {url}")
                        markdown = self._clean_markdown(fallback)

                # Warn if still low content after fallback
                word_count = len(markdown.split())
                if word_count < 50:
                    print(f"      ⚠️  Low content warning: {word_count} words from {url}")

                # ── Append extracted tables ───────────────────────────────────
                if result.tables:
                    table_md = []
                    for table in result.tables:
                        lines   = []
                        caption = table.get("caption", "")
                        headers = table.get("headers", [])
                        rows    = table.get("rows", [])
                        if caption:
                            lines.append(f"\n### {caption}\n")
                        if headers:
                            lines.append("| " + " | ".join(str(h) for h in headers) + " |")
                            lines.append("|" + " --- |" * len(headers))
                        for row in rows:
                            lines.append("| " + " | ".join(str(c) for c in row) + " |")
                        if lines:
                            table_md.append("\n".join(lines))
                    if table_md:
                        markdown += "\n\n" + "\n\n".join(table_md)
                        print(f"      📊 {len(result.tables)} table(s) appended → {url[:60]}")

                # Discover internal links for queue
                discovered = [
                    urljoin(url, li.get("href", ""))
                    for li in (result.links or {}).get("internal", [])
                    if li.get("href")
                ]

                page = PageData(url=url, title=title, markdown=markdown,
                                image_urls=[], crawl_depth=depth)
                page._discovered_links = discovered  # type: ignore[attr-defined]
                return page

            except Exception as e:
                raise RuntimeError(str(e)) from e

    # ------------------------------------------------------------------
    # BS4 + markdownify extraction (primary pipeline)
    # ------------------------------------------------------------------

    @staticmethod
    def _html_to_clean_markdown(html: str) -> str:
        """
        Strip nav/sidebar/footer from rendered HTML,
        find the main content element, convert to clean markdown.

        Selector priority (most-specific first):
          1. .sl-markdown-content   — Starlight/Astro docs
          2. .content-panel         — Starlight fallback
          3. .markdown-body         — GitHub-style docs
          4. .prose                 — Tailwind prose
          5. #content               — generic id
          6. <main>                 — semantic HTML
          7. role=main              — ARIA
          8. largest <article>      — avoids card wrappers
          9. <body>                 — last resort
        """
        soup = BeautifulSoup(html, "html.parser")

        # Remove noise elements
        noise_selectors = [
            "nav", "header", "footer", "aside",
            "[role='navigation']", "[role='banner']", "[role='contentinfo']",
            ".sidebar", ".navbar", ".nav", ".toc", ".breadcrumb",
            "#sidebar", "#nav", "#header", "#footer",
            ".on-this-page", "[aria-label='On this page']",
            ".pagination", ".prev-next", ".edit-page",
            # Starlight/Astro specific
            "starlight-menu-button", ".sl-sidebar", ".sl-nav",
            "[data-pagefind-ignore]",
        ]
        for sel in noise_selectors:
            for tag in soup.select(sel):
                tag.decompose()

        # Find main content area
        def find_main(s):
            el = s.find(class_="sl-markdown-content")
            if el: return el
            el = s.find(class_="content-panel")
            if el: return el
            el = s.find(class_="markdown-body")
            if el: return el
            el = s.find(class_="prose")
            if el: return el
            el = s.find(id="content")
            if el: return el
            el = s.find("main")
            if el: return el
            el = s.find(attrs={"role": "main"})
            if el: return el
            articles = s.find_all("article")
            if articles:
                return max(articles, key=lambda t: len(t.get_text()))
            return s.body or s

        main = find_main(soup)

        # Strip noise tags inside the selected element
        for tag in main.find_all(["script", "style", "svg", "button", "noscript"]):
            tag.decompose()

        # Convert to markdown
        # NOTE: markdownify does NOT allow strip= and convert= simultaneously
        clean_md = md_convert(
            str(main),
            heading_style="ATX",
            bullets="-",
            strip=["script", "style", "svg", "button", "noscript", "img"],
        )

        # Remove lines that are purely nav links (e.g. sidebar TOC items)
        lines = clean_md.splitlines()
        filtered = [
            line for line in lines
            if not re.match(r'^\s*\[([^\]]{1,80})\]\([^\)]+\)\s*$', line.strip())
        ]
        clean_md = "\n".join(filtered)

        # Collapse 3+ blank lines → 2
        clean_md = re.sub(r'\n{3,}', '\n\n', clean_md)
        return clean_md.strip()

    # ------------------------------------------------------------------
    # crawl4ai markdown cleaner (fallback only)
    # ------------------------------------------------------------------

    @staticmethod
    def _clean_markdown(md: str) -> str:
        """Clean crawl4ai raw markdown — used only as fallback."""
        lines = [l for l in md.splitlines()
                 if not re.match(r"^\[.{1,40}\]\(.*\)$", l.strip())]
        result = []
        skip = False
        for line in lines:
            if re.match(r'^#+\s*(on this page|table of contents|contents)', line, re.I):
                skip = True
            elif skip and re.match(r'^#+\s', line):
                skip = False
            if not skip:
                result.append(line)
        cleaned = "\n".join(result).strip()
        cleaned = re.sub(r'\n{3,}', '\n\n', cleaned)
        return cleaned

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_url(url: str) -> str:
        return urlparse(url)._replace(fragment="").geturl().rstrip("/")

    @staticmethod
    def _base_domain(url: str) -> str:
        p = urlparse(url)
        return f"{p.scheme}://{p.netloc}"

    @staticmethod
    def _is_allowed(url: str, start_url: str, base_domain: str) -> bool:
        if not url.startswith(base_domain):
            return False
        start_path  = urlparse(start_url).path.rstrip("/")
        target_path = urlparse(url).path
        if start_path and not target_path.startswith(start_path):
            return False
        skip = {".pdf",".zip",".tar",".gz",".png",".jpg",".jpeg",
                ".gif",".svg",".webp",".ico",".mp4",".mp3",".css",".js"}
        if any(urlparse(url).path.lower().endswith(e) for e in skip):
            return False
        return True

    @staticmethod
    def _extract_image_urls(result, page_url: str) -> List[str]:
        seen, images = set(), []
        for img in (getattr(result, "media", {}) or {}).get("images", []):
            src = img.get("src", "")
            if src:
                abs_url = urljoin(page_url, src)
                if abs_url not in seen:
                    seen.add(abs_url)
                    images.append(abs_url)
        return images