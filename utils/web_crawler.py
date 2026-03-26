# utils/web_crawler.py
import asyncio
import re
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import List, Optional, Set
from urllib.parse import urljoin, urlparse

import aiohttp
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.extraction_strategy import NoExtractionStrategy


@dataclass
class PageData:
    """Holds all extracted data for a single crawled page."""
    url: str
    title: str
    markdown: str
    image_urls: List[str] = field(default_factory=list)
    crawl_depth: int = 0


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

        # ── Step 0: Seed from sitemap (catches pages not linked anywhere) ──
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

        browser_cfg = BrowserConfig(headless=True, verbose=False)
        run_cfg = CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            extraction_strategy=NoExtractionStrategy(),
            word_count_threshold=10,
            remove_overlay_elements=True,
            exclude_external_links=True,
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

        # ── Summary ───────────────────────────────────────────────────
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
        """
        Try to fetch sitemap.xml (and sitemap_index.xml).
        Returns a flat list of all page URLs found that belong to base_domain.
        Falls back gracefully — never raises.
        """
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
        """
        Fetch one sitemap URL. Handles both regular sitemaps (<url><loc>)
        and sitemap indexes (<sitemap><loc>) recursively (max 2 levels).
        """
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

        # Strip XML namespace for simpler tag matching
        ns_re = re.compile(r"\{[^}]+\}")
        tag = lambda el: ns_re.sub("", el.tag)

        urls: List[str] = []

        for child in root:
            child_tag = tag(child)

            if child_tag == "sitemap":
                # Sitemap index — recurse into each child sitemap
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

    async def _crawl_one(self, crawler, run_cfg, semaphore, url, depth) -> Optional[PageData]:
        async with semaphore:
            try:
                result = await crawler.arun(url=url, config=run_cfg)
                if not result.success:
                    return None

                title     = result.metadata.get("title", "") or urlparse(url).path or url
                markdown  = self._clean_markdown(result.markdown or "")
                img_urls  = []

                discovered = [
                    urljoin(url, li.get("href", ""))
                    for li in (result.links or {}).get("internal", [])
                    if li.get("href")
                ]

                page = PageData(url=url, title=title, markdown=markdown,
                                image_urls=img_urls, crawl_depth=depth)
                page._discovered_links = discovered  # type: ignore[attr-defined]
                return page

            except Exception as e:
                raise RuntimeError(str(e)) from e

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
    def _clean_markdown(md: str) -> str:
        lines = [l for l in md.splitlines()
                 if not re.match(r"^\[.{1,40}\]\(.*\)$", l.strip())]
        return "\n".join(lines).strip()

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