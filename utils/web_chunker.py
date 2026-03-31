# utils/web_chunker.py
import re
import time
from typing import List, Dict, Any
from urllib.parse import urlparse

from utils.web_crawler import PageData

MAX_CHUNK_WORDS   = 300
MIN_CHUNK_WORDS   = 60
OVERLAP_WORDS     = 50
SMALL_PAGE_THRESH = 250


class WebChunker:

    def chunk_pages(self, pages: List[PageData], site_slug: str) -> List[Dict[str, Any]]:
        print(f"\n{'='*60}")
        print(f"CHUNKING STARTED  ({len(pages)} pages)")
        print(f"   Max words/chunk : {MAX_CHUNK_WORDS}")
        print(f"   Min words/chunk : {MIN_CHUNK_WORDS}")
        print(f"   Overlap words   : {OVERLAP_WORDS}")
        print(f"{'='*60}")

        t0 = time.time()
        all_chunks: List[Dict[str, Any]] = []
        page_chunk_counts = []

        for idx, page in enumerate(pages, 1):
            page_chunks = self._chunk_page(page, site_slug)
            all_chunks.extend(page_chunks)
            page_chunk_counts.append(len(page_chunks))

            words = len(page.markdown.split()) if page.markdown else 0
            print(
                f"   [{idx:>3}/{len(pages)}]  "
                f"chunks={len(page_chunks):>4}  words={words:>6}  "
                f"| {page.url}"
            )

        elapsed = time.time() - t0

        if page_chunk_counts:
            avg_chunks = sum(page_chunk_counts) / len(page_chunk_counts)
            max_chunks = max(page_chunk_counts)
            min_chunks = min(page_chunk_counts)
        else:
            avg_chunks = max_chunks = min_chunks = 0

        text_chunks = [c for c in all_chunks if c["type"] == "text"]
        total_words = sum(c["metadata"].get("word_count", 0) for c in text_chunks)

        print(f"\n{'='*60}")
        print(f"CHUNKING COMPLETE  ({elapsed:.1f}s)")
        print(f"   Pages processed : {len(pages)}")
        print(f"   Total chunks    : {len(all_chunks)}")
        print(f"   Total words     : {total_words:,}")
        print(f"   Avg chunks/page : {avg_chunks:.1f}")
        print(f"   Max chunks/page : {max_chunks}")
        print(f"   Min chunks/page : {min_chunks}")
        print(f"{'='*60}\n")

        return all_chunks

    # ------------------------------------------------------------------

    def _chunk_page(self, page: PageData, site_slug: str) -> List[Dict[str, Any]]:
        if not page.markdown or not page.markdown.strip():
            return []

        # Skip if content is mostly links (nav pages, sitemaps)
        lines = page.markdown.strip().splitlines()
        link_lines = sum(1 for l in lines if re.match(r'^\s*\*?\s*\[.+\]\(.+\)', l))
        if len(lines) > 0 and link_lines / len(lines) > 0.6:
            print(f"   ⏭️  Skipping nav-heavy page: {page.url} ({link_lines}/{len(lines)} link lines)")
            return []

        word_count = len(page.markdown.split())

        if word_count <= SMALL_PAGE_THRESH:
            return self._make_chunks_from_text(
                page.markdown.strip(), page, site_slug, strategy="single"
            )

        return self._make_chunks_from_text(
            page.markdown.strip(), page, site_slug, strategy="sliding"
        )

    # ------------------------------------------------------------------

    def _make_chunks_from_text(
        self,
        text: str,
        page: PageData,
        site_slug: str,
        strategy: str,
    ) -> List[Dict[str, Any]]:

        # Prefix built separately — NOT counted in chunk word sizing
        context_prefix = f"[{page.title}]\nSource: {page.url}\n\n"

        chunks: List[Dict[str, Any]] = []

        if strategy == "single":
            wc = len(text.split())
            if wc >= MIN_CHUNK_WORDS:
                content = context_prefix + text
                chunks.append(self._make_chunk(content, page, site_slug, 1, wc))
            return chunks

        # Sliding window strategy
        paragraphs = [p.strip() for p in re.split(r"\n{2,}", text) if p.strip()]

        # Only merge truly tiny paragraphs (under 20 words)
        merged_paragraphs = self._merge_short_paragraphs(paragraphs, min_words=20)

        # Build sliding windows — sized by content words only
        window_texts = self._sliding_window(merged_paragraphs, MAX_CHUNK_WORDS, OVERLAP_WORDS)

        for idx, window_text in enumerate(window_texts):
            wc = len(window_text.split())  # content words only, no prefix
            if wc >= MIN_CHUNK_WORDS:
                content = context_prefix + window_text  # prefix added AFTER sizing
                chunks.append(self._make_chunk(content, page, site_slug, idx + 1, wc))

        return chunks

    # ------------------------------------------------------------------

    def _merge_short_paragraphs(
        self, paragraphs: List[str], min_words: int = 20
    ) -> List[str]:
        """Merge paragraphs under min_words into their successor."""
        if not paragraphs:
            return []

        merged = []
        buffer = ""

        for para in paragraphs:
            if buffer:
                if len(buffer.split()) < min_words:
                    buffer = buffer + "\n\n" + para
                else:
                    merged.append(buffer)
                    buffer = para
            else:
                buffer = para

        if buffer:
            merged.append(buffer)

        return merged

    # ------------------------------------------------------------------

    def _sliding_window(
        self, paragraphs: List[str], max_words: int, overlap_words: int
    ) -> List[str]:
        """
        Build overlapping chunks from paragraphs.
        Handles paragraphs larger than max_words via sentence splitting.
        Never gets stuck in an infinite loop.
        """
        if not paragraphs:
            return []

        chunks: List[str] = []
        current_paras: List[str] = []
        current_words = 0

        for para in paragraphs:
            pw = len(para.split())

            # Single paragraph bigger than max — split by sentences first
            if pw > max_words:
                # Flush current buffer before handling giant paragraph
                if current_paras:
                    chunks.append("\n\n".join(current_paras))
                    current_paras = []
                    current_words = 0

                sub_chunks = self._split_by_sentences(para, max_words)
                chunks.extend(sub_chunks)
                continue

            if current_words + pw > max_words and current_paras:
                # Emit current window
                chunks.append("\n\n".join(current_paras))

                # Build overlap from tail of current window
                overlap_paras: List[str] = []
                overlap_count = 0
                for p in reversed(current_paras):
                    wc = len(p.split())
                    if overlap_count + wc <= overlap_words:
                        overlap_paras.insert(0, p)
                        overlap_count += wc
                    else:
                        break

                # Start next window with overlap + current paragraph
                current_paras = overlap_paras + [para]
                current_words = overlap_count + pw
            else:
                current_paras.append(para)
                current_words += pw

        # Flush remaining
        if current_paras:
            chunks.append("\n\n".join(current_paras))

        return chunks

    # ------------------------------------------------------------------

    def _split_by_sentences(self, text: str, max_words: int) -> List[str]:
        """
        Fallback for paragraphs larger than max_words.
        Splits on sentence boundaries (.  !  ?) into max_words chunks.
        """
        sentences = re.split(r'(?<=[.!?])\s+', text)
        chunks: List[str] = []
        current: List[str] = []
        current_wc = 0

        for sent in sentences:
            sw = len(sent.split())

            # Single sentence larger than max — hard split by words
            if sw > max_words:
                if current:
                    chunks.append(" ".join(current))
                    current = []
                    current_wc = 0
                words = sent.split()
                for i in range(0, len(words), max_words):
                    chunks.append(" ".join(words[i:i + max_words]))
                continue

            if current_wc + sw > max_words and current:
                chunks.append(" ".join(current))
                current = [sent]
                current_wc = sw
            else:
                current.append(sent)
                current_wc += sw

        if current:
            chunks.append(" ".join(current))

        return [c for c in chunks if c.strip()]

    # ------------------------------------------------------------------

    def _make_chunk(
        self,
        content: str,
        page: PageData,
        site_slug: str,
        chunk_index: int,
        word_count: int,
    ) -> Dict[str, Any]:
        return {
            "content":     content,
            "type":        "text",
            "page_number": chunk_index,
            "pdf_name":    site_slug,
            "metadata": {
                "source_type":  "web",
                "source_url":   page.url,
                "page_title":   page.title,
                "chunk_index":  chunk_index,
                "word_count":   word_count,
                "chunk_id":     self._make_chunk_id(page.url, chunk_index),
            },
        }

    # ------------------------------------------------------------------

    @staticmethod
    def _url_to_slug(url: str) -> str:
        parsed = urlparse(url)
        raw  = f"{parsed.netloc}{parsed.path}"
        slug = re.sub(r"[^a-z0-9]+", "-", raw.lower()).strip("-")
        return slug or "web-page"

    @staticmethod
    def _make_chunk_id(url: str, chunk_index: int) -> str:
        parsed = urlparse(url)
        raw  = f"{parsed.netloc}{parsed.path}"
        slug = re.sub(r"[^a-z0-9]+", "-", raw.lower()).strip("-")
        return f"{slug}-c{chunk_index}"