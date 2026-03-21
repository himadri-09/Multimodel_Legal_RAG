# utils/web_chunker.py
import re
import time
from typing import List, Dict, Any
from urllib.parse import urlparse

from utils.web_crawler import PageData

MAX_CHUNK_WORDS = 400
MIN_CHUNK_WORDS = 15


class WebChunker:

    def chunk_pages(self, pages: List[PageData]) -> List[Dict[str, Any]]:
        print(f"\n{'='*60}")
        print(f"CHUNKING STARTED  ({len(pages)} pages)")
        print(f"   Max words/chunk : {MAX_CHUNK_WORDS}")
        print(f"   Min words/chunk : {MIN_CHUNK_WORDS}")
        print(f"{'='*60}")

        t0 = time.time()
        all_chunks: List[Dict[str, Any]] = []

        # Per-page stats
        page_chunk_counts = []

        for idx, page in enumerate(pages, 1):
            page_chunks = self._chunk_page(page)
            all_chunks.extend(page_chunks)
            page_chunk_counts.append(len(page_chunks))

            words = len(page.markdown.split()) if page.markdown else 0
            print(
                f"   [{idx:>3}/{len(pages)}]  "
                f"chunks={len(page_chunks):>4}  "
                f"words={words:>6}  "
                f"| {page.url}"
            )

        elapsed = time.time() - t0

        # Stats
        if page_chunk_counts:
            avg_chunks = sum(page_chunk_counts) / len(page_chunk_counts)
            max_chunks = max(page_chunk_counts)
            min_chunks = min(page_chunk_counts)
        else:
            avg_chunks = max_chunks = min_chunks = 0

        text_chunks  = [c for c in all_chunks if c["type"] == "text"]
        total_words  = sum(c["metadata"].get("word_count", 0) for c in text_chunks)

        print(f"\n{'='*60}")
        print(f"✂️  CHUNKING COMPLETE  ({elapsed:.1f}s)")
        print(f"   Pages processed : {len(pages)}")
        print(f"   Total chunks    : {len(all_chunks)}")
        print(f"   Total words     : {total_words:,}")
        print(f"   Avg chunks/page : {avg_chunks:.1f}")
        print(f"   Max chunks/page : {max_chunks}")
        print(f"   Min chunks/page : {min_chunks}")
        print(f"{'='*60}\n")

        return all_chunks

    # ------------------------------------------------------------------

    def _chunk_page(self, page: PageData) -> List[Dict[str, Any]]:
        sections = self._split_by_headings(page.markdown)
        chunks: List[Dict[str, Any]] = []

        for section_idx, (heading_path, section_text) in enumerate(sections):
            sub_texts = self._split_large_section(section_text)
            for sub_idx, text in enumerate(sub_texts):
                word_count = len(text.split())
                if word_count < MIN_CHUNK_WORDS:
                    continue

                chunks.append({
                    "content":     text,
                    "type":        "text",
                    "page_number": section_idx + 1,
                    "pdf_name":    self._url_to_slug(page.url),
                    "metadata": {
                        "source_type":  "web",
                        "source_url":   page.url,
                        "page_title":   page.title,
                        "heading_path": heading_path,
                        "chunk_id":     self._make_chunk_id(page.url, section_idx, sub_idx),
                        "word_count":   word_count,
                    },
                })

        return chunks

    def _split_by_headings(self, markdown: str) -> List[tuple]:
        heading_re = re.compile(r"^(#{1,3})\s+(.+)$", re.MULTILINE)
        splits = [(m.start(), len(m.group(1)), m.group(2).strip())
                  for m in heading_re.finditer(markdown)]

        if not splits:
            return [("", markdown.strip())]

        sections = []
        heading_stack: List[str] = []

        for i, (start, level, title) in enumerate(splits):
            idx = level - 1
            heading_stack = heading_stack[:idx] + [title]
            heading_path  = " > ".join(heading_stack)

            nl_pos = markdown.find("\n", start)
            text_start = nl_pos + 1 if nl_pos != -1 else start
            text_end   = splits[i + 1][0] if i + 1 < len(splits) else len(markdown)
            section_text = markdown[text_start:text_end].strip()

            if section_text:
                sections.append((heading_path, section_text))

        preamble = markdown[:splits[0][0]].strip()
        if len(preamble.split()) >= MIN_CHUNK_WORDS:
            sections.insert(0, ("", preamble))

        return sections or [("", markdown.strip())]

    def _split_large_section(self, text: str) -> List[str]:
        if len(text.split()) <= MAX_CHUNK_WORDS:
            return [text]

        paragraphs = [p.strip() for p in re.split(r"\n{2,}", text) if p.strip()]
        chunks: List[str] = []
        current_paras: List[str] = []
        current_words = 0

        for para in paragraphs:
            pw = len(para.split())
            if current_words + pw > MAX_CHUNK_WORDS and current_paras:
                chunks.append("\n\n".join(current_paras))
                current_paras = [para]
                current_words = pw
            else:
                current_paras.append(para)
                current_words += pw

        if current_paras:
            chunks.append("\n\n".join(current_paras))

        return chunks or [text]

    @staticmethod
    def _url_to_slug(url: str) -> str:
        parsed = urlparse(url)
        raw  = f"{parsed.netloc}{parsed.path}"
        slug = re.sub(r"[^a-z0-9]+", "-", raw.lower()).strip("-")
        return slug or "web-page"

    @staticmethod
    def _make_chunk_id(url: str, section_idx: int, sub_idx: int) -> str:
        parsed = urlparse(url)
        raw  = f"{parsed.netloc}{parsed.path}"
        slug = re.sub(r"[^a-z0-9]+", "-", raw.lower()).strip("-")
        return f"{slug}-s{section_idx}-p{sub_idx}"