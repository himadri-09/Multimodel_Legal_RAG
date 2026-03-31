# utils/bm25_store.py
"""
BM25 keyword index for hybrid retrieval.

Why this exists:
  Dense vector search misses exact terms — API names, CLI flags, error codes,
  config keys, version strings. BM25 catches these. Hybrid = dense + BM25.

How it works:
  - Built in-memory from chunks at crawl time, persisted to disk as JSON
  - Loaded on first query, stays in memory for the process lifetime
  - One index file per site_slug, stored under bm25_indexes/
  - Query time: score all docs, return top-k by BM25 score

No external service needed. Pure Python via rank_bm25.
"""

import json
import math
import os
import re
import time
from pathlib import Path
from typing import Dict, List, Any, Tuple

# rank_bm25 is a lightweight pure-Python library — add to requirements.txt
try:
    from rank_bm25 import BM25Okapi
    BM25_AVAILABLE = True
except ImportError:
    BM25_AVAILABLE = False
    print("⚠️  rank_bm25 not installed — BM25 retrieval disabled. Run: pip install rank-bm25")

BM25_INDEX_DIR = Path("bm25_indexes")
BM25_INDEX_DIR.mkdir(exist_ok=True)

# Minimal stop words — keep domain terms like "api", "config", "error"
STOP_WORDS = {
    "a", "an", "the", "and", "or", "but", "in", "on", "at", "to",
    "for", "of", "with", "by", "from", "is", "are", "was", "were",
    "be", "been", "being", "have", "has", "had", "do", "does", "did",
    "will", "would", "could", "should", "may", "might", "shall",
    "this", "that", "these", "those", "it", "its", "i", "you", "we",
    "he", "she", "they", "not", "no", "can", "also", "as", "if",
}


def _tokenize(text: str) -> List[str]:
    """
    Simple tokenizer that preserves technical terms.
    Splits on whitespace and punctuation, lowercases, removes stop words.
    Keeps: camelCase split, snake_case split, version numbers, error codes.
    """
    if not text:
        return []

    # Split camelCase: "getApiKey" → "get Api Key"
    text = re.sub(r'([a-z])([A-Z])', r'\1 \2', text)

    # Split on non-alphanumeric (keep hyphens inside words)
    tokens = re.split(r'[^\w\-]+', text.lower())

    # Split hyphenated: "rate-limit" → ["rate", "limit"] + "rate-limit"
    expanded = []
    for tok in tokens:
        if '-' in tok:
            parts = tok.split('-')
            expanded.extend(parts)
            expanded.append(tok)  # keep original too
        else:
            expanded.append(tok)

    # Filter: remove stop words, very short tokens, pure numbers < 3 digits
    result = []
    for tok in expanded:
        tok = tok.strip('-')
        if not tok:
            continue
        if tok in STOP_WORDS:
            continue
        if len(tok) < 2:
            continue
        result.append(tok)

    return result


class BM25Store:
    """
    Per-site BM25 index.

    Usage:
        # At crawl time — build and save
        store = BM25Store(site_slug)
        store.build(chunks)          # chunks from WebChunker
        store.save()

        # At query time — load and search
        store = BM25Store(site_slug)
        store.load()
        results = store.search(query, top_k=20)
    """

    def __init__(self, site_slug: str):
        self.site_slug  = site_slug
        self.index_path = BM25_INDEX_DIR / f"{site_slug}.json"
        self.bm25       = None
        self.chunks     = []          # parallel list to BM25 corpus
        self._loaded    = False

    # ── Build ─────────────────────────────────────────────────────────────────

    def build(self, chunks: List[Dict[str, Any]]) -> None:
        """Build BM25 index from a list of chunks (output of WebChunker)."""
        if not BM25_AVAILABLE:
            print("⚠️  BM25 build skipped — rank_bm25 not installed")
            return

        t0 = time.time()
        self.chunks = [c for c in chunks if c.get('content', '').strip()]

        corpus = [_tokenize(c['content']) for c in self.chunks]
        self.bm25 = BM25Okapi(corpus)
        self._loaded = True

        print(f"✅ BM25 index built: {len(self.chunks)} docs in {time.time()-t0:.2f}s")

    # ── Persist ───────────────────────────────────────────────────────────────

    def save(self) -> None:
        """Persist index to disk as JSON so it survives process restarts."""
        if not self.chunks:
            return

        payload = {
            "site_slug": self.site_slug,
            "chunks":    self.chunks,
            "corpus":    [_tokenize(c['content']) for c in self.chunks],
        }
        with open(self.index_path, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False)

        size_kb = self.index_path.stat().st_size / 1024
        print(f"💾 BM25 index saved: {self.index_path} ({size_kb:.1f} KB)")

    def load(self) -> bool:
        """Load index from disk. Returns True if successful."""
        if self._loaded:
            return True

        if not BM25_AVAILABLE:
            return False

        if not self.index_path.exists():
            print(f"⚠️  No BM25 index found for '{self.site_slug}' — keyword search disabled")
            return False

        try:
            t0 = time.time()
            with open(self.index_path, 'r', encoding='utf-8') as f:
                payload = json.load(f)

            self.chunks = payload['chunks']
            corpus      = payload['corpus']
            self.bm25   = BM25Okapi(corpus)
            self._loaded = True

            print(f"✅ BM25 index loaded: {len(self.chunks)} docs in {time.time()-t0:.2f}s")
            return True

        except Exception as e:
            print(f"❌ Failed to load BM25 index: {e}")
            return False

    def delete(self) -> None:
        """Delete persisted index (called when site is deleted)."""
        if self.index_path.exists():
            self.index_path.unlink()
            print(f"🗑️  BM25 index deleted: {self.index_path}")

    # ── Search ────────────────────────────────────────────────────────────────

    def search(
        self,
        query: str,
        top_k: int = 20,
        min_score: float = 0.0,
    ) -> List[Dict[str, Any]]:
        """
        BM25 keyword search.
        Returns list of chunks with 'bm25_score' field added.
        Results are sorted by score descending.
        """
        if not self._loaded or self.bm25 is None or not self.chunks:
            return []

        tokens = _tokenize(query)
        if not tokens:
            return []

        scores = self.bm25.get_scores(tokens)

        # Pair with chunks, filter zero scores, sort
        scored = [
            {**chunk, 'bm25_score': float(score)}
            for chunk, score in zip(self.chunks, scores)
            if score > min_score
        ]
        scored.sort(key=lambda x: x['bm25_score'], reverse=True)

        results = scored[:top_k]
        print(f"🔤 BM25: {len(results)} results for query '{query[:50]}'")
        return results

    @property
    def is_ready(self) -> bool:
        return self._loaded and self.bm25 is not None


# ── In-memory cache of loaded indexes (one per process) ──────────────────────

_index_cache: Dict[str, BM25Store] = {}


def get_bm25_store(site_slug: str) -> BM25Store:
    """Get or load a BM25Store from the in-memory cache."""
    if site_slug not in _index_cache:
        store = BM25Store(site_slug)
        store.load()
        _index_cache[site_slug] = store
    return _index_cache[site_slug]


def invalidate_bm25_cache(site_slug: str) -> None:
    """Remove a site from the in-memory cache (call after re-crawl)."""
    _index_cache.pop(site_slug, None)