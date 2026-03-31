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
from typing import Dict, List, Any, Optional, Tuple

# rank_bm25 is a lightweight pure-Python library — add to requirements.txt
try:
    from rank_bm25 import BM25Okapi
    BM25_AVAILABLE = True
except ImportError:
    BM25_AVAILABLE = False
    print("⚠️  rank_bm25 not installed — BM25 retrieval disabled. Run: pip install rank-bm25")

# Azure Blob Storage client
try:
    from azure.storage.blob import BlobServiceClient
    AZURE_BLOB_AVAILABLE = True
except ImportError:
    AZURE_BLOB_AVAILABLE = False
    print("⚠️  azure-storage-blob not installed — BM25 blob storage disabled. Run: pip install azure-storage-blob")

from config import AZURE_STORAGE_CONNECTION_STRING

BM25_INDEX_DIR = Path("bm25_indexes")
BM25_INDEX_DIR.mkdir(exist_ok=True)

# Azure blob container name used for saving/loading BM25 JSON blobs.
# Use a lowercase container name (no underscores); adjust if your blob container differs.
BM25_BLOB_CONTAINER = "bm25-indexes"

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
    def __init__(self, site_slug: str, user_id: str = None):
        self.site_slug = site_slug
        self.user_id   = user_id   # needed for Supabase save/load
        self.bm25      = None
        self.chunks    = []
        self._loaded   = False
        self._blob_url = None

    def build(self, chunks: List[Dict[str, Any]]) -> None:
        """Build index from chunks — same as before."""
        if not BM25_AVAILABLE:
            return
        t0 = time.time()
        self.chunks = [c for c in chunks if c.get('content', '').strip()]
        corpus = [_tokenize(c['content']) for c in self.chunks]
        self.bm25 = BM25Okapi(corpus)
        self._loaded = True
        print(f"✅ BM25 built: {len(self.chunks)} docs in {time.time()-t0:.2f}s")

    # ── Azure Blob Storage helper ─────────────────────────────────────────

    def _get_blob_service(self):
        """
        Get Azure Blob Storage service client.
        Uses connection string from config.
        Creates container if it doesn't exist.
        """
        if not AZURE_BLOB_AVAILABLE:
            raise RuntimeError("azure-storage-blob not installed. Run: pip install azure-storage-blob")
        if not AZURE_STORAGE_CONNECTION_STRING:
            raise ValueError("AZURE_STORAGE_CONNECTION_STRING not set in environment")
        
        blob_service = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        
        # Create container if it doesn't exist
        try:
            container_client = blob_service.get_container_client(container=BM25_BLOB_CONTAINER)
            container_client.get_container_properties()  # Check if exists
        except Exception:
            # Container doesn't exist, create it
            try:
                blob_service.create_container(name=BM25_BLOB_CONTAINER)
                print(f"✅ Created blob container: {BM25_BLOB_CONTAINER}")
            except Exception as e:
                print(f"⚠️  Container creation failed (may already exist): {e}")
        
        return blob_service

    # ── Supabase persistence ──────────────────────────────────────────

    def save_to_blob(self, supabase_client) -> Optional[str]:
        if not self.chunks:
            print("⚠️  BM25 save skipped — no chunks")
            return None
        if not self.user_id:
            print("⚠️  BM25 save skipped — user_id required")
            return None

        try:
            # Serialize + upload to blob — same as before
            payload       = json.dumps({
                "site_slug": self.site_slug,
                "chunks":    self.chunks,
                "corpus":    [_tokenize(c['content']) for c in self.chunks],
            }, ensure_ascii=False).encode("utf-8")

            blob_name   = f"{self.user_id}/{self.site_slug}.json"
            blob_svc    = self._get_blob_service()
            blob_client = blob_svc.get_blob_client(
                container=BM25_BLOB_CONTAINER, blob=blob_name
            )
            blob_client.upload_blob(payload, overwrite=True)

            account_name   = blob_svc.account_name
            blob_url       = (
                f"https://{account_name}.blob.core.windows.net"
                f"/{BM25_BLOB_CONTAINER}/{blob_name}"
            )
            self._blob_url = blob_url
            print(f"✅ BM25 uploaded: {blob_name} ({len(payload)/1024:.1f} KB)")

            # ── Store URL in user_pdfs (no separate table) ────────────────
            supabase_client.table("user_pdfs") \
                .update({"bm25_blob_url": blob_url}) \
                .eq("user_id", self.user_id) \
                .eq("pdf_name", self.site_slug) \
                .execute()

            print(f"✅ BM25 blob URL saved to user_pdfs: {self.site_slug}")
            return blob_url

        except Exception as e:
            print(f"❌ BM25 save failed: {e}")
            return None

    def load_from_blob(self, supabase_client) -> bool:
        if self._loaded:
            return True
        if not BM25_AVAILABLE:
            return False

        try:
            # Get blob URL from user_pdfs
            response = supabase_client.table("user_pdfs") \
                .select("bm25_blob_url") \
                .eq("pdf_name", self.site_slug) \
                .single() \
                .execute()

            if not response.data or not response.data.get("bm25_blob_url"):
                print(f"⚠️  No BM25 blob URL in user_pdfs for '{self.site_slug}'")
                return False

            blob_url       = response.data["bm25_blob_url"]
            self._blob_url = blob_url

            # Download + rebuild — same as before
            parts     = blob_url.split(f"{BM25_BLOB_CONTAINER}/", 1)
            blob_name = parts[1] if len(parts) == 2 else None
            if not blob_name:
                print(f"❌ Could not parse blob name from: {blob_url}")
                return False

            t0          = time.time()
            blob_svc    = self._get_blob_service()
            blob_client = blob_svc.get_blob_client(
                container=BM25_BLOB_CONTAINER, blob=blob_name
            )
            data        = json.loads(blob_client.download_blob().readall().decode("utf-8"))

            self.chunks  = data["chunks"]
            self.bm25    = BM25Okapi(data["corpus"])
            self._loaded = True

            print(f"✅ BM25 loaded: {len(self.chunks)} docs in {time.time()-t0:.2f}s")
            return True

        except Exception as e:
            print(f"❌ BM25 load failed for '{self.site_slug}': {e}")
            return False

    def delete_from_blob(self, supabase_client) -> None:
        # Delete the actual blob
        try:
            if not self._blob_url:
                response = supabase_client.table("user_pdfs") \
                    .select("bm25_blob_url") \
                    .eq("pdf_name", self.site_slug) \
                    .single() \
                    .execute()
                if response.data:
                    self._blob_url = response.data.get("bm25_blob_url")

            if self._blob_url:
                parts     = self._blob_url.split(f"{BM25_BLOB_CONTAINER}/", 1)
                blob_name = parts[1] if len(parts) == 2 else None
                if blob_name:
                    self._get_blob_service().get_blob_client(
                        container=BM25_BLOB_CONTAINER, blob=blob_name
                    ).delete_blob()
                    print(f"🗑️  BM25 blob deleted: {blob_name}")
        except Exception as e:
            print(f"⚠️  BM25 blob delete failed (non-fatal): {e}")

        # Clear the field in user_pdfs (don't delete the row)
        try:
            supabase_client.table("user_pdfs") \
                .update({"bm25_blob_url": None}) \
                .eq("pdf_name", self.site_slug) \
                .execute()
            print(f"🗑️  BM25 blob URL cleared in user_pdfs: {self.site_slug}")
        except Exception as e:
            print(f"⚠️  BM25 field clear failed: {e}")

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
    """Get or create a BM25Store from the in-memory cache."""
    if site_slug not in _index_cache:
        store = BM25Store(site_slug)
        _index_cache[site_slug] = store
    return _index_cache[site_slug]


def invalidate_bm25_cache(site_slug: str) -> None:
    """Remove a site from the in-memory cache (call after re-crawl)."""
    _index_cache.pop(site_slug, None)