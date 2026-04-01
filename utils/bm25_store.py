# utils/bm25_store.py
"""
BM25 keyword index for hybrid retrieval.

Changes vs previous version:
  - load_from_blob: added user_id filter so .single() never matches multiple
    rows — fixes PGRST116 "result contains N rows" error
  - save_to_blob: no longer writes bm25_blob_url to user_pdfs itself.
    Returns the URL instead. The crawl pipeline stores it after log_pdf_upload()
    creates the row, via db.update_pdf_status(..., bm25_blob_url=url).
  - load_from_blob: fallback direct-blob-path lookup when DB URL is missing
    (handles race conditions during crawl)
  - In-memory cache keyed by (user_id, site_slug) so different users with
    the same slug never share an index
"""

import json
import re
import time
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

try:
    from rank_bm25 import BM25Okapi
    BM25_AVAILABLE = True
except ImportError:
    BM25_AVAILABLE = False
    print("⚠️  rank_bm25 not installed — BM25 retrieval disabled. Run: pip install rank-bm25")

try:
    from azure.storage.blob import BlobServiceClient
    AZURE_BLOB_AVAILABLE = True
except ImportError:
    AZURE_BLOB_AVAILABLE = False
    print("⚠️  azure-storage-blob not installed. Run: pip install azure-storage-blob")

from config import AZURE_STORAGE_CONNECTION_STRING

BM25_INDEX_DIR      = Path("bm25_indexes")
BM25_INDEX_DIR.mkdir(exist_ok=True)
BM25_BLOB_CONTAINER = "bm25-indexes"

STOP_WORDS = {
    "a", "an", "the", "and", "or", "but", "in", "on", "at", "to",
    "for", "of", "with", "by", "from", "is", "are", "was", "were",
    "be", "been", "being", "have", "has", "had", "do", "does", "did",
    "will", "would", "could", "should", "may", "might", "shall",
    "this", "that", "these", "those", "it", "its", "i", "you", "we",
    "he", "she", "they", "not", "no", "can", "also", "as", "if",
}


def _tokenize(text: str) -> List[str]:
    if not text:
        return []
    text   = re.sub(r'([a-z])([A-Z])', r'\1 \2', text)
    tokens = re.split(r'[^\w\-]+', text.lower())
    expanded = []
    for tok in tokens:
        if '-' in tok:
            expanded.extend(tok.split('-'))
            expanded.append(tok)
        else:
            expanded.append(tok)
    result = []
    for tok in expanded:
        tok = tok.strip('-')
        if not tok or tok in STOP_WORDS or len(tok) < 2:
            continue
        result.append(tok)
    return result


class BM25Store:
    def __init__(self, site_slug: str, user_id: str = None):
        self.site_slug = site_slug
        self.user_id   = user_id
        self.bm25      = None
        self.chunks: List[Dict[str, Any]] = []
        self._loaded   = False
        self._blob_url: Optional[str] = None

    # ── Build ─────────────────────────────────────────────────────────────────

    def build(self, chunks: List[Dict[str, Any]]) -> None:
        """Build BM25 index from a list of text chunks."""
        if not BM25_AVAILABLE:
            print("⚠️  BM25 build skipped — rank_bm25 not installed")
            return
        t0           = time.time()
        self.chunks  = [c for c in chunks if c.get('content', '').strip()]
        corpus       = [_tokenize(c['content']) for c in self.chunks]
        self.bm25    = BM25Okapi(corpus)
        self._loaded = True
        print(f"✅ BM25 built: {len(self.chunks)} docs in {time.time()-t0:.2f}s")

    # ── Azure Blob helpers ────────────────────────────────────────────────────

    def _get_blob_service(self) -> "BlobServiceClient":
        if not AZURE_BLOB_AVAILABLE:
            raise RuntimeError("azure-storage-blob not installed.")
        if not AZURE_STORAGE_CONNECTION_STRING:
            raise ValueError("AZURE_STORAGE_CONNECTION_STRING not set.")
        blob_service = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        try:
            blob_service.get_container_client(BM25_BLOB_CONTAINER).get_container_properties()
        except Exception:
            try:
                blob_service.create_container(BM25_BLOB_CONTAINER)
                print(f"✅ Created blob container: {BM25_BLOB_CONTAINER}")
            except Exception as e:
                print(f"⚠️  Container creation: {e}")
        return blob_service

    @property
    def _blob_name(self) -> str:
        """Blob path: {user_id}/{site_slug}.json"""
        prefix = self.user_id or "shared"
        return f"{prefix}/{self.site_slug}.json"

    # ── Save ──────────────────────────────────────────────────────────────────

    def save_to_blob(self, supabase_client) -> Optional[str]:
        """
        Upload the serialized BM25 index to Azure Blob Storage.

        Returns the blob URL on success, None on failure.

        IMPORTANT: This method does NOT update user_pdfs.bm25_blob_url.
        The caller must do that explicitly after the user_pdfs row exists:

            bm25_url = bm25_store.save_to_blob(supabase)
            # ... later, after log_pdf_upload() has been called ...
            await db.update_pdf_status(user_id, site_slug, "completed",
                                       bm25_blob_url=bm25_url)
        """
        if not self.chunks:
            print("⚠️  BM25 save skipped — no chunks")
            return None
        if not self.user_id:
            print("⚠️  BM25 save skipped — user_id required")
            return None
        if not BM25_AVAILABLE:
            print("⚠️  BM25 save skipped — rank_bm25 not installed")
            return None

        try:
            payload = json.dumps(
                {
                    "site_slug": self.site_slug,
                    "user_id":   self.user_id,
                    "chunks":    self.chunks,
                    "corpus":    [_tokenize(c['content']) for c in self.chunks],
                },
                ensure_ascii=False,
            ).encode("utf-8")

            blob_svc    = self._get_blob_service()
            blob_client = blob_svc.get_blob_client(
                container=BM25_BLOB_CONTAINER, blob=self._blob_name
            )
            blob_client.upload_blob(payload, overwrite=True)

            account_name   = blob_svc.account_name
            blob_url       = (
                f"https://{account_name}.blob.core.windows.net"
                f"/{BM25_BLOB_CONTAINER}/{self._blob_name}"
            )
            self._blob_url = blob_url
            print(f"✅ BM25 uploaded: {self._blob_name} ({len(payload)/1024:.1f} KB)")
            return blob_url

        except Exception as e:
            print(f"❌ BM25 save failed: {e}")
            return None

    # ── Load ──────────────────────────────────────────────────────────────────

    def load_from_blob(self, supabase_client) -> bool:
        """
        Load BM25 index from Azure Blob.

        Strategy:
          1. If already loaded (in-memory), return immediately — O(1).
          2. Try DB lookup (user_pdfs.bm25_blob_url) filtered by BOTH
             user_id AND pdf_name so .single() never returns multiple rows.
          3. If no DB URL, fall back to deriving the blob path from user_id
             directly — handles the window between save_to_blob() and the
             update_pdf_status() call that persists the URL.
          4. Download JSON and rebuild BM25Okapi in-memory.
        """
        if self._loaded:
            return True
        if not BM25_AVAILABLE:
            return False

        blob_url = self._blob_url  # may already be set from save_to_blob()

        # ── Step 1: DB lookup ─────────────────────────────────────────────────
        if not blob_url:
            try:
                query = (
                    supabase_client.table("user_pdfs")
                    .select("bm25_blob_url")
                    .eq("pdf_name", self.site_slug)
                )
                # FIX: always filter by user_id when available so .single()
                # matches exactly one row instead of N rows (PGRST116).
                if self.user_id:
                    query = query.eq("user_id", self.user_id)

                response = query.single().execute()

                if response.data and response.data.get("bm25_blob_url"):
                    blob_url       = response.data["bm25_blob_url"]
                    self._blob_url = blob_url
                    print(f"ℹ️  BM25 URL from DB: {blob_url}")
                else:
                    print(
                        f"⚠️  bm25_blob_url not set in user_pdfs for "
                        f"'{self.site_slug}' (user={self.user_id})"
                    )

            except Exception as e:
                print(f"⚠️  DB lookup for BM25 URL failed: {e}")

        # ── Step 2: fallback — derive path directly from user_id ─────────────
        if not blob_url and self.user_id:
            try:
                blob_svc    = self._get_blob_service()
                blob_client = blob_svc.get_blob_client(
                    container=BM25_BLOB_CONTAINER, blob=self._blob_name
                )
                blob_client.get_blob_properties()   # raises if not found
                account_name = blob_svc.account_name
                blob_url     = (
                    f"https://{account_name}.blob.core.windows.net"
                    f"/{BM25_BLOB_CONTAINER}/{self._blob_name}"
                )
                self._blob_url = blob_url
                print(f"ℹ️  BM25 blob found via direct path: {self._blob_name}")
            except Exception:
                print(f"⚠️  BM25 blob not found at path: {self._blob_name}")
                return False

        if not blob_url:
            return False

        # ── Step 3: download and rebuild ─────────────────────────────────────
        try:
            t0        = time.time()
            parts     = blob_url.split(f"{BM25_BLOB_CONTAINER}/", 1)
            blob_name = parts[1] if len(parts) == 2 else None
            if not blob_name:
                print(f"❌ Cannot parse blob name from: {blob_url}")
                return False

            blob_svc    = self._get_blob_service()
            blob_client = blob_svc.get_blob_client(
                container=BM25_BLOB_CONTAINER, blob=blob_name
            )
            data = json.loads(blob_client.download_blob().readall().decode("utf-8"))

            self.chunks  = data["chunks"]
            self.bm25    = BM25Okapi(data["corpus"])
            self._loaded = True
            print(
                f"✅ BM25 loaded: {len(self.chunks)} docs "
                f"in {time.time()-t0:.2f}s  [{self.site_slug}]"
            )
            return True

        except Exception as e:
            print(f"❌ BM25 download/rebuild failed for '{self.site_slug}': {e}")
            return False

    # ── Delete ────────────────────────────────────────────────────────────────

    def delete_from_blob(self, supabase_client) -> None:
        """Delete the blob and clear the URL in user_pdfs."""
        try:
            if not self._blob_url:
                try:
                    query = (
                        supabase_client.table("user_pdfs")
                        .select("bm25_blob_url")
                        .eq("pdf_name", self.site_slug)
                    )
                    if self.user_id:
                        query = query.eq("user_id", self.user_id)
                    response = query.single().execute()
                    if response.data:
                        self._blob_url = response.data.get("bm25_blob_url")
                except Exception:
                    pass

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

        try:
            q = (
                supabase_client.table("user_pdfs")
                .update({"bm25_blob_url": None})
                .eq("pdf_name", self.site_slug)
            )
            if self.user_id:
                q = q.eq("user_id", self.user_id)
            q.execute()
            print(f"🗑️  bm25_blob_url cleared: {self.site_slug}")
        except Exception as e:
            print(f"⚠️  BM25 field clear failed: {e}")

    # ── Search ────────────────────────────────────────────────────────────────

    def search(
        self,
        query:     str,
        top_k:     int   = 20,
        min_score: float = 0.0,
    ) -> List[Dict[str, Any]]:
        """BM25 keyword search. Returns top_k chunks sorted by score."""
        if not self._loaded or self.bm25 is None or not self.chunks:
            return []
        tokens = _tokenize(query)
        if not tokens:
            return []
        scores = self.bm25.get_scores(tokens)
        scored = [
            {**chunk, 'bm25_score': float(score)}
            for chunk, score in zip(self.chunks, scores)
            if score > min_score
        ]
        scored.sort(key=lambda x: x['bm25_score'], reverse=True)
        results = scored[:top_k]
        print(f"🔤 BM25: {len(results)} results for '{query[:50]}'")
        return results

    @property
    def is_ready(self) -> bool:
        return self._loaded and self.bm25 is not None


# ── In-memory cache keyed by (user_id, site_slug) ────────────────────────────
# Keyed by (user_id, site_slug) so different users with the same slug
# never share an index.  Stays alive for the process lifetime — zero
# repeated downloads after first load per (user, site).

_index_cache: Dict[Tuple[str, str], BM25Store] = {}


def get_bm25_store(site_slug: str, user_id: str = None) -> BM25Store:
    """Get or create a BM25Store from the in-memory cache."""
    key = (user_id or "", site_slug)
    if key not in _index_cache:
        store = BM25Store(site_slug, user_id=user_id)
        _index_cache[key] = store
    else:
        # Ensure user_id is always set even if the cached entry was created
        # without one (e.g. from an older code path).
        if user_id and not _index_cache[key].user_id:
            _index_cache[key].user_id = user_id
    return _index_cache[key]


def invalidate_bm25_cache(site_slug: str, user_id: str = None) -> None:
    """Remove a site from the in-memory cache (call after re-crawl or delete)."""
    key = (user_id or "", site_slug)
    _index_cache.pop(key, None)
    _index_cache.pop(("", site_slug), None)   # also clear any legacy entry