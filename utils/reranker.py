# utils/reranker.py
"""
Cross-encoder reranker using Cohere Rerank API.

Why cross-encoder vs MMR:
  MMR removes redundancy but doesn't re-score relevance.
  A cross-encoder reads query + chunk TOGETHER and scores
  whether this specific chunk answers this specific question.
  It catches cases where a high-similarity chunk is topically
  related but doesn't actually answer the question.

Setup:
  1. pip install cohere
  2. Set COHERE_API_KEY in your .env

Fallback:
  If COHERE_API_KEY is not set or Cohere call fails,
  falls back to score-weighted selection (MMR output order).
  This means the pipeline never breaks — reranker is additive.
"""

import os
import time
from typing import List, Dict, Any, Optional

try:
    import cohere
    COHERE_AVAILABLE = True
except ImportError:
    COHERE_AVAILABLE = False
    print("⚠️  cohere not installed — reranker will use fallback. Run: pip install cohere")

COHERE_API_KEY    = os.getenv("COHERE_API_KEY", "")
RERANK_MODEL      = "rerank-english-v3.0"
RERANK_TOP_N      = 10      # final chunks sent to LLM after reranking
RERANK_MAX_INPUT  = 40     # max chunks fed into reranker (from hybrid pool)

class Reranker:
    """
    Wraps Cohere Rerank with a clean interface and graceful fallback.

    Usage:
        reranker = Reranker()
        final_chunks = reranker.rerank(query, candidate_chunks, top_k=10)
    """

    def __init__(self):
        self.client   = None
        self.enabled  = False

        if not COHERE_AVAILABLE:
            print("ℹ️  Reranker: cohere library not installed — using fallback")
            return

        if not COHERE_API_KEY:
            print("ℹ️  Reranker: COHERE_API_KEY not set — using fallback")
            return

        try:
            self.client  = cohere.Client(COHERE_API_KEY)
            self.enabled = True
            print("✅ Reranker: Cohere client initialized")
        except Exception as e:
            print(f"⚠️  Reranker: init failed ({e}) — using fallback")

    def rerank(
        self,
        query: str,
        chunks: List[Dict[str, Any]],
        top_k: int = RERANK_TOP_N,
    ) -> List[Dict[str, Any]]:
        """
        Rerank chunks by cross-encoder relevance to query.

        Args:
            query:  Original user query
            chunks: Candidate chunks (output of hybrid retrieval)
            top_k:  Number of chunks to return after reranking

        Returns:
            top_k chunks sorted by reranker relevance score (descending)
        """
        if not chunks:
            return []

        # Cap input to avoid excessive API cost
        candidates = chunks[:RERANK_MAX_INPUT]

        if not self.enabled or not self.client:
            return self._fallback(candidates, top_k)

        try:
            t0   = time.time()
            docs = [c['content'] for c in candidates]

            # Ask for more than top_k so we can log the score distribution
            # and catch cases where all scores are very low
            request_n = min(len(candidates), max(top_k + 4, 10))

            response = self.client.rerank(
                query=query,
                documents=docs,
                top_n=request_n,
                model=RERANK_MODEL,
                return_documents=False,
            )

            # Map reranker results back to original chunks
            all_reranked = []
            for result in response.results:
                chunk = dict(candidates[result.index])
                chunk['rerank_score']     = result.relevance_score
                chunk['similarity_score'] = result.relevance_score
                all_reranked.append(chunk)

            elapsed   = time.time() - t0
            top_score = all_reranked[0]['rerank_score'] if all_reranked else 0

            # ── Debug: log all reranker scores ───────────────────────────────
            print(f"Reranker scores ({len(candidates)} → {len(all_reranked)} in {elapsed:.2f}s):")
            for i, r in enumerate(all_reranked[:10]):
                url     = r.get('source_url', '')[:50]
                preview = r.get('content', '')[:80].replace('\n', ' ')
                marker  = " ←" if i < top_k else ""
                print(f"  [{i+1}] score={r['rerank_score']:.4f}  {url}{marker}")
                print(f"       {preview!r}")

            # ── Warn if all scores are very low ───────────────────────────────
            # Low scores mean the right chunks may not be in the candidate pool
            # — suggests a retrieval problem, not a reranking problem
            if top_score < 0.05:
                print(
                    f"⚠️  Reranker top score is very low ({top_score:.4f}) — "
                    f"the answer may not be in the crawled content, "
                    f"or the relevant page was not crawled."
                )

            # Return top_k
            reranked = all_reranked[:top_k]
            print(f"✅ Reranker: {len(candidates)} → {len(reranked)} chunks | top score: {top_score:.3f}")
            return reranked

        except Exception as e:
            print(f"⚠️  Reranker API error: {e} — using fallback")
            return self._fallback(candidates, top_k)

    def _fallback(
        self,
        chunks: List[Dict[str, Any]],
        top_k: int,
    ) -> List[Dict[str, Any]]:
        """
        Fallback when Cohere unavailable.
        Returns top_k chunks sorted by existing similarity_score.
        This is essentially the pre-reranker MMR output — still good,
        just not cross-encoder quality.
        """
        print("ℹ️  Reranker: using similarity score fallback")
        sorted_chunks = sorted(
            chunks,
            key=lambda c: c.get('similarity_score', 0),
            reverse=True
        )
        return sorted_chunks[:top_k]


# ── Singleton ─────────────────────────────────────────────────────────────────
# One instance per process — Cohere client is thread-safe

_reranker_instance: Optional[Reranker] = None


def get_reranker() -> Reranker:
    global _reranker_instance
    if _reranker_instance is None:
        _reranker_instance = Reranker()
    return _reranker_instance