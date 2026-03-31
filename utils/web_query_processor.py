# utils/web_query_processor.py
"""
Web/docs RAG pipeline — full implementation.

Complete flow:
  1.  Classify query intent
  2.  Selective decomposition (only multi_part / comparison)
  3.  Hybrid retrieval — dense (Pinecone) + keyword (BM25) in parallel
  4.  Score fusion — merge dense + BM25 results with RRF
  5.  Threshold filter — drop low-confidence chunks
  6.  Cross-encoder reranker (Cohere) → top RERANK_TOP_N chunks
  7.  Adjacent chunk merging — join consecutive chunks from same page
  8.  Grounded generation with abstention
"""

import asyncio
import json
import re
from collections import defaultdict
from openai import AsyncAzureOpenAI
from typing import List, Dict, Any, Optional
from config import (
    AZURE_OPENAI_API_KEY, AZURE_OPENAI_ENDPOINT,
    AZURE_API_VERSION, AZURE_DEPLOYMENT_NAME,
)
from utils.bm25_store import get_bm25_store
from utils.reranker import get_reranker

# ── Tunable constants ─────────────────────────────────────────────────────────
MIN_SIMILARITY_SCORE = 0.70   # drop dense chunks below this — tune against eval
BM25_TOP_K           = 20     # keyword candidates per query
CANDIDATE_TOP_K      = 25     # dense candidates per query
RRF_K                = 60     # Reciprocal Rank Fusion constant (standard = 60)
RERANK_TOP_N         = 6      # final chunks after reranking → sent to LLM
MAX_MERGED_WORDS     = 600    # cap merged adjacent chunk size in words
# ─────────────────────────────────────────────────────────────────────────────

QUERY_TYPES = {
    "simple":       "Direct factual question about a single topic",
    "multi_part":   "Asks about multiple things or requires sequential steps",
    "troubleshoot": "Debugging an error, unexpected behavior, or failure",
    "comparison":   "Comparing or contrasting two or more options or features",
}


class WebQueryProcessor:
    """
    Full docs RAG pipeline. Use as async context manager.

    Primary entry point:
        result = await wqp.run(query, pdf_name, user_id, vector_store, history)
    """

    def __init__(self):
        self.client = AsyncAzureOpenAI(
            api_key=AZURE_OPENAI_API_KEY,
            azure_endpoint=AZURE_OPENAI_ENDPOINT,
            api_version=AZURE_API_VERSION,
        )
        self.reranker = get_reranker()

    # =========================================================================
    # PUBLIC ENTRY POINT
    # =========================================================================

    async def run(
        self,
        query:                str,
        pdf_name:             str,
        user_id:              str,
        vector_store,
        conversation_history: List[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Run the complete web RAG pipeline end-to-end.

        Returns:
            {
                "answer":     str,
                "sources":    List[Dict],
                "abstained":  bool,
                "query_type": str,
                "chunks_used": int,
            }
        """
        print(f"\n{'='*55}")
        print(f"WEB RAG PIPELINE")
        print(f"Query : {query[:80]}")
        print(f"{'='*55}")

        # 1. Classify
        query_type = await self.classify_query(query)

        # 3. Get search queries (single or decomposed)
        search_queries = await self.get_search_queries(
            query=query,
            query_type=query_type,
            conversation_history=conversation_history,
        )

        # 4. Hybrid retrieval (dense + BM25 + RRF fusion)
        candidates = await self._hybrid_retrieve(
            queries=search_queries,
            pdf_name=pdf_name,
            user_id=user_id,
            vector_store=vector_store,
        )

        # 5. Threshold filter
        filtered = self.threshold_filter(candidates)

        if not filtered:
            print("All candidates below threshold — abstaining")
            return {
                "answer":      "I couldn't find relevant information in the "
                               "documentation to answer your question.",
                "sources":     [],
                "raw_chunks":  [],
                "abstained":   True,
                "query_type":  query_type,
                "chunks_used": 0,
            }

        # 6. Cross-encoder reranker
        reranked = self.reranker.rerank(
            query=query,
            chunks=filtered,
            top_k=RERANK_TOP_N,
        )

        # 7. Adjacent chunk merging
        final_chunks = self._merge_adjacent_chunks(reranked)

        # 8. Grounded generation
        gen_result = await self.generate_grounded_answer(
            query=query,
            chunks=final_chunks,
            conversation_history=conversation_history,
        )

        sources = self._build_sources(final_chunks)

        print(
            f"Pipeline done — type={query_type} | "
            f"candidates={len(candidates)} | filtered={len(filtered)} | "
            f"reranked={len(reranked)} | merged={len(final_chunks)} | "
            f"abstained={gen_result['abstained']}"
        )

        return {
            "answer":      gen_result["answer"],
            "sources":     sources,
            "raw_chunks":  final_chunks,        # needed for PDF image extraction
            "abstained":   gen_result["abstained"],
            "query_type":  query_type,
            "chunks_used": len(final_chunks),
        }

    # =========================================================================
    # 1. CLASSIFY
    # =========================================================================

    async def classify_query(self, query: str) -> str:
        type_descriptions = "\n".join(
            f"- {k}: {v}" for k, v in QUERY_TYPES.items()
        )
        prompt = f"""Classify the user query into exactly one category.

Categories:
{type_descriptions}

Query: "{query}"

Rules:
- Return ONLY the category label, nothing else.
- If unsure between simple and another type, prefer simple.
- out_of_scope only when clearly unrelated to any software product docs.

Category:"""

        try:
            response = await self.client.chat.completions.create(
                model=AZURE_DEPLOYMENT_NAME,
                messages=[
                    {"role": "system", "content": "You are a query classifier. Reply with only the category label."},
                    {"role": "user",   "content": prompt},
                ],
                temperature=0.0,
                max_tokens=20,
            )
            label = response.choices[0].message.content.strip().lower()
            for valid in QUERY_TYPES:
                if valid in label:
                    print(f"Classified as: {valid}")
                    return valid
            print(f"Unrecognised label '{label}' — defaulting to simple")
            return "simple"

        except Exception as e:
            print(f"classify_query error: {e} — defaulting to simple")
            return "simple"

    # =========================================================================
    # 2. SELECTIVE DECOMPOSITION
    # =========================================================================

    async def get_search_queries(
        self,
        query:                str,
        query_type:           str,
        conversation_history: List[Dict[str, Any]] = None,
    ) -> List[str]:
        if query_type in ("simple", "troubleshoot"):
            print(f"Single retrieval path (type={query_type})")
            return [query]
        print(f"Decomposing query (type={query_type})")
        return await self._decompose(query, query_type, conversation_history)

    async def _decompose(
        self,
        query:                str,
        query_type:           str,
        conversation_history: List[Dict[str, Any]] = None,
    ) -> List[str]:
        conv_ctx = ""
        if conversation_history:
            parts = []
            for msg in conversation_history[-4:]:
                role = "User" if msg.get("role") == "user" else "Assistant"
                parts.append(f"{role}: {msg.get('content', '')[:200]}")
            conv_ctx = "Previous conversation:\n" + "\n".join(parts) + "\n\n"

        guidance = {
            "multi_part": "Break into 2-3 focused sub-questions, each independently answerable.",
            "comparison": "One query per option being compared, plus one combining both for contrast.",
        }.get(query_type, "Break into 2-3 focused sub-questions.")

        prompt = f"""{conv_ctx}Query: "{query}"

{guidance}

Rules:
- Each sub-question must be self-contained (no pronouns like "it", "this", "that").
- Maximum 3 sub-questions.
- Return ONLY a JSON array of strings.

JSON array:"""

        try:
            response = await self.client.chat.completions.create(
                model=AZURE_DEPLOYMENT_NAME,
                messages=[
                    {"role": "system", "content": "Decompose queries. Return only a JSON array of strings."},
                    {"role": "user",   "content": prompt},
                ],
                temperature=0.2,
            )
            text  = response.choices[0].message.content.strip()
            match = re.search(r'\[.*?\]', text, re.DOTALL)
            if match:
                sub_queries = json.loads(match.group())
                all_queries = [query] + [q for q in sub_queries if q != query]
                print(f"Decomposed into {len(all_queries)} queries:")
                for i, q in enumerate(all_queries):
                    print(f"  {i+1}. {q}")
                return all_queries[:4]
        except Exception as e:
            print(f"Decompose error: {e}")
        return [query]

    # =========================================================================
    # 3+4. HYBRID RETRIEVAL + RRF FUSION
    # =========================================================================

    async def _hybrid_retrieve(
        self,
        queries:      List[str],
        pdf_name:     str,
        user_id:      str,
        vector_store,
    ) -> List[Dict[str, Any]]:
        print(f"Hybrid retrieval — {len(queries)} queries")

        # Dense retrieval (async)
        dense_task = vector_store.search_web_candidates(
            queries=queries,
            pdf_name=pdf_name,
            user_id=user_id,
            candidate_top_k=CANDIDATE_TOP_K,
        )

        # BM25 retrieval (sync in executor to avoid blocking)
        bm25_results = []
        bm25_store   = get_bm25_store(pdf_name)

        if bm25_store.is_ready:
            loop = asyncio.get_event_loop()
            for q in queries:
                results = await loop.run_in_executor(
                    None, bm25_store.search, q, BM25_TOP_K
                )
                bm25_results.extend(results)
            print(f"BM25 results: {len(bm25_results)} across {len(queries)} queries")
        else:
            print("BM25 not available — dense only")

        dense_results = await dense_task
        fused = self._rrf_fusion(dense_results, bm25_results)

        print(
            f"Dense={len(dense_results)} | "
            f"BM25={len(bm25_results)} | "
            f"After RRF={len(fused)} unique chunks"
        )
        return fused

    def _rrf_fusion(
        self,
        dense_results: List[Dict[str, Any]],
        bm25_results:  List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Reciprocal Rank Fusion.
        score = 1/(RRF_K + rank_dense) + 1/(RRF_K + rank_bm25)
        """
        chunk_map:  Dict[str, Dict[str, Any]] = {}
        rrf_scores: Dict[str, float]          = {}

        def key(c: Dict) -> str:
            return c['content'][:200]

        for rank, chunk in enumerate(dense_results, start=1):
            k = key(chunk)
            if k not in chunk_map:
                chunk_map[k]  = chunk
                rrf_scores[k] = 0.0
            rrf_scores[k] += 1.0 / (RRF_K + rank)

        for rank, chunk in enumerate(bm25_results, start=1):
            k = key(chunk)
            if k not in chunk_map:
                chunk['similarity_score'] = chunk.get('similarity_score', 0.0)
                chunk_map[k]  = chunk
                rrf_scores[k] = 0.0
            rrf_scores[k] += 1.0 / (RRF_K + rank)

        sorted_keys = sorted(rrf_scores, key=lambda k: rrf_scores[k], reverse=True)
        fused = []
        for k in sorted_keys:
            chunk = dict(chunk_map[k])
            chunk['rrf_score'] = rrf_scores[k]
            fused.append(chunk)

        return fused

    # =========================================================================
    # 5. THRESHOLD FILTER
    # =========================================================================

    def threshold_filter(
        self,
        chunks:    List[Dict[str, Any]],
        min_score: float = MIN_SIMILARITY_SCORE,
    ) -> List[Dict[str, Any]]:
        """
        Drop chunks below similarity threshold.
        BM25-only chunks (similarity_score == 0) kept if rrf_score > 0 —
        they matched on exact keyword terms which is a valid signal.
        """
        before   = len(chunks)
        filtered = [
            c for c in chunks
            if c.get('similarity_score', 0) >= min_score
            or (c.get('similarity_score', 0) == 0 and c.get('rrf_score', 0) > 0)
        ]
        dropped = before - len(filtered)
        if dropped:
            print(f"Threshold filter: dropped {dropped}/{before} chunks")
        return filtered

    # =========================================================================
    # 7. ADJACENT CHUNK MERGING
    # =========================================================================

    def _merge_adjacent_chunks(
        self,
        chunks: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Merge consecutive chunks from the same source URL and adjacent
        page_number (chunk_index in WebChunker).
        Restores procedural context split across chunk boundaries.
        """
        if not chunks:
            return chunks

        url_groups: Dict[str, List[Dict]] = defaultdict(list)
        no_url: List[Dict] = []

        for chunk in chunks:
            url = chunk.get('source_url', '')
            if url:
                url_groups[url].append(chunk)
            else:
                no_url.append(chunk)

        result = list(no_url)

        for url, group in url_groups.items():
            if len(group) == 1:
                result.extend(group)
                continue

            # Sort by page_number (= chunk_index from WebChunker)
            group_sorted = sorted(group, key=lambda c: c.get('page_number', 0))
            current = dict(group_sorted[0])

            for nxt in group_sorted[1:]:
                curr_idx   = current.get('page_number', -99)
                next_idx   = nxt.get('page_number', -98)
                curr_words = len(current['content'].split())

                if next_idx == curr_idx + 1 and curr_words < MAX_MERGED_WORDS:
                    # Adjacent — merge
                    current['content'] = (
                        current['content'].rstrip() + "\n\n" +
                        nxt['content'].lstrip()
                    )
                    current['similarity_score'] = max(
                        current.get('similarity_score', 0),
                        nxt.get('similarity_score', 0),
                    )
                    current['rrf_score'] = max(
                        current.get('rrf_score', 0),
                        nxt.get('rrf_score', 0),
                    )
                    current['page_number'] = next_idx
                    print(f"Merged chunks {curr_idx}+{next_idx} from {url[:50]}")
                else:
                    result.append(current)
                    current = dict(nxt)

            result.append(current)

        print(f"Adjacent merge: {len(chunks)} -> {len(result)} chunks")
        return result

    # =========================================================================
    # 8. GROUNDED GENERATION
    # =========================================================================

    async def generate_grounded_answer(
        self,
        query:                str,
        chunks:               List[Dict[str, Any]],
        conversation_history: List[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if not chunks:
            return {
                "answer":    "I couldn't find relevant information in the documentation.",
                "abstained": True,
            }

        # ── Build numbered context ────────────────────────────────────────────
        context_parts = []
        for i, chunk in enumerate(chunks):
            content    = chunk.get('content', '').strip()
            source_url = chunk.get('source_url', '')
            page_title = chunk.get('page_title', '')
            label      = page_title or source_url or f"Source {i+1}"
            rerank_score = chunk.get('rerank_score', chunk.get('similarity_score', 0))
            context_parts.append(f"[{i+1}] {label}\n{content}")

        context_text = "\n\n---\n\n".join(context_parts)

        # ── LangSmith debug trace — logs exact chunks sent to LLM ────────────
        print(f"\n{'─'*55}")
        print(f"LLM INPUT — query: {query[:80]}")
        print(f"Chunks sent: {len(chunks)}")
        for i, chunk in enumerate(chunks):
            score = chunk.get('rerank_score', chunk.get('similarity_score', 0))
            url   = chunk.get('source_url', '')
            words = len(chunk.get('content', '').split())
            print(f"  [{i+1}] score={score:.3f} words={words} url={url[:60]}")
            print(f"       preview: {chunk.get('content', '')[:120].strip()!r}")
        print(f"{'─'*55}\n")

        # ── Conversation context ──────────────────────────────────────────────
        conv_ctx = ""
        if conversation_history:
            parts = []
            for msg in conversation_history[-4:]:
                role = "User" if msg.get("role") == "user" else "Assistant"
                parts.append(f"{role}: {msg.get('content', '')[:300]}")
            conv_ctx = "Conversation so far:\n" + "\n".join(parts) + "\n\n"

        # ── FIX: relaxed grounding prompt ─────────────────────────────────────
        # Old prompt told LLM to say "I couldn't find" whenever context was
        # incomplete — even when context had partial answers. This caused false
        # abstentions. New prompt instructs partial answers when possible and
        # reserves "not found" only for truly missing information.
        system_prompt = """You are a documentation assistant for CodePup AI. Answer questions using the provided documentation context.

Rules:
- Use ONLY information from the provided context. Do not use outside knowledge.
- If the context contains relevant information, answer directly and completely — even if it only partially answers the question.
- Cite sources using [1], [2] etc. when referencing specific sections.
- Be concise and direct. No padding or filler.
- For code, CLI commands, or config values — always use code blocks.
- Never fabricate specific numbers, API names, flags, or URLs not present in context.
- Only say "I couldn't find that in the documentation" if the context contains NO relevant information at all — not if it contains partial information."""

        user_prompt = f"""{conv_ctx}Documentation context:
{context_text}

Question: {query}

Answer based on the documentation above. If the context has relevant information, use it even if incomplete:"""

        try:
            response = await self.client.chat.completions.create(
                model=AZURE_DEPLOYMENT_NAME,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user",   "content": user_prompt},
                ],
                temperature=0.2,
            )
            answer = response.choices[0].message.content.strip()

            # ── FIX: smarter abstention detection ─────────────────────────────
            # Only mark as abstained if:
            # 1. Answer contains abstention phrase AND
            # 2. Answer is short (< 60 words) — a real answer with a caveat
            #    like "while the docs don't cover X, they do say Y" should NOT
            #    be marked as abstained
            abstention_phrases = [
                "couldn't find that",
                "not in the documentation",
                "not covered in the documentation",
                "cannot find that",
                "no information about",
                "not mentioned in the documentation",
            ]
            answer_words     = len(answer.split())
            has_abstention   = any(p in answer.lower() for p in abstention_phrases)
            abstained        = has_abstention and answer_words < 60

            # Debug log
            print(f"LLM OUTPUT — words={answer_words} has_abstention={has_abstention} abstained={abstained}")
            print(f"  Answer preview: {answer[:200]!r}")

            return {"answer": answer, "abstained": abstained}

        except Exception as e:
            print(f"generate_grounded_answer error: {e}")
            return {
                "answer":    "An error occurred generating the answer. Please try again.",
                "abstained": True,
            }

    # =========================================================================
    # HELPERS
    # =========================================================================

    def _build_sources(self, chunks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        sources = []
        for chunk in chunks:
            content = chunk.get('content', '')
            sources.append({
                "type":            chunk.get('type', 'text'),
                "page":            chunk.get('page_number', 'N/A'),
                "content_preview": content[:100] + "..." if len(content) > 100 else content,
                "source_url":      chunk.get('source_url', ''),
                "page_title":      chunk.get('page_title', ''),
                "score":           round(chunk.get('similarity_score', 0), 3),
            })
        return sources

    # ── Context manager ───────────────────────────────────────────────────────

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.close()