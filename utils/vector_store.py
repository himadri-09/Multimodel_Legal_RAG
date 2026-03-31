import asyncio
import re
import time
import numpy as np
from openai import AsyncAzureOpenAI
from typing import List, Dict, Any
from pinecone import Pinecone, ServerlessSpec
from config import (
    AZURE_OPENAI_API_KEY, AZURE_OPENAI_ENDPOINT, AZURE_API_VERSION,
    AZURE_EMBEDDING_DEPLOYMENT_NAME, PINECONE_API_KEY, 
    PINECONE_INDEX_NAME, EMBEDDING_DIMENSION,
    MAX_CONCURRENT_EMBEDDINGS
)
from langsmith import traceable
import uuid

class PineconeVectorStore:
    def __init__(self):
        # Initialize Pinecone with modern client
        self.pc = Pinecone(api_key=PINECONE_API_KEY)
        
        # Check if index exists, create if not
        existing_indexes = [index.name for index in self.pc.list_indexes()]
        
        if PINECONE_INDEX_NAME not in existing_indexes:
            print(f"🔧 Creating new Pinecone index: {PINECONE_INDEX_NAME}")
            self.pc.create_index(
                name=PINECONE_INDEX_NAME,
                dimension=EMBEDDING_DIMENSION,
                metric="cosine",
                spec=ServerlessSpec(
                    cloud="aws",
                    region="us-east-1"
                )
            )
        
        self.index = self.pc.Index(PINECONE_INDEX_NAME)
        
        self.client = AsyncAzureOpenAI(
            api_key=AZURE_OPENAI_API_KEY,
            azure_endpoint=AZURE_OPENAI_ENDPOINT,
            api_version=AZURE_API_VERSION,
            timeout=60.0,
            max_retries=3
        )
        
        self.semaphore = asyncio.Semaphore(min(MAX_CONCURRENT_EMBEDDINGS, 50))
        print(f"📌 Connected to Pinecone index: {PINECONE_INDEX_NAME}")
        print(f"🎛️  Concurrent embedding limit: {min(MAX_CONCURRENT_EMBEDDINGS, 50)}")

    # ── ID sanitisation ───────────────────────────────────────────────────────

    def sanitize_for_pinecone_id(self, text: str) -> str:
        """Sanitize text to be valid for Pinecone vector IDs"""
        if not text:
            return "default"
        
        text = str(text).lower()
        
        sanitized = ""
        for char in text:
            if char.isalnum():
                sanitized += char
            else:
                sanitized += "-"
        
        sanitized = re.sub(r'-+', '-', sanitized)
        sanitized = sanitized.strip('-')
        
        if not sanitized:
            return "default"
        
        if sanitized[0].isdigit():
            sanitized = f"item-{sanitized}"
        
        for i, char in enumerate(sanitized):
            if not (char.isalnum() or char == '-'):
                print(f"❌ Invalid character found at position {i}: '{char}' (ord: {ord(char)})")
                sanitized = sanitized[:i] + '-' + sanitized[i+1:]
        
        sanitized = re.sub(r'-+', '-', sanitized).strip('-')
        
        return sanitized or "default"

    # ── FIX: real embedding for existence checks (replaces zero vector) ───────

    async def _get_query_vector(self, text: str = "documentation") -> list:
        """
        Returns a real embedding for use in existence-check queries.
        Zero vectors cause cosine similarity to return 0 matches even when
        data exists — this fixes that by using a real non-zero embedding.
        The actual text doesn't matter for filter-based existence checks.
        """
        vec = await self.create_single_embedding(text)
        return vec.tolist()

    # ── Embeddings ────────────────────────────────────────────────────────────

    async def create_single_embedding(self, text: str) -> np.ndarray:
        """Create embedding for a single text with improved error handling"""
        async with self.semaphore:
            try:
                response = await self.client.embeddings.create(
                    model=AZURE_EMBEDDING_DEPLOYMENT_NAME,
                    input=text,
                    encoding_format="float"
                )
                return np.array(response.data[0].embedding, dtype=np.float32)
            except Exception as e:
                print(f"❌ Error creating embedding: {e}")
                return np.zeros(EMBEDDING_DIMENSION, dtype=np.float32)
    
    async def create_embeddings_batch(self, texts: List[str]) -> List[np.ndarray]:
        """
        Create embeddings for multiple texts asynchronously with progress tracking
        """
        if not texts:
            return []
        
        total_chunks = len(texts)
        max_concurrent = min(MAX_CONCURRENT_EMBEDDINGS, 50)
        total_batches = (total_chunks + max_concurrent - 1) // max_concurrent
        
        print(f"🚀 Creating embeddings for {total_chunks} chunks...")
        print(f"🎛️  Max concurrent requests: {max_concurrent}")
        print(f"📦 Total batches to process: {total_batches}")
        
        start_time = time.time()
        completed = 0
        
        async def track_embedding_with_progress(text: str, index: int) -> np.ndarray:
            nonlocal completed
            
            batch_num = (index // max_concurrent) + 1
            position_in_batch = (index % max_concurrent) + 1
            
            try:
                result = await self.create_single_embedding(text)
                completed += 1
                
                if (completed % 50 == 0 or completed <= 10 or 
                    completed == total_chunks or index < 5):
                    
                    elapsed = time.time() - start_time
                    rate = completed / elapsed if elapsed > 0 else 0
                    remaining = total_chunks - completed
                    eta = remaining / rate if rate > 0 else 0
                    progress_pct = (completed / total_chunks) * 100
                    
                    print(f"📈 Progress: {completed}/{total_chunks} ({progress_pct:.1f}%) | "
                          f"Batch {batch_num}/{total_batches} | "
                          f"Rate: {rate:.1f}/s | ETA: {eta:.0f}s")
                
                return result
                
            except Exception as e:
                completed += 1
                print(f"❌ Failed embedding in batch {batch_num} (item {position_in_batch}): {e}")
                return np.zeros(EMBEDDING_DIMENSION, dtype=np.float32)
        
        print(f"⏳ Starting all {total_chunks} embedding tasks across {total_batches} batches...")
        tasks = [track_embedding_with_progress(text, i) for i, text in enumerate(texts)]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        final_embeddings = []
        failed_count = 0
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                print(f"❌ Task exception for embedding {i}: {result}")
                final_embeddings.append(np.zeros(EMBEDDING_DIMENSION, dtype=np.float32))
                failed_count += 1
            elif isinstance(result, np.ndarray) and result.sum() == 0:
                failed_count += 1
                final_embeddings.append(result)
            else:
                final_embeddings.append(result)
        
        end_time = time.time()
        total_time = end_time - start_time
        success_count = total_chunks - failed_count
        
        print(f"🎉 All {total_batches} batches completed!")
        print(f"📊 Total time: {total_time:.2f}s")
        print(f"📊 Successful: {success_count}/{total_chunks} ({success_count/total_chunks*100:.1f}%)")
        
        if failed_count > 0:
            print(f"📊 Failed: {failed_count}/{total_chunks} ({failed_count/total_chunks*100:.1f}%)")
        
        if success_count > 0:
            avg_time = total_time / total_chunks
            throughput = success_count / total_time if total_time > 0 else 0
            print(f"📊 Average time per embedding: {avg_time:.3f}s")
            print(f"📊 Throughput: {throughput:.1f} embeddings/second")
            print(f"📊 Effective batches per second: {total_batches/total_time:.2f}")
        
        return final_embeddings

    # ── Store chunks ──────────────────────────────────────────────────────────

    async def store_chunks(self, chunks: List[Dict[str, Any]], pdf_name: str, user_id: str):
        """Store chunks in Pinecone with embeddings and user isolation"""
        if not chunks:
            return

        print(f"📦 Storing {len(chunks)} chunks for PDF: {pdf_name} (user: {user_id})")

        sanitized_pdf_name = self.sanitize_for_pinecone_id(pdf_name)
        print(f"🔍 Sanitized PDF name: '{pdf_name}' -> '{sanitized_pdf_name}'")
        
        texts = []
        for chunk in chunks:
            if chunk['type'] == 'image':
                text = f"Image from page {chunk['page_number']}: {chunk['content']}"
            else:
                text = chunk['content']
            texts.append(text)
        
        print(f"🤖 Creating embeddings for {len(texts)} chunks...")
        embeddings = await self.create_embeddings_batch(texts)
        
        vectors = []

        for i, (chunk, embedding) in enumerate(zip(chunks, embeddings)):
            chunk_type = self.sanitize_for_pinecone_id(chunk.get('type', 'unknown'))
            page_num = str(chunk.get('page_number', 0))

            base_id = f"{sanitized_pdf_name}-{chunk_type}-page{page_num}-{i}"
            
            vector_id = self.sanitize_for_pinecone_id(base_id)
            
            final_id = ""
            for char in vector_id:
                if char.isalnum() or char == '-':
                    final_id += char
                else:
                    print(f"❌ Replacing invalid char '{char}' (ord: {ord(char)}) with '-'")
                    final_id += '-'
            
            final_id = re.sub(r'-+', '-', final_id).strip('-')
            if not final_id:
                final_id = f"chunk-{i}"
            
            if len(final_id) < 8:
                final_id = f"{final_id}-{str(uuid.uuid4())[:8]}"
            
            vector_id = final_id
            
            if i < 3:
                print(f"🔍 Vector ID [{i}]: '{vector_id}'")
            
            # ── Base metadata ─────────────────────────────────────────────────
            metadata = {
                'pdf_name':    pdf_name,
                'user_id':     user_id,
                'type':        chunk['type'],
                'page_number': chunk['page_number'],
                'content':     chunk['content'][:1000],
            }
            
            # ── Type-specific metadata ────────────────────────────────────────
            if chunk['type'] == 'image':
                metadata['image_path'] = chunk.get('image_path', '')

            elif chunk['type'] == 'table':
                metadata.update(chunk.get('metadata', {}))

            else:
                # text chunk — persist web source fields if present
                chunk_meta = chunk.get('metadata', {})
                if chunk_meta.get('source_type') == 'web':
                    metadata['source_type'] = 'web'
                    metadata['source_url']  = chunk_meta.get('source_url', '')
                    metadata['page_title']  = chunk_meta.get('page_title', '')
                    metadata['chunk_index'] = chunk_meta.get('chunk_index', 0)
            
            vectors.append({
                'id':       vector_id,
                'values':   embedding.tolist(),
                'metadata': metadata,
            })
        
        # ── Upsert in batches with retry ──────────────────────────────────────
        batch_size    = 50
        total_batches = (len(vectors) + batch_size - 1) // batch_size

        print(f"📤 Uploading {len(vectors)} vectors to Pinecone in {total_batches} batches...")

        failed_batches    = []
        start_upload_time = time.time()

        for i in range(0, len(vectors), batch_size):
            batch     = vectors[i:i + batch_size]
            batch_num = i // batch_size + 1

            max_retries = 3
            retry_delay = 2

            for attempt in range(max_retries):
                try:
                    batch_start = time.time()

                    self.index.upsert(vectors=batch, async_req=False)

                    await asyncio.sleep(0.5)

                    batch_duration = time.time() - batch_start
                    print(f"   ✅ Upserted batch {batch_num}/{total_batches} ({len(batch)} vectors) in {batch_duration:.2f}s")

                    if batch_num % 5 == 0 or batch_num == total_batches:
                        await asyncio.sleep(1)
                        stats = self.index.describe_index_stats()
                        total_vectors = stats.get('total_vector_count', 0)
                        print(f"   📊 Pinecone total vectors: {total_vectors}")

                    break

                except Exception as e:
                    if attempt < max_retries - 1:
                        print(f"   ⚠️  Batch {batch_num} failed (attempt {attempt + 1}/{max_retries}): {e}")
                        print(f"   ⏳ Retrying in {retry_delay}s...")
                        await asyncio.sleep(retry_delay)
                        retry_delay *= 2
                    else:
                        print(f"   ❌ Batch {batch_num} failed after {max_retries} attempts: {e}")
                        for j, vec in enumerate(batch[:3]):
                            print(f"     [{j}]: '{vec['id']}'")
                        failed_batches.append((batch_num, batch, str(e)))

        # ── FIX: verify by fetching first vector ID directly ──────────────────
        # Zero-vector queries return 0 matches due to undefined cosine similarity.
        # fetch() is a direct ID lookup — always accurate.
        print(f"⏳ Waiting 3s for Pinecone to finish indexing...")
        await asyncio.sleep(3)

        try:
            final_stats   = self.index.describe_index_stats()
            total_vectors = final_stats.get('total_vector_count', 0)
            print(f"📊 Final Pinecone index stats: {total_vectors} total vectors")

            first_vector_id = vectors[0]['id']
            fetch_response  = self.index.fetch(ids=[first_vector_id])
            fetched         = fetch_response.get('vectors', {})

            if first_vector_id in fetched:
                print(f"✅ Verified: Vectors for user {user_id} / PDF '{pdf_name}' exist in Pinecone")
            else:
                # Not necessarily an error — Pinecone may still be indexing
                print(f"⚠️  Fetch verification inconclusive for '{pdf_name}' — data likely still indexing")

        except Exception as e:
            print(f"⚠️  Could not verify upload: {e}")

        # ── Report ────────────────────────────────────────────────────────────
        total_upload_time = time.time() - start_upload_time
        success_count     = total_batches - len(failed_batches)

        if failed_batches:
            print(f"⚠️  Upload completed with errors:")
            print(f"   ✅ Successful: {success_count}/{total_batches} batches")
            print(f"   ❌ Failed: {len(failed_batches)}/{total_batches} batches")
            for batch_num, _, error in failed_batches:
                print(f"      - Batch {batch_num}: {error[:100]}")
            if len(failed_batches) > total_batches * 0.5:
                raise Exception(f"More than 50% of batches failed ({len(failed_batches)}/{total_batches})")
        else:
            print(f"🎉 Successfully stored all {len(vectors)} chunks in Pinecone (took {total_upload_time:.2f}s)")

    # ── Search ────────────────────────────────────────────────────────────────

    async def search_similar_chunks(
        self,
        query: str,
        top_k: int = 5,
        pdf_name: str = None,
        user_id: str = None,
        return_embeddings: bool = False,
    ) -> List[Dict[str, Any]]:
        """Search for similar chunks with user isolation"""
        if not user_id:
            raise ValueError("user_id is required for search to ensure data isolation")

        print(f"🔍 Searching for top {top_k} chunks for query: '{query}' (user: {user_id})")

        query_embedding = await self.create_single_embedding(query)

        filter_dict = {'user_id': user_id}
        if pdf_name:
            filter_dict['pdf_name'] = pdf_name

        search_response = self.index.query(
            vector=query_embedding.tolist(),
            top_k=top_k,
            include_metadata=True,
            filter=filter_dict
        )
        
        results = []
        for match in search_response['matches']:
            meta  = match['metadata']
            chunk = {
                'content':          meta.get('content', ''),
                'type':             meta.get('type', 'text'),
                'page_number':      meta.get('page_number', 0),
                'pdf_name':         meta.get('pdf_name', ''),
                'similarity_score': match['score'],
                # web source fields — empty string for PDF chunks
                'source_url':       meta.get('source_url', ''),
                'page_title':       meta.get('page_title', ''),
                'source_type':      meta.get('source_type', 'pdf'),
            }
            
            if meta.get('type') == 'image' and 'image_path' in meta:
                chunk['image_path'] = meta['image_path']

            if return_embeddings:
                chunk['embedding'] = query_embedding.tolist()
            
            results.append(chunk)
        
        print(f"✅ Found {len(results)} relevant chunks")
        return results

    # ── Web candidate pool search ─────────────────────────────────────────────

    async def search_web_candidates(
        self,
        queries: List[str],
        pdf_name: str,
        user_id: str,
        candidate_top_k: int = 25,
    ) -> List[Dict[str, Any]]:
        """
        Run multiple queries in parallel, merge and deduplicate results.
        Returns a sorted candidate pool for threshold filter + MMR.
        Used only by the web RAG pipeline.
        """
        if not queries:
            return []

        print(f"🌐 Web candidate search — {len(queries)} queries, top_k={candidate_top_k} each")

        tasks = [
            self.search_similar_chunks(
                query=q,
                top_k=candidate_top_k,
                pdf_name=pdf_name,
                user_id=user_id,
                return_embeddings=True,
            )
            for q in queries
        ]
        all_results = await asyncio.gather(*tasks, return_exceptions=True)

        seen_hashes: set = set()
        merged: List[Dict[str, Any]] = []

        for result_set in all_results:
            if isinstance(result_set, Exception):
                print(f"⚠️  Search query error: {result_set}")
                continue
            for chunk in result_set:
                h = hash(chunk['content'][:300])
                if h not in seen_hashes:
                    seen_hashes.add(h)
                    merged.append(chunk)

        merged.sort(key=lambda c: c.get('similarity_score', 0), reverse=True)
        print(f"📦 Merged candidate pool: {len(merged)} unique chunks")
        return merged

    # ── Utility ───────────────────────────────────────────────────────────────

    async def check_pdf_exists(self, pdf_name: str, user_id: str) -> bool:
        """
        Check if PDF is already processed for this user.
        Uses a real embedding instead of zero vector — zero vectors cause
        cosine similarity to return 0 matches even when data exists.
        """
        try:
            print(f"🔍 Checking if PDF '{pdf_name}' exists for user {user_id}...")

            # FIX: use real embedding, not zero vector
            query_vector = await self._get_query_vector()

            response = self.index.query(
                vector=query_vector,
                top_k=1,
                filter={'pdf_name': pdf_name, 'user_id': user_id},
                include_metadata=False
            )

            exists = len(response['matches']) > 0

            if exists:
                print(f"✅ PDF '{pdf_name}' found in database for user {user_id}")
            else:
                print(f"❌ PDF '{pdf_name}' not found in database for user {user_id}")

            return exists

        except Exception as e:
            print(f"❌ Error checking PDF existence: {e}")
            return False

    async def get_pdf_chunk_count(self, pdf_name: str, user_id: str) -> int:
        """
        Get approximate number of chunks for this PDF for a specific user.
        Uses a real embedding instead of zero vector.
        """
        try:
            # FIX: use real embedding, not zero vector
            query_vector = await self._get_query_vector()

            response = self.index.query(
                vector=query_vector,
                top_k=100,
                filter={'pdf_name': pdf_name, 'user_id': user_id},
                include_metadata=False
            )

            count = len(response['matches'])
            print(f"📊 Found ~{count} chunks for PDF '{pdf_name}' (user: {user_id}, sample)")
            return count

        except Exception as e:
            print(f"❌ Error getting chunk count: {e}")
            return 0

    async def delete_pdf_vectors(self, pdf_name: str, user_id: str) -> bool:
        """Delete all vectors for a specific PDF and user from Pinecone"""
        try:
            print(f"🗑️  Deleting vectors for PDF '{pdf_name}' (user: {user_id})...")

            self.index.delete(
                filter={'pdf_name': pdf_name, 'user_id': user_id}
            )

            print(f"✅ Successfully deleted vectors for PDF '{pdf_name}' (user: {user_id})")
            return True

        except Exception as e:
            print(f"❌ Error deleting vectors: {e}")
            return False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.close()