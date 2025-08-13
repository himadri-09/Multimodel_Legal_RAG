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
                    region="us-east-1"  # Change this to your preferred region
                )
            )
        
        self.index = self.pc.Index(PINECONE_INDEX_NAME)
        
        # Initialize Azure OpenAI client with timeout and retry settings
        self.client = AsyncAzureOpenAI(
            api_key=AZURE_OPENAI_API_KEY,
            azure_endpoint=AZURE_OPENAI_ENDPOINT,
            api_version=AZURE_API_VERSION,
            timeout=60.0,  # Add timeout to prevent hanging
            max_retries=3   # Add retry logic
        )
        
        # Reduce concurrent embeddings for better stability
        # Original was potentially too high and causing API rate limits
        self.semaphore = asyncio.Semaphore(min(MAX_CONCURRENT_EMBEDDINGS, 50))
        print(f"📌 Connected to Pinecone index: {PINECONE_INDEX_NAME}")
        print(f"🎛️  Concurrent embedding limit: {min(MAX_CONCURRENT_EMBEDDINGS, 50)}")
    
    def sanitize_for_pinecone_id(self, text: str) -> str:
        """Sanitize text to be valid for Pinecone vector IDs"""
        if not text:
            return "default"
        
        # Convert to string and lowercase
        text = str(text).lower()
        
        # More aggressive: only keep a-z, 0-9, and convert everything else to dash
        sanitized = ""
        for char in text:
            if char.isalnum():
                sanitized += char
            else:
                sanitized += "-"
        
        # Remove consecutive dashes
        sanitized = re.sub(r'-+', '-', sanitized)
        
        # Remove leading/trailing dashes
        sanitized = sanitized.strip('-')
        
        # Ensure it's not empty
        if not sanitized:
            return "default"
        
        # Ensure it doesn't start with a number (optional safety measure)
        if sanitized[0].isdigit():
            sanitized = f"item-{sanitized}"
        
        # Final validation - check each character
        for i, char in enumerate(sanitized):
            if not (char.isalnum() or char == '-'):
                print(f"❌ Invalid character found at position {i}: '{char}' (ord: {ord(char)})")
                # Replace invalid character with dash
                sanitized = sanitized[:i] + '-' + sanitized[i+1:]
        
        # Clean up any double dashes created by the fix above
        sanitized = re.sub(r'-+', '-', sanitized).strip('-')
        
        return sanitized or "default"
    
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
                # Return zero vector as fallback instead of crashing
                return np.zeros(EMBEDDING_DIMENSION, dtype=np.float32)
    
    async def create_embeddings_batch(self, texts: List[str]) -> List[np.ndarray]:
        """
        Create embeddings for multiple texts asynchronously with progress tracking
        
        Shows batch progress and completion status in real-time
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
            """Wrapper to track individual embedding progress"""
            nonlocal completed
            
            # Calculate which batch this embedding belongs to
            batch_num = (index // max_concurrent) + 1
            position_in_batch = (index % max_concurrent) + 1
            
            try:
                result = await self.create_single_embedding(text)
                completed += 1
                
                # Show progress every 50 completions or for the first few
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
        
        # Create all tasks with progress tracking
        print(f"⏳ Starting all {total_chunks} embedding tasks across {total_batches} batches...")
        tasks = [track_embedding_with_progress(text, i) for i, text in enumerate(texts)]
        
        # Execute all tasks concurrently (semaphore controls actual concurrency)
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results and count successes/failures
        final_embeddings = []
        failed_count = 0
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                print(f"❌ Task exception for embedding {i}: {result}")
                final_embeddings.append(np.zeros(EMBEDDING_DIMENSION, dtype=np.float32))
                failed_count += 1
            elif isinstance(result, np.ndarray) and result.sum() == 0:
                # This was a failed embedding that returned zero vector
                failed_count += 1
                final_embeddings.append(result)
            else:
                final_embeddings.append(result)
        
        # Performance statistics
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

    async def store_chunks(self, chunks: List[Dict[str, Any]], pdf_name: str):
        """Store chunks in Pinecone with embeddings"""
        if not chunks:
            return
        
        print(f"📦 Storing {len(chunks)} chunks for PDF: {pdf_name}")

        # Sanitize pdf_name using the improved function
        sanitized_pdf_name = self.sanitize_for_pinecone_id(pdf_name)
        print(f"🔍 Sanitized PDF name: '{pdf_name}' -> '{sanitized_pdf_name}'")
        
        # Extract texts for embedding
        texts = []
        for chunk in chunks:
            if chunk['type'] == 'image':
                # For images, combine caption with metadata
                text = f"Image from page {chunk['page_number']}: {chunk['content']}"
            else:
                text = chunk['content']
            texts.append(text)
        
        # Create embeddings using the fixed method
        print(f"🤖 Creating embeddings for {len(texts)} chunks...")
        embeddings = await self.create_embeddings_batch(texts)
        
        # Prepare vectors for Pinecone
        vectors = []

        for i, (chunk, embedding) in enumerate(zip(chunks, embeddings)):
            # Use improved sanitization
            chunk_type = self.sanitize_for_pinecone_id(chunk.get('type', 'unknown'))
            page_num = self.sanitize_for_pinecone_id(str(chunk.get('page_number', '0')))
            
            # Create a unique vector ID
            base_id = f"{sanitized_pdf_name}-{chunk_type}-page{page_num}-{i}"
            
            # Apply sanitization to the complete ID
            vector_id = self.sanitize_for_pinecone_id(base_id)
            
            # Final safety check - validate each character manually
            final_id = ""
            for char in vector_id:
                if char.isalnum() or char == '-':
                    final_id += char
                else:
                    print(f"❌ Replacing invalid char '{char}' (ord: {ord(char)}) with '-'")
                    final_id += '-'
            
            # Clean up and ensure uniqueness
            final_id = re.sub(r'-+', '-', final_id).strip('-')
            if not final_id:
                final_id = f"chunk-{i}"
            
            # Add UUID suffix if needed for uniqueness
            if len(final_id) < 8:
                final_id = f"{final_id}-{str(uuid.uuid4())[:8]}"
            
            vector_id = final_id
            
            # DEBUG: Print sample vector IDs for verification
            if i < 3:  # Show first 3 examples
                print(f"🔍 Vector ID [{i}]: '{vector_id}'")
            
            metadata = {
                'pdf_name': pdf_name,
                'type': chunk['type'],
                'page_number': chunk['page_number'],
                'content': chunk['content'][:1000],  # Truncate for metadata size limit
            }
            
            # Add type-specific metadata
            if chunk['type'] == 'image':
                metadata['image_path'] = chunk.get('image_path', '')
            elif chunk['type'] == 'table':
                metadata.update(chunk.get('metadata', {}))
            
            vectors.append({
                'id': vector_id,
                'values': embedding.tolist(),
                'metadata': metadata
            })
        
        # Upsert to Pinecone in batches (unchanged - this part was working fine)
        batch_size = 100
        total_batches = (len(vectors) + batch_size - 1) // batch_size
        
        print(f"📤 Uploading {len(vectors)} vectors to Pinecone in {total_batches} batches...")
        
        for i in range(0, len(vectors), batch_size):
            batch = vectors[i:i + batch_size]
            batch_num = i // batch_size + 1
            
            try:
                self.index.upsert(vectors=batch)
                print(f"   ✅ Upserted batch {batch_num}/{total_batches} ({len(batch)} vectors)")
            except Exception as e:
                print(f"   ❌ Error upserting batch {batch_num}: {e}")
                # Print sample IDs from failed batch for debugging
                print(f"   Sample vector IDs in failed batch:")
                for j, vec in enumerate(batch[:3]):
                    print(f"     [{j}]: '{vec['id']}'")
                raise
        
        print(f"🎉 Successfully stored {len(vectors)} chunks in Pinecone")
    
    async def search_similar_chunks(self, query: str, top_k: int = 5, pdf_name: str = None) -> List[Dict[str, Any]]:
        """Search for similar chunks"""
        print(f"🔍 Searching for top {top_k} chunks for query: '{query}'")
        
        # Create query embedding
        query_embedding = await self.create_single_embedding(query)
        
        # Build filter
        filter_dict = {}
        if pdf_name:
            filter_dict['pdf_name'] = pdf_name
        
        # Search in Pinecone
        search_response = self.index.query(
            vector=query_embedding.tolist(),
            top_k=top_k,
            include_metadata=True,
            filter=filter_dict if filter_dict else None
        )
        
        # Convert to chunks format
        results = []
        for match in search_response['matches']:
            chunk = {
                'content': match['metadata']['content'],
                'type': match['metadata']['type'],
                'page_number': match['metadata']['page_number'],
                'pdf_name': match['metadata']['pdf_name'],
                'similarity_score': match['score']
            }
            
            # Add image path if available
            if chunk['type'] == 'image' and 'image_path' in match['metadata']:
                chunk['image_path'] = match['metadata']['image_path']
            
            results.append(chunk)
        
        print(f"✅ Found {len(results)} relevant chunks")
        return results
    
    async def check_pdf_exists(self, pdf_name: str) -> bool:
        """
        Check if PDF is already processed by looking for any vectors with this pdf_name
        
        How it works:
        1. Query Pinecone with a dummy vector (all zeros)
        2. Filter by pdf_name metadata
        3. Ask for just 1 result (top_k=1)
        4. If any match found, PDF exists
        
        Time: ~10-50ms regardless of database size
        """
        try:
            print(f"🔍 Checking if PDF '{pdf_name}' exists in database...")
            
            response = self.index.query(
                vector=[0.0] * EMBEDDING_DIMENSION,  # Dummy vector - don't care about similarity
                top_k=1,                             # Just need to know if ANY exist
                filter={'pdf_name': pdf_name},       # This is the key filter
                include_metadata=False               # Don't need metadata, just existence
            )
            
            exists = len(response['matches']) > 0
            
            if exists:
                print(f"✅ PDF '{pdf_name}' found in database")
            else:
                print(f"❌ PDF '{pdf_name}' not found in database")
                
            return exists
            
        except Exception as e:
            print(f"❌ Error checking PDF existence: {e}")
            return False  # If error, assume not cached and process normally

    async def get_pdf_chunk_count(self, pdf_name: str) -> int:
        """
        Get approximate number of chunks for this PDF
        Useful for logging and verification
        """
        try:
            # Query more results to get better count estimate
            response = self.index.query(
                vector=[0.0] * EMBEDDING_DIMENSION,
                top_k=100,  # Get up to 100 to estimate total
                filter={'pdf_name': pdf_name},
                include_metadata=False
            )
            
            count = len(response['matches'])
            print(f"📊 Found ~{count} chunks for PDF '{pdf_name}' (sample)")
            return count
            
        except Exception as e:
            print(f"❌ Error getting chunk count: {e}")
            return 0
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.close()