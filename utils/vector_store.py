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
        
        # Initialize Azure OpenAI client
        self.client = AsyncAzureOpenAI(
            api_key=AZURE_OPENAI_API_KEY,
            azure_endpoint=AZURE_OPENAI_ENDPOINT,
            api_version=AZURE_API_VERSION,
        )
        
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_EMBEDDINGS)
        print(f"🔌 Connected to Pinecone index: {PINECONE_INDEX_NAME}")
    
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
        """Create embedding for a single text"""
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
        """Create embeddings for multiple texts asynchronously in batches"""
        if not texts:
            return []
        total_chunks = len(texts)
        print(f"✅ Valid chunks: {total_chunks}")
        print(f"🚀 Creating embeddings in ASYNC batches of {MAX_CONCURRENT_EMBEDDINGS} for {total_chunks} chunks...")
        
        # --- Batch Processing Logic ---
        valid_embeddings = []
        batch_size = MAX_CONCURRENT_EMBEDDINGS # Use the config constant for batch size
        total_batches = (total_chunks + batch_size - 1) // batch_size # Calculate number of batches
        tasks = [] # List to hold all async tasks
        batch_info = {} # To store start times for calculating duration
        
        # Create tasks for all batches
        for i in range(0, total_chunks, batch_size):
            batch_num = i // batch_size + 1
            batch_texts = texts[i:i + batch_size]
            batch_actual_size = len(batch_texts)
            
            print(f"📦 Processing async batch {batch_num}/{total_batches} ({batch_actual_size} chunks)")
            
            # Create a task for each text in the current batch
            batch_tasks = []
            for text in batch_texts:
                 # Each task calls create_single_embedding
                 task = asyncio.create_task(self.create_single_embedding(text))
                 batch_tasks.append(task)
            
            # Store batch info for timing
            batch_info[batch_num] = {
                'start_time': time.time(),
                'tasks': batch_tasks,
                'size': batch_actual_size
            }
            tasks.extend(batch_tasks) # Add batch tasks to the main list
        
        # --- Gather results and log timing ---
        start_time_all_batches = time.time()
        # Gather results as they complete (more efficient for timing individual completions)
        results = []
        pending = set(tasks)
        
        while pending:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            results.extend(done)
        
        # Recreate batch structure for gathering
        final_embeddings = []
        batch_durations = []
        
        for batch_num in range(1, total_batches + 1):
            info = batch_info[batch_num]
            batch_tasks = info['tasks']
            batch_start_time = info['start_time']
            
            # Wait for all tasks in this specific batch to complete
            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            
            # Process results for this batch
            for i, emb in enumerate(batch_results):
                 if isinstance(emb, Exception):
                     print(f"❌ Failed embedding in batch {batch_num} (index {i}): {emb}")
                     final_embeddings.append(np.zeros(EMBEDDING_DIMENSION, dtype=np.float32))
                 else:
                     final_embeddings.append(emb)
            
            # Calculate and log duration for this batch
            batch_end_time = time.time()
            batch_duration = batch_end_time - batch_start_time
            batch_durations.append(batch_duration)
            print(f"✅ Async batch {batch_num} completed in {batch_duration:.2f}s")
            
        end_time_all_batches = time.time()
        total_time = end_time_all_batches - start_time_all_batches
        
        # --- Final Statistics ---
        print(f"🎉 All async batches completed!")
        print(f"📊 Total time: {total_time:.2f}s")
        if len(final_embeddings) > 0:
            avg_time_per_embedding = total_time / len(final_embeddings)
            throughput = len(final_embeddings) / total_time if total_time > 0 else 0
            print(f"📊 Average time per embedding: {avg_time_per_embedding:.3f}s")
            print(f"📊 Throughput: {throughput:.1f} embeddings/second")
        else:
            print(f"📊 No embeddings were successfully created.")
        
        return final_embeddings

    
    async def store_chunks(self, chunks: List[Dict[str, Any]], pdf_name: str):
        """Store chunks in Pinecone with embeddings"""
        if not chunks:
            return
        
        print(f"📦 Storing {len(chunks)} chunks for PDF: {pdf_name}")

        # Sanitize pdf_name using the improved function
        sanitized_pdf_name = self.sanitize_for_pinecone_id(pdf_name)
        print(f"📝 Sanitized PDF name: '{pdf_name}' -> '{sanitized_pdf_name}'")
        
        # Extract texts for embedding
        texts = []
        for chunk in chunks:
            if chunk['type'] == 'image':
                # For images, combine caption with metadata
                text = f"Image from page {chunk['page_number']}: {chunk['content']}"
            else:
                text = chunk['content']
            texts.append(text)
        
        # Create embeddings
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
            
            # DEBUG: Print ALL vector IDs and validate them
            if i < 5:  # Show more examples
                print(f"🔍 Vector ID [{i}]: '{vector_id}'")
                # Validate each character
                for j, char in enumerate(vector_id):
                    if not (char.isalnum() or char == '-'):
                        print(f"   ❌ INVALID CHAR at pos {j}: '{char}' (ord: {ord(char)})")
                    elif j == 0 and char == '-':
                        print(f"   ⚠️  WARNING: ID starts with dash")
                    elif j == len(vector_id) - 1 and char == '-':
                        print(f"   ⚠️  WARNING: ID ends with dash")
            
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
        
        # Upsert to Pinecone in batches
        batch_size = 100
        total_batches = (len(vectors) + batch_size - 1) // batch_size
        for i in range(0, len(vectors), batch_size):
            batch = vectors[i:i + batch_size]
            batch_num = i // batch_size + 1
            print(f"   Upserting batch {batch_num}/{total_batches} (size: {len(batch)})...")
            
            try:
                # Final validation before upsert - check every ID in the batch
                print(f"   🔍 Validating {len(batch)} vector IDs in batch {batch_num}...")
                for idx, vec in enumerate(batch):
                    vid = vec['id']
                    # Check for invalid characters
                    invalid_chars = []
                    for pos, char in enumerate(vid):
                        if not (char.isalnum() or char == '-'):
                            invalid_chars.append(f"pos {pos}: '{char}' (ord: {ord(char)})")
                    
                    if invalid_chars:
                        print(f"   ❌ INVALID ID in batch: '{vid}'")
                        print(f"      Invalid characters: {', '.join(invalid_chars)}")
                        raise ValueError(f"Invalid vector ID: {vid}")
                    
                    # Check for leading/trailing dashes
                    if vid.startswith('-') or vid.endswith('-'):
                        print(f"   ❌ ID with leading/trailing dash: '{vid}'")
                        raise ValueError(f"ID starts/ends with dash: {vid}")
                    
                    # Show first few IDs for verification
                    if idx < 2:
                        print(f"      ✅ Valid ID example: '{vid}'")
                
                self.index.upsert(vectors=batch)
                print(f"   ✅ Upserted batch {batch_num}")
            except Exception as e:
                print(f"   ❌ Error upserting batch {batch_num}: {e}")
                # Print some sample IDs from the failed batch for debugging
                print(f"   Sample vector IDs in failed batch:")
                for j, vec in enumerate(batch[:5]):  # Show first 5 IDs
                    vid = vec['id']
                    print(f"     [{j}]: '{vid}' (len: {len(vid)})")
                    # Check each character
                    for pos, char in enumerate(vid):
                        if not (char.isalnum() or char == '-'):
                            print(f"        ❌ Invalid at pos {pos}: '{char}' (ord: {ord(char)})")
                raise
        
        print(f"🎉 Successfully stored {len(vectors)} chunks in Pinecone")
    
    @traceable(name="search_similar_chunks")
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
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.close()