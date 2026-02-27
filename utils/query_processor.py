import asyncio
import json
import re
from openai import AsyncAzureOpenAI
from typing import List, Dict, Any
from config import (
    AZURE_OPENAI_API_KEY, AZURE_OPENAI_ENDPOINT, 
    AZURE_API_VERSION, AZURE_DEPLOYMENT_NAME
)
from langsmith import traceable

class QueryProcessor:
    def __init__(self):
        self.client = AsyncAzureOpenAI(
            api_key=AZURE_OPENAI_API_KEY,
            azure_endpoint=AZURE_OPENAI_ENDPOINT,
            api_version=AZURE_API_VERSION,
        )
    
    @traceable(name="decompose_query")
    async def decompose_query(self, query: str, conversation_history: List[Dict[str, Any]] = None) -> List[str]:
        """Decompose complex query into sub-questions with conversation context"""
        print(f"🧩 Decomposing query: '{query}'")

        # Build conversation context if available
        conversation_context = ""
        if conversation_history and len(conversation_history) > 0:
            history_parts = []
            # Show recent conversation (reversed for chronological order)
            for msg in reversed(conversation_history):  # Use all messages from history
                role = msg.get("role", "user")
                content = msg.get("content", "")
                if role == "user":
                    history_parts.append(f"User: {content}")
                else:
                    history_parts.append(f"Assistant: {content[:200]}...")  # Truncate long responses

            conversation_context = f"""
        Previous Conversation:
        {chr(10).join(history_parts)}
        """
            print(f"💬 Using conversation context with {len(conversation_history)} previous messages")

        prompt = f"""
        {conversation_context}

        Current Query: "{query}"

        Instructions:
        1. If the query has unclear references like "this", "it", "that", "these", resolve them using the conversation history
        2. Break down the query into 2-4 simpler, focused sub-questions
        3. Each sub-question should:
           - Be independently answerable
           - Include full context (no pronouns like "it" or "this")
           - Focus on one specific aspect
           - Be clear and concise

        Return ONLY a JSON array of sub-questions.

        Examples:
        - If previous question was about "Multi-Head Attention" and current query is "what is the use of this"
          → ["What are the applications of Multi-Head Attention?", "What are the benefits of using Multi-Head Attention?"]

        - If query is "explain contract law"
          → ["What is contract law?", "What are the key principles of contract law?", "How does contract law work in practice?"]
        """
        
        try:
            response = await self.client.chat.completions.create(
                model=AZURE_DEPLOYMENT_NAME,
                messages=[
                    {"role": "system", "content": "You are an expert at query decomposition."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3
            )
            
            response_text = response.choices[0].message.content.strip()
            json_match = re.search(r'\[.*\]', response_text, re.DOTALL)
            
            if json_match:
                sub_queries = json.loads(json_match.group())
                print(f"✅ Decomposed into {len(sub_queries)} sub-queries:")
                for i, sq in enumerate(sub_queries, 1):
                    print(f"   {i}. {sq}")
                return sub_queries
            else:
                print("❌ Failed to extract JSON, using original query")
                return [query]
        
        except Exception as e:
            print(f"❌ Error in query decomposition: {e}")
            return [query]
    
    @traceable(name="rerank_chunks")
    def rerank_chunks(self, all_chunks: List[Dict[str, Any]], original_query: str) -> List[Dict[str, Any]]:
        """Remove duplicates and rerank chunks by similarity score"""
        print(f"🔄 Reranking {len(all_chunks)} chunks")
        
        # Remove duplicates based on content hash
        seen_content = set()
        unique_chunks = []
        
        for chunk in all_chunks:
            content_hash = hash(chunk['content'])
            if content_hash not in seen_content:
                seen_content.add(content_hash)
                unique_chunks.append(chunk)
        
        print(f"📊 Removed {len(all_chunks) - len(unique_chunks)} duplicate chunks")
        
        # Sort by similarity score (descending)
        reranked_chunks = sorted(
            unique_chunks, 
            key=lambda x: x.get('similarity_score', 0), 
            reverse=True
        )
        
        # Take top 8 chunks
        top_chunks = reranked_chunks[:8]
        
        print(f"📈 Selected top {len(top_chunks)} chunks:")
        for i, chunk in enumerate(top_chunks, 1):
            score = chunk.get('similarity_score', 0)
            chunk_type = chunk.get('type', 'unknown')
            page = chunk.get('page_number', 'N/A')
            print(f"   {i}. {chunk_type} (Page {page}, Score: {score:.4f})")
        
        return top_chunks
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.close()