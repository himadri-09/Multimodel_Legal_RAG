from fastapi import FastAPI, UploadFile, File, HTTPException, Depends
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List, Dict
import asyncio
import time
from pathlib import Path
from werkzeug.utils import secure_filename
import traceback
import uuid
from config import UPLOADS_DIR
from utils.pdf_processor import PDFProcessor
from utils.image_captioner import ImageCaptioner
from utils.vector_store import PineconeVectorStore
from utils.query_processor import QueryProcessor
from utils.response_generator import ResponseGenerator
from utils.auth import get_current_user
from utils.database import DatabaseManager

app = FastAPI(
    title="PDF RAG System API",
    description="A RAG system for PDF documents with authentication, image support, and caching",
    version="2.0.0"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Ensure directories exist
UPLOADS_DIR.mkdir(exist_ok=True)

class ProcessingStatus:
    PENDING = "pending"
    PROCESSING = "processing" 
    COMPLETED = "completed"
    FAILED = "failed"
    CACHED = "cached"

# Pydantic models for request/response validation
class QueryRequest(BaseModel):
    query: str
    pdf_name: Optional[str] = None
    conversation_id: Optional[str] = None  # Add conversation_id to request body

class UploadResponse(BaseModel):
    job_id: Optional[str] = None
    message: str
    status: str
    requires_polling: bool = False
    check_status_url: Optional[str] = None
    file_size_mb: Optional[str] = None
    cached: Optional[bool] = None
    chunks_processed: Optional[int] = None

class QueryResponse(BaseModel):
    conversation_id: Optional[str] = None
    answer: str
    images: List[Dict]
    sources: List[Dict]

@app.get("/", include_in_schema=False)
async def index():
    """API root endpoint."""
    return {"message": "Legal RAG API", "docs": "/docs", "health": "/health"}

@app.get("/health", tags=["Health"])
async def health_check():
    """Simple health check endpoint"""
    return {"status": "healthy"}

async def process_pdf_sync(pdf_path, pdf_name, filename, user_id: str):
    """Synchronous PDF processing for small files"""
    print(f"📄 Starting synchronous processing for: {filename} (user: {user_id})")
    start_time = time.time()

    db = DatabaseManager()

    # Database cache check is already done in upload endpoint
    # Proceed directly to processing
    print(f"🔥 Processing '{pdf_name}' for user {user_id}...")

    processor = PDFProcessor()

    # Upload PDF to blob storage before processing
    try:
        blob_url = processor.upload_pdf_to_blob(str(pdf_path), user_id, filename)
        print(f"💾 PDF stored in blob: {blob_url}")
    except Exception as e:
        print(f"⚠️ Warning: Could not upload PDF to blob storage: {e}")
        # Continue processing even if blob upload fails
    print(f"📝 Step 1/3: Extracting text from {pdf_name}...")
    text_chunks = processor.extract_text_from_pdf(str(pdf_path), pdf_name)

    print(f"🖼️ Step 2/3: Extracting and uploading images from {pdf_name}...")
    image_chunks = processor.extract_images_from_pdf(str(pdf_path), pdf_name)

    async with ImageCaptioner() as captioner:
        print(f"🤖 Step 3/3: AI captioning {len(image_chunks)} images...")
        captioned_images = await captioner.caption_images_async(image_chunks)

        # Combine and store in Pinecone
        all_chunks = text_chunks + captioned_images
        print(f"📦 Storing {len(all_chunks)} total chunks in vector database...")

        async with PineconeVectorStore() as vector_store:
            await vector_store.store_chunks(all_chunks, pdf_name, user_id)

    processing_end = time.time()
    total_processing_time = processing_end - start_time

    print(f"🎉 PDF '{pdf_name}' processing completed in {total_processing_time:.2f}s")

    # Update database status
    await db.update_pdf_status(
        user_id, pdf_name, "completed",
        chunks_count=len(all_chunks)
    )

    # Clean up file
    try:
        pdf_path.unlink()
        print(f"🧹 Cleaned up processed file: {filename}")
    except Exception as cleanup_error:
        print(f"⚠️ Could not clean up file {filename}: {cleanup_error}")

    return {
        "message": f"PDF '{pdf_name}' processed and stored successfully",
        "pdf_name": pdf_name,
        "cached": False,
        "chunks_processed": len(all_chunks),
        "text_chunks": len(text_chunks),
        "image_chunks": len(captioned_images),
        "processing_duration": f"{total_processing_time:.2f}s",
        "status": "newly_processed"
    }

@app.post("/upload",
    response_model=UploadResponse,
    tags=["PDF Processing"],
    summary="Upload and process a PDF document")
async def upload_pdf(
    file: UploadFile = File(...),
    current_user: dict = Depends(get_current_user)
):
    """
    Upload a PDF file for processing.

    - **file**: PDF file to upload (max 50MB)

    Requires authentication via Bearer token.
    Returns job_id for large files (>5MB) which requires polling /status endpoint.
    Small files are processed synchronously and return results immediately.
    """
    user_id = current_user["id"]

    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Only PDF files are allowed")

    # Read file content to check size
    content = await file.read()
    file_size = len(content)

    max_size = 50 * 1024 * 1024  # 50MB
    if file_size > max_size:
        raise HTTPException(
            status_code=400,
            detail=f"File too large. Maximum size is {max_size // (1024*1024)}MB"
        )

    filename = secure_filename(file.filename)
    pdf_name = Path(filename).stem
    pdf_path = UPLOADS_DIR / filename

    # Check if user already uploaded this PDF
    db = DatabaseManager()
    if await db.check_user_pdf_exists(user_id, pdf_name):
        print(f"✅ PDF '{pdf_name}' already exists for user {user_id}, returning cached response")

        # Get existing PDF info
        pdf_info = await db.get_pdf_info(user_id, pdf_name)

        if pdf_info and pdf_info.get("upload_status") == "completed":
            return UploadResponse(
                message=f"PDF '{pdf_name}' already exists and is ready for queries",
                status="already_exists",
                cached=True,
                chunks_processed=pdf_info.get("chunks_count", 0),
                requires_polling=False
            )
        else:
            # PDF exists but processing failed or incomplete
            raise HTTPException(
                status_code=400,
                detail=f"PDF '{pdf_name}' exists but processing is incomplete or failed. Please try with a different name."
            )

    # Write file
    with open(pdf_path, "wb") as f:
        f.write(content)

    # Log PDF upload to database
    await db.log_pdf_upload(
        user_id=user_id,
        pdf_name=pdf_name,
        original_filename=filename,
        file_size_bytes=file_size,
        upload_status="processing"
    )

    # For small files (< 5MB), process synchronously
    small_file_threshold = 5 * 1024 * 1024  # 5MB

    if file_size < small_file_threshold:
        print(f"📄 Small file ({file_size/1024/1024:.1f}MB), processing synchronously (user: {user_id})")
        try:
            result = await process_pdf_sync(pdf_path, pdf_name, filename, user_id)
            return result
        except Exception as e:
            print(f"❌ Error processing PDF: {e}")
            traceback.print_exc()

            # Update database status to failed
            await db.update_pdf_status(user_id, pdf_name, "failed", 0)

            try:
                if pdf_path.exists():
                    pdf_path.unlink()
            except:
                pass

            raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")

    # For large files, async processing with database tracking
    job_id = str(uuid.uuid4())

    # Create job in database instead of in-memory dict
    await db.create_processing_job(
        job_id=job_id,
        user_id=user_id,
        pdf_name=pdf_name,
        filename=filename
    )

    # Start background task
    asyncio.create_task(process_pdf_background(job_id, pdf_path, pdf_name, filename, user_id))

    return {
        "job_id": job_id,
        "message": f"Large file processing started for '{pdf_name}'",
        "status": "started",
        "requires_polling": True,
        "check_status_url": f"/status/{job_id}",
        "file_size_mb": f"{file_size/1024/1024:.1f}"
    }

async def process_pdf_background(job_id, pdf_path, pdf_name, filename, user_id: str):
    """Background task for processing large PDFs"""
    db = DatabaseManager()

    try:
        print(f"📄 Starting async processing for job {job_id}: {filename} (user: {user_id})")

        await db.update_processing_job(job_id, status="processing", stage="Initializing", progress=0.1)

        # Database cache check is already done in upload endpoint
        # Proceed directly to processing
        print(f"🔥 Processing '{pdf_name}' for user {user_id}...")

        processor = PDFProcessor()

        # Upload PDF to blob storage before processing
        try:
            await db.update_processing_job(job_id, stage="Uploading to blob storage", progress=0.15)
            blob_url = processor.upload_pdf_to_blob(str(pdf_path), user_id, filename)
            print(f"💾 PDF stored in blob: {blob_url}")
        except Exception as e:
            print(f"⚠️ Warning: Could not upload PDF to blob storage: {e}")
            # Continue processing even if blob upload fails

        print(f"📝 Step 1/3: Extracting text from {pdf_name}...")
        await db.update_processing_job(job_id, stage="Extracting text", progress=0.2)
        text_chunks = processor.extract_text_from_pdf(str(pdf_path), pdf_name)

        print(f"🖼️ Step 2/3: Extracting and uploading images from {pdf_name}...")
        await db.update_processing_job(job_id, stage="Processing images", progress=0.4)
        image_chunks = processor.extract_images_from_pdf(str(pdf_path), pdf_name)

        async with ImageCaptioner() as captioner:
            print(f"🤖 Step 3/3: AI captioning {len(image_chunks)} images...")
            await db.update_processing_job(job_id, stage=f"Captioning {len(image_chunks)} images", progress=0.6)
            captioned_images = await captioner.caption_images_async(image_chunks)

            all_chunks = text_chunks + captioned_images
            print(f"📦 Storing {len(all_chunks)} total chunks in vector database...")
            await db.update_processing_job(job_id, stage=f"Storing {len(all_chunks)} chunks", progress=0.9)

            async with PineconeVectorStore() as vector_store:
                await vector_store.store_chunks(all_chunks, pdf_name, user_id)

        print(f"🎉 PDF '{pdf_name}' processing completed")

        # Update database status
        await db.update_pdf_status(user_id, pdf_name, "completed", chunks_count=len(all_chunks))

        try:
            pdf_path.unlink()
            print(f"🧹 Cleaned up processed file: {filename}")
        except Exception as e:
            print(f"⚠️ Cleanup error: {e}")

        result = {
            "message": f"PDF '{pdf_name}' processed successfully",
            "pdf_name": pdf_name,
            "cached": False,
            "chunks_processed": len(all_chunks),
            "text_chunks": len(text_chunks),
            "image_chunks": len(captioned_images),
            "status": "newly_processed"
        }

        await db.update_processing_job(
            job_id,
            status="completed",
            stage="Completed",
            progress=1.0,
            result=result
        )

    except Exception as e:
        print(f"❌ Error in background processing: {e}")
        traceback.print_exc()

        # Update database status to failed
        await db.update_pdf_status(user_id, pdf_name, "failed", 0)

        try:
            if pdf_path.exists():
                pdf_path.unlink()
        except:
            pass

        await db.update_processing_job(
            job_id,
            status="failed",
            stage="Failed",
            progress=0.0,
            error=str(e)
        )

@app.get("/status/{job_id}",
    tags=["PDF Processing"],
    summary="Check processing job status")
async def get_processing_status(
    job_id: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Check the status of a PDF processing job.

    - **job_id**: The job ID returned from the upload endpoint

    Requires authentication via Bearer token.
    Users can only check status of their own jobs.
    """
    user_id = current_user["id"]
    db = DatabaseManager()

    job = await db.get_processing_job(job_id, user_id)

    if not job:
        raise HTTPException(
            status_code=404,
            detail="Job not found or you don't have permission to access it"
        )

    return job

@app.get("/documents",
    tags=["Documents"],
    summary="List all uploaded PDFs")
async def list_documents(
    current_user: dict = Depends(get_current_user)
):
    """
    Get list of all PDFs uploaded by the current user.

    Returns:
        List of PDFs with metadata (name, upload date, status, size, etc.)

    Requires authentication via Bearer token.
    """
    user_id = current_user["id"]

    db = DatabaseManager()
    documents = await db.get_user_pdfs(user_id)

    return {
        "documents": documents,
        "total_count": len(documents)
    }

@app.get("/documents/processed",
    tags=["Documents"],
    summary="List successfully processed PDFs")
async def list_processed_documents(
    current_user: dict = Depends(get_current_user)
):
    """
    Get list of successfully processed PDFs (status = 'completed').

    Returns:
        Simplified list of completed PDFs with id and name only

    Requires authentication via Bearer token.
    """
    user_id = current_user["id"]

    db = DatabaseManager()
    all_documents = await db.get_user_pdfs(user_id)

    # Filter for completed documents and format response
    processed_documents = [
        {
            "id": doc.get("id"),
            "name": doc.get("pdf_name")
        }
        for doc in all_documents
        if doc.get("upload_status") == "completed"
    ]

    return {
        "documents": processed_documents
    }

@app.delete("/documents/{document_id}",
    tags=["Documents"],
    summary="Delete a document")
async def delete_document(
    document_id: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Delete a document and all its vectors from the system.

    - **document_id**: UUID of the document to delete

    This will:
    1. Delete all vectors from Pinecone vector store
    2. Delete the document record from the database

    Returns:
        Success message

    Requires authentication via Bearer token.
    Users can only delete their own documents.
    """
    user_id = current_user["id"]
    db = DatabaseManager()

    # Get all user documents to find the one with this ID
    all_documents = await db.get_user_pdfs(user_id)
    document = next((doc for doc in all_documents if doc.get("id") == document_id), None)

    if not document:
        raise HTTPException(
            status_code=404,
            detail="Document not found or you don't have permission to delete it"
        )

    pdf_name = document.get("pdf_name")

    try:
        # Delete vectors from Pinecone
        async with PineconeVectorStore() as vector_store:
            vector_deleted = await vector_store.delete_pdf_vectors(pdf_name, user_id)
            if not vector_deleted:
                print(f"⚠️  Warning: Failed to delete vectors for {pdf_name}, continuing with database deletion")

        # Delete from database
        db_deleted = await db.delete_user_pdf(user_id, pdf_name)
        if not db_deleted:
            raise HTTPException(
                status_code=500,
                detail="Failed to delete document from database"
            )

        return {
            "message": f"Document '{pdf_name}' deleted successfully",
            "document_id": document_id,
            "pdf_name": pdf_name
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error deleting document: {e}")
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Failed to delete document: {str(e)}"
        )

@app.get("/conversations",
    tags=["Chat History"],
    summary="List all conversation sessions")
async def list_conversations(
    limit: int = 50,
    offset: int = 0,
    current_user: dict = Depends(get_current_user)
):
    """
    Get all conversation sessions for the current user.

    - **limit**: Maximum number of conversations to return (default: 50)
    - **offset**: Number of conversations to skip for pagination (default: 0)

    Returns:
        List of conversations ordered by most recent first, with message count

    Requires authentication via Bearer token.
    """
    user_id = current_user["id"]
    db = DatabaseManager()

    conversations = await db.get_conversations(user_id, limit=limit, offset=offset)

    # Add message_count to each conversation
    for conversation in conversations:
        messages = await db.get_conversation_messages(
            conversation["id"],
            user_id,
            limit=1000  # Get all messages to count
        )
        conversation["message_count"] = len(messages)

    return {
        "conversations": conversations,
        "total_count": len(conversations)
    }

@app.get("/conversations/{conversation_id}/messages",
    tags=["Chat History"],
    summary="Get conversation message history")
async def get_conversation_history(
    conversation_id: str,
    limit: int = 100,
    current_user: dict = Depends(get_current_user)
):
    """
    Get all messages in a specific conversation.

    - **conversation_id**: UUID of the conversation
    - **limit**: Maximum number of messages to return (default: 100)

    Returns:
        List of messages (user and assistant) ordered chronologically

    Requires authentication via Bearer token.
    Users can only access their own conversation history.
    """
    user_id = current_user["id"]
    db = DatabaseManager()

    messages = await db.get_conversation_messages(conversation_id, user_id, limit=limit)

    if not messages:
        # Verify conversation exists and belongs to user
        conversations = await db.get_conversations(user_id)
        conversation_exists = any(c["id"] == conversation_id for c in conversations)

        if not conversation_exists:
            raise HTTPException(
                status_code=404,
                detail="Conversation not found or you don't have permission to access it"
            )

    return {
        "conversation_id": conversation_id,
        "messages": messages,
        "message_count": len(messages)
    }

@app.delete("/conversations/{conversation_id}",
    tags=["Chat History"],
    summary="Delete a conversation")
async def delete_conversation(
    conversation_id: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Delete a conversation and all its messages.

    - **conversation_id**: UUID of the conversation to delete

    Returns:
        Success message

    Requires authentication via Bearer token.
    Users can only delete their own conversations.
    Messages are automatically cascade deleted.
    """
    user_id = current_user["id"]
    db = DatabaseManager()

    success = await db.delete_conversation(conversation_id, user_id)

    if not success:
        raise HTTPException(
            status_code=404,
            detail="Conversation not found or you don't have permission to delete it"
        )

    return {
        "message": "Conversation deleted successfully",
        "conversation_id": conversation_id
    }

@app.post("/query",
    response_model=QueryResponse,
    tags=["Querying"],
    summary="Query processed PDFs")
async def handle_query(
    request: QueryRequest,
    current_user: dict = Depends(get_current_user)
):
    """
    Query the processed PDF documents.

    - **query**: The question to ask
    - **pdf_name**: Optional - limit search to specific PDF (without .pdf extension)
    - **conversation_id**: Optional - continue existing conversation (pass in request body)

    Requires authentication via Bearer token.
    Users can only query their own PDFs.
    Conversation history is automatically saved.

    Returns answer with relevant images and source citations.
    """
    user_id = current_user["id"]

    if not request.query:
        raise HTTPException(status_code=400, detail="Query is required")

    db = DatabaseManager()

    # Create or get conversation
    conversation_id = request.conversation_id
    if not conversation_id:
        # Create new conversation with query as title (first 50 chars)
        conversation_title = request.query[:50] + "..." if len(request.query) > 50 else request.query
        conversation_id = await db.create_conversation(user_id, conversation_title)
        print(f"📝 Created new conversation: {conversation_id}")
    else:
        print(f"📝 Continuing conversation: {conversation_id}")

    # Retrieve conversation history BEFORE adding current message (last 4 messages = 2 turns)
    conversation_history = await db.get_conversation_messages(
        conversation_id=conversation_id,
        user_id=user_id,
        limit=4
    )
    print(f"💬 Retrieved {len(conversation_history)} previous messages for context")

    # Log user question AFTER retrieving history
    await db.add_message(
        conversation_id=conversation_id,
        user_id=user_id,
        role="user",
        content=request.query,
        query=request.query,
        pdf_name=request.pdf_name
    )

    try:
        print(f"❓ Handling query: '{request.query}' (user: {user_id})")

        async with QueryProcessor() as query_processor, \
                   PineconeVectorStore() as vector_store, \
                   ResponseGenerator() as response_generator:

            # 1. Decompose query with conversation context
            sub_queries = await query_processor.decompose_query(request.query, conversation_history)
            print(f"🧩 Decomposed into {len(sub_queries)} sub-queries")

            # 2. Process sub-queries with user_id filter
            sub_answers = []
            all_relevant_chunks = []

            for i, sq in enumerate(sub_queries):
                # CRITICAL: Pass user_id to ensure data isolation
                chunks = await vector_store.search_similar_chunks(
                    sq, top_k=5, pdf_name=request.pdf_name, user_id=user_id
                )
                all_relevant_chunks.extend(chunks)

                answer = await response_generator.generate_answer_for_subquery(
                    sq, chunks, conversation_history
                )
                sub_answers.append({
                    "sub_query": sq,
                    "answer": answer,
                    "context": chunks
                })

            if not sub_answers:
                no_result_msg = "No relevant information found in your uploaded PDFs."

                # Log assistant response even if no results
                await db.add_message(
                    conversation_id=conversation_id,
                    user_id=user_id,
                    role="assistant",
                    content=no_result_msg,
                    query=request.query
                )

                return {
                    "conversation_id": conversation_id,
                    "answer": no_result_msg,
                    "images": [],
                    "sources": []
                }

            # 3. Rerank
            final_chunks = query_processor.rerank_chunks(all_relevant_chunks, request.query)

            # 4. Combine answers
            final_result = await response_generator.combine_sub_answers(
                request.query, sub_answers, conversation_history
            )

            # 5. Extract images
            images = []
            seen_urls = set()

            for chunk in final_chunks:
                if chunk.get("type") == "image":
                    url = chunk.get("image_path")
                    if url and url not in seen_urls:
                        seen_urls.add(url)
                        images.append({
                            "url": url,
                            "page": chunk.get("page_number", "N/A"),
                            "caption": chunk.get("content", "")
                        })

            # 6. Prepare sources
            sources = [
                {
                    "type": c["type"],
                    "page": c["page_number"],
                    "content_preview": c["content"][:100] + "..."
                        if len(c["content"]) > 100 else c["content"],
                }
                for c in final_chunks
            ]

            final_answer = final_result.get("answer", "Unable to generate answer") \
                if isinstance(final_result, dict) else str(final_result)

            # Log assistant response with sources and images
            await db.add_message(
                conversation_id=conversation_id,
                user_id=user_id,
                role="assistant",
                content=final_answer,
                query=request.query,
                sources=sources,
                images=images
            )

            return {
                "conversation_id": conversation_id,
                "answer": final_answer,
                "images": images,
                "sources": sources
            }

    except Exception as e:
        print(f"❌ Error: {e}")
        traceback.print_exc()

        # Log error as assistant message for debugging
        error_msg = "I encountered an error processing your query. Please try again."
        await db.add_message(
            conversation_id=conversation_id,
            user_id=user_id,
            role="assistant",
            content=error_msg,
            query=request.query
        )

        raise HTTPException(status_code=500, detail=f"Query processing failed: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)