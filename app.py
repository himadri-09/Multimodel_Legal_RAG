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
from utils.web_query_processor import WebQueryProcessor
from utils.auth import get_current_user
from utils.database import DatabaseManager

from routers.crawl import router as crawl_router

app = FastAPI(
    title="PDF RAG System API",
    description="A RAG system for PDF documents and websites with authentication, image support, and caching",
    version="3.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(crawl_router)

UPLOADS_DIR.mkdir(exist_ok=True)


class ProcessingStatus:
    PENDING    = "pending"
    PROCESSING = "processing"
    COMPLETED  = "completed"
    FAILED     = "failed"
    CACHED     = "cached"


class QueryRequest(BaseModel):
    query:           str
    pdf_name:        Optional[str] = None   # slug OR uuid — both handled
    conversation_id: Optional[str] = None


class UploadResponse(BaseModel):
    job_id:           Optional[str]  = None
    message:          str
    status:           str
    requires_polling: bool           = False
    check_status_url: Optional[str]  = None
    file_size_mb:     Optional[str]  = None
    cached:           Optional[bool] = None
    chunks_processed: Optional[int]  = None


class QueryResponse(BaseModel):
    conversation_id: Optional[str] = None
    answer:          str
    images:          List[Dict]
    sources:         List[Dict]


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/", include_in_schema=False)
async def index():
    return {"message": "RAG API", "docs": "/docs", "health": "/health"}


@app.get("/health", tags=["Health"])
async def health_check():
    return {"status": "healthy"}


# ── PDF processing (unchanged) ────────────────────────────────────────────────

async def process_pdf_sync(pdf_path, pdf_name, filename, user_id: str):
    print(f"📄 Sync processing: {filename} (user: {user_id})")
    start_time = time.time()
    db = DatabaseManager()

    processor = PDFProcessor()
    try:
        blob_url = processor.upload_pdf_to_blob(str(pdf_path), user_id, filename)
        print(f"💾 PDF stored in blob: {blob_url}")
    except Exception as e:
        print(f"⚠️ Blob upload warning: {e}")

    text_chunks  = processor.extract_text_from_pdf(str(pdf_path), pdf_name)
    image_chunks = processor.extract_images_from_pdf(str(pdf_path), pdf_name)

    async with ImageCaptioner() as captioner:
        captioned_images = await captioner.caption_images_async(image_chunks)
        all_chunks = text_chunks + captioned_images

        async with PineconeVectorStore() as vector_store:
            await vector_store.store_chunks(all_chunks, pdf_name, user_id)

    duration = time.time() - start_time
    print(f"🎉 PDF '{pdf_name}' done in {duration:.2f}s")

    await db.update_pdf_status(user_id, pdf_name, "completed", chunks_count=len(all_chunks))

    try:
        pdf_path.unlink()
    except Exception as e:
        print(f"⚠️ Cleanup warning: {e}")

    return {
        "message":             f"PDF '{pdf_name}' processed successfully",
        "pdf_name":            pdf_name,
        "cached":              False,
        "chunks_processed":    len(all_chunks),
        "text_chunks":         len(text_chunks),
        "image_chunks":        len(captioned_images),
        "processing_duration": f"{duration:.2f}s",
        "status":              "newly_processed",
    }


async def process_pdf_background(job_id, pdf_path, pdf_name, filename, user_id: str):
    db = DatabaseManager()
    try:
        print(f"📄 Async processing job {job_id}: {filename} (user: {user_id})")
        await db.update_processing_job(job_id, status="processing", stage="Initializing", progress=0.1)

        processor = PDFProcessor()
        try:
            await db.update_processing_job(job_id, stage="Uploading to blob storage", progress=0.15)
            blob_url = processor.upload_pdf_to_blob(str(pdf_path), user_id, filename)
            print(f"💾 Blob: {blob_url}")
        except Exception as e:
            print(f"⚠️ Blob warning: {e}")

        await db.update_processing_job(job_id, stage="Extracting text", progress=0.2)
        text_chunks = processor.extract_text_from_pdf(str(pdf_path), pdf_name)

        await db.update_processing_job(job_id, stage="Processing images", progress=0.4)
        image_chunks = processor.extract_images_from_pdf(str(pdf_path), pdf_name)

        async with ImageCaptioner() as captioner:
            await db.update_processing_job(job_id, stage=f"Captioning {len(image_chunks)} images", progress=0.6)
            captioned_images = await captioner.caption_images_async(image_chunks)
            all_chunks = text_chunks + captioned_images

            await db.update_processing_job(job_id, stage=f"Storing {len(all_chunks)} chunks", progress=0.9)
            async with PineconeVectorStore() as vector_store:
                await vector_store.store_chunks(all_chunks, pdf_name, user_id)

        await db.update_pdf_status(user_id, pdf_name, "completed", chunks_count=len(all_chunks))

        try:
            pdf_path.unlink()
        except Exception as e:
            print(f"⚠️ Cleanup: {e}")

        result = {
            "message":          f"PDF '{pdf_name}' processed successfully",
            "pdf_name":         pdf_name,
            "cached":           False,
            "chunks_processed": len(all_chunks),
            "text_chunks":      len(text_chunks),
            "image_chunks":     len(captioned_images),
            "status":           "newly_processed",
        }
        await db.update_processing_job(
            job_id, status="completed", stage="Completed", progress=1.0, result=result
        )

    except Exception as e:
        print(f"❌ Background processing error: {e}")
        traceback.print_exc()
        await db.update_pdf_status(user_id, pdf_name, "failed", 0)
        try:
            if pdf_path.exists():
                pdf_path.unlink()
        except:
            pass
        await db.update_processing_job(
            job_id, status="failed", stage="Failed", progress=0.0, error=str(e)
        )


# ── Upload ────────────────────────────────────────────────────────────────────

@app.post("/upload", response_model=UploadResponse, tags=["PDF Processing"])
async def upload_pdf(
    file: UploadFile = File(...),
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]

    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Only PDF files are allowed")

    content   = await file.read()
    file_size = len(content)

    if file_size > 50 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="File too large. Maximum 50MB.")

    filename = secure_filename(file.filename)
    pdf_name = Path(filename).stem
    pdf_path = UPLOADS_DIR / filename

    db = DatabaseManager()
    if await db.check_user_pdf_exists(user_id, pdf_name):
        pdf_info = await db.get_pdf_info(user_id, pdf_name)
        if pdf_info and pdf_info.get("upload_status") == "completed":
            return UploadResponse(
                message=f"PDF '{pdf_name}' already exists and is ready",
                status="already_exists",
                cached=True,
                chunks_processed=pdf_info.get("chunks_count", 0),
                requires_polling=False,
            )
        raise HTTPException(
            status_code=400,
            detail=f"PDF '{pdf_name}' exists but processing is incomplete or failed."
        )

    with open(pdf_path, "wb") as f:
        f.write(content)

    await db.log_pdf_upload(
        user_id=user_id,
        pdf_name=pdf_name,
        original_filename=filename,
        file_size_bytes=file_size,
        upload_status="processing",
    )

    if file_size < 5 * 1024 * 1024:
        try:
            result = await process_pdf_sync(pdf_path, pdf_name, filename, user_id)
            return result
        except Exception as e:
            traceback.print_exc()
            await db.update_pdf_status(user_id, pdf_name, "failed", 0)
            try:
                if pdf_path.exists():
                    pdf_path.unlink()
            except:
                pass
            raise HTTPException(status_code=500, detail=f"Processing failed: {e}")

    job_id = str(uuid.uuid4())
    await db.create_processing_job(
        job_id=job_id, user_id=user_id, pdf_name=pdf_name, filename=filename
    )
    asyncio.create_task(
        process_pdf_background(job_id, pdf_path, pdf_name, filename, user_id)
    )

    return {
        "job_id":           job_id,
        "message":          f"Large file processing started for '{pdf_name}'",
        "status":           "started",
        "requires_polling": True,
        "check_status_url": f"/status/{job_id}",
        "file_size_mb":     f"{file_size/1024/1024:.1f}",
    }


# ── Query — unified single pipeline ──────────────────────────────────────────

@app.post("/query", response_model=QueryResponse, tags=["Querying"])
async def handle_query(
    request:      QueryRequest,
    current_user: dict = Depends(get_current_user),
):
    """
    Unified query endpoint for PDFs and crawled websites.

    - pdf_name can be the slug ("docs-codepup-ai") OR the Supabase UUID —
      both are resolved automatically.
    - pdf_name=None searches across ALL of the user's content.
    - Routing to web vs PDF pipeline is automatic based on source_type in DB.
    - Both pipelines now use the same smart flow:
        classify → hybrid retrieval → rerank → grounded generation
    """
    user_id = current_user["id"]

    if not request.query:
        raise HTTPException(status_code=400, detail="Query is required")

    db = DatabaseManager()

    # ── Resolve pdf_name (slug or UUID) and detect source_type ───────────────
    source_type     = "pdf"    # safe default
    actual_pdf_name = request.pdf_name

    if request.pdf_name:
        # Try direct slug lookup first
        pdf_info = await db.get_pdf_info(user_id, request.pdf_name)

        if not pdf_info:
            # Frontend may have sent the Supabase UUID instead of the slug
            all_docs = await db.get_user_pdfs(user_id)
            pdf_info = next(
                (d for d in all_docs if str(d.get("id")) == request.pdf_name),
                None
            )
            if pdf_info:
                # Use the real slug for all downstream operations
                actual_pdf_name = pdf_info.get("pdf_name")
                print(f"📎 Resolved UUID {request.pdf_name} → slug '{actual_pdf_name}'")

        if pdf_info:
            source_type = pdf_info.get("source_type", "pdf")

    print(f"📨 Query — source_type={source_type}  pdf_name={actual_pdf_name}  original={request.pdf_name}")

    # ── Conversation setup ────────────────────────────────────────────────────
    conversation_id = request.conversation_id
    if not conversation_id:
        title           = request.query[:50] + "..." if len(request.query) > 50 else request.query
        conversation_id = await db.create_conversation(user_id, title)
        print(f"📝 Created new conversation: {conversation_id}")
    else:
        print(f"📝 Continuing conversation: {conversation_id}")

    conversation_history = await db.get_conversation_messages(
        conversation_id=conversation_id,
        user_id=user_id,
        limit=4,
    )
    print(f"💬 Retrieved {len(conversation_history)} previous messages")

    await db.add_message(
        conversation_id=conversation_id,
        user_id=user_id,
        role="user",
        content=request.query,
        query=request.query,
        pdf_name=actual_pdf_name,
    )

    try:
        # ── Single unified pipeline for all source types ──────────────────────
        answer, images, sources = await _unified_rag_pipeline(
            query=request.query,
            pdf_name=actual_pdf_name,     # None = search all user content
            source_type=source_type,
            user_id=user_id,
            conversation_history=conversation_history,
        )

        await db.add_message(
            conversation_id=conversation_id,
            user_id=user_id,
            role="assistant",
            content=answer,
            query=request.query,
            sources=sources,
            images=images,
        )

        return {
            "conversation_id": conversation_id,
            "answer":          answer,
            "images":          images,
            "sources":         sources,
        }

    except Exception as e:
        print(f"❌ Query error: {e}")
        traceback.print_exc()

        error_msg = "I encountered an error processing your query. Please try again."
        await db.add_message(
            conversation_id=conversation_id,
            user_id=user_id,
            role="assistant",
            content=error_msg,
            query=request.query,
        )
        raise HTTPException(status_code=500, detail=f"Query processing failed: {e}")


# ── Unified RAG pipeline ──────────────────────────────────────────────────────

async def _unified_rag_pipeline(
    query:                str,
    pdf_name:             Optional[str],
    source_type:          str,
    user_id:              str,
    conversation_history: list,
) -> tuple:
    """
    Single smart pipeline for ALL content — PDFs and web.

    Flow:
      classify intent
      → selective decompose (only multi_part / comparison)
      → hybrid retrieval: dense (Pinecone) + keyword (BM25 for web sources)
      → RRF score fusion
      → threshold filter
      → cross-encoder reranker (Cohere, falls back gracefully)
      → adjacent chunk merge (web sources)
      → grounded generation with abstention

    pdf_name=None → search across all user content (no filter)
    source_type   → logged for debugging; BM25 only available for web sources

    Returns (answer, images, sources).
    """
    print(f"\n{'='*55}")
    print(f"UNIFIED RAG PIPELINE  source_type={source_type}")
    print(f"Query   : {query[:80]}")
    print(f"Content : {pdf_name or 'ALL user content'}")
    print(f"{'='*55}")

    async with WebQueryProcessor() as wqp, PineconeVectorStore() as vs:
        result = await wqp.run(
            query=query,
            pdf_name=pdf_name,        # None searches all user content
            user_id=user_id,
            vector_store=vs,
            conversation_history=conversation_history,
        )

    # ── Extract images from PDF chunks (web sources have no images) ───────────
    images    = []
    seen_urls = set()

    for chunk in result.get("raw_chunks", []):
        if chunk.get("type") == "image":
            url = chunk.get("image_path", "")
            if url and url not in seen_urls:
                seen_urls.add(url)
                images.append({
                    "url":     url,
                    "page":    chunk.get("page_number", "N/A"),
                    "caption": chunk.get("content", ""),
                })

    return result["answer"], images, result["sources"]


# ── Status / Documents / Conversations (unchanged) ────────────────────────────

@app.get("/status/{job_id}", tags=["PDF Processing"])
async def get_processing_status(
    job_id: str, current_user: dict = Depends(get_current_user)
):
    user_id = current_user["id"]
    db      = DatabaseManager()
    job     = await db.get_processing_job(job_id, user_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found or no permission.")
    return job


@app.get("/documents", tags=["Documents"])
async def list_documents(current_user: dict = Depends(get_current_user)):
    user_id   = current_user["id"]
    db        = DatabaseManager()
    documents = await db.get_user_pdfs(user_id)
    return {"documents": documents, "total_count": len(documents)}


@app.get("/documents/processed", tags=["Documents"])
async def list_processed_documents(current_user: dict = Depends(get_current_user)):
    user_id       = current_user["id"]
    db            = DatabaseManager()
    all_documents = await db.get_user_pdfs(user_id)
    processed     = [
        {
            "id":          doc.get("id"),
            "name":        doc.get("pdf_name"),
            "source_type": doc.get("source_type", "pdf"),
        }
        for doc in all_documents
        if doc.get("upload_status") == "completed"
    ]
    return {"documents": processed}


@app.delete("/documents/{document_id}", tags=["Documents"])
async def delete_document(
    document_id: str, current_user: dict = Depends(get_current_user)
):
    user_id       = current_user["id"]
    db            = DatabaseManager()
    all_documents = await db.get_user_pdfs(user_id)
    document      = next(
        (d for d in all_documents if str(d.get("id")) == document_id), None
    )

    if not document:
        raise HTTPException(status_code=404, detail="Document not found or no permission.")

    pdf_name = document.get("pdf_name")
    try:
        async with PineconeVectorStore() as vs:
            await vs.delete_pdf_vectors(pdf_name, user_id)
        await db.delete_user_pdf(user_id, pdf_name)
        return {
            "message":     f"Document '{pdf_name}' deleted successfully",
            "document_id": document_id,
            "pdf_name":    pdf_name,
        }
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to delete: {e}")


@app.get("/conversations", tags=["Chat History"])
async def list_conversations(
    limit: int = 50, offset: int = 0,
    current_user: dict = Depends(get_current_user),
):
    user_id       = current_user["id"]
    db            = DatabaseManager()
    conversations = await db.get_conversations(user_id, limit=limit, offset=offset)
    for conv in conversations:
        msgs = await db.get_conversation_messages(conv["id"], user_id, limit=1000)
        conv["message_count"] = len(msgs)
    return {"conversations": conversations, "total_count": len(conversations)}


@app.get("/conversations/{conversation_id}/messages", tags=["Chat History"])
async def get_conversation_history(
    conversation_id: str, limit: int = 100,
    current_user: dict = Depends(get_current_user),
):
    user_id  = current_user["id"]
    db       = DatabaseManager()
    messages = await db.get_conversation_messages(conversation_id, user_id, limit=limit)

    if not messages:
        convs = await db.get_conversations(user_id)
        if not any(c["id"] == conversation_id for c in convs):
            raise HTTPException(status_code=404, detail="Conversation not found.")

    return {
        "conversation_id": conversation_id,
        "messages":        messages,
        "message_count":   len(messages),
    }


@app.delete("/conversations/{conversation_id}", tags=["Chat History"])
async def delete_conversation(
    conversation_id: str, current_user: dict = Depends(get_current_user)
):
    user_id = current_user["id"]
    db      = DatabaseManager()
    success = await db.delete_conversation(conversation_id, user_id)
    if not success:
        raise HTTPException(status_code=404, detail="Conversation not found.")
    return {"message": "Conversation deleted", "conversation_id": conversation_id}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)