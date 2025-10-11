from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
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

app = FastAPI(
    title="PDF RAG System API",
    description="A RAG system for PDF documents with image support and caching",
    version="1.0.0"
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

# Global dictionary to track processing jobs
processing_jobs = {}

class ProcessingStatus:
    PENDING = "pending"
    PROCESSING = "processing" 
    COMPLETED = "completed"
    FAILED = "failed"
    CACHED = "cached"

# Pydantic models for request/response validation
class QueryRequest(BaseModel):
    query: str
    pdf_name: str | None = None

class UploadResponse(BaseModel):
    job_id: str | None = None
    message: str
    status: str
    requires_polling: bool = False
    check_status_url: str | None = None
    file_size_mb: str | None = None
    cached: bool | None = None
    chunks_processed: int | None = None

class QueryResponse(BaseModel):
    answer: str
    images: list[dict]
    sources: list[dict]

@app.get("/", include_in_schema=False)
async def index():
    """Serve the main HTML page."""
    return FileResponse("static/index.html")

@app.get("/health", tags=["Health"])
async def health_check():
    """Simple health check endpoint"""
    return {"status": "healthy"}

async def process_pdf_sync(pdf_path, pdf_name, filename):
    """Synchronous PDF processing for small files"""
    print(f"📄 Starting synchronous processing for: {filename}")
    start_time = time.time()
    
    async with PineconeVectorStore() as vector_store:
        # CACHE CHECK
        print(f"🔍 Checking cache for PDF: '{pdf_name}'")
        pdf_exists = await vector_store.check_pdf_exists(pdf_name)
        
        if pdf_exists:
            chunk_count = await vector_store.get_pdf_chunk_count(pdf_name)
            cache_end_time = time.time()
            cache_check_duration = cache_end_time - start_time
            
            print(f"🚀 CACHE HIT! Skipping processing for '{pdf_name}'")
            
            # Clean up file
            try:
                pdf_path.unlink()
                print(f"🧹 Cleaned up unnecessary uploaded file: {filename}")
            except Exception as cleanup_error:
                print(f"⚠️ Could not clean up file {filename}: {cleanup_error}")
            
            return {
                "message": f"PDF '{pdf_name}' already processed and available in database",
                "pdf_name": pdf_name,
                "cached": True,
                "estimated_chunks": chunk_count,
                "processing_time_saved": "~30-120 seconds",
                "cache_check_duration": f"{cache_check_duration:.2f}s",
                "status": "cache_hit"
            }
        
        # PROCESS NEW PDF
        print(f"🔥 CACHE MISS! Processing '{pdf_name}' for first time...")
        
        processor = PDFProcessor()
        print(f"📝 Step 1/3: Extracting text from {pdf_name}...")
        text_chunks = processor.extract_text_from_pdf(str(pdf_path), pdf_name)
        
        print(f"🖼️ Step 2/3: Extracting and uploading images from {pdf_name}...")
        image_chunks = processor.extract_images_from_pdf(str(pdf_path), pdf_name)

        async with ImageCaptioner() as captioner:
            print(f"🤖 Step 3/3: AI captioning {len(image_chunks)} images...")
            captioned_images = await captioner.caption_images_async(image_chunks)
            
            # Combine and store
            all_chunks = text_chunks + captioned_images
            print(f"📦 Storing {len(all_chunks)} total chunks in vector database...")

            await vector_store.store_chunks(all_chunks, pdf_name)
            
        processing_end = time.time()
        total_processing_time = processing_end - start_time
        
        print(f"🎉 PDF '{pdf_name}' processing completed in {total_processing_time:.2f}s")
        
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
async def upload_pdf(file: UploadFile = File(...)):
    """
    Upload a PDF file for processing.
    
    - **file**: PDF file to upload (max 50MB)
    
    Returns job_id for large files (>5MB) which requires polling /status endpoint.
    Small files are processed synchronously and return results immediately.
    """
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
    
    # Write file
    with open(pdf_path, "wb") as f:
        f.write(content)

    # For small files (< 5MB), process synchronously
    small_file_threshold = 5 * 1024 * 1024  # 5MB
    
    if file_size < small_file_threshold:
        print(f"📄 Small file ({file_size/1024/1024:.1f}MB), processing synchronously")
        try:
            result = await process_pdf_sync(pdf_path, pdf_name, filename)
            return result
        except Exception as e:
            print(f"❌ Error processing PDF: {e}")
            traceback.print_exc()
            
            try:
                if pdf_path.exists():
                    pdf_path.unlink()
            except:
                pass
                
            raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")
    
    # For large files, async processing
    job_id = str(uuid.uuid4())
    processing_jobs[job_id] = {
        "job_id": job_id,
        "filename": filename,
        "pdf_name": pdf_name,
        "status": ProcessingStatus.PENDING,
        "stage": "Queued",
        "progress": 0.0,
        "start_time": time.time(),
    }

    # Start background task
    asyncio.create_task(process_pdf_background(job_id, pdf_path, pdf_name, filename))

    return {
        "job_id": job_id,
        "message": f"Large file processing started for '{pdf_name}'",
        "status": "started",
        "requires_polling": True,
        "check_status_url": f"/status/{job_id}",
        "file_size_mb": f"{file_size/1024/1024:.1f}"
    }

async def process_pdf_background(job_id, pdf_path, pdf_name, filename):
    """Background task for processing large PDFs"""
    try:
        print(f"📄 Starting async processing for job {job_id}: {filename}")
        
        processing_jobs[job_id]["status"] = ProcessingStatus.PROCESSING
        processing_jobs[job_id]["stage"] = "Initializing"
        
        async with PineconeVectorStore() as vector_store:
            # CACHE CHECK
            print(f"🔍 Checking cache for PDF: '{pdf_name}'")
            processing_jobs[job_id]["stage"] = "Checking cache"
            
            pdf_exists = await vector_store.check_pdf_exists(pdf_name)
            
            if pdf_exists:
                chunk_count = await vector_store.get_pdf_chunk_count(pdf_name)
                print(f"🚀 CACHE HIT! Skipping processing for '{pdf_name}'")
                
                try:
                    pdf_path.unlink()
                except Exception as e:
                    print(f"⚠️ Could not clean up file: {e}")
                
                processing_jobs[job_id].update({
                    "status": ProcessingStatus.CACHED,
                    "stage": "Completed (cached)",
                    "end_time": time.time(),
                    "result": {
                        "message": f"PDF '{pdf_name}' already in database",
                        "pdf_name": pdf_name,
                        "cached": True,
                        "estimated_chunks": chunk_count,
                        "status": "cache_hit"
                    }
                })
                return
            
            # PROCESS NEW PDF
            print(f"🔥 Processing '{pdf_name}' for first time...")
            processor = PDFProcessor()
            
            processing_jobs[job_id]["stage"] = "Extracting text"
            processing_jobs[job_id]["progress"] = 0.2
            text_chunks = processor.extract_text_from_pdf(str(pdf_path), pdf_name)
            
            processing_jobs[job_id]["stage"] = "Processing images"
            processing_jobs[job_id]["progress"] = 0.4
            image_chunks = processor.extract_images_from_pdf(str(pdf_path), pdf_name)

            async with ImageCaptioner() as captioner:
                processing_jobs[job_id]["stage"] = f"Captioning {len(image_chunks)} images"
                processing_jobs[job_id]["progress"] = 0.6
                captioned_images = await captioner.caption_images_async(image_chunks)
                
                all_chunks = text_chunks + captioned_images
                processing_jobs[job_id]["stage"] = f"Storing {len(all_chunks)} chunks"
                processing_jobs[job_id]["progress"] = 0.9
                await vector_store.store_chunks(all_chunks, pdf_name)
                processing_jobs[job_id]["progress"] = 1.0
            
            try:
                pdf_path.unlink()
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
            
            processing_jobs[job_id].update({
                "status": ProcessingStatus.COMPLETED,
                "stage": "Completed",
                "end_time": time.time(),
                "result": result
            })

    except Exception as e:
        print(f"❌ Error in background processing: {e}")
        traceback.print_exc()
        
        try:
            if pdf_path.exists():
                pdf_path.unlink()
        except:
            pass
        
        processing_jobs[job_id].update({
            "status": ProcessingStatus.FAILED,
            "stage": "Failed",
            "end_time": time.time(),
            "error": str(e)
        })

@app.get("/status/{job_id}",
    tags=["PDF Processing"],
    summary="Check processing job status")
async def get_processing_status(job_id: str):
    """
    Check the status of a PDF processing job.
    
    - **job_id**: The job ID returned from the upload endpoint
    """
    if job_id not in processing_jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job = processing_jobs[job_id].copy()
    
    # Calculate elapsed time
    if job.get("start_time"):
        end_time = job.get("end_time", time.time())
        job["elapsed_time"] = f"{end_time - job['start_time']:.2f}s"
    
    return job

@app.post("/query",
    response_model=QueryResponse,
    tags=["Querying"],
    summary="Query processed PDFs")
async def handle_query(request: QueryRequest):
    """
    Query the processed PDF documents.
    
    - **query**: The question to ask
    - **pdf_name**: Optional - limit search to specific PDF and without the .pdf extension
    
    Returns answer with relevant images and source citations.
    """
    if not request.query:
        raise HTTPException(status_code=400, detail="Query is required")

    try:
        print(f"❓ Handling query: '{request.query}'")
        
        async with QueryProcessor() as query_processor, \
                   PineconeVectorStore() as vector_store, \
                   ResponseGenerator() as response_generator:

            # 1. Decompose query
            sub_queries = await query_processor.decompose_query(request.query)
            print(f"🧩 Decomposed into {len(sub_queries)} sub-queries")

            # 2. Process sub-queries
            sub_answers = []
            all_relevant_chunks = []

            for i, sq in enumerate(sub_queries):
                chunks = await vector_store.search_similar_chunks(
                    sq, top_k=5, pdf_name=request.pdf_name
                )
                all_relevant_chunks.extend(chunks)

                answer = await response_generator.generate_answer_for_subquery(sq, chunks)
                sub_answers.append({
                    "sub_query": sq,
                    "answer": answer,
                    "context": chunks
                })

            if not sub_answers:
                return {
                    "answer": "No relevant information found.",
                    "images": [],
                    "sources": []
                }

            # 3. Rerank
            final_chunks = query_processor.rerank_chunks(all_relevant_chunks, request.query)

            # 4. Combine answers
            final_result = await response_generator.combine_sub_answers(
                request.query, sub_answers
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

            return {
                "answer": final_result.get("answer", "Unable to generate answer") 
                    if isinstance(final_result, dict) else str(final_result),
                "images": images,
                "sources": sources
            }

    except Exception as e:
        print(f"❌ Error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Query processing failed: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)