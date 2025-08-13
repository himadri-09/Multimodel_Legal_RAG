# app.py - TEXT ONLY VERSION
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import asyncio
import time
import threading
from pathlib import Path
from werkzeug.utils import secure_filename
import traceback
import uuid
from config import UPLOADS_DIR 
from utils.pdf_processor import PDFProcessor
# REMOVED: from utils.image_captioner import ImageCaptioner
from utils.vector_store import PineconeVectorStore
from utils.query_processor import QueryProcessor
from utils.response_generator import ResponseGenerator

app = Flask(__name__)
CORS(app)

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

# --- Routes for serving static files ---
@app.route("/")
def index():
    """Serve the main HTML page."""
    return send_from_directory(app.static_folder, 'index.html')

@app.route("/static/<path:filename>")
def serve_static_files(filename):
    """Serve static files like CSS, JS, and images directly from the 'static' folder."""
    try:
        return send_from_directory(app.static_folder, filename)
    except FileNotFoundError:
        print(f"⚠️ Static file not found: {filename} in {app.static_folder}")
        return "File not found", 404
    except Exception as e:
        print(f"⚠️ Error serving static file {filename}: {e}")
        return "Error serving file", 500

async def process_pdf_sync(pdf_path, pdf_name, filename):
    """Synchronous PDF processing for small files (TEXT ONLY)"""
    print(f"📄 Starting synchronous TEXT-ONLY processing for: {filename}")
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
                "processing_time_saved": "~10-30 seconds",
                "cache_check_duration": f"{cache_check_duration:.2f}s",
                "status": "cache_hit"
            }
        
        # PROCESS NEW PDF (TEXT ONLY)
        print(f"🔥 CACHE MISS! Processing '{pdf_name}' for first time... (TEXT ONLY)")
        
        processor = PDFProcessor()
        print(f"📖 Step 1/2: Extracting text from {pdf_name}...")
        text_chunks = processor.extract_text_from_pdf(str(pdf_path), pdf_name)
        
        print(f"🚫 Step 2/2: Image processing DISABLED - skipping images")
        image_chunks = processor.extract_images_from_pdf(str(pdf_path), pdf_name)  # Returns []

        # NO IMAGE CAPTIONING NEEDED
        print(f"📦 Storing {len(text_chunks)} text chunks in vector database...")
        all_chunks = text_chunks  # Only text chunks
        await vector_store.store_chunks(all_chunks, pdf_name)
            
        processing_end = time.time()
        total_processing_time = processing_end - start_time
        
        print(f"🎉 PDF '{pdf_name}' TEXT-ONLY processing completed in {total_processing_time:.2f}s")
        
        # Clean up file
        try:
            pdf_path.unlink()
            print(f"🧹 Cleaned up processed file: {filename}")
        except Exception as cleanup_error:
            print(f"⚠️ Could not clean up file {filename}: {cleanup_error}")
        
        return {
            "message": f"PDF '{pdf_name}' processed and stored successfully (TEXT ONLY)", 
            "pdf_name": pdf_name,
            "cached": False,
            "chunks_processed": len(all_chunks),
            "text_chunks": len(text_chunks),
            "image_chunks": 0,  # Always 0 since disabled
            "processing_duration": f"{total_processing_time:.2f}s",
            "status": "newly_processed"
        }

def run_async_processing(job_id, pdf_path, pdf_name, filename):
    """Run the async processing in a separate thread"""
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = loop.run_until_complete(process_pdf_async(job_id, pdf_path, pdf_name, filename))
        loop.close()
        return result
    except Exception as e:
        processing_jobs[job_id]["status"] = ProcessingStatus.FAILED
        processing_jobs[job_id]["error"] = str(e)
        processing_jobs[job_id]["end_time"] = time.time()
        print(f"⚠️ Async processing failed for job {job_id}: {e}")
        traceback.print_exc()

async def process_pdf_async(job_id, pdf_path, pdf_name, filename):
    """Async PDF processing with progress updates (TEXT ONLY)"""
    try:
        print(f"📄 Starting async TEXT-ONLY processing for job {job_id}: {filename}")
        start_time = time.time()
        
        # Update status
        processing_jobs[job_id]["status"] = ProcessingStatus.PROCESSING
        processing_jobs[job_id]["stage"] = "Initializing (TEXT ONLY)"
        
        async with PineconeVectorStore() as vector_store:
            # CACHE CHECK
            print(f"🔍 Checking cache for PDF: '{pdf_name}'")
            processing_jobs[job_id]["stage"] = "Checking cache"
            
            pdf_exists = await vector_store.check_pdf_exists(pdf_name)
            
            if pdf_exists:
                # PDF already processed
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
                
                # Update job status
                processing_jobs[job_id].update({
                    "status": ProcessingStatus.CACHED,
                    "stage": "Completed (cached)",
                    "end_time": time.time(),
                    "result": {
                        "message": f"PDF '{pdf_name}' already processed and available in database",
                        "pdf_name": pdf_name,
                        "cached": True,
                        "estimated_chunks": chunk_count,
                        "processing_time_saved": "~10-30 seconds",
                        "cache_check_duration": f"{cache_check_duration:.2f}s",
                        "status": "cache_hit"
                    }
                })
                return processing_jobs[job_id]["result"]
            
            # PROCESS NEW PDF (TEXT ONLY)
            print(f"🔥 CACHE MISS! Processing '{pdf_name}' for first time... (TEXT ONLY)")
            processing_start = time.time()
            
            # Step 1: Extract text
            processor = PDFProcessor()
            print(f"📖 Step 1/2: Extracting text from {pdf_name}...")
            processing_jobs[job_id]["stage"] = "Extracting text"
            processing_jobs[job_id]["progress"] = 0.2
            
            text_chunks = processor.extract_text_from_pdf(str(pdf_path), pdf_name)
            
            # Step 2: Skip images completely
            print(f"🚫 Step 2/2: Image processing DISABLED")
            processing_jobs[job_id]["stage"] = "Image processing disabled"
            processing_jobs[job_id]["progress"] = 0.6
            
            image_chunks = processor.extract_images_from_pdf(str(pdf_path), pdf_name)  # Returns []

            # NO IMAGE CAPTIONING
            print(f"📦 Storing {len(text_chunks)} text chunks in vector database...")
            processing_jobs[job_id]["stage"] = f"Storing {len(text_chunks)} text chunks"
            processing_jobs[job_id]["progress"] = 0.8

            all_chunks = text_chunks  # Only text chunks
            await vector_store.store_chunks(all_chunks, pdf_name)
            processing_jobs[job_id]["progress"] = 1.0
                
            processing_end = time.time()
            total_processing_time = processing_end - start_time
            
            print(f"🎉 PDF '{pdf_name}' TEXT-ONLY processing completed in {total_processing_time:.2f}s")
            
            # Clean up file
            try:
                pdf_path.unlink()
                print(f"🧹 Cleaned up processed file: {filename}")
            except Exception as cleanup_error:
                print(f"⚠️ Could not clean up file {filename}: {cleanup_error}")
            
            result = {
                "message": f"PDF '{pdf_name}' processed and stored successfully (TEXT ONLY)", 
                "pdf_name": pdf_name,
                "cached": False,
                "chunks_processed": len(all_chunks),
                "text_chunks": len(text_chunks),
                "image_chunks": 0,  # Always 0 since disabled
                "processing_duration": f"{total_processing_time:.2f}s",
                "status": "newly_processed"
            }
            
            # Update job status
            processing_jobs[job_id].update({
                "status": ProcessingStatus.COMPLETED,
                "stage": "Completed (TEXT ONLY)",
                "end_time": time.time(),
                "result": result
            })
            
            return result

    except Exception as e:
        print(f"⚠️ Error in async processing for '{pdf_name}': {e}")
        traceback.print_exc()
        
        # Clean up file in case of error
        try:
            if pdf_path.exists():
                pdf_path.unlink()
                print(f"🧹 Cleaned up file after error: {filename}")
        except Exception as cleanup_error:
            print(f"⚠️ Could not clean up file after error: {cleanup_error}")
        
        # Update job status
        processing_jobs[job_id].update({
            "status": ProcessingStatus.FAILED,
            "stage": "Failed",
            "end_time": time.time(),
            "error": str(e)
        })
        
        raise

# REST OF THE CODE REMAINS THE SAME...
@app.route("/upload", methods=["POST"])
def upload_pdf():
    """Handle PDF upload - async for large files, sync for small files"""
    if "file" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400

    file = request.files["file"]
    if file.filename == "":
        return jsonify({"error": "No file selected"}), 400

    if not file.filename.lower().endswith(".pdf"):
        return jsonify({"error": "Only PDF files are allowed"}), 400

    # Check file size
    file.seek(0, 2)  # Seek to end
    file_size = file.tell()
    file.seek(0)  # Reset to beginning
    
    max_size = 50 * 1024 * 1024  # 50MB
    if file_size > max_size:
        return jsonify({"error": f"File too large. Maximum size is {max_size // (1024*1024)}MB"}), 400

    filename = secure_filename(file.filename)
    pdf_name = Path(filename).stem
    pdf_path = UPLOADS_DIR / filename
    file.save(pdf_path)

    # For small files (< 5MB), process synchronously for compatibility
    small_file_threshold = 5 * 1024 * 1024  # 5MB
    
    if file_size < small_file_threshold:
        print(f"📄 Small file detected ({file_size/1024/1024:.1f}MB), processing synchronously: {filename}")
        try:
            result = asyncio.run(process_pdf_sync(pdf_path, pdf_name, filename))
            return jsonify(result)
        except Exception as e:
            print(f"⚠️ Error processing small PDF '{filename}': {e}")
            traceback.print_exc()
            
            # Clean up file in case of error
            try:
                if pdf_path.exists():
                    pdf_path.unlink()
            except:
                pass
                
            return jsonify({
                "error": f"Processing failed: {str(e)}",
                "pdf_name": pdf_name,
                "status": "error"
            }), 500
    
    # For large files, use async processing
    print(f"📄 Large file detected ({file_size/1024/1024:.1f}MB), using async processing: {filename}")
    
    # Create unique job ID
    job_id = str(uuid.uuid4())
    
    # Initialize job tracking
    processing_jobs[job_id] = {
        "job_id": job_id,
        "filename": filename,
        "pdf_name": pdf_name,
        "status": ProcessingStatus.PENDING,
        "stage": "Queued (TEXT ONLY)",
        "progress": 0.0,
        "start_time": time.time(),
        "end_time": None,
        "result": None,
        "error": None,
        "file_size_mb": f"{file_size/1024/1024:.1f}"
    }

    # Start processing in background thread
    thread = threading.Thread(
        target=run_async_processing,
        args=(job_id, pdf_path, pdf_name, filename)
    )
    thread.daemon = True
    thread.start()

    print(f"📤 Started background TEXT-ONLY processing for job {job_id}: {filename}")
    
    return jsonify({
        "job_id": job_id,
        "message": f"Large file processing started for '{pdf_name}' (TEXT ONLY)",
        "status": "started",
        "requires_polling": True,
        "check_status_url": f"/status/{job_id}",
        "file_size_mb": f"{file_size/1024/1024:.1f}"
    })

@app.route("/status/<job_id>", methods=["GET"])
def get_processing_status(job_id):
    """Get the status of a processing job"""
    if job_id not in processing_jobs:
        return jsonify({"error": "Job not found"}), 404
    
    job = processing_jobs[job_id].copy()
    
    # Calculate elapsed time
    if job["start_time"]:
        if job["end_time"]:
            job["elapsed_time"] = job["end_time"] - job["start_time"]
        else:
            job["elapsed_time"] = time.time() - job["start_time"]
        job["elapsed_time"] = f"{job['elapsed_time']:.2f}s"
    
    return jsonify(job)

@app.route("/health", methods=["GET"])
def health_check():
    """Simple health check for Azure App Service"""
    return jsonify({"status": "healthy", "mode": "text_only"}), 200

@app.route("/query", methods=["POST"])
def handle_query():
    """Handle user queries by decomposing, searching, answering, and combining."""
    data = request.json
    query = data.get("query")
    pdf_name = data.get("pdf_name")

    if not query:
        return jsonify({"error": "Query is required"}), 400

    try:
        async def process_query():
            print(f"❓ Handling query: '{query}' (TEXT ONLY MODE)")
            
            async with QueryProcessor() as query_processor, \
                       PineconeVectorStore() as vector_store, \
                       ResponseGenerator() as response_generator:

                # 1. Decompose the main query
                sub_queries = await query_processor.decompose_query(query)
                print(f"🧩 Decomposed into {len(sub_queries)} sub-queries.")

                # 2. Process each sub-query
                sub_answers = []
                all_relevant_chunks = []

                for i, sq in enumerate(sub_queries):
                    print(f"🔍 Processing sub-query {i+1}/{len(sub_queries)}: '{sq}'")
                    
                    chunks = await vector_store.search_similar_chunks(
                        sq, top_k=5, pdf_name=pdf_name
                    )
                    print(f"   Found {len(chunks)} relevant chunks.")
                    all_relevant_chunks.extend(chunks)

                    answer = await response_generator.generate_answer_for_subquery(sq, chunks)
                    print(f"   Generated answer for sub-query {i+1}.")

                    sub_answers.append({
                        "sub_query": sq,
                        "answer": answer,
                        "context": chunks
                    })

                if not sub_answers:
                    return {
                        "answer": "No relevant information found for any part of the query.",
                        "images": [],  # Always empty in TEXT ONLY mode
                        "sources": []
                    }

                # 3. Rerank all collected chunks
                print("📊 Reranking all collected chunks from sub-queries...")
                final_reranked_chunks = query_processor.rerank_chunks(all_relevant_chunks, query)
                print(f"📈 Reranking complete. Top {len(final_reranked_chunks)} unique chunks selected.")

                # 4. Combine sub-answers
                print("🧩 Combining sub-answers into final response...")
                final_result = await response_generator.combine_sub_answers(query, sub_answers)
                print("✅ Final answer generated.")
                
                # 5. NO IMAGES in TEXT ONLY mode
                consolidated_images = []  # Always empty

                # 6. Prepare sources (text only)
                sources = [
                    {
                        "type": c["type"],
                        "page": c["page_number"],
                        "content_preview": c["content"][:100] + "..." if len(c["content"]) > 100 else c["content"],
                    }
                    for c in final_reranked_chunks 
                ]

                response_data = {
                    "answer": final_result.get("answer", "Unable to generate final answer.") if isinstance(final_result, dict) else str(final_result),
                    "images": consolidated_images,  # Always empty
                    "sources": sources
                }

                return response_data

        result = asyncio.run(process_query())
        print(f"📤 Returning response for query: '{query}' (TEXT ONLY)")
        return jsonify(result)

    except Exception as e:
        print(f"⚠️ Error handling query '{query}': {e}")
        traceback.print_exc()
        return jsonify({"error": f"Query processing failed: {str(e)}"}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)