# app.py - Updated with neon DB integration

from flask import Flask, request, jsonify
from flask_cors import CORS
import asyncio
import os
import uuid
import time
import traceback
import threading
from pathlib import Path
from typing import Dict, Any, List, Optional
from werkzeug.utils import secure_filename
from config import UPLOADS_DIR
from utils.pdf_processor import PDFProcessor
from utils.vector_store import PineconeVectorStore
from utils.query_processor import QueryProcessor
from utils.response_generator import ResponseGenerator
from utils.neon_document_manager import NeonDocumentManager, DocumentStatus 

app = Flask(__name__)
CORS(app, origins="*")

UPLOADS_DIR.mkdir(exist_ok=True)

processing_jobs = {}

class ProcessingStatus:
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CACHED = "cached"

# Initialize connection pool on startup
def initialize_db_pool():
    """Initialize database connection pool on app startup"""
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(NeonDocumentManager.get_pool())
        loop.close()
        print("Database connection pool initialized")
    except Exception as e:
        print(f"Warning: Database initialization failed: {e}")

# Initialize immediately when the app starts
with app.app_context():
    initialize_db_pool()

async def process_pdf_async(job_id: str, pdf_path: Path, pdf_name: str,
                           filename: str, file_size_mb: float, blob_url: str) -> Dict[str, Any]:
    """Async PDF processing with neon DB tracking"""
    try:
        job = processing_jobs[job_id]
        job["status"] = ProcessingStatus.PROCESSING
        job["stage"] = "Initializing"

        # 🆕 Initialize neon DB manager
        async with NeonDocumentManager() as neon_manager:
            
            # 🆕 Update status to processing in neon DB
            await neon_manager.update_document_status(
                pdf_name, 
                DocumentStatus.PROCESSING,
                processing_stage="Extracting text"
            )

            processor = PDFProcessor()

            # Extract text chunks (blob upload already done)
            job["stage"] = "Extracting text"
            text_chunks = processor.extract_text_from_pdf(str(pdf_path), pdf_name)

            # 🆕 Update status: embedding
            await neon_manager.update_document_status(
                pdf_name, 
                DocumentStatus.PROCESSING,
                processing_stage="Creating embeddings"
            )

            # Store chunks in vector DB
            job["stage"] = "Storing chunks"
            async with PineconeVectorStore() as vector_store:
                await vector_store.store_chunks(text_chunks, pdf_name)
                chunk_count = len(text_chunks)  # We know the count from text_chunks

            # 🆕 Update status to completed in neon DB
            await neon_manager.update_document_status(
                pdf_name, 
                DocumentStatus.ANALYZED,
                chunk_count=chunk_count
            )

        # Clean up
        try:
            pdf_path.unlink()
        except:
            pass

        # Return processing result
        result = {
            "message": "PDF processed and stored successfully",
            "pdf_name": pdf_name,
            "blob_url": blob_url,
            "text_chunks": len(text_chunks),
            "status": "completed"
        }
        
        job.update({
            "status": ProcessingStatus.COMPLETED,
            "result": result,
            "end_time": time.time(),
            "chunk_count": chunk_count
        })
        return result

    except Exception as e:
        # 🆕 Update status to failed in Neon DB
        try:
            async with NeonDocumentManager() as neon_manager:
                await neon_manager.update_document_status(
                    pdf_name, 
                    DocumentStatus.FAILED,
                    error_message=str(e)
                )
        except:
            pass  # Don't let neon errors break the main error handling
        
        job.update({
            "status": ProcessingStatus.FAILED,
            "error": str(e),
            "end_time": time.time()
        })
        try:
            pdf_path.unlink()
        except:
            pass
        raise

def run_async_processing(job_id, pdf_path, pdf_name, filename, file_size_mb, blob_url):
    """Run async processing in a thread."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    result = loop.run_until_complete(
        process_pdf_async(job_id, pdf_path, pdf_name, filename, file_size_mb, blob_url))
    loop.close()
    return result

@app.route("/upload", methods=["POST"])
def upload_pdf():
    """Upload, process, and store a PDF with neon DB tracking"""
    if "file" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400
    file = request.files["file"]
    if file.filename.strip() == "":
        return jsonify({"error": "No file selected"}), 400
    if not file.filename.lower().endswith(".pdf"):
        return jsonify({"error": "Only PDFs are allowed"}), 400

    # File size check
    file.seek(0, 2)  # End
    file_size = file.tell()
    file.seek(0)     # Reset
    file_size_mb = round(file_size / (1024 * 1024), 2)

    filename = secure_filename(file.filename)
    pdf_name = Path(filename).stem
    pdf_path = UPLOADS_DIR / filename
    file.save(pdf_path)

    async def create_neon_record_and_upload():
        # Upload to blob storage first
        processor = PDFProcessor()
        blob_url = processor.upload_pdf_to_blob(str(pdf_path), filename)
        
        # 🆕 Create record in neon DB
        async with NeonDocumentManager() as neon_manager:
            # Check if already exists
            existing_doc = await neon_manager.get_document_by_pdf_name(pdf_name)
            if existing_doc:
                if existing_doc["status"] == DocumentStatus.ANALYZED:
                    return {
                        "status": "cached",
                        "message": f"PDF '{pdf_name}' already processed",
                        "blob_url": blob_url
                    }
            
            # Create new record
            await neon_manager.create_document_record(
                pdf_name=pdf_name,
                file_name=filename,
                file_size_mb=file_size_mb,
                blob_url=blob_url
            )
        
        return {"blob_url": blob_url, "status": "created"}

    try:
        # Create neon record synchronously
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        neon_result = loop.run_until_complete(create_neon_record_and_upload())
        loop.close()
        
        # Check if already processed (cached)
        if neon_result.get("status") == "cached":
            try:
                pdf_path.unlink()  # Clean up uploaded file
            except:
                pass
            return jsonify({
                "job_id": None,
                "message": neon_result["message"],
                "status": "cached",
                "pdf_name": pdf_name
            })

        # Start async processing job
        job_id = str(uuid.uuid4())
        processing_jobs[job_id] = {
            "job_id": job_id,
            "pdf_name": pdf_name,
            "file_name": filename,  # 🆕 Add file_name for UI
            "status": ProcessingStatus.PENDING,
            "start_time": time.time(),
            "file_size_mb": file_size_mb
        }
        
        thread = threading.Thread(
            target=run_async_processing,
            args=(job_id, pdf_path, pdf_name, filename, file_size_mb, neon_result["blob_url"]),
            daemon=True)
        thread.start()
        
        return jsonify({
            "job_id": job_id,
            "message": "Processing started",
            "status": "started",
            "check_status_url": f"/status/{job_id}",
            "pdf_name": pdf_name,
            "file_name": filename
        })
        
    except Exception as e:
        try:
            pdf_path.unlink()
        except:
            pass
        return jsonify({"error": str(e)}), 500

@app.route("/documents", methods=["GET"])
def get_all_documents():
    """🚀 FAST: Get all documents from neon DB (no vector queries)"""
    try:
        async def fetch_from_neon():
            async with NeonDocumentManager() as neon_manager:
                return await neon_manager.get_all_documents()
        
        # Get documents from neon DB
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        neon_documents = loop.run_until_complete(fetch_from_neon())
        loop.close()
        
        # Format for frontend
        documents = []
        for doc in neon_documents:
            # Map neon DB status to frontend status
            status_mapping = {
                DocumentStatus.PENDING: "Processing",
                DocumentStatus.PROCESSING: "Processing", 
                DocumentStatus.ANALYZED: "Analyzed",
                DocumentStatus.FAILED: "Failed"
            }
            # Handle datetime conversion
            created_at = doc["created_at"]
            if hasattr(created_at, 'date'):
                date_str = created_at.date().isoformat()
            else:
                date_str = str(created_at)[:10]
            documents.append({
                "id": doc["file_name"],  # Use file_name as ID for blob operations
                "name": doc["file_name"],
                "pdf_name": doc["pdf_name"],
                "date": date_str,
                "size": f"{doc['file_size_mb']} MB",
                "status": status_mapping.get(doc["status"], "Unknown"),
                "chunk_count": doc.get("chunk_count", 0),
                "blob_url": doc.get("blob_url", ""),
                "processing_stage": doc.get("processing_stage"),
                "error_message": doc.get("error_message")
            })
        
        print(f"📋 Retrieved {len(documents)} documents from neon DB (FAST)")
        return jsonify({"documents": documents})
        
    except Exception as e:
        print(f"❌ Error fetching documents: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/documents/processed", methods=["GET"])  
def get_processed_documents():
    """🚀 FAST: Get only processed documents from neon DB"""
    try:
        async def fetch_processed():
            async with NeonDocumentManager() as neon_manager:
                return await neon_manager.get_processed_documents()
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        processed_docs = loop.run_until_complete(fetch_processed())
        loop.close()
        
        print(f"📋 Retrieved {len(processed_docs)} processed documents (FAST)")
        return jsonify({"documents": processed_docs})
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500
# Add cleanup on app shutdown
@app.teardown_appcontext
def close_db_connections(error):
    """Don't close connection pool on each request - it's persistent"""
    pass

@app.route("/documents/<document_id>", methods=["DELETE"])
def delete_document(document_id):
    """Delete document from blob storage, vector database, and neon DB"""
    try:
        processor = PDFProcessor()
        pdf_name = Path(document_id).stem
        
        async def delete_from_all_sources():
            async with NeonDocumentManager() as neon_manager, \
                       PineconeVectorStore() as vector_store:
                
                # Delete from neon DB first to get metadata
                neon_doc = await neon_manager.get_document_by_pdf_name(pdf_name)
                neon_deleted = await neon_manager.delete_document(pdf_name)
                
                # Delete from vector database
                vector_result = await vector_store.delete_pdf_vectors(pdf_name)
                
                return neon_deleted, vector_result, neon_doc
        
        # Execute deletions
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        neon_deleted, vector_result, neon_doc = loop.run_until_complete(delete_from_all_sources())
        loop.close()
        
        # Delete from blob storage
        blob_deleted = True
        try:
            blob_client = processor.blob_service_client.get_blob_client(
                container=processor.pdf_container_name,
                blob=document_id
            )
            blob_client.delete_blob()
        except Exception as e:
            print(f"⚠️  Blob deletion warning: {e}")
            blob_deleted = False
        
        return jsonify({
            "message": "Document deleted successfully",
            "pdf_name": pdf_name,
            "neon_deleted": neon_deleted,
            "blob_deleted": blob_deleted,
            "vector_deletion": vector_result
        })
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/status/<job_id>", methods=["GET"])
def get_job_status(job_id):
    """Check processing job status - unchanged"""
    job = processing_jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    if job.get("end_time"):
        job["elapsed_time"] = job["end_time"] - job["start_time"]
    else:
        job["elapsed_time"] = time.time() - job["start_time"]
    return jsonify(job)

@app.route("/query", methods=["POST"])
def handle_query():
    """Query endpoint - unchanged (still uses vector DB for search)"""
    data = request.get_json()
    query = data.get("query")
    pdf_name = data.get("pdf_name")
    if not query:
        return jsonify({"error": "Query required"}), 400

    async def process_query():
        async with QueryProcessor() as query_processor, \
            PineconeVectorStore() as vector_store, \
            ResponseGenerator() as response_generator:
            # Decompose query
            sub_queries = await query_processor.decompose_query(query)
            # Process each sub-query
            sub_answers = []
            all_chunks = []
            for sq in sub_queries:
                chunks = await vector_store.search_similar_chunks(sq, top_k=5, pdf_name=pdf_name)
                answer = await response_generator.generate_answer_for_subquery(sq, chunks)
                sub_answers.append({
                    "sub_query": sq,
                    "answer": answer,
                    "context": chunks
                })
                all_chunks.extend(chunks)
            # Rerank and synthesize
            final_chunks = query_processor.rerank_chunks(all_chunks, query)
            sources = [{
                "type": c["type"],
                "page": c["page_number"],
                "content_preview": c["content"][:100] + ("..." if len(c["content"]) > 100 else "")
            } for c in final_chunks]
            # Final answer
            result = await response_generator.combine_sub_answers(query, sub_answers)
            response = {
                "answer": result.get("answer", "No answer generated."),
                "sources": sources
            }
            return response

    try:
        result = asyncio.run(process_query())
        return jsonify(result)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

# 🆕 New endpoint for neon DB statistics (optional)
@app.route("/statistics", methods=["GET"])
def get_statistics():
    """Get processing statistics from neon DB"""
    try:
        async def get_stats():
            async with NeonDocumentManager() as neon_manager:
                return await neon_manager.get_processing_statistics()
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        stats = loop.run_until_complete(get_stats())
        loop.close()
        
        return jsonify(stats)
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/health", methods=["GET"])
def health_check():
    """Health check for devops/liveness."""
    return jsonify({"status": "ok"}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)