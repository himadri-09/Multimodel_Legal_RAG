# app.py (simplified, backend-only)

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

from typing import Dict, List

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

async def process_pdf_async(job_id: str, pdf_path: Path, pdf_name: str,
                           filename: str) -> Dict[str, Any]:
    """Async PDF processing: upload to blob, extract/chunk text, embed/store in Pinecone."""
    try:
        job = processing_jobs[job_id]
        job["status"] = ProcessingStatus.PROCESSING
        job["stage"] = "Initializing"

        processor = PDFProcessor()

        # 1. Upload PDF to Blob Storage
        job["stage"] = "Uploading PDF to blob storage"
        blob_url = processor.upload_pdf_to_blob(str(pdf_path), filename)
        job["blob_url"] = blob_url

        # 2. Extract text chunks
        job["stage"] = "Extracting text"
        text_chunks = processor.extract_text_from_pdf(str(pdf_path), pdf_name)

        # 3. Store chunks in vector DB
        async with PineconeVectorStore() as vector_store:
            job["stage"] = "Storing chunks"
            await vector_store.store_chunks(text_chunks, pdf_name)
            chunk_count = await vector_store.get_pdf_chunk_count(pdf_name)

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

def run_async_processing(job_id, pdf_path, pdf_name, filename):
    """Run async processing in a thread."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    result = loop.run_until_complete(
        process_pdf_async(job_id, pdf_path, pdf_name, filename))
    loop.close()
    return result

@app.route("/upload", methods=["POST"])
def upload_pdf():
    """Backend endpoint: upload, process, and store a PDF."""
    if "file" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400
    file = request.files["file"]
    if file.filename.strip() == "":
        return jsonify({"error": "No file selected"}), 400
    if not file.filename.lower().endswith(".pdf"):
        return jsonify({"error": "Only PDFs are allowed"}), 400

    # File size sanity check
    file.seek(0, 2)  # End
    file_size = file.tell()
    file.seek(0)     # Reset

    # [Optional] Add max size check here

    filename = secure_filename(file.filename)
    pdf_name = Path(filename).stem
    pdf_path = UPLOADS_DIR / filename
    file.save(pdf_path)

    # Start async job
    job_id = str(uuid.uuid4())
    processing_jobs[job_id] = {
        "job_id": job_id,
        "pdf_name": pdf_name,
        "status": ProcessingStatus.PENDING,
        "start_time": time.time(),
        "file_size_mb": round(file_size / (1024 * 1024), 2)
    }
    thread = threading.Thread(
        target=run_async_processing,
        args=(job_id, pdf_path, pdf_name, filename),
        daemon=True)
    thread.start()
    return jsonify({
        "job_id": job_id,
        "message": "Processing started",
        "status": "started",
        "check_status_url": f"/status/{job_id}"
    })
    

@app.route("/documents", methods=["GET"])
def get_all_documents():
    """Get all processed PDFs from blob storage and their status."""
    try:
        processor = PDFProcessor()
        
        # Get all blobs from the container
        container_client = processor.blob_service_client.get_container_client(
            processor.pdf_container_name
        )
        
        documents = []
        for blob in container_client.list_blobs():
            # Get blob properties
            blob_client = processor.blob_service_client.get_blob_client(
                container=processor.pdf_container_name,
                blob=blob.name
            )
            properties = blob_client.get_blob_properties()
            
            # Check if PDF is processed in vector store
            pdf_name = Path(blob.name).stem
            async def check_status():
                async with PineconeVectorStore() as vector_store:
                    exists = await vector_store.check_pdf_exists(pdf_name)
                    chunk_count = await vector_store.get_pdf_chunk_count(pdf_name) if exists else 0
                    return exists, chunk_count
            
            is_processed, chunk_count = asyncio.run(check_status())
            
            documents.append({
                "id": blob.name,
                "name": blob.name,
                "pdf_name": pdf_name,
                "date": properties.creation_time.strftime("%Y-%m-%d"),
                "size": f"{properties.size / (1024 * 1024):.1f} MB",
                "status": "Analyzed" if is_processed else "Failed",
                "chunk_count": chunk_count,
                "blob_url": f"https://{processor.blob_service_client.account_name}.blob.core.windows.net/{processor.pdf_container_name}/{blob.name}"
            })
        
        return jsonify({"documents": documents})
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/documents/<document_id>", methods=["DELETE"])
def delete_document(document_id):
    """Delete document from blob storage and vector database."""
    try:
        processor = PDFProcessor()
        pdf_name = Path(document_id).stem
        
        # Delete from blob storage
        blob_client = processor.blob_service_client.get_blob_client(
            container=processor.pdf_container_name,
            blob=document_id
        )
        blob_client.delete_blob()
        
        # Delete from vector database
        async def delete_vectors():
            async with PineconeVectorStore() as vector_store:
                return await vector_store.delete_pdf_vectors(pdf_name)
        
        vector_result = asyncio.run(delete_vectors())
        
        return jsonify({
            "message": "Document deleted successfully",
            "pdf_name": pdf_name,
            "blob_deleted": True,
            "vector_deletion": vector_result
        })
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/documents/processed", methods=["GET"])
def get_processed_documents():
    """Get only processed documents for chat dropdown."""
    try:
        all_docs_response = get_all_documents()
        all_docs = all_docs_response.get_json()
        
        if "documents" in all_docs:
            processed_docs = [
                {"id": doc["pdf_name"], "name": doc["name"]} 
                for doc in all_docs["documents"] 
                if doc["status"] == "Analyzed"
            ]
            return jsonify({"documents": processed_docs})
        
        return jsonify({"documents": []})
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/status/<job_id>", methods=["GET"])
def get_job_status(job_id):
    """Check processing job status."""
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
    """Backend endpoint: query stored chunks."""
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

@app.route("/health", methods=["GET"])
def health_check():
    """Health check for devops/liveness."""
    return jsonify({"status": "ok"}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
