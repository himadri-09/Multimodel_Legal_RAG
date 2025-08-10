# app.py
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import asyncio
import time
from pathlib import Path
from werkzeug.utils import secure_filename
import traceback
from config import UPLOADS_DIR 
from utils.pdf_processor import PDFProcessor
from utils.image_captioner import ImageCaptioner
from utils.vector_store import PineconeVectorStore
from utils.query_processor import QueryProcessor
from utils.response_generator import ResponseGenerator

app = Flask(__name__)
CORS(app)

# Ensure directories exist
UPLOADS_DIR.mkdir(exist_ok=True)

# --- Routes for serving static files ---
@app.route("/")
def index():
    """Serve the main HTML page."""
    return send_from_directory(app.static_folder, 'index.html')

@app.route("/static/<path:filename>")
def serve_static_files(filename):
    """Serve static files like CSS, JS, and images directly from the 'static' folder."""
    # This single route handles all static files based on Flask's default static folder setup.
    # It will correctly serve index.html, script.js, style.css, etc.
    try:
        return send_from_directory(app.static_folder, filename)
    except FileNotFoundError:
        # Log the specific file not found for easier debugging
        print(f"❌ Static file not found: {filename} in {app.static_folder}")
        return "File not found", 404
    except Exception as e:
        print(f"❌ Error serving static file {filename}: {e}")
        return "Error serving file", 500
# --- End Static File Routes ---

@app.route("/upload", methods=["POST"])
def upload_pdf():
    """Handle PDF upload, processing, and storage with enhanced cache feedback."""
    if "file" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400

    file = request.files["file"]
    if file.filename == "":
        return jsonify({"error": "No file selected"}), 400

    if not file.filename.lower().endswith(".pdf"):
        return jsonify({"error": "Only PDF files are allowed"}), 400

    filename = secure_filename(file.filename)
    pdf_name = Path(filename).stem
    pdf_path = UPLOADS_DIR / filename
    file.save(pdf_path)

    try:
        print(f"📁 Starting processing for: {filename}")
        import time
        start_time = time.time()
        
        async def check_and_process():
            async with PineconeVectorStore() as vector_store:
                
                # 🎯 CACHE CHECK - Enhanced with better feedback
                print(f"🔍 Checking cache for PDF: '{pdf_name}'")
                pdf_exists = await vector_store.check_pdf_exists(pdf_name)
                
                if pdf_exists:
                    # PDF already processed - skip all processing
                    chunk_count = await vector_store.get_pdf_chunk_count(pdf_name)
                    cache_end_time = time.time()
                    cache_check_duration = cache_end_time - start_time
                    
                    print(f"🚀 CACHE HIT! Skipping processing for '{pdf_name}'")
                    print(f"📊 Estimated chunks in database: {chunk_count}")
                    print(f"⚡ Cache check completed in {cache_check_duration:.2f}s")
                    
                    # Clean up uploaded file since we don't need it
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
                
                # 🔄 CACHE MISS - Process normally with enhanced logging
                print(f"📝 CACHE MISS! Processing '{pdf_name}' for first time...")
                processing_start = time.time()
                
                # Original processing logic with enhanced feedback
                processor = PDFProcessor()
                print(f"📄 Step 1/4: Extracting text from {pdf_name}...")
                text_chunks = processor.extract_text_from_pdf(str(pdf_path), pdf_name)
                
                print(f"📊 Step 2/4: Extracting tables from {pdf_name}...")
                table_chunks = processor.extract_tables_from_pdf(str(pdf_path), pdf_name)
                
                print(f"🖼️ Step 3/4: Extracting and uploading images from {pdf_name}...")
                image_chunks = processor.extract_images_from_pdf(str(pdf_path), pdf_name)

                async with ImageCaptioner() as captioner:
                    print(f"🤖 Step 4/4: AI captioning {len(image_chunks)} images...")
                    captioned_images = await captioner.caption_images_async(image_chunks)
                    
                    all_chunks = text_chunks + table_chunks + captioned_images
                    print(f"📦 Storing {len(all_chunks)} total chunks in vector database...")

                    # Store everything in Pinecone with pdf_name metadata
                    await vector_store.store_chunks(all_chunks, pdf_name)
                    
                processing_end = time.time()
                total_processing_time = processing_end - start_time
                
                print(f"🎉 PDF '{pdf_name}' processing completed in {total_processing_time:.2f}s")
                
                # Clean up uploaded file after successful processing
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
                    "table_chunks": len(table_chunks),
                    "image_chunks": len(captioned_images),
                    "processing_duration": f"{total_processing_time:.2f}s",
                    "status": "newly_processed"
                }

        result = asyncio.run(check_and_process())
        return jsonify(result)

    except Exception as e:
        print(f"❌ Error processing PDF '{filename}': {e}")
        traceback.print_exc()
        
        # Clean up file in case of error
        try:
            if pdf_path.exists():
                pdf_path.unlink()
                print(f"🧹 Cleaned up file after error: {filename}")
        except Exception as cleanup_error:
            print(f"⚠️ Could not clean up file after error: {cleanup_error}")
            
        return jsonify({
            "error": f"Processing failed: {str(e)}",
            "pdf_name": pdf_name,
            "status": "error"
        }), 500

@app.route("/query", methods=["POST"])
def handle_query():
    """Handle user queries by decomposing, searching, answering, and combining."""
    data = request.json
    query = data.get("query")
    pdf_name = data.get("pdf_name") # Optional filter

    if not query:
        return jsonify({"error": "Query is required"}), 400

    try:
        async def process_query():
            print(f"❓ Handling query: '{query}'")
            
            # --- Use async context managers for resource management ---
            async with QueryProcessor() as query_processor, \
                       PineconeVectorStore() as vector_store, \
                       ResponseGenerator() as response_generator:

                # --- 1. Decompose the main query ---
                sub_queries = await query_processor.decompose_query(query)
                print(f"🧩 Decomposed into {len(sub_queries)} sub-queries.")

                # --- 2. Process each sub-query: Search -> Answer ---
                sub_answers = []
                all_relevant_chunks = [] # Collect chunks for final reranking

                for i, sq in enumerate(sub_queries):
                    print(f"🔍 Processing sub-query {i+1}/{len(sub_queries)}: '{sq}'")
                    
                    # a. Search for relevant chunks in Pinecone
                    chunks = await vector_store.search_similar_chunks(
                        sq, top_k=5, pdf_name=pdf_name
                    )
                    print(f"   Found {len(chunks)} relevant chunks.")
                    all_relevant_chunks.extend(chunks)

                    # b. Generate an answer for this specific sub-query using its context
                    answer = await response_generator.generate_answer_for_subquery(sq, chunks)
                    print(f"   Generated answer for sub-query {i+1}.")

                    # c. Store the sub-answer and its context for later combination
                    sub_answers.append({
                        "sub_query": sq,
                        "answer": answer,
                        "context": chunks # Context used for this sub-answer
                    })

                if not sub_answers:
                    return {
                        "answer": "No relevant information found for any part of the query.",
                        "images": [],
                        "sources": []
                    }

                # --- 3. Rerank all collected chunks from all sub-queries ---
                print("🔄 Reranking all collected chunks from sub-queries...")
                final_reranked_chunks = query_processor.rerank_chunks(all_relevant_chunks, query)
                print(f"📈 Reranking complete. Top {len(final_reranked_chunks)} unique chunks selected.")

                # --- 4. Combine the sub-answers into a final, coherent answer ---
                print("🧩 Combining sub-answers into final response...")
                # This function is expected to return a dict like {"answer": "...", "images": [...]}
                final_result = await response_generator.combine_sub_answers(query, sub_answers)
                print("✅ Final answer generated.")
                
                # --- 5. Extract and Consolidate Images ---
                # Assumes image paths in metadata are already public Blob URLs.
                # Collect images from the final result and the reranked chunks to ensure completeness.
                seen_image_urls = set()
                consolidated_images = []

                # a. Add images from the ResponseGenerator's result (if any)
                # Ensure final_result is a dict and has 'images'
                if isinstance(final_result, dict) and "images" in final_result:
                    for img_info in final_result["images"]:
                         img_url = img_info.get("url") or img_info.get("image_path") # Handle potential key names
                         if img_url and img_url not in seen_image_urls:
                             seen_image_urls.add(img_url)
                             # Ensure consistent structure for frontend
                             consolidated_images.append({
                                 "url": img_url,
                                 "page": img_info.get("page", "N/A"),
                                 "caption": img_info.get("caption", "")
                             })

                # b. Add images from the reranked chunks (source of truth for metadata like path, page, content)
                for chunk in final_reranked_chunks:
                    if chunk.get("type") == "image":
                         # The 'image_path' stored in Pinecone metadata should be the public Blob URL
                         blob_url = chunk.get("image_path")
                         if blob_url and blob_url not in seen_image_urls:
                             seen_image_urls.add(blob_url)
                             # Use chunk content as caption if available
                             caption = chunk.get("content", "")
                             page = chunk.get("page_number", "N/A")
                             consolidated_images.append({
                                 "url": blob_url,
                                 "page": page,
                                 "caption": caption
                             })

                # --- 6. Prepare sources (using the *reranked* chunks for better relevance) ---
                sources = [
                    {
                        "type": c["type"],
                        "page": c["page_number"],
                        "content_preview": c["content"][:100] + "..." if len(c["content"]) > 100 else c["content"],
                    }
                    for c in final_reranked_chunks 
                ]

                # Final response structure
                response_data = {
                    "answer": final_result.get("answer", "Unable to generate final answer.") if isinstance(final_result, dict) else str(final_result),
                    "images": consolidated_images, # List of image info with public URLs
                    "sources": sources # List of reranked source info
                }

                return response_data

        result = asyncio.run(process_query())
        print(f"📤 Returning response for query: '{query}'")
        return jsonify(result)

    except Exception as e:
        print(f"❌ Error handling query '{query}': {e}")
        traceback.print_exc() # Print full traceback for server-side debugging
        return jsonify({"error": f"Query processing failed: {str(e)}"}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
