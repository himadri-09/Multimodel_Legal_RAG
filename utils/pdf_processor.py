# utils/pdf_processor.py
import fitz  # PyMuPDF
import pdfplumber
import pandas as pd
from typing import List, Dict, Any
from pathlib import Path
from azure.storage.blob import BlobServiceClient
import io
import os
from PIL import Image

from config import CHUNK_SIZE, CHUNK_OVERLAP, MIN_IMAGE_SIZE_BYTES
from langsmith import traceable

class PDFProcessor:
    def __init__(self):
        # Initialize Blob Service Client
        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        if not connection_string:
            raise ValueError("AZURE_STORAGE_CONNECTION_STRING environment variable is not set.")
        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        self.container_name = "rag-pdf-images"

    def extract_text_from_pdf(self, pdf_path: str, pdf_name: str) -> List[Dict[str, Any]]:
        """Extract text chunks from PDF"""
        print(f"📝 Extracting text from: {pdf_name}")
        doc = fitz.open(pdf_path)
        text_chunks = []
        for page_num in range(len(doc)):
            page = doc.load_page(page_num)
            text = page.get_text()
            if text.strip():
                print(f"   - Processing text on page {page_num + 1}") # Added page log
                # Split text into overlapping chunks
                words = text.split()
                for i in range(0, len(words), CHUNK_SIZE - CHUNK_OVERLAP):
                    chunk_words = words[i:i + CHUNK_SIZE]
                    chunk_text = " ".join(chunk_words)
                    text_chunks.append({
                        'content': chunk_text,
                        'type': 'text',
                        'page_number': page_num + 1,
                        'pdf_name': pdf_name,
                        'metadata': {
                            'word_count': len(chunk_words),
                            'char_count': len(chunk_text)
                        }
                    })
        doc.close()
        print(f"✅ Extracted {len(text_chunks)} text chunks")
        return text_chunks

    
    def extract_tables_from_pdf(self, pdf_path: str, pdf_name: str) -> List[Dict[str, Any]]:
        """Extract tables from PDF - each table is one chunk"""
        print(f"📊 Extracting tables from: {pdf_name}")
        table_chunks = []
        with pdfplumber.open(pdf_path) as pdf:
            # --- Add page iteration logging ---
            for page_num, page in enumerate(pdf.pages):
                # Log that we are processing this page
                print(f"   - Searching for tables on page {page_num + 1}")
                tables = page.extract_tables()
                # Log the number of tables found (or if none)
                if tables:
                    print(f"     Found {len(tables)} table(s) on page {page_num + 1}")
                # --- End page iteration logging ---
                for table_index, table in enumerate(tables):
                    if table and len(table) > 1:
                        try:
                            # Convert to DataFrame
                            df = pd.DataFrame(table[1:], columns=table[0])
                            df = df.dropna(how='all')
                            if not df.empty:
                                # Convert to markdown
                                markdown_table = df.to_markdown(index=False)
                                table_chunks.append({
                                    'content': markdown_table,
                                    'type': 'table',
                                    'page_number': page_num + 1,
                                    'pdf_name': pdf_name,
                                    'metadata': {
                                        'rows': len(df),
                                        'columns': len(df.columns),
                                        'table_index': table_index,
                                        'column_names': list(df.columns)
                                    }
                                })
                                # Optional: Log successful processing of a specific table
                                # print(f"       Processed table {table_index + 1} on page {page_num + 1}")
                        except Exception as e:
                            print(f"❌ Error processing table {table_index + 1} on page {page_num + 1}: {e}")
                            continue
        print(f"✅ Extracted {len(table_chunks)} table chunks")
        return table_chunks
    
    def extract_images_from_pdf(self, pdf_path: str, pdf_name: str) -> List[Dict[str, Any]]:
        """Extract images from PDF and save to Azure Blob Storage as WebP"""
        print(f"🖼️ Extracting images from: {pdf_name}")
        doc = fitz.open(pdf_path)
        image_chunks = []

        for page_num, page in enumerate(doc):
            images = page.get_images(full=True)
            for img_index, img in enumerate(images):
                xref = img[0]
                try:
                    pix = fitz.Pixmap(doc, xref)
                    if pix.n - pix.alpha < 4:  # GRAY or RGB
                        if pix.alpha:
                            pix = fitz.Pixmap(fitz.csRGB, pix)
                        
                        # Convert to WebP using PIL
                        from PIL import Image
                        import io
                        
                        # Convert Pixmap to PPM bytes (supported by PyMuPDF)
                        img_data = pix.tobytes("ppm")
                        pil_image = Image.open(io.BytesIO(img_data))
                        
                        # Convert to WebP in memory
                        webp_buffer = io.BytesIO()
                        pil_image.save(
                            webp_buffer, 
                            format='WEBP', 
                            quality=85,
                            optimize=True,
                            method=6
                        )
                        webp_data = webp_buffer.getvalue()
                        
                        # CHECK FILE SIZE BEFORE UPLOADING
                        file_size_kb = len(webp_data) / 1024
                        if file_size_kb < 10:  # Skip if smaller than 10KB
                            print(f"⏭️ Skipping small image on page {page_num + 1}, img {img_index} - {file_size_kb:.1f} KB < 10 KB")
                            # Clean up
                            webp_buffer.close()
                            pil_image.close()
                            pix = None
                            continue  # Skip to next image
                        
                        # Only upload if size is acceptable
                        blob_name = f"{pdf_name}/page_{page_num + 1}_img_{img_index}.webp"
                        
                        # Save to Blob Storage
                        blob_client = self.blob_service_client.get_blob_client(
                            container=self.container_name, 
                            blob=blob_name
                        )
                        # Upload the WebP image data
                        blob_client.upload_blob(webp_data, overwrite=True)
                        print(f"✅ Uploaded image to blob: {blob_name} ({file_size_kb:.1f} KB)")
                        
                        # Create the public URL
                        account_url = self.blob_service_client.account_name
                        image_url = f"https://{account_url}.blob.core.windows.net/{self.container_name}/{blob_name}"
                        
                        # Add to image chunks
                        image_chunks.append({
                            'content': '',  # Will be filled by captioning
                            'type': 'image',
                            'page_number': page_num + 1,
                            'pdf_name': pdf_name,
                            'image_path': image_url,
                            'metadata': {
                                'width': pix.width,
                                'height': pix.height,
                                'image_index': img_index,
                                'file_size_kb': file_size_kb,
                                'format': 'webp'
                            }
                        })
                        
                        # Clean up
                        webp_buffer.close()
                        pil_image.close()
                        
                    pix = None
                except Exception as e:
                    print(f"❌ Error processing image: {e}")
        doc.close()
        print(f"✅ Extracted {len(image_chunks)} image chunks")
        return image_chunks

    # def extract_images_from_pdf(self, pdf_path: str, pdf_name: str) -> List[Dict[str, Any]]:
    #     """Extract images from PDF and save to data/{pdf_name}/images/"""
    #     print(f"🖼️ Extracting images from: {pdf_name}")
    #     # Create images directory for this PDF
    #     images_dir = DATA_DIR / pdf_name / "images"
    #     images_dir.mkdir(parents=True, exist_ok=True)
    #     doc = fitz.open(pdf_path)
    #     image_chunks = []
    #     # --- Iterate with page number for logging ---
    #     for page_num, page in enumerate(doc):
    #         images = page.get_images(full=True)
    #         # --- Added page log for images ---
    #         if images:
    #             print(f"   - Found {len(images)} image(s) on page {page_num + 1}")
    #         # ---
    #         for img_index, img in enumerate(images):
    #             xref = img[0]
    #             try:
    #                 pix = fitz.Pixmap(doc, xref)
    #                 # --- Use GRAY or RGB check (more robust) ---
    #                 if pix.n < 5 and pix.n > 0:  # Usually 1=GRAY, 3=RGB, 4=RGBA
    #                     # --- Convert RGBA to RGB if necessary for PIL ---
    #                     if pix.alpha:
    #                         pix = fitz.Pixmap(fitz.csRGB, pix)

    #                     # --- Use PIL approach for WEBP saving (from reference) ---
    #                     # Convert Pixmap to bytes (PPM format is good for PIL)
    #                     img_data = pix.tobytes("ppm")
    #                     pil_image = Image.open(io.BytesIO(img_data))

    #                     # Define image path
    #                     img_filename = f"page_{page_num + 1}_img_{img_index}.webp"
    #                     img_path = images_dir / img_filename

    #                     # Save using PIL with quality/optimization
    #                     pil_image.save(str(img_path), "WEBP", quality=85, optimize=True)
    #                     # --- End PIL approach ---

    #                     # --- Check file size AFTER saving using the config constant ---
    #                     file_size = img_path.stat().st_size
    #                     if file_size >= MIN_IMAGE_SIZE_BYTES:
    #                         image_chunks.append({
    #                             'content': '',  # Will be filled by captioning
    #                             'type': 'image',
    #                             'page_number': page_num + 1,
    #                             'pdf_name': pdf_name,
    #                             'image_path': str(img_path),
    #                             'metadata': {
    #                                 'width': pix.width, # Width from original pixmap
    #                                 'height': pix.height, # Height from original pixmap
    #                                 'image_index': img_index,
    #                                 'file_size_kb': file_size / 1024
    #                             }
    #                         })
    #                         # --- Added log for individual image ---
    #                         print(f"     - Saved image {img_index + 1} (Size: {file_size/1024:.1f}KB)")
    #                         # ---
    #                     else:
    #                         # Remove small image
    #                         # --- Added log for skipped image ---
    #                         print(f"     - Skipping small image {img_index + 1} (Size: {file_size/1024:.1f}KB < {MIN_IMAGE_SIZE_BYTES/1024}KB)")
    #                         # ---
    #                         img_path.unlink() # Delete the small file
    #                 else:
    #                     # --- Log unsupported format ---
    #                     print(f"     - Skipping image {img_index + 1} on page {page_num + 1}, unsupported format (n={pix.n})")
    #                     # ---
    #                 pix = None # Ensure pixmap is freed
    #             except Exception as e:
    #                 # --- Improved error log for images ---
    #                 print(f"❌ Error extracting image {img_index + 1} on page {page_num + 1}: {e}")
    #                 # Ensure pix is closed even if an error occurs
    #                 if 'pix' in locals() and pix:
    #                     pix = None
    #     doc.close()
    #     print(f"✅ Extracted {len(image_chunks)} image chunks (after size filter)")
    #     return image_chunks
