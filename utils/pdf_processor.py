# utils/pdf_processor.py
import fitz  # PyMuPDF
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
        self.pdf_container_name = "rag-pdf-uploads"

    def upload_pdf_to_blob(self, pdf_path: str, user_id: str, filename: str) -> str:
        """
        Upload original PDF to Azure Blob Storage organized by user ID

        Args:
            pdf_path: Local path to the PDF file
            user_id: User's UUID (for folder organization)
            filename: Original filename

        Returns:
            str: Blob URL of the uploaded PDF
        """
        try:
            # Create blob path: {user_id}/{filename}
            blob_name = f"{user_id}/{filename}"

            print(f"📤 Uploading PDF to blob storage: {blob_name}")

            # Read PDF file
            with open(pdf_path, 'rb') as pdf_file:
                pdf_data = pdf_file.read()

            # Get blob client
            blob_client = self.blob_service_client.get_blob_client(
                container=self.pdf_container_name,
                blob=blob_name
            )

            # Upload PDF
            blob_client.upload_blob(pdf_data, overwrite=True, content_type='application/pdf')

            # Construct blob URL
            account_name = self.blob_service_client.account_name
            blob_url = f"https://{account_name}.blob.core.windows.net/{self.pdf_container_name}/{blob_name}"

            print(f"✅ PDF uploaded successfully to: {blob_url}")
            return blob_url

        except Exception as e:
            print(f"❌ Error uploading PDF to blob storage: {e}")
            raise

    def extract_text_from_pdf(self, pdf_path: str, pdf_name: str) -> List[Dict[str, Any]]:
        """Extract text chunks from PDF"""
        print(f"📝 Extracting text from: {pdf_name}")
        doc = fitz.open(pdf_path)
        text_chunks = []
        for page_num in range(len(doc)):
            page = doc.load_page(page_num)
            text = page.get_text()
            if text.strip():
                print(f"   - Processing text on page {page_num + 1}")
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
    
    def extract_images_from_pdf(self, pdf_path: str, pdf_name: str) -> List[Dict[str, Any]]:
        """Extract images from PDF and save to Azure Blob Storage as WebP"""
        print(f"🖼️ Extracting images from: {pdf_name}")
        doc = fitz.open(pdf_path)
        image_chunks = []

        for page_num, page in enumerate(doc):
            images = page.get_images(full=True)
            if images:
                print(f"   - Found {len(images)} image(s) on page {page_num + 1}")
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
                        
                    else:
                        print(f"     - Skipping image {img_index + 1} on page {page_num + 1}, unsupported format (n={pix.n})")
                        
                    pix = None
                except Exception as e:
                    print(f"❌ Error processing image {img_index + 1} on page {page_num + 1}: {e}")
        doc.close()
        print(f"✅ Extracted {len(image_chunks)} image chunks")
        return image_chunks