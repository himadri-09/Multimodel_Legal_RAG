# utils/pdf_processor.py

import fitz  # PyMuPDF
from typing import List, Dict, Any
from pathlib import Path
from azure.storage.blob import BlobServiceClient
import os
from config import CHUNK_SIZE, CHUNK_OVERLAP

class PDFProcessor:
    def __init__(self):
        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        if not connection_string:
            raise ValueError("AZURE_STORAGE_CONNECTION_STRING environment variable is not set.")
        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        self.pdf_container_name = os.getenv("PDF_BLOB_CONTAINER", "rag-pdf-uploads")

    def extract_text_from_pdf(self, local_pdf_path: str, pdf_name: str) -> List[Dict[str, Any]]:
        """Extract plain text chunks from PDF."""
        doc = fitz.open(local_pdf_path)
        text_chunks = []
        for page_num in range(len(doc)):
            page = doc.load_page(page_num)
            text = page.get_text().strip()
            if text:
                # Split into overlapping chunks
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
        return text_chunks

    def upload_pdf_to_blob(self, local_pdf_path: str, filename: str) -> str:
        """Upload original PDF to Azure Blob Storage and return the blob URL."""
        blob_client = self.blob_service_client.get_blob_client(
            container=self.pdf_container_name,
            blob=filename
        )
        with open(local_pdf_path, "rb") as pdf_file:
            blob_client.upload_blob(pdf_file, overwrite=True)
        # Build the public URL (if stored as public)
        account_url = self.blob_service_client.account_name
        pdf_url = f"https://{account_url}.blob.core.windows.net/{self.pdf_container_name}/{filename}"
        return pdf_url
