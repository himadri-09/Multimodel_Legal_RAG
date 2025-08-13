# utils/pdf_processor.py - TEXT ONLY VERSION
import fitz  # PyMuPDF
from typing import List, Dict, Any
from pathlib import Path

from config import CHUNK_SIZE, CHUNK_OVERLAP
from langsmith import traceable

class PDFProcessor:
    def __init__(self):
        # Removed Azure Blob Storage client since we're not processing images
        print("📄 PDF Processor initialized - TEXT ONLY MODE")

    def extract_text_from_pdf(self, pdf_path: str, pdf_name: str) -> List[Dict[str, Any]]:
        """Extract text chunks from PDF"""
        print(f"📖 Extracting text from: {pdf_name}")
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
        """IMAGE PROCESSING DISABLED - Returns empty list"""
        print(f"🚫 Image processing disabled for: {pdf_name}")
        print("📄 Running in TEXT-ONLY mode - skipping all images")
        return []  # Return empty list instead of processing images