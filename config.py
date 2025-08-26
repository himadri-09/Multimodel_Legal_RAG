import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Azure OpenAI Configuration
AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_DEPLOYMENT_NAME = os.getenv("AZURE_DEPLOYMENT_NAME")
AZURE_EMBEDDING_DEPLOYMENT_NAME = os.getenv("AZURE_EMBEDDING_DEPLOYMENT_NAME")
# REMOVED: AZURE_VISION_DEPLOYMENT_NAME (not needed for text-only)
AZURE_API_VERSION = os.getenv("AZURE_API_VERSION", "2024-02-15-preview")

# 🆕 Neon PostgreSQL Configuration
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable is not set.")

# Connection Pool Settings
DB_MIN_CONNECTIONS = int(os.getenv("DB_MIN_CONNECTIONS", "2"))
DB_MAX_CONNECTIONS = int(os.getenv("DB_MAX_CONNECTIONS", "10"))

# Pinecone Configuration
PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
PINECONE_INDEX_NAME = os.getenv("PINECONE_INDEX_NAME")

# LangSmith Configuration
LANGCHAIN_TRACING_V2 = os.getenv("LANGCHAIN_TRACING_V2", "true")
LANGCHAIN_ENDPOINT = os.getenv("LANGCHAIN_ENDPOINT")
LANGCHAIN_API_KEY = os.getenv("LANGCHAIN_API_KEY")
LANGCHAIN_PROJECT = os.getenv("LANGCHAIN_PROJECT")

# Processing Configuration
EMBEDDING_DIMENSION = int(os.getenv("EMBEDDING_DIMENSION", "1536"))
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "512"))
CHUNK_OVERLAP = int(os.getenv("CHUNK_OVERLAP", "50"))
TOP_K_RETRIEVAL = int(os.getenv("TOP_K_RETRIEVAL", "5"))

MAX_CONCURRENT_EMBEDDINGS = int(os.getenv("MAX_CONCURRENT_EMBEDDINGS", "30"))
# REMOVED: Image-related config variables since we're disabling image processing
# MAX_CONCURRENT_IMAGE_CAPTIONS = int(os.getenv("MAX_CONCURRENT_IMAGE_CAPTIONS", "5"))
# MIN_IMAGE_SIZE_BYTES = int(os.getenv("MIN_IMAGE_SIZE_KB", "10")) * 1024

# Storage Configuration
UPLOADS_DIR = Path("uploads")
STATIC_DIR = Path("static")

# Create directories
for dir_path in [UPLOADS_DIR, STATIC_DIR]:
    dir_path.mkdir(exist_ok=True)

print(f"📌 Configuration loaded - Pinecone Index: {PINECONE_INDEX_NAME}")
print("📄 TEXT ONLY MODE - Image processing disabled")