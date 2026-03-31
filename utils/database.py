# utils/database.py
from typing import Optional, List, Dict, Any
from datetime import datetime
from utils.auth import get_supabase_client


class DatabaseManager:
    """
    Manages all Supabase database operations for user data,
    PDFs, chat history, and processing jobs.
    """

    def __init__(self):
        self.client = get_supabase_client()

    # ==================== USER PDF / SITE OPERATIONS ====================

    async def log_pdf_upload(
        self,
        user_id: str,
        pdf_name: str,
        original_filename: str,
        file_size_bytes: int,
        upload_status: str = "processing",
        source_type: str = "pdf",        # ← NEW: "pdf" or "web"
        source_url: str = None,          # ← NEW: original URL for web crawls
    ) -> Dict:
        """
        Log a PDF upload or web crawl to the database.

        Args:
            user_id:           User UUID
            pdf_name:          Sanitized PDF stem or site slug (used as pdf_name in Pinecone)
            original_filename: Original filename (PDF) or root URL (web crawl)
            file_size_bytes:   File size in bytes; pass 0 for web crawls
            upload_status:     Initial status (default: "processing")
            source_type:       "pdf" for uploaded documents, "web" for crawled sites
            source_url:        Root URL of the crawl (web only)
        """
        try:
            data = {
                "user_id":           user_id,
                "pdf_name":          pdf_name,
                "original_filename": original_filename,
                "file_size_bytes":   file_size_bytes,
                "upload_status":     upload_status,
                "source_type":       source_type,
                "uploaded_at":       datetime.utcnow().isoformat(),
            }

            if source_url:
                data["source_url"] = source_url

            response = self.client.table("user_pdfs").insert(data).execute()
            print(f"✅ Logged {source_type} record: {pdf_name} for user {user_id}")
            return response.data[0] if response.data else {}

        except Exception as e:
            print(f"❌ Error logging upload: {e}")
            raise

    async def update_pdf_status(
        self,
        user_id: str,
        pdf_name: str,
        status: str,
        chunks_count: Optional[int] = None,
        error_message: Optional[str] = None,
        bm25_blob_url: Optional[str] = None,
    ) -> Dict:
        """Update PDF / site processing status."""
        try:
            update_data = {
                "upload_status": status,
                "processed_at":  datetime.utcnow().isoformat(),
            }

            if chunks_count is not None:
                update_data["chunks_count"] = chunks_count

            if error_message:
                update_data["error_message"] = error_message
            if bm25_blob_url:
                update_data["bm25_blob_url"] = bm25_blob_url

            response = (
                self.client.table("user_pdfs")
                .update(update_data)
                .eq("user_id", user_id)
                .eq("pdf_name", pdf_name)
                .execute()
            )

            print(f"✅ Updated status: {pdf_name} → {status}")
            return response.data[0] if response.data else {}

        except Exception as e:
            print(f"❌ Error updating PDF status: {e}")
            raise

    async def check_user_pdf_exists(self, user_id: str, pdf_name: str) -> bool:
        """Check if a PDF or site slug already exists for this user."""
        try:
            response = (
                self.client.table("user_pdfs")
                .select("id")
                .eq("user_id", user_id)
                .eq("pdf_name", pdf_name)
                .execute()
            )
            return len(response.data) > 0
        except Exception as e:
            print(f"❌ Error checking existence: {e}")
            return False

    async def get_pdf_info(self, user_id: str, pdf_name: str) -> Optional[Dict]:
        """Get metadata for a specific PDF or crawled site."""
        try:
            response = (
                self.client.table("user_pdfs")
                .select(
                    "pdf_name, chunks_count, upload_status, uploaded_at, "
                    "processed_at, original_filename, file_size_bytes, "
                    "source_type, source_url"   # ← include new columns
                )
                .eq("user_id", user_id)
                .eq("pdf_name", pdf_name)
                .execute()
            )

            if response.data:
                return response.data[0]
            return None

        except Exception as e:
            print(f"❌ Error getting pdf info: {e}")
            return None

    async def get_user_pdfs(
        self,
        user_id: str,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict]:
        """Get all PDFs and crawled sites for a user."""
        try:
            response = (
                self.client.table("user_pdfs")
                .select("*")
                .eq("user_id", user_id)
                .order("uploaded_at", desc=True)
                .limit(limit)
                .offset(offset)
                .execute()
            )
            return response.data
        except Exception as e:
            print(f"❌ Error fetching user pdfs: {e}")
            return []

    async def delete_user_pdf(self, user_id: str, pdf_name: str) -> bool:
        """Delete a PDF or crawled site record."""
        try:
            self.client.table("user_pdfs").delete() \
                .eq("user_id", user_id) \
                .eq("pdf_name", pdf_name) \
                .execute()
            print(f"✅ Deleted record: {pdf_name} for user {user_id}")
            return True
        except Exception as e:
            print(f"❌ Error deleting record: {e}")
            return False

    # ==================== CHAT CONVERSATION OPERATIONS ====================

    async def create_conversation(
        self,
        user_id: str,
        title: Optional[str] = None,
    ) -> str:
        try:
            data = {
                "user_id":    user_id,
                "title":      title or "New Conversation",
                "created_at": datetime.utcnow().isoformat(),
                "updated_at": datetime.utcnow().isoformat(),
            }
            response = self.client.table("chat_conversations").insert(data).execute()
            conversation_id = response.data[0]["id"]
            print(f"✅ Created conversation: {conversation_id}")
            return conversation_id
        except Exception as e:
            print(f"❌ Error creating conversation: {e}")
            raise

    async def add_message(
        self,
        conversation_id: str,
        user_id: str,
        role: str,
        content: str,
        query: Optional[str] = None,
        pdf_name: Optional[str] = None,
        sources: Optional[List[Dict]] = None,
        images: Optional[List[Dict]] = None,
    ) -> Dict:
        try:
            data = {
                "conversation_id": conversation_id,
                "user_id":         user_id,
                "role":            role,
                "content":         content,
                "created_at":      datetime.utcnow().isoformat(),
            }
            if query:    data["query"]    = query
            if pdf_name: data["pdf_name"] = pdf_name
            if sources:  data["sources"]  = sources
            if images:   data["images"]   = images

            response = self.client.table("chat_messages").insert(data).execute()
            print(f"✅ Added {role} message to conversation {conversation_id}")
            return response.data[0] if response.data else {}
        except Exception as e:
            print(f"❌ Error adding message: {e}")
            raise

    async def get_conversations(
        self,
        user_id: str,
        limit: int = 50,
        offset: int = 0,
    ) -> List[Dict]:
        try:
            response = (
                self.client.table("chat_conversations")
                .select("*")
                .eq("user_id", user_id)
                .order("updated_at", desc=True)
                .limit(limit)
                .offset(offset)
                .execute()
            )
            return response.data
        except Exception as e:
            print(f"❌ Error fetching conversations: {e}")
            return []

    async def get_conversation_messages(
        self,
        conversation_id: str,
        user_id: str,
        limit: int = 100,
    ) -> List[Dict]:
        try:
            response = (
                self.client.table("chat_messages")
                .select("*")
                .eq("conversation_id", conversation_id)
                .eq("user_id", user_id)
                .order("created_at", desc=False)
                .limit(limit)
                .execute()
            )
            return response.data
        except Exception as e:
            print(f"❌ Error fetching messages: {e}")
            return []

    async def delete_conversation(self, conversation_id: str, user_id: str) -> bool:
        try:
            self.client.table("chat_conversations").delete() \
                .eq("id", conversation_id) \
                .eq("user_id", user_id) \
                .execute()
            print(f"✅ Deleted conversation: {conversation_id}")
            return True
        except Exception as e:
            print(f"❌ Error deleting conversation: {e}")
            return False

    # ==================== PROCESSING JOB OPERATIONS ====================

    async def create_processing_job(
        self,
        job_id: str,
        user_id: str,
        pdf_name: str,
        filename: str,
    ) -> Dict:
        try:
            data = {
                "job_id":     job_id,
                "user_id":    user_id,
                "pdf_name":   pdf_name,
                "filename":   filename,
                "status":     "pending",
                "progress":   0.0,
                "start_time": datetime.utcnow().isoformat(),
            }
            response = self.client.table("processing_jobs").insert(data).execute()
            print(f"✅ Created processing job: {job_id}")
            return response.data[0] if response.data else {}
        except Exception as e:
            print(f"❌ Error creating processing job: {e}")
            raise

    async def update_processing_job(
        self,
        job_id: str,
        status: Optional[str] = None,
        stage: Optional[str] = None,
        progress: Optional[float] = None,
        result: Optional[Dict] = None,
        error: Optional[str] = None,
    ) -> Dict:
        try:
            update_data = {}
            if status   is not None: update_data["status"]   = status
            if stage    is not None: update_data["stage"]    = stage
            if progress is not None: update_data["progress"] = progress
            if result   is not None: update_data["result"]   = result
            if error    is not None: update_data["error"]    = error

            if status in ("completed", "failed", "cached"):
                update_data["end_time"] = datetime.utcnow().isoformat()

            response = (
                self.client.table("processing_jobs")
                .update(update_data)
                .eq("job_id", job_id)
                .execute()
            )
            return response.data[0] if response.data else {}
        except Exception as e:
            print(f"❌ Error updating processing job: {e}")
            raise

    async def get_processing_job(self, job_id: str, user_id: str) -> Optional[Dict]:
        try:
            response = (
                self.client.table("processing_jobs")
                .select("*")
                .eq("job_id", job_id)
                .eq("user_id", user_id)
                .execute()
            )
            return response.data[0] if response.data else None
        except Exception as e:
            print(f"❌ Error fetching processing job: {e}")
            return None

    async def get_user_jobs(
        self,
        user_id: str,
        limit: int = 50,
        offset: int = 0,
    ) -> List[Dict]:
        try:
            response = (
                self.client.table("processing_jobs")
                .select("*")
                .eq("user_id", user_id)
                .order("start_time", desc=True)
                .limit(limit)
                .offset(offset)
                .execute()
            )
            return response.data
        except Exception as e:
            print(f"❌ Error fetching user jobs: {e}")
            return []