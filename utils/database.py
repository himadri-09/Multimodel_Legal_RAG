# utils/database.py
from typing import Optional, List, Dict, Any
from datetime import datetime
from utils.auth import get_supabase_client


class DatabaseManager:
    """
    Manages all Supabase database operations for user data,
    PDFs, chat history, and processing jobs
    """

    def __init__(self):
        self.client = get_supabase_client()

    # ==================== USER PDF OPERATIONS ====================

    async def log_pdf_upload(
        self,
        user_id: str,
        pdf_name: str,
        original_filename: str,
        file_size_bytes: int,
        upload_status: str = "processing"
    ) -> Dict:
        """
        Log a PDF upload to the database

        Args:
            user_id: User UUID
            pdf_name: Sanitized PDF name (stem)
            original_filename: Original filename with extension
            file_size_bytes: Size of the PDF in bytes
            upload_status: Initial status (default: "processing")

        Returns:
            Dict: Inserted record
        """
        try:
            data = {
                "user_id": user_id,
                "pdf_name": pdf_name,
                "original_filename": original_filename,
                "file_size_bytes": file_size_bytes,
                "upload_status": upload_status,
                "uploaded_at": datetime.utcnow().isoformat()
            }

            response = self.client.table("user_pdfs").insert(data).execute()
            print(f"✅ Logged PDF upload: {pdf_name} for user {user_id}")
            return response.data[0] if response.data else {}

        except Exception as e:
            print(f"❌ Error logging PDF upload: {e}")
            raise

    async def update_pdf_status(
        self,
        user_id: str,
        pdf_name: str,
        status: str,
        chunks_count: Optional[int] = None,
        error_message: Optional[str] = None
    ) -> Dict:
        """
        Update PDF processing status

        Args:
            user_id: User UUID
            pdf_name: PDF name
            status: New status
            chunks_count: Number of chunks processed (optional)
            error_message: Error message if failed (optional)

        Returns:
            Dict: Updated record
        """
        try:
            update_data = {
                "upload_status": status,
                "processed_at": datetime.utcnow().isoformat()
            }

            if chunks_count is not None:
                update_data["chunks_count"] = chunks_count

            if error_message:
                update_data["error_message"] = error_message

            response = (
                self.client.table("user_pdfs")
                .update(update_data)
                .eq("user_id", user_id)
                .eq("pdf_name", pdf_name)
                .execute()
            )

            print(f"✅ Updated PDF status: {pdf_name} -> {status}")
            return response.data[0] if response.data else {}

        except Exception as e:
            print(f"❌ Error updating PDF status: {e}")
            raise

    async def check_user_pdf_exists(self, user_id: str, pdf_name: str) -> bool:
        """
        Check if user has already uploaded this PDF

        Args:
            user_id: User UUID
            pdf_name: PDF name to check

        Returns:
            bool: True if PDF exists, False otherwise
        """
        try:
            response = (
                self.client.table("user_pdfs")
                .select("id")
                .eq("user_id", user_id)
                .eq("pdf_name", pdf_name)
                .execute()
            )

            exists = len(response.data) > 0
            if exists:
                print(f"✅ PDF '{pdf_name}' already exists for user {user_id}")
            return exists

        except Exception as e:
            print(f"❌ Error checking PDF existence: {e}")
            return False

    async def get_pdf_info(self, user_id: str, pdf_name: str) -> Optional[Dict]:
        """
        Get PDF metadata from database

        Args:
            user_id: User UUID
            pdf_name: PDF name to retrieve

        Returns:
            Optional[Dict]: PDF info including chunks_count, upload_status, etc.
                          Returns None if not found
        """
        try:
            response = (
                self.client.table("user_pdfs")
                .select("pdf_name, chunks_count, upload_status, uploaded_at, processed_at, original_filename, file_size_bytes")
                .eq("user_id", user_id)
                .eq("pdf_name", pdf_name)
                .execute()
            )

            if response.data and len(response.data) > 0:
                print(f"✅ Retrieved PDF info for '{pdf_name}' (user: {user_id})")
                return response.data[0]

            print(f"⚠️ No PDF info found for '{pdf_name}' (user: {user_id})")
            return None

        except Exception as e:
            print(f"❌ Error getting PDF info: {e}")
            return None

    async def get_user_pdfs(
        self,
        user_id: str,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict]:
        """
        Get all PDFs uploaded by a user

        Args:
            user_id: User UUID
            limit: Maximum number of records to return
            offset: Number of records to skip

        Returns:
            List[Dict]: List of PDF records
        """
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
            print(f"❌ Error fetching user PDFs: {e}")
            return []

    async def delete_user_pdf(self, user_id: str, pdf_name: str) -> bool:
        """
        Delete a PDF record from database

        Args:
            user_id: User UUID
            pdf_name: PDF name to delete

        Returns:
            bool: True if deleted successfully
        """
        try:
            response = (
                self.client.table("user_pdfs")
                .delete()
                .eq("user_id", user_id)
                .eq("pdf_name", pdf_name)
                .execute()
            )

            print(f"✅ Deleted PDF: {pdf_name} for user {user_id}")
            return True

        except Exception as e:
            print(f"❌ Error deleting PDF: {e}")
            return False

    # ==================== CHAT CONVERSATION OPERATIONS ====================

    async def create_conversation(
        self,
        user_id: str,
        title: Optional[str] = None
    ) -> str:
        """
        Create a new conversation

        Args:
            user_id: User UUID
            title: Optional conversation title

        Returns:
            str: Conversation ID (UUID)
        """
        try:
            data = {
                "user_id": user_id,
                "title": title or "New Conversation",
                "created_at": datetime.utcnow().isoformat(),
                "updated_at": datetime.utcnow().isoformat()
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
        images: Optional[List[Dict]] = None
    ) -> Dict:
        """
        Add a message to a conversation

        Args:
            conversation_id: Conversation UUID
            user_id: User UUID
            role: Message role ('user' or 'assistant')
            content: Message content
            query: Original query (for assistant messages)
            pdf_name: PDF queried (optional)
            sources: Source chunks used (optional)
            images: Images included (optional)

        Returns:
            Dict: Inserted message record
        """
        try:
            data = {
                "conversation_id": conversation_id,
                "user_id": user_id,
                "role": role,
                "content": content,
                "created_at": datetime.utcnow().isoformat()
            }

            if query:
                data["query"] = query
            if pdf_name:
                data["pdf_name"] = pdf_name
            if sources:
                data["sources"] = sources
            if images:
                data["images"] = images

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
        offset: int = 0
    ) -> List[Dict]:
        """
        Get user's conversations

        Args:
            user_id: User UUID
            limit: Maximum number of conversations to return
            offset: Number of conversations to skip

        Returns:
            List[Dict]: List of conversation records
        """
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
        limit: int = 100
    ) -> List[Dict]:
        """
        Get messages in a conversation

        Args:
            conversation_id: Conversation UUID
            user_id: User UUID (for authorization)
            limit: Maximum number of messages to return

        Returns:
            List[Dict]: List of message records
        """
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
        """
        Delete a conversation and all its messages

        Args:
            conversation_id: Conversation UUID
            user_id: User UUID (for authorization)

        Returns:
            bool: True if deleted successfully
        """
        try:
            # Delete conversation (messages will be cascade deleted)
            response = (
                self.client.table("chat_conversations")
                .delete()
                .eq("id", conversation_id)
                .eq("user_id", user_id)
                .execute()
            )

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
        filename: str
    ) -> Dict:
        """
        Create a processing job record

        Args:
            job_id: Job UUID
            user_id: User UUID
            pdf_name: PDF name
            filename: Original filename

        Returns:
            Dict: Inserted job record
        """
        try:
            data = {
                "job_id": job_id,
                "user_id": user_id,
                "pdf_name": pdf_name,
                "filename": filename,
                "status": "pending",
                "progress": 0.0,
                "start_time": datetime.utcnow().isoformat()
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
        error: Optional[str] = None
    ) -> Dict:
        """
        Update a processing job

        Args:
            job_id: Job UUID
            status: Job status (optional)
            stage: Current processing stage (optional)
            progress: Progress percentage 0.0-1.0 (optional)
            result: Result data (optional)
            error: Error message (optional)

        Returns:
            Dict: Updated job record
        """
        try:
            update_data = {}

            if status:
                update_data["status"] = status
            if stage:
                update_data["stage"] = stage
            if progress is not None:
                update_data["progress"] = progress
            if result:
                update_data["result"] = result
            if error:
                update_data["error"] = error

            # If status is completed/failed/cached, set end_time
            if status in ["completed", "failed", "cached"]:
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
        """
        Get a processing job by ID

        Args:
            job_id: Job UUID
            user_id: User UUID (for authorization)

        Returns:
            Optional[Dict]: Job record or None if not found
        """
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
        offset: int = 0
    ) -> List[Dict]:
        """
        Get user's processing jobs

        Args:
            user_id: User UUID
            limit: Maximum number of jobs to return
            offset: Number of jobs to skip

        Returns:
            List[Dict]: List of job records
        """
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
