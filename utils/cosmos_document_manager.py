# utils/cosmos_document_manager.py

import asyncio
import time
from azure.cosmos.aio import CosmosClient
from azure.cosmos import exceptions
from typing import Dict, List, Any, Optional
from datetime import datetime
import os
from config import COSMOS_ENDPOINT, COSMOS_KEY, COSMOS_DATABASE_NAME, COSMOS_CONTAINER_NAME

class DocumentStatus:
    PENDING = "pending"
    PROCESSING = "processing" 
    ANALYZED = "analyzed"
    FAILED = "failed"

class CosmosDocumentManager:
    def __init__(self):
        self.cosmos_client = CosmosClient(COSMOS_ENDPOINT, COSMOS_KEY)
        self.database_name = COSMOS_DATABASE_NAME
        self.container_name = COSMOS_CONTAINER_NAME
        self.database = None
        self.container = None
    
    async def __aenter__(self):
        """Initialize Cosmos DB connection"""
        # Get database (create if doesn't exist)
        try:
            self.database = await self.cosmos_client.create_database_if_not_exists(
                id=self.database_name
            )
        except exceptions.CosmosResourceExistsError:
            self.database = self.cosmos_client.get_database_client(self.database_name)
        
        # Get container (create if doesn't exist)
        try:
            self.container = await self.database.create_container_if_not_exists(
                id=self.container_name,
                partition_key="/pdf_name",  # Partition by PDF name for efficiency
                offer_throughput=400  # Minimum RU/s
            )
        except exceptions.CosmosResourceExistsError:
            self.container = self.database.get_container_client(self.container_name)
        
        print(f"✅ Connected to Cosmos DB: {self.database_name}/{self.container_name}")
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Close Cosmos DB connection"""
        await self.cosmos_client.close()
    
    def _create_document_record(
        self, 
        pdf_name: str, 
        file_name: str, 
        file_size_mb: float,
        blob_url: str,
        status: str = DocumentStatus.PENDING
    ) -> Dict[str, Any]:
        """Create a document record for Cosmos DB"""
        return {
            "id": pdf_name,  # Use pdf_name as document ID
            "pdf_name": pdf_name,  # Also partition key
            "file_name": file_name,
            "file_size_mb": file_size_mb,
            "blob_url": blob_url,
            "status": status,
            "chunk_count": 0,
            "processing_stage": None,
            "error_message": None,
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
            "processing_start_time": None,
            "processing_end_time": None,
            "total_processing_time": None
        }
    
    async def create_document_record(
        self, 
        pdf_name: str, 
        file_name: str, 
        file_size_mb: float,
        blob_url: str
    ) -> Dict[str, Any]:
        """Create new document record in Cosmos DB"""
        try:
            document = self._create_document_record(
                pdf_name, file_name, file_size_mb, blob_url
            )
            
            result = await self.container.create_item(document)
            print(f"📝 Created document record: {pdf_name}")
            return result
            
        except exceptions.CosmosResourceExistsError:
            print(f"⚠️  Document record already exists: {pdf_name}")
            return await self.get_document_by_pdf_name(pdf_name)
        except Exception as e:
            print(f"❌ Error creating document record: {e}")
            raise
    
    async def update_document_status(
        self, 
        pdf_name: str, 
        status: str, 
        processing_stage: Optional[str] = None,
        chunk_count: Optional[int] = None,
        error_message: Optional[str] = None
    ) -> Dict[str, Any]:
        """Update document processing status"""
        try:
            # Get existing document
            document = await self.get_document_by_pdf_name(pdf_name)
            if not document:
                raise ValueError(f"Document not found: {pdf_name}")
            
            # Update fields
            document["status"] = status
            document["updated_at"] = datetime.utcnow().isoformat()
            
            if processing_stage is not None:
                document["processing_stage"] = processing_stage
            
            if chunk_count is not None:
                document["chunk_count"] = chunk_count
                
            if error_message is not None:
                document["error_message"] = error_message
            
            # Set processing timestamps
            if status == DocumentStatus.PROCESSING and not document.get("processing_start_time"):
                document["processing_start_time"] = datetime.utcnow().isoformat()
            elif status in [DocumentStatus.ANALYZED, DocumentStatus.FAILED]:
                if document.get("processing_start_time") and not document.get("processing_end_time"):
                    start_time = datetime.fromisoformat(document["processing_start_time"])
                    end_time = datetime.utcnow()
                    document["processing_end_time"] = end_time.isoformat()
                    document["total_processing_time"] = (end_time - start_time).total_seconds()
            
            # Update in Cosmos DB
            result = await self.container.replace_item(
                item=document["id"], 
                body=document
            )
            
            print(f"📝 Updated document status: {pdf_name} -> {status}")
            return result
            
        except Exception as e:
            print(f"❌ Error updating document status: {e}")
            raise
    
    async def get_document_by_pdf_name(self, pdf_name: str) -> Optional[Dict[str, Any]]:
        """Get document by PDF name"""
        try:
            item = await self.container.read_item(
                item=pdf_name,
                partition_key=pdf_name
            )
            return item
        except exceptions.CosmosResourceNotFoundError:
            return None
        except Exception as e:
            print(f"❌ Error getting document {pdf_name}: {e}")
            return None
    
    async def get_all_documents(self) -> List[Dict[str, Any]]:
        """Get all documents (fast - single query)"""
        try:
            query = "SELECT * FROM c ORDER BY c.created_at DESC"
            
            items = []
            async for item in self.container.query_items(
                query=query,
            ):
                items.append(item)
            
            print(f"📋 Retrieved {len(items)} documents from Cosmos DB")
            return items
            
        except Exception as e:
            print(f"❌ Error getting all documents: {e}")
            return []
    
    async def get_processed_documents(self) -> List[Dict[str, str]]:
        """Get only processed documents for chat dropdown"""
        try:
            query = f"SELECT c.pdf_name, c.file_name FROM c WHERE c.status = '{DocumentStatus.ANALYZED}'"
            
            items = []
            async for item in self.container.query_items(
                query=query,
            ):
                items.append({
                    "id": item["pdf_name"],
                    "name": item["file_name"]
                })
            
            print(f"📋 Retrieved {len(items)} processed documents")
            return items
            
        except Exception as e:
            print(f"❌ Error getting processed documents: {e}")
            return []
    
    async def delete_document(self, pdf_name: str) -> bool:
        """Delete document record"""
        try:
            await self.container.delete_item(
                item=pdf_name,
                partition_key=pdf_name
            )
            print(f"🗑️  Deleted document record: {pdf_name}")
            return True
            
        except exceptions.CosmosResourceNotFoundError:
            print(f"⚠️  Document record not found for deletion: {pdf_name}")
            return False
        except Exception as e:
            print(f"❌ Error deleting document: {e}")
            return False
    
    async def check_document_exists(self, pdf_name: str) -> bool:
        """Fast check if document exists"""
        document = await self.get_document_by_pdf_name(pdf_name)
        return document is not None
    
    async def get_document_status(self, pdf_name: str) -> Optional[str]:
        """Get just the status of a document"""
        document = await self.get_document_by_pdf_name(pdf_name)
        return document["status"] if document else None
    
    async def sync_with_blob_storage(self, blob_documents: List[Dict[str, Any]]):
        """Sync Cosmos DB with blob storage (run periodically)"""
        print("🔄 Syncing Cosmos DB with Blob Storage...")
        
        # Get all documents from Cosmos
        cosmos_docs = {doc["pdf_name"]: doc for doc in await self.get_all_documents()}
        
        # Check for new documents in blob storage
        for blob_doc in blob_documents:
            pdf_name = blob_doc["pdf_name"]
            
            if pdf_name not in cosmos_docs:
                # New document found in blob storage
                print(f"🆕 Found new document in blob storage: {pdf_name}")
                await self.create_document_record(
                    pdf_name=pdf_name,
                    file_name=blob_doc["name"],
                    file_size_mb=float(blob_doc["size"].replace(" MB", "")),
                    blob_url=blob_doc.get("blob_url", "")
                )
        
        # Check for deleted documents (exist in Cosmos but not in blob)
        blob_pdf_names = {doc["pdf_name"] for doc in blob_documents}
        for pdf_name in cosmos_docs.keys():
            if pdf_name not in blob_pdf_names:
                print(f"🗑️  Document deleted from blob storage: {pdf_name}")
                await self.delete_document(pdf_name)
    
    async def get_processing_statistics(self) -> Dict[str, Any]:
        """Get processing statistics"""
        try:
            # Count by status
            query = """
                SELECT c.status, COUNT(1) as count 
                FROM c 
                GROUP BY c.status
            """
            
            stats = {
                "total_documents": 0,
                "by_status": {},
                "average_processing_time": 0
            }
            
            async for item in self.container.query_items(
                query=query,
                enable_cross_partition_query=True
            ):
                stats["by_status"][item["status"]] = item["count"]
                stats["total_documents"] += item["count"]
            
            # Get average processing time for completed documents
            time_query = """
                SELECT AVG(c.total_processing_time) as avg_time 
                FROM c 
                WHERE c.total_processing_time != null
            """
            
            async for item in self.container.query_items(
                query=time_query,
                enable_cross_partition_query=True
            ):
                stats["average_processing_time"] = item.get("avg_time", 0)
            
            return stats
            
        except Exception as e:
            print(f"❌ Error getting statistics: {e}")
            return {"total_documents": 0, "by_status": {}, "average_processing_time": 0}