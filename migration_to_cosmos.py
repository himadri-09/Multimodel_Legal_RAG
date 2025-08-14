# migration_to_cosmos.py - One-time migration script

import asyncio
import os
from pathlib import Path
from azure.storage.blob import BlobServiceClient
from utils.cosmos_document_manager import CosmosDocumentManager, DocumentStatus
from utils.vector_store import PineconeVectorStore
from config import *

async def migrate_existing_data():
    """
    One-time migration script to populate Cosmos DB with existing data
    Run this after setting up Cosmos DB to sync existing blob storage + vector data
    """
    print("🚀 Starting migration to Cosmos DB...")
    
    async with CosmosDocumentManager() as cosmos_manager:
        # Get all blobs from Azure Storage
        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        pdf_container_name = os.getenv("PDF_BLOB_CONTAINER", "rag-pdf-uploads")
        
        container_client = blob_service_client.get_container_client(pdf_container_name)
        
        migrated_count = 0
        skipped_count = 0
        
        print("📂 Scanning blob storage...")
        
        async with PineconeVectorStore() as vector_store:
            for blob in container_client.list_blobs():
                try:
                    # Get blob properties
                    blob_client = blob_service_client.get_blob_client(
                        container=pdf_container_name,
                        blob=blob.name
                    )
                    properties = blob_client.get_blob_properties()
                    
                    # Extract metadata
                    pdf_name = Path(blob.name).stem
                    file_name = blob.name
                    file_size_mb = round(properties.size / (1024 * 1024), 2)
                    blob_url = f"https://{blob_service_client.account_name}.blob.core.windows.net/{pdf_container_name}/{blob.name}"
                    
                    # Check if already exists in Cosmos
                    existing_doc = await cosmos_manager.get_document_by_pdf_name(pdf_name)
                    if existing_doc:
                        print(f"⏭️  Skipping {pdf_name} (already in Cosmos DB)")
                        skipped_count += 1
                        continue
                    
                    # Check if processed in vector database
                    vector_exists = await vector_store.check_pdf_exists(pdf_name)
                    chunk_count = await vector_store.get_pdf_chunk_count(pdf_name) if vector_exists else 0
                    
                    # Determine status based on vector DB presence
                    status = DocumentStatus.ANALYZED if vector_exists else DocumentStatus.FAILED
                    
                    # Create document record in Cosmos DB
                    await cosmos_manager.create_document_record(
                        pdf_name=pdf_name,
                        file_name=file_name,
                        file_size_mb=file_size_mb,
                        blob_url=blob_url
                    )
                    
                    # Update status and chunk count
                    await cosmos_manager.update_document_status(
                        pdf_name=pdf_name,
                        status=status,
                        chunk_count=chunk_count if vector_exists else 0,
                        error_message="Migrated - no processing data" if not vector_exists else None
                    )
                    
                    print(f"✅ Migrated {pdf_name} -> {status} ({chunk_count} chunks)")
                    migrated_count += 1
                    
                except Exception as e:
                    print(f"❌ Error migrating {blob.name}: {e}")
                    continue
    
    print(f"\n🎉 Migration completed!")
    print(f"   ✅ Migrated: {migrated_count} documents")
    print(f"   ⏭️  Skipped: {skipped_count} documents")
    print(f"   📊 Total processed: {migrated_count + skipped_count}")
    
    # Show statistics
    async with CosmosDocumentManager() as cosmos_manager:
        stats = await cosmos_manager.get_processing_statistics()
        print(f"\n📈 Current Cosmos DB Statistics:")
        print(f"   Total documents: {stats['total_documents']}")
        for status, count in stats['by_status'].items():
            print(f"   {status.title()}: {count}")

if __name__ == "__main__":
    print("🔄 Running Cosmos DB migration...")
    print("⚠️  Make sure your .env file has COSMOS_ENDPOINT and COSMOS_KEY set!")
    
    # Confirm before running
    confirm = input("\nProceed with migration? (y/N): ")
    if confirm.lower() != 'y':
        print("❌ Migration cancelled")
        exit()
    
    asyncio.run(migrate_existing_data())