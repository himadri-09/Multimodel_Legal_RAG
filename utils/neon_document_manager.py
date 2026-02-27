import asyncio
import asyncpg
from typing import Dict, List, Any, Optional
from datetime import datetime
from config import DATABASE_URL, DB_MIN_CONNECTIONS, DB_MAX_CONNECTIONS

class DocumentStatus:
    PENDING = "pending"
    PROCESSING = "processing"
    ANALYZED = "analyzed"
    FAILED = "failed"

class NeonDocumentManager:
    _pool = None
    _pool_lock = asyncio.Lock()
    
    def __init__(self):
        self.pool = None
        self.connection = None  # For single connection mode
    
    @classmethod
    async def get_pool(cls):
        """Get or create connection pool (singleton pattern for efficiency)"""
        if cls._pool is None:
            async with cls._pool_lock:
                if cls._pool is None:  # Double-check locking
                    print("Creating new Neon PostgreSQL connection pool...")
                    try:
                        cls._pool = await asyncpg.create_pool(
                            DATABASE_URL,
                            min_size=DB_MIN_CONNECTIONS,
                            max_size=DB_MAX_CONNECTIONS,
                            command_timeout=30,  # Reduced from 60
                            max_inactive_connection_lifetime=300,  # 5 minutes
                            server_settings={
                                'jit': 'off',
                                'statement_timeout': '30000',  # 30 seconds
                            }
                        )
                        
                        # Initialize table on first connection
                        async with cls._pool.acquire() as conn:
                            await cls._create_tables(conn)
                        
                        print(f"Neon connection pool created ({DB_MIN_CONNECTIONS}-{DB_MAX_CONNECTIONS} connections)")
                    except Exception as e:
                        print(f"Error creating connection pool: {e}")
                        # Fallback to single connection
                        cls._pool = await asyncpg.connect(DATABASE_URL)
                        await cls._create_tables(cls._pool)
                        print("Using single connection as fallback")
        return cls._pool
    
    @staticmethod
    async def _create_tables(conn):
        """Create tables if they don't exist"""
        try:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS document_metadata (
                    pdf_name VARCHAR(255) PRIMARY KEY,
                    file_name VARCHAR(255) NOT NULL,
                    file_size_mb DECIMAL(10,2) NOT NULL,
                    blob_url TEXT NOT NULL,
                    status VARCHAR(50) NOT NULL,
                    chunk_count INTEGER DEFAULT 0,
                    processing_stage VARCHAR(100),
                    error_message TEXT,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    processing_start_time TIMESTAMP WITH TIME ZONE,
                    processing_end_time TIMESTAMP WITH TIME ZONE,
                    total_processing_time DECIMAL(10,2)
                )
            ''')
            
            # Create indexes for performance
            await conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_document_status 
                ON document_metadata(status)
            ''')
            await conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_document_created_at 
                ON document_metadata(created_at)
            ''')
        except Exception as e:
            print(f"Error creating tables: {e}")
    
    async def __aenter__(self):
        """Async context manager entry - use dedicated connection"""
        try:
            # Use a dedicated connection for this context to avoid pool conflicts
            self.connection = await asyncpg.connect(DATABASE_URL)
            return self
        except Exception as e:
            print(f"Error creating dedicated connection: {e}")
            # Fallback to pool if available
            self.pool = await self.get_pool()
            return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit - properly close connection"""
        if self.connection:
            try:
                await self.connection.close()
            except:
                pass
            self.connection = None
    
    async def _execute_query(self, query: str, *params):
        """Execute query with proper connection handling"""
        if self.connection:
            return await self.connection.fetchrow(query, *params)
        elif self.pool and hasattr(self.pool, 'acquire'):
            async with self.pool.acquire() as conn:
                return await conn.fetchrow(query, *params)
        else:
            # Single connection fallback
            return await self.pool.fetchrow(query, *params)
    
    async def _fetch_query(self, query: str, *params):
        """Fetch multiple rows with proper connection handling"""
        if self.connection:
            return await self.connection.fetch(query, *params)
        elif self.pool and hasattr(self.pool, 'acquire'):
            async with self.pool.acquire() as conn:
                return await conn.fetch(query, *params)
        else:
            # Single connection fallback
            return await self.pool.fetch(query, *params)
    
    async def _execute_only(self, query: str, *params):
        """Execute query without return with proper connection handling"""
        if self.connection:
            return await self.connection.execute(query, *params)
        elif self.pool and hasattr(self.pool, 'acquire'):
            async with self.pool.acquire() as conn:
                return await conn.execute(query, *params)
        else:
            # Single connection fallback
            return await self.pool.execute(query, *params)
    
    async def create_document_record(
        self,
        pdf_name: str,
        file_name: str,
        file_size_mb: float,
        blob_url: str
    ) -> Dict[str, Any]:
        """Create new document record in Neon"""
        try:
            row = await self._execute_query('''
                INSERT INTO document_metadata 
                (pdf_name, file_name, file_size_mb, blob_url, status, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (pdf_name) DO UPDATE SET
                    updated_at = $7
                RETURNING *
            ''', pdf_name, file_name, file_size_mb, blob_url, 
                 DocumentStatus.PENDING, datetime.utcnow(), datetime.utcnow())
            
            result = dict(row) if row else {}
            print(f"Created document record: {pdf_name}")
            return result
                
        except Exception as e:
            print(f"Error creating document record: {e}")
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
            # Build dynamic update query
            set_clauses = ["status = $2", "updated_at = $3"]
            params = [pdf_name, status, datetime.utcnow()]
            param_count = 3
            
            if processing_stage is not None:
                param_count += 1
                set_clauses.append(f"processing_stage = ${param_count}")
                params.append(processing_stage)
            
            if chunk_count is not None:
                param_count += 1
                set_clauses.append(f"chunk_count = ${param_count}")
                params.append(chunk_count)
            
            if error_message is not None:
                param_count += 1
                set_clauses.append(f"error_message = ${param_count}")
                params.append(error_message)
            
            # Handle processing timestamps
            if status == DocumentStatus.PROCESSING:
                param_count += 1
                set_clauses.append(f"processing_start_time = COALESCE(processing_start_time, ${param_count})")
                params.append(datetime.utcnow())
            elif status in [DocumentStatus.ANALYZED, DocumentStatus.FAILED]:
                param_count += 1
                set_clauses.append(f"processing_end_time = ${param_count}")
                params.append(datetime.utcnow())
                set_clauses.append("total_processing_time = EXTRACT(EPOCH FROM (processing_end_time - processing_start_time))")
            
            query = f'''
                UPDATE document_metadata 
                SET {", ".join(set_clauses)}
                WHERE pdf_name = $1
                RETURNING *
            '''
            
            row = await self._execute_query(query, *params)
            if row:
                result = dict(row)
                print(f"Updated document status: {pdf_name} -> {status}")
                return result
            else:
                raise ValueError(f"Document not found: {pdf_name}")
                
        except Exception as e:
            print(f"Error updating document status: {e}")
            raise
    
    async def get_document_by_pdf_name(self, pdf_name: str) -> Optional[Dict[str, Any]]:
        """Get document by PDF name"""
        try:
            row = await self._execute_query(
                'SELECT * FROM document_metadata WHERE pdf_name = $1',
                pdf_name
            )
            return dict(row) if row else None
                
        except Exception as e:
            print(f"Error getting document {pdf_name}: {e}")
            return None
    
    async def get_all_documents(self) -> List[Dict[str, Any]]:
        """Get all documents"""
        try:
            rows = await self._fetch_query('''
                SELECT * FROM document_metadata 
                ORDER BY created_at DESC
            ''')
            
            documents = [dict(row) for row in rows]
            print(f"Retrieved {len(documents)} documents from Neon")
            return documents
                
        except Exception as e:
            print(f"Error getting all documents: {e}")
            return []
    
    async def get_processed_documents(self) -> List[Dict[str, str]]:
        """Get only processed documents for chat dropdown"""
        try:
            rows = await self._fetch_query('''
                SELECT pdf_name, file_name 
                FROM document_metadata 
                WHERE status = $1
                ORDER BY created_at DESC
            ''', DocumentStatus.ANALYZED)
            
            documents = [
                {"id": row["pdf_name"], "name": row["file_name"]} 
                for row in rows
            ]
            print(f"Retrieved {len(documents)} processed documents")
            return documents
                
        except Exception as e:
            print(f"Error getting processed documents: {e}")
            return []
    
    async def delete_document(self, pdf_name: str) -> bool:
        """Delete document record"""
        try:
            result = await self._execute_only(
                'DELETE FROM document_metadata WHERE pdf_name = $1',
                pdf_name
            )
            deleted = result.split()[-1] != '0' if result else False
            if deleted:
                print(f"Deleted document record: {pdf_name}")
            else:
                print(f"Document record not found for deletion: {pdf_name}")
            return deleted
                
        except Exception as e:
            print(f"Error deleting document: {e}")
            return False
    
    async def check_document_exists(self, pdf_name: str) -> bool:
        """Fast check if document exists"""
        try:
            if self.connection:
                exists = await self.connection.fetchval(
                    'SELECT EXISTS(SELECT 1 FROM document_metadata WHERE pdf_name = $1)',
                    pdf_name
                )
            elif self.pool and hasattr(self.pool, 'acquire'):
                async with self.pool.acquire() as conn:
                    exists = await conn.fetchval(
                        'SELECT EXISTS(SELECT 1 FROM document_metadata WHERE pdf_name = $1)',
                        pdf_name
                    )
            else:
                exists = await self.pool.fetchval(
                    'SELECT EXISTS(SELECT 1 FROM document_metadata WHERE pdf_name = $1)',
                    pdf_name
                )
            return bool(exists)
        except Exception as e:
            print(f"Error checking document existence: {e}")
            return False
    
    async def get_document_status(self, pdf_name: str) -> Optional[str]:
        """Get just the status of a document"""
        try:
            if self.connection:
                status = await self.connection.fetchval(
                    'SELECT status FROM document_metadata WHERE pdf_name = $1',
                    pdf_name
                )
            elif self.pool and hasattr(self.pool, 'acquire'):
                async with self.pool.acquire() as conn:
                    status = await conn.fetchval(
                        'SELECT status FROM document_metadata WHERE pdf_name = $1',
                        pdf_name
                    )
            else:
                status = await self.pool.fetchval(
                    'SELECT status FROM document_metadata WHERE pdf_name = $1',
                    pdf_name
                )
            return status
        except Exception as e:
            print(f"Error getting document status: {e}")
            return None
    
    async def get_processing_statistics(self) -> Dict[str, Any]:
        """Get processing statistics"""
        try:
            # Get count by status
            rows = await self._fetch_query('''
                SELECT status, COUNT(*) as count
                FROM document_metadata
                GROUP BY status
            ''')
            
            stats = {
                "total_documents": 0,
                "by_status": {},
                "average_processing_time": 0
            }
            
            for row in rows:
                stats["by_status"][row["status"]] = row["count"]
                stats["total_documents"] += row["count"]
            
            # Get average processing time
            if self.connection:
                avg_time = await self.connection.fetchval('''
                    SELECT AVG(total_processing_time)
                    FROM document_metadata
                    WHERE total_processing_time IS NOT NULL
                ''')
            elif self.pool and hasattr(self.pool, 'acquire'):
                async with self.pool.acquire() as conn:
                    avg_time = await conn.fetchval('''
                        SELECT AVG(total_processing_time)
                        FROM document_metadata
                        WHERE total_processing_time IS NOT NULL
                    ''')
            else:
                avg_time = await self.pool.fetchval('''
                    SELECT AVG(total_processing_time)
                    FROM document_metadata
                    WHERE total_processing_time IS NOT NULL
                ''')
            
            stats["average_processing_time"] = float(avg_time) if avg_time else 0
            return stats
                
        except Exception as e:
            print(f"Error getting statistics: {e}")
            return {"total_documents": 0, "by_status": {}, "average_processing_time": 0}
    
    @classmethod
    async def close_pool(cls):
        """Close connection pool (call on app shutdown)"""
        if cls._pool:
            try:
                if hasattr(cls._pool, 'close'):
                    await cls._pool.close()
                else:
                    await cls._pool.close()
                cls._pool = None
                print("Closed Neon connection pool")
            except Exception as e:
                print(f"Error closing pool: {e}")