-- ============================================
-- Supabase Database Schema for Legal RAG
-- ============================================
-- Run this SQL in Supabase SQL Editor
-- Dashboard: https://vydeswheyqgbpgcayuqu.supabase.co

-- ============================================
-- 1. USER_PDFS TABLE
-- Track PDFs uploaded by each user
-- ============================================
CREATE TABLE IF NOT EXISTS public.user_pdfs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
    pdf_name TEXT NOT NULL,
    original_filename TEXT NOT NULL,
    file_size_bytes BIGINT NOT NULL,
    chunks_count INTEGER DEFAULT 0,
    upload_status TEXT NOT NULL,
    uploaded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    processed_at TIMESTAMP WITH TIME ZONE,
    CONSTRAINT unique_user_pdf UNIQUE (user_id, pdf_name)
);

-- Indexes for user_pdfs
CREATE INDEX IF NOT EXISTS idx_user_pdfs_user_id ON public.user_pdfs(user_id);
CREATE INDEX IF NOT EXISTS idx_user_pdfs_uploaded_at ON public.user_pdfs(uploaded_at DESC);

-- Enable Row Level Security
ALTER TABLE public.user_pdfs ENABLE ROW LEVEL SECURITY;

-- RLS Policy: Users can only access their own PDFs
DROP POLICY IF EXISTS "Users can only access their own PDFs" ON public.user_pdfs;
CREATE POLICY "Users can only access their own PDFs"
    ON public.user_pdfs
    USING (auth.uid() = user_id);

-- ============================================
-- 2. CHAT_CONVERSATIONS TABLE
-- Track conversation sessions
-- ============================================
CREATE TABLE IF NOT EXISTS public.chat_conversations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
    title TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for chat_conversations
CREATE INDEX IF NOT EXISTS idx_conversations_user_id ON public.chat_conversations(user_id);

-- Enable Row Level Security
ALTER TABLE public.chat_conversations ENABLE ROW LEVEL SECURITY;

-- RLS Policy: Users can only access their own conversations
DROP POLICY IF EXISTS "Users can only access their own conversations" ON public.chat_conversations;
CREATE POLICY "Users can only access their own conversations"
    ON public.chat_conversations
    USING (auth.uid() = user_id);

-- ============================================
-- 3. CHAT_MESSAGES TABLE
-- Store individual messages in conversations
-- ============================================
CREATE TABLE IF NOT EXISTS public.chat_messages (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    conversation_id UUID NOT NULL REFERENCES public.chat_conversations(id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
    role TEXT NOT NULL CHECK (role IN ('user', 'assistant')),
    content TEXT NOT NULL,
    query TEXT,
    pdf_name TEXT,
    sources JSONB,
    images JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for chat_messages
CREATE INDEX IF NOT EXISTS idx_messages_conversation_id ON public.chat_messages(conversation_id);
CREATE INDEX IF NOT EXISTS idx_messages_created_at ON public.chat_messages(created_at DESC);

-- Enable Row Level Security
ALTER TABLE public.chat_messages ENABLE ROW LEVEL SECURITY;

-- RLS Policy: Users can only access their own messages
DROP POLICY IF EXISTS "Users can only access their own messages" ON public.chat_messages;
CREATE POLICY "Users can only access their own messages"
    ON public.chat_messages
    USING (auth.uid() = user_id);

-- ============================================
-- 4. PROCESSING_JOBS TABLE
-- Track PDF processing jobs (replaces in-memory dictionary)
-- ============================================
CREATE TABLE IF NOT EXISTS public.processing_jobs (
    job_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
    pdf_name TEXT NOT NULL,
    filename TEXT NOT NULL,
    status TEXT NOT NULL,
    stage TEXT,
    progress DECIMAL(3, 2) DEFAULT 0.0,
    start_time TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    end_time TIMESTAMP WITH TIME ZONE,
    result JSONB,
    error TEXT
);

-- Indexes for processing_jobs
CREATE INDEX IF NOT EXISTS idx_jobs_user_id ON public.processing_jobs(user_id);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON public.processing_jobs(status);

-- Enable Row Level Security
ALTER TABLE public.processing_jobs ENABLE ROW LEVEL SECURITY;

-- RLS Policy: Users can only access their own jobs
DROP POLICY IF EXISTS "Users can only access their own jobs" ON public.processing_jobs;
CREATE POLICY "Users can only access their own jobs"
    ON public.processing_jobs
    USING (auth.uid() = user_id);

-- ============================================
-- VERIFICATION QUERIES
-- Run these after schema creation to verify
-- ============================================

-- Check all tables exist
-- SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name IN ('user_pdfs', 'chat_conversations', 'chat_messages', 'processing_jobs');

-- Check RLS is enabled
-- SELECT tablename, rowsecurity FROM pg_tables WHERE schemaname = 'public' AND tablename IN ('user_pdfs', 'chat_conversations', 'chat_messages', 'processing_jobs');

-- ============================================
-- SUCCESS!
-- ============================================
-- If you see "Success. No rows returned" after running this,
-- the schema has been created successfully!
--
-- Verify by going to:
-- Database → Tables (you should see 4 tables)
-- ============================================
