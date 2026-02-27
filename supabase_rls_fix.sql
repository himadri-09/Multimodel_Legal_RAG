-- ============================================
-- FIX: Update RLS Policies to Support INSERT/UPDATE/DELETE
-- ============================================
-- Run this in Supabase SQL Editor to fix the RLS policies
-- The current policies only have USING (for SELECT), but need WITH CHECK (for INSERT/UPDATE/DELETE)

-- ============================================
-- 1. Fix USER_PDFS RLS Policy
-- ============================================
DROP POLICY IF EXISTS "Users can only access their own PDFs" ON public.user_pdfs;

CREATE POLICY "Users can only access their own PDFs"
    ON public.user_pdfs
    FOR ALL
    USING (auth.uid() = user_id)
    WITH CHECK (auth.uid() = user_id);

-- ============================================
-- 2. Fix CHAT_CONVERSATIONS RLS Policy
-- ============================================
DROP POLICY IF EXISTS "Users can only access their own conversations" ON public.chat_conversations;

CREATE POLICY "Users can only access their own conversations"
    ON public.chat_conversations
    FOR ALL
    USING (auth.uid() = user_id)
    WITH CHECK (auth.uid() = user_id);

-- ============================================
-- 3. Fix CHAT_MESSAGES RLS Policy
-- ============================================
DROP POLICY IF EXISTS "Users can only access their own messages" ON public.chat_messages;

CREATE POLICY "Users can only access their own messages"
    ON public.chat_messages
    FOR ALL
    USING (auth.uid() = user_id)
    WITH CHECK (auth.uid() = user_id);

-- ============================================
-- 4. Fix PROCESSING_JOBS RLS Policy
-- ============================================
DROP POLICY IF EXISTS "Users can only access their own jobs" ON public.processing_jobs;

CREATE POLICY "Users can only access their own jobs"
    ON public.processing_jobs
    FOR ALL
    USING (auth.uid() = user_id)
    WITH CHECK (auth.uid() = user_id);

-- ============================================
-- VERIFICATION
-- ============================================
-- Verify policies are updated
SELECT schemaname, tablename, policyname, cmd, qual, with_check
FROM pg_policies
WHERE tablename IN ('user_pdfs', 'chat_conversations', 'chat_messages', 'processing_jobs');

-- ============================================
-- NOTE: Service Role Key Bypasses RLS
-- ============================================
-- If your backend uses the SERVICE ROLE KEY (not the anon key),
-- these policies are bypassed entirely for backend operations.
--
-- However, it's good practice to have proper RLS policies for:
-- 1. Debugging with anon key
-- 2. Direct database access from frontend (if needed)
-- 3. Supabase dashboard queries
