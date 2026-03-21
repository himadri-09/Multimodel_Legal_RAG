-- ============================================
-- Web Crawling Feature – No new tables needed!
-- ============================================
-- The crawl feature reuses your EXISTING tables:
--
--   processing_jobs   → tracks crawl job progress (job_id, status, stage, progress)
--   user_pdfs         → stores crawled sites (original_filename = URL, pdf_name = site_slug)
--
-- The only change needed is adding two informational columns to user_pdfs
-- so the frontend can distinguish PDFs from crawled sites.
-- ============================================

-- ── 1. Add source_type column to user_pdfs ───────────────────────────────
--    'pdf'  = uploaded document  (default, keeps backwards compatibility)
--    'web'  = crawled website
ALTER TABLE public.user_pdfs
ADD COLUMN IF NOT EXISTS source_type TEXT NOT NULL DEFAULT 'pdf';

-- ── 2. Add source_url column (the original crawl URL, same as original_filename for web) ─
ALTER TABLE public.user_pdfs
ADD COLUMN IF NOT EXISTS source_url TEXT;

-- ── 3. Add pages_crawled count for web sources ───────────────────────────
ALTER TABLE public.user_pdfs
ADD COLUMN IF NOT EXISTS pages_crawled INTEGER;

-- ── 4. Update existing rows to explicitly mark them as 'pdf' ─────────────
UPDATE public.user_pdfs
SET source_type = 'pdf'
WHERE source_type IS NULL OR source_type = '';

-- ── 5. Index for fast filtering by source_type ───────────────────────────
CREATE INDEX IF NOT EXISTS idx_user_pdfs_source_type
ON public.user_pdfs(source_type);

-- ── 6. Index for fast filtering by user + source_type ────────────────────
CREATE INDEX IF NOT EXISTS idx_user_pdfs_user_source
ON public.user_pdfs(user_id, source_type);


-- ============================================
-- VERIFICATION
-- ============================================
-- Run this to confirm columns were added:
--
-- SELECT column_name, data_type, column_default
-- FROM information_schema.columns
-- WHERE table_name = 'user_pdfs'
-- ORDER BY ordinal_position;
--
-- Expected new columns: source_type, source_url, pages_crawled
-- ============================================


-- ============================================
-- OPTIONAL: Useful view for debugging
-- ============================================
CREATE OR REPLACE VIEW public.user_content AS
SELECT
    id,
    user_id,
    pdf_name,
    original_filename,
    source_type,
    source_url,
    pages_crawled,
    chunks_count,
    upload_status,
    uploaded_at,
    processed_at
FROM public.user_pdfs
ORDER BY uploaded_at DESC;

-- Grant access (adjust role name if needed)
-- GRANT SELECT ON public.user_content TO authenticated;