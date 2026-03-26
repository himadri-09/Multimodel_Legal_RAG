# routers/crawl.py
import asyncio
import re
import time
import traceback
import uuid
from urllib.parse import urlparse

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from utils.auth import get_current_user
from utils.database import DatabaseManager
from utils.web_crawler import WebCrawler
from utils.web_chunker import WebChunker
from utils.vector_store import PineconeVectorStore


router = APIRouter(prefix="/crawl", tags=["Web Crawling"])


# ------------------------------------------------------------------
# Pydantic models
# ------------------------------------------------------------------

class CrawlRequest(BaseModel):
    url: str
    max_pages: int = 100
    max_depth: int = 5


class CrawlResponse(BaseModel):
    job_id: str
    message: str
    status: str
    site_slug: str
    check_status_url: str


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

async def _update_job(db: DatabaseManager, job_id: str, **kwargs):
    try:
        await db.update_processing_job(job_id, **kwargs)
    except Exception as e:
        print(f"⚠️  Could not update job {job_id}: {e}")


def _url_to_slug(url: str) -> str:
    parsed = urlparse(url.rstrip("/"))
    raw  = f"{parsed.netloc}{parsed.path}"
    slug = re.sub(r"[^a-z0-9]+", "-", raw.lower()).strip("-")
    return slug or "web-site"


# ------------------------------------------------------------------
# Background pipeline
# ------------------------------------------------------------------

async def _process_crawl_job(
    job_id: str,
    start_url: str,
    site_slug: str,
    max_pages: int,
    max_depth: int,
    user_id: str,
):
    db = DatabaseManager()
    pipeline_start = time.time()

    try:
        print(f"\n{'#'*60}")
        print(f"🚀 PIPELINE START  job={job_id}")
        print(f"   URL       : {start_url}")
        print(f"   site_slug : {site_slug}")
        print(f"   user_id   : {user_id}")
        print(f"{'#'*60}")

        # ── Step 1: Crawl ─────────────────────────────────────────────
        print(f"\n[STEP 1/3] CRAWLING")
        await _update_job(db, job_id, status="processing", stage="Crawling website", progress=0.05)

        pages_done = 0

        async def progress_cb(done, found):
            nonlocal pages_done
            pages_done = done
            pct = min(0.05 + (done / max(max_pages, 1)) * 0.45, 0.50)
            await _update_job(db, job_id,
                              stage=f"Crawled {done}/{found} pages",
                              progress=round(pct, 2))

        crawler = WebCrawler(max_pages=max_pages, max_depth=max_depth)
        pages   = await crawler.crawl(start_url, progress_callback=progress_cb)

        if not pages:
            raise ValueError("No pages were successfully crawled from the given URL.")

        t_crawl = time.time() - pipeline_start
        print(f"\n[STEP 1/3] ✅ DONE  pages={len(pages)}  ({t_crawl:.1f}s elapsed)")

        # ── Step 2: Chunk text ────────────────────────────────────────
        print(f"\n[STEP 2/3] CHUNKING TEXT")
        await _update_job(db, job_id, stage=f"Chunking {len(pages)} pages", progress=0.55)

        chunker     = WebChunker()
        text_chunks = chunker.chunk_pages(pages, site_slug)

        t_chunk = time.time() - pipeline_start
        print(f"\n[STEP 2/3] ✅ DONE  text_chunks={len(text_chunks)}  ({t_chunk:.1f}s elapsed)")

        # ── Step 3: Store in Pinecone ─────────────────────────────────
        print(f"\n[STEP 3/3] STORING IN VECTOR DB")
        print(f"   Text chunks : {len(text_chunks)}")
        print(f"   site_slug   : {site_slug}")
        print(f"   user_id     : {user_id}")

        await _update_job(db, job_id,
                          stage=f"Storing {len(text_chunks)} chunks in vector DB",
                          progress=0.70)

        async with PineconeVectorStore() as vector_store:
            await vector_store.store_chunks(text_chunks, site_slug, user_id)

        t_store = time.time() - pipeline_start
        print(f"\n[STEP 3/3] ✅ DONE  ({t_store:.1f}s elapsed)")

        # ── Persist to Supabase user_pdfs ────────────────────────────
        await db.log_pdf_upload(
            user_id=user_id,
            pdf_name=site_slug,
            original_filename=start_url,
            file_size_bytes=0,
            upload_status="processing",
            source_type="web",
            source_url=start_url,
        )
        await db.update_pdf_status(
            user_id=user_id,
            pdf_name=site_slug,
            status="completed",
            chunks_count=len(text_chunks),
        )

        # ── Final summary ─────────────────────────────────────────────
        total_elapsed = time.time() - pipeline_start
        result = {
            "message":       f"Website '{start_url}' indexed successfully",
            "site_slug":     site_slug,
            "pages_crawled": len(pages),
            "text_chunks":   len(text_chunks),
            "image_chunks":  0,
            "total_chunks":  len(text_chunks),
            "elapsed_s":     round(total_elapsed, 1),
            "status":        "completed",
        }

        await _update_job(db, job_id,
                          status="completed",
                          stage="Completed",
                          progress=1.0,
                          result=result)

        print(f"\n{'#'*60}")
        print(f"🎉 PIPELINE COMPLETE  job={job_id}")
        print(f"   Pages crawled  : {len(pages)}")
        print(f"   Text chunks    : {len(text_chunks)}")
        print(f"   Total stored   : {len(text_chunks)}")
        print(f"   Total time     : {total_elapsed:.1f}s")
        print(f"{'#'*60}\n")

    except Exception as exc:
        elapsed = time.time() - pipeline_start
        print(f"\n{'#'*60}")
        print(f"❌ PIPELINE FAILED  job={job_id}  ({elapsed:.1f}s)")
        print(f"   Error: {exc}")
        traceback.print_exc()
        print(f"{'#'*60}\n")

        try:
            await db.update_pdf_status(user_id, site_slug, "failed", 0)
        except Exception:
            pass

        await _update_job(db, job_id,
                          status="failed",
                          stage="Failed",
                          progress=0.0,
                          error=str(exc))


# ------------------------------------------------------------------
# Endpoints
# ------------------------------------------------------------------

@router.post("", response_model=CrawlResponse, summary="Crawl a website and index its content")
async def submit_crawl(
    request: CrawlRequest,
    current_user: dict = Depends(get_current_user),
):
    """
    Submit a URL for recursive crawling and RAG indexing.

    - **url**: Root URL to crawl (e.g. `https://docs.example.com/`)
    - **max_pages**: Hard cap on pages to crawl (default 100)
    - **max_depth**: Maximum link depth from root (default 5)

    Always runs in background. Poll `GET /crawl/status/{job_id}`.
    Once complete the site appears in `GET /documents/processed` and
    can be queried via `POST /query` using `site_slug` as `pdf_name`.
    """
    user_id = current_user["id"]

    try:
        parsed = urlparse(request.url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise ValueError
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid URL. Must be http:// or https://")

    site_slug = _url_to_slug(request.url)
    db        = DatabaseManager()

    if await db.check_user_pdf_exists(user_id, site_slug):
        existing = await db.get_pdf_info(user_id, site_slug)
        if existing and existing.get("upload_status") == "completed":
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Site '{request.url}' is already indexed as '{site_slug}'. "
                    "Delete it first via DELETE /crawl/sites/{site_slug} to re-crawl."
                ),
            )

    job_id = str(uuid.uuid4())
    await db.create_processing_job(
        job_id=job_id,
        user_id=user_id,
        pdf_name=site_slug,
        filename=request.url,
    )

    print(f"📥 Crawl job submitted: {job_id}  url={request.url}  user={user_id}")

    asyncio.create_task(
        _process_crawl_job(
            job_id=job_id,
            start_url=request.url.rstrip("/"),
            site_slug=site_slug,
            max_pages=request.max_pages,
            max_depth=request.max_depth,
            user_id=user_id,
        )
    )

    return CrawlResponse(
        job_id=job_id,
        message=f"Crawl started for '{request.url}'",
        status="started",
        site_slug=site_slug,
        check_status_url=f"/crawl/status/{job_id}",
    )


@router.get("/status/{job_id}", summary="Poll crawl job status")
async def get_crawl_status(job_id: str, current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    db      = DatabaseManager()
    job     = await db.get_processing_job(job_id, user_id)
    if not job:
        raise HTTPException(status_code=404,
                            detail="Job not found or no permission to access it.")
    return job


@router.get("/sites", summary="List all indexed websites")
async def list_crawled_sites(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    db      = DatabaseManager()
    all_docs = await db.get_user_pdfs(user_id)
    sites = [d for d in all_docs
             if (d.get("original_filename") or "").startswith("http")]
    return {"sites": sites, "total_count": len(sites)}


@router.delete("/sites/{site_slug}", summary="Delete an indexed website")
async def delete_crawled_site(
    site_slug: str,
    current_user: dict = Depends(get_current_user),
):
    user_id  = current_user["id"]
    db       = DatabaseManager()
    pdf_info = await db.get_pdf_info(user_id, site_slug)
    if not pdf_info:
        raise HTTPException(status_code=404,
                            detail="Site not found or no permission to delete it.")
    try:
        async with PineconeVectorStore() as vs:
            await vs.delete_pdf_vectors(site_slug, user_id)
        await db.delete_user_pdf(user_id, site_slug)
        print(f"🗑️  Deleted site '{site_slug}' for user {user_id}")
        return {"message": f"Site '{site_slug}' deleted successfully.", "site_slug": site_slug}
    except Exception as exc:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to delete site: {exc}")