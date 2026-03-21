# utils/web_image_processor.py
import asyncio
import io
import time
from typing import List, Dict, Any
from urllib.parse import urlparse

import aiohttp
from PIL import Image
from azure.storage.blob import BlobServiceClient
import os

from utils.image_captioner import ImageCaptioner

MIN_IMAGE_BYTES = 10 * 1024  # 10 KB


class WebImageProcessor:

    def __init__(self):
        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        if not connection_string:
            raise ValueError("AZURE_STORAGE_CONNECTION_STRING is not set.")
        self.blob_service  = BlobServiceClient.from_connection_string(connection_string)
        self.container_name = "rag-pdf-images"

    async def process_page_images(
        self,
        image_urls: List[str],
        source_url: str,
        page_slug: str,
        page_number: int = 1,
    ) -> List[Dict[str, Any]]:

        if not image_urls:
            return []

        print(f"\n   🖼️  IMAGE PIPELINE  page={page_number}  found={len(image_urls)}  {source_url}")

        t0 = time.time()

        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
            tasks = [
                self._download_and_upload(session, url, page_slug, i)
                for i, url in enumerate(image_urls)
            ]
            raw_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Tally download results
        uploaded, skipped_small, failed = [], 0, 0
        for i, res in enumerate(raw_results):
            if isinstance(res, Exception):
                print(f"      ❌ img[{i:>3}] exception: {res}")
                failed += 1
            elif res is None:
                skipped_small += 1
            else:
                res["page_number"] = page_number
                res["pdf_name"]    = page_slug
                res["metadata"]["source_type"] = "web"
                res["metadata"]["source_url"]  = source_url
                sz = res["metadata"].get("file_size_kb", 0)
                print(
                    f"      ✅ img[{len(uploaded):>3}]  "
                    f"{sz:>6.1f} KB  "
                    f"{res['metadata'].get('width','?')}x{res['metadata'].get('height','?')}  "
                    f"→ blob"
                )
                uploaded.append(res)

        print(
            f"      📊 upload done: {len(uploaded)} uploaded, "
            f"{skipped_small} too-small, {failed} failed  "
            f"({time.time()-t0:.1f}s)"
        )

        if not uploaded:
            return []

        # ── Caption ──────────────────────────────────────────────────
        print(f"      🤖 captioning {len(uploaded)} images…")
        t1 = time.time()

        async with ImageCaptioner() as captioner:
            captioned = await captioner.caption_images_async(uploaded)

        caption_ok = sum(1 for c in captioned if c.get("content") and
                         not c["content"].startswith("Image from page"))

        print(
            f"      ✅ captioned: {caption_ok}/{len(captioned)} successful  "
            f"({time.time()-t1:.1f}s)"
        )

        return captioned

    # ------------------------------------------------------------------

    async def _download_and_upload(
        self,
        session: aiohttp.ClientSession,
        image_url: str,
        page_slug: str,
        img_index: int,
    ):
        try:
            async with session.get(image_url) as resp:
                if resp.status != 200:
                    return None
                raw_bytes = await resp.read()

            if len(raw_bytes) < MIN_IMAGE_BYTES:
                return None   # too small – icon/tracker

            pil_img = Image.open(io.BytesIO(raw_bytes))
            if pil_img.mode not in ("RGB", "L"):
                pil_img = pil_img.convert("RGB")

            webp_buf = io.BytesIO()
            pil_img.save(webp_buf, format="WEBP", quality=85, method=6)
            webp_data = webp_buf.getvalue()

            if len(webp_data) < MIN_IMAGE_BYTES:
                return None

            blob_name   = f"{page_slug}/web_img_{img_index:04d}.webp"
            blob_client = self.blob_service.get_blob_client(
                container=self.container_name, blob=blob_name
            )
            blob_client.upload_blob(webp_data, overwrite=True)

            account_name = self.blob_service.account_name
            blob_url = (
                f"https://{account_name}.blob.core.windows.net"
                f"/{self.container_name}/{blob_name}"
            )

            return {
                "content":    "",
                "type":       "image",
                "image_path": blob_url,
                "metadata": {
                    "original_url": image_url,
                    "width":        pil_img.width,
                    "height":       pil_img.height,
                    "format":       "webp",
                    "file_size_kb": len(webp_data) / 1024,
                },
            }

        except Exception as e:
            print(f"      ⚠️  img[{img_index}] {image_url[:60]}… → {e}")
            return None