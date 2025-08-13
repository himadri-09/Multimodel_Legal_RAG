import asyncio
import base64
import aiohttp
from openai import AsyncAzureOpenAI
from typing import List, Dict, Any
from config import (
    AZURE_OPENAI_API_KEY, AZURE_OPENAI_ENDPOINT, 
    AZURE_API_VERSION, AZURE_VISION_DEPLOYMENT_NAME,
    MAX_CONCURRENT_IMAGE_CAPTIONS
)
from langsmith import traceable

class ImageCaptioner:
    def __init__(self):
        self.client = AsyncAzureOpenAI(
            api_key=AZURE_OPENAI_API_KEY,
            azure_endpoint=AZURE_OPENAI_ENDPOINT,
            api_version=AZURE_API_VERSION,
        )
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_IMAGE_CAPTIONS)
        self.http_session = None
    
    async def __aenter__(self):
        # Create HTTP session for downloading images
        self.http_session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Close HTTP session
        if self.http_session:
            await self.http_session.close()
        await self.client.close()
    
    def encode_image_from_local_path(self, image_path: str) -> str:
        """Encode local image file to base64 (legacy method)"""
        with open(image_path, "rb") as image_file:
            return base64.b64encode(image_file.read()).decode('utf-8')
    
    async def download_and_encode_image(self, image_url: str) -> str:
        """Download image from URL and encode to base64"""
        async with self.http_session.get(image_url) as response:
            if response.status == 200:
                image_bytes = await response.read()
                return base64.b64encode(image_bytes).decode('utf-8')
            else:
                raise Exception(f"Failed to download image: HTTP {response.status}")
    
    @traceable(name="caption_single_image")
    async def caption_single_image(self, image_chunk: Dict[str, Any]) -> Dict[str, Any]:
        """Caption a single image using Azure OpenAI Vision"""
        async with self.semaphore:
            try:
                image_path = image_chunk['image_path']
                
                # Determine if it's a URL or local path
                if image_path.startswith('http'):
                    # It's a URL - download and encode
                    base64_image = await self.download_and_encode_image(image_path)
                    print(f"📥 Downloaded and encoded image from URL: {image_path}")
                else:
                    # It's a local path - encode directly
                    base64_image = self.encode_image_from_local_path(image_path)
                    print(f"📁 Encoded local image: {image_path}")
                
                response = await self.client.chat.completions.create(
                    model=AZURE_VISION_DEPLOYMENT_NAME,
                    messages=[
                        {
                            "role": "user",
                            "content": [
                                {
                                    "type": "text",
                                    "text": (
                                        "Describe this image in detail. Focus on visible objects, structure, labels, text, and any spatial patterns. "
                                        "Be helpful and accurate — the output will be used as part of a document understanding pipeline."
                                    )
                                },
                                {
                                    "type": "image_url",
                                    "image_url": {
                                        "url": f"data:image/webp;base64,{base64_image}"
                                    }
                                }
                            ]
                        }
                    ],
                    max_tokens=300
                )
                
                caption = response.choices[0].message.content
                image_chunk['content'] = caption
                
                print(f"✅ Captioned image: {image_chunk['image_path']}")
                return image_chunk
                
            except Exception as e:
                print(f"❌ Error captioning image {image_chunk['image_path']}: {e}")
                image_chunk['content'] = f"Image from page {image_chunk['page_number']}"
                return image_chunk
    
    @traceable(name="caption_images_batch")
    async def caption_images_async(self, image_chunks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Caption multiple images asynchronously"""
        if not image_chunks:
            return []
        
        print(f"🖼️ Captioning {len(image_chunks)} images asynchronously...")
        
        tasks = [self.caption_single_image(chunk) for chunk in image_chunks]
        captioned_chunks = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Handle exceptions
        valid_chunks = []
        for i, result in enumerate(captioned_chunks):
            if isinstance(result, Exception):
                print(f"❌ Failed to caption image {i}: {result}")
                # Add original chunk with basic caption
                image_chunks[i]['content'] = f"Image from page {image_chunks[i]['page_number']}"
                valid_chunks.append(image_chunks[i])
            else:
                valid_chunks.append(result)
        
        print(f"✅ Successfully captioned {len(valid_chunks)} images")
        return valid_chunks