# eval/generate_questions.py
"""
Auto-generates evaluation questions from your crawled docs.

How it works:
  1. Loads BM25 index (already built during crawl) to get all chunks
  2. Samples chunks across different pages
  3. Uses LLM to generate realistic questions from each chunk
  4. Saves to eval_questions.json with metadata

Run:
    python eval/generate_questions.py --slug docs-codepup-ai --count 80
"""

import asyncio
import json
import random
import argparse
from pathlib import Path
from openai import AsyncAzureOpenAI

import sys
sys.path.append(str(Path(__file__).parent.parent))

from config import (
    AZURE_OPENAI_API_KEY, AZURE_OPENAI_ENDPOINT,
    AZURE_API_VERSION, AZURE_DEPLOYMENT_NAME,
)
from utils.bm25_store import BM25Store

OUTPUT_DIR = Path("eval")
OUTPUT_DIR.mkdir(exist_ok=True)

# Question type distribution — mirrors real user behaviour
QUESTION_TYPES = {
    "simple":       0.40,   # "what is X", "how do I Y"
    "multi_part":   0.20,   # "how do I set up A and then do B"
    "troubleshoot": 0.20,   # "why does X fail", "error when doing Y"
    "comparison":   0.10,   # "difference between X and Y"
    "out_of_scope": 0.10,   # questions the docs can't answer
}

GENERATION_PROMPTS = {
    "simple": """Based on this documentation chunk, write ONE simple factual question 
a user would genuinely ask. The question should be answerable from this chunk.

Chunk:
{chunk}

Write only the question, nothing else.""",

    "multi_part": """Based on this documentation chunk, write ONE question that asks 
about multiple things or requires sequential steps. Should be answerable from the docs.

Chunk:
{chunk}

Write only the question, nothing else.""",

    "troubleshoot": """Based on this documentation chunk, write ONE troubleshooting question
— something a user would ask when something isn't working or they got an error.

Chunk:
{chunk}

Write only the question, nothing else.""",

    "comparison": """Based on this documentation chunk, write ONE comparison question
— asking about differences between two options, features, or approaches.

Chunk:
{chunk}

Write only the question, nothing else.""",

    "out_of_scope": """Write ONE question that is completely unrelated to software 
documentation, developer tools, or web applications. Something like asking about 
cooking, sports, geography, or history.

Write only the question, nothing else.""",
}


async def generate_question(
    client: AsyncAzureOpenAI,
    chunk: dict,
    question_type: str,
) -> dict | None:
    prompt = GENERATION_PROMPTS[question_type].format(
        chunk=chunk.get("content", "")[:800]
    )

    try:
        response = await client.chat.completions.create(
            model=AZURE_DEPLOYMENT_NAME,
            messages=[
                {"role": "system", "content": "You generate realistic user questions for documentation QA evaluation. Return only the question text."},
                {"role": "user",   "content": prompt},
            ],
            temperature=0.8,
            max_tokens=100,
        )
        question = response.choices[0].message.content.strip().strip('"')

        if not question.endswith("?"):
            question += "?"

        return {
            "question":        question,
            "question_type":   question_type,
            "source_url":      chunk.get("metadata", {}).get("source_url", "") or chunk.get("source_url", ""),
            "source_chunk":    chunk.get("content", "")[:400],
            "gold_answer":     None,    # filled manually or by separate step
            "gold_sources":    [chunk.get("metadata", {}).get("source_url", "")],
        }

    except Exception as e:
        print(f"Error generating {question_type} question: {e}")
        return None


async def generate_gold_answer(
    client: AsyncAzureOpenAI,
    question: str,
    chunk: str,
) -> str:
    """Generate a reference answer from the source chunk."""
    try:
        response = await client.chat.completions.create(
            model=AZURE_DEPLOYMENT_NAME,
            messages=[
                {"role": "system", "content": "Answer the question based only on the provided context. Be concise and accurate."},
                {"role": "user",   "content": f"Context:\n{chunk}\n\nQuestion: {question}\n\nAnswer:"},
            ],
            temperature=0.1,
            max_tokens=300,
        )
        return response.choices[0].message.content.strip()
    except Exception:
        return ""


async def main(slug: str, count: int):
    print(f"Loading BM25 index for '{slug}'...")

    bm25_store = BM25Store(slug)
    if not bm25_store.load():
        print(f"ERROR: No BM25 index found for '{slug}'.")
        print(f"Make sure you have crawled the site first.")
        return

    all_chunks = bm25_store.chunks
    print(f"Loaded {len(all_chunks)} chunks from {slug}")

    # Group chunks by source URL for better coverage
    url_chunks: dict = {}
    for chunk in all_chunks:
        url = (chunk.get("metadata", {}) or {}).get("source_url", "") or chunk.get("source_url", "unknown")
        if url not in url_chunks:
            url_chunks[url] = []
        url_chunks[url].append(chunk)

    print(f"Found {len(url_chunks)} unique pages")

    client = AsyncAzureOpenAI(
        api_key=AZURE_OPENAI_API_KEY,
        azure_endpoint=AZURE_OPENAI_ENDPOINT,
        api_version=AZURE_API_VERSION,
    )

    # Decide how many of each type
    type_counts = {
        qtype: max(1, int(count * ratio))
        for qtype, ratio in QUESTION_TYPES.items()
    }
    # Adjust to hit exact count
    total = sum(type_counts.values())
    if total < count:
        type_counts["simple"] += count - total

    print(f"\nGenerating {count} questions:")
    for qtype, n in type_counts.items():
        print(f"  {qtype}: {n}")

    questions = []
    tasks     = []

    for qtype, n in type_counts.items():
        if qtype == "out_of_scope":
            # Out of scope doesn't need a real chunk
            dummy_chunk = {"content": "", "source_url": "", "metadata": {}}
            for _ in range(n):
                tasks.append((dummy_chunk, qtype))
        else:
            # Sample chunks proportionally across pages
            available_urls = list(url_chunks.keys())
            for i in range(n):
                url    = available_urls[i % len(available_urls)]
                chunk  = random.choice(url_chunks[url])
                tasks.append((chunk, qtype))

    # Shuffle so questions are interleaved by type
    random.shuffle(tasks)

    print(f"\nGenerating {len(tasks)} questions...")
    semaphore = asyncio.Semaphore(5)

    async def bounded_generate(chunk, qtype):
        async with semaphore:
            return await generate_question(client, chunk, qtype)

    results = await asyncio.gather(
        *[bounded_generate(chunk, qtype) for chunk, qtype in tasks],
        return_exceptions=True,
    )

    # Generate gold answers for non-out-of-scope questions
    print("\nGenerating gold answers...")
    answer_tasks = []
    valid_results = []

    for result, (chunk, qtype) in zip(results, tasks):
        if isinstance(result, Exception) or result is None:
            continue
        valid_results.append((result, chunk, qtype))

    async def bounded_gold(item):
        result, chunk, qtype = item
        async with semaphore:
            if qtype != "out_of_scope" and chunk.get("content"):
                gold = await generate_gold_answer(
                    client, result["question"], chunk["content"][:600]
                )
                result["gold_answer"] = gold
            else:
                result["gold_answer"] = "This question is out of scope for the documentation."
            return result

    final_questions = await asyncio.gather(
        *[bounded_gold(item) for item in valid_results],
        return_exceptions=True,
    )

    questions = [q for q in final_questions if not isinstance(q, Exception) and q]

    await client.close()

    # Save
    output = {
        "slug":            slug,
        "total_questions": len(questions),
        "type_distribution": {
            qtype: sum(1 for q in questions if q["question_type"] == qtype)
            for qtype in QUESTION_TYPES
        },
        "questions": questions,
    }

    output_path = OUTPUT_DIR / f"questions_{slug}.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Generated {len(questions)} questions")
    print(f"   Saved to: {output_path}")
    print(f"\nType distribution:")
    for qtype, n in output["type_distribution"].items():
        print(f"  {qtype}: {n}")

    print(f"\nSample questions:")
    for q in questions[:5]:
        print(f"  [{q['question_type']}] {q['question']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--slug",  default="docs-codepup-ai", help="Site slug")
    parser.add_argument("--count", type=int, default=80,      help="Number of questions")
    args = parser.parse_args()

    asyncio.run(main(args.slug, args.count))