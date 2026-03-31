# eval/merge_responses.py
"""
Merges two separate response files (one for RAG, one for Fin)
into a single file that score_eval.py can process.

Run:
    python eval/merge_responses.py \
        --rag  eval/responses_docs-codepup-ai_20260330_152113.json \
        --fin  eval/responses_docs-codepup-ai_20260330_212931.json
"""

import json
import argparse
from pathlib import Path
from datetime import datetime

OUTPUT_DIR = Path("eval")


def merge(rag_path: str, fin_path: str):
    with open(rag_path, "r", encoding="utf-8") as f:
        rag_data = json.load(f)

    with open(fin_path, "r", encoding="utf-8") as f:
        fin_data = json.load(f)

    rag_results = rag_data["results"]
    fin_results = fin_data["results"]

    slug = rag_data.get("slug") or fin_data.get("slug") or "unknown"

    print(f"RAG results : {len(rag_results)} questions")
    print(f"Fin results : {len(fin_results)} questions")

    # Match by question text — more reliable than position
    # because the two runs may have different question counts
    fin_by_question = {
        r["question"].strip().lower(): r
        for r in fin_results
    }

    merged = []
    matched   = 0
    unmatched = 0

    for rag_result in rag_results:
        question_key = rag_result["question"].strip().lower()

        merged_result = {
            "question":      rag_result["question"],
            "question_type": rag_result["question_type"],
            "gold_answer":   rag_result.get("gold_answer", ""),
            "gold_sources":  rag_result.get("gold_sources", []),
            "source_url":    rag_result.get("source_url", ""),
            "rag":           rag_result.get("rag"),
            "fin":           None,
        }

        if question_key in fin_by_question:
            merged_result["fin"] = fin_by_question[question_key].get("fin")
            matched += 1
        else:
            # Try partial match — first 60 chars
            short_key = question_key[:60]
            partial   = next(
                (v for k, v in fin_by_question.items() if k[:60] == short_key),
                None
            )
            if partial:
                merged_result["fin"] = partial.get("fin")
                matched += 1
            else:
                unmatched += 1
                print(f"  ⚠️  No Fin match for: {rag_result['question'][:70]}")

        merged.append(merged_result)

    # Also add any Fin-only questions not in RAG
    rag_questions = {r["question"].strip().lower() for r in rag_results}
    fin_only = [
        r for r in fin_results
        if r["question"].strip().lower() not in rag_questions
    ]
    if fin_only:
        print(f"\n  ℹ️  {len(fin_only)} Fin-only questions (no RAG match) — included with rag=null")
        for r in fin_only:
            merged.append({
                "question":      r["question"],
                "question_type": r.get("question_type", "simple"),
                "gold_answer":   r.get("gold_answer", ""),
                "gold_sources":  [],
                "source_url":    "",
                "rag":           None,
                "fin":           r.get("fin"),
            })

    timestamp   = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = OUTPUT_DIR / f"responses_{slug}_merged_{timestamp}.json"

    output = {
        "slug":      slug,
        "timestamp": timestamp,
        "systems":   "both",
        "results":   merged,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Merged {len(merged)} questions")
    print(f"   Matched       : {matched}")
    print(f"   Unmatched RAG : {unmatched}")
    print(f"   Fin-only      : {len(fin_only)}")
    print(f"\nSaved to: {output_path}")
    print(f"\nNext step:")
    print(f"  python eval/score_eval.py --responses {output_path}")

    return str(output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rag", required=True, help="Path to RAG responses JSON")
    parser.add_argument("--fin", required=True, help="Path to Fin responses JSON")
    args = parser.parse_args()

    merge(args.rag, args.fin)