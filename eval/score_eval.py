# eval/score_eval.py
"""
Scores eval responses using LLM-as-judge for all 4 RAGAS-style metrics.

Metrics:
  - faithfulness       : is every claim in the answer supported by retrieved context?
  - answer_relevancy   : does the response address what was asked?
  - context_precision  : were the retrieved sources actually relevant?
  - resolution_rate    : would this response actually solve the user's problem? (human proxy)

Also computes:
  - latency stats (p50, p95, avg)
  - abstention rate
  - per-question-type breakdown

Run:
    python eval/score_eval.py --responses eval/responses_docs-codepup-ai_20240101_120000.json
"""

import asyncio
import json
import argparse
import re
from pathlib import Path
from datetime import datetime
from openai import AsyncAzureOpenAI

import sys
sys.path.append(str(Path(__file__).parent.parent))

from config import (
    AZURE_OPENAI_API_KEY, AZURE_OPENAI_ENDPOINT,
    AZURE_API_VERSION, AZURE_DEPLOYMENT_NAME,
)

OUTPUT_DIR = Path("eval")


# ── LLM judge prompts ─────────────────────────────────────────────────────────

FAITHFULNESS_PROMPT = """You are evaluating whether an AI answer is faithful to its source context.

Question: {question}

Retrieved context (what the AI was given):
{context}

AI Answer: {answer}

Score the FAITHFULNESS: Does the answer contain ONLY information supported by the retrieved context?
- Does not introduce facts not in the context
- Does not contradict the context
- Cites or references context appropriately

Score from 0.0 to 1.0:
- 1.0: Fully grounded, every claim traceable to context
- 0.7: Mostly grounded, minor unsupported details
- 0.4: Mix of grounded and hallucinated content
- 0.0: Mostly hallucinated or contradicts context

Return ONLY a JSON: {{"score": 0.0, "reason": "brief explanation"}}"""


ANSWER_RELEVANCY_PROMPT = """You are evaluating whether an AI answer is relevant to the question asked.

Question: {question}
AI Answer: {answer}

Score the ANSWER RELEVANCY: Does the response directly address what was asked?
- Does it answer the actual question?
- Is it appropriately complete (not too vague, not rambling)?
- Is it on-topic?

Score from 0.0 to 1.0:
- 1.0: Directly and completely addresses the question
- 0.7: Addresses the question but misses some aspects
- 0.4: Partially relevant, significant off-topic content
- 0.0: Does not address the question at all

Return ONLY a JSON: {{"score": 0.0, "reason": "brief explanation"}}"""


CONTEXT_PRECISION_PROMPT = """You are evaluating whether retrieved sources were relevant to the question.

Question: {question}

Retrieved sources:
{sources}

Score the CONTEXT PRECISION: Were the retrieved sources actually useful for answering this question?
- Do the sources contain information needed to answer?
- Are the sources on the right topic?
- Would a human use these sources to answer this question?

Score from 0.0 to 1.0:
- 1.0: All sources are highly relevant and necessary
- 0.7: Most sources are relevant, some noise
- 0.4: Mixed — some relevant, some irrelevant
- 0.0: Sources are irrelevant to the question

Return ONLY a JSON: {{"score": 0.0, "reason": "brief explanation"}}"""


RESOLUTION_PROMPT = """You are evaluating whether an AI response would actually resolve a user's question 
about a software product's documentation.

Question: {question}
Question type: {question_type}

Gold answer (reference): {gold_answer}

AI Answer: {answer}

Score the RESOLUTION RATE: Would this response actually help the user accomplish their goal?
- Is the answer correct and actionable?
- Does it match the key facts in the gold answer?
- Would a user reading this be unblocked?

Score from 0.0 to 1.0:
- 1.0: Fully resolves the question, matches gold answer well
- 0.7: Mostly resolves it, minor gaps
- 0.4: Partially helpful but missing key information
- 0.0: Does not resolve the question or is incorrect

Return ONLY a JSON: {{"score": 0.0, "reason": "brief explanation"}}"""


ABSTENTION_QUALITY_PROMPT = """You are evaluating whether an AI correctly identified an out-of-scope question.

Question: {question}
Question type: {question_type}

AI Answer: {answer}

For OUT_OF_SCOPE questions, the AI should:
- Decline to answer or say it can't find the info in docs
- NOT make up an answer
- Ideally suggest the question is outside the product docs

Score from 0.0 to 1.0:
- 1.0: Correctly abstained or redirected
- 0.5: Partially declined but also tried to answer
- 0.0: Hallucinated an answer for an out-of-scope question

Return ONLY a JSON: {{"score": 0.0, "reason": "brief explanation"}}"""


# ── Judge ─────────────────────────────────────────────────────────────────────

async def judge(
    client:  AsyncAzureOpenAI,
    prompt:  str,
    semaphore: asyncio.Semaphore,
) -> dict:
    async with semaphore:
        try:
            response = await client.chat.completions.create(
                model=AZURE_DEPLOYMENT_NAME,
                messages=[
                    {"role": "system", "content": "You are a precise evaluator. Return only valid JSON."},
                    {"role": "user",   "content": prompt},
                ],
                temperature=0.0,
                max_tokens=150,
            )
            text = response.choices[0].message.content.strip()

            # Extract JSON even if model adds extra text
            match = re.search(r'\{[^}]+\}', text)
            if match:
                return json.loads(match.group())
            return {"score": 0.0, "reason": "parse error"}

        except Exception as e:
            return {"score": 0.0, "reason": f"error: {e}"}


async def score_single(
    client:    AsyncAzureOpenAI,
    result:    dict,
    system:    str,              # "rag" or "fin"
    semaphore: asyncio.Semaphore,
) -> dict:
    """Score one question-answer pair for one system."""
    sys_data      = result.get(system)
    question      = result["question"]
    question_type = result["question_type"]
    gold_answer   = result.get("gold_answer", "")

    if not sys_data or sys_data.get("error") or not sys_data.get("answer"):
        return {
            "faithfulness":     0.0,
            "answer_relevancy": 0.0,
            "context_precision": 0.0,
            "resolution_rate":  0.0,
            "latency":          sys_data.get("latency", 0) if sys_data else 0,
            "abstained":        False,
            "error":            sys_data.get("error") if sys_data else "no data",
            "reasons":          {},
        }

    answer  = sys_data["answer"]
    sources = sys_data.get("sources", [])
    latency = sys_data.get("latency", 0)

    # Detect abstention
    abstained = any(p in answer.lower() for p in [
        "couldn't find", "not in the documentation", "not covered",
        "cannot find", "no response", "fin_not_configured",
        "don't have information",
    ])

    # Build context string from sources
    context_str = "\n\n".join([
        f"[{i+1}] {s.get('page_title', '')} ({s.get('source_url', '')})\n{s.get('content_preview', '')}"
        for i, s in enumerate(sources[:5])
    ]) or "No sources retrieved"

    sources_str = "\n".join([
        f"- {s.get('page_title', s.get('source_url', 'unknown'))}"
        for s in sources[:5]
    ]) or "No sources"

    # Run all judges concurrently
    if question_type == "out_of_scope":
        # For out-of-scope, only score abstention quality
        ab_result = await judge(
            client,
            ABSTENTION_QUALITY_PROMPT.format(
                question=question,
                question_type=question_type,
                answer=answer,
            ),
            semaphore,
        )
        return {
            "faithfulness":      1.0 if abstained else 0.0,
            "answer_relevancy":  ab_result["score"],
            "context_precision": 1.0,    # N/A for out of scope
            "resolution_rate":   ab_result["score"],
            "latency":           latency,
            "abstained":         abstained,
            "error":             None,
            "reasons": {
                "abstention": ab_result.get("reason", ""),
            },
        }

    # Normal question — all 4 metrics
    faith_task = judge(
        client,
        FAITHFULNESS_PROMPT.format(
            question=question,
            context=context_str,
            answer=answer,
        ),
        semaphore,
    )
    relevancy_task = judge(
        client,
        ANSWER_RELEVANCY_PROMPT.format(question=question, answer=answer),
        semaphore,
    )
    precision_task = judge(
        client,
        CONTEXT_PRECISION_PROMPT.format(question=question, sources=sources_str),
        semaphore,
    )
    resolution_task = judge(
        client,
        RESOLUTION_PROMPT.format(
            question=question,
            question_type=question_type,
            gold_answer=gold_answer,
            answer=answer,
        ),
        semaphore,
    )

    faith, relevancy, precision, resolution = await asyncio.gather(
        faith_task, relevancy_task, precision_task, resolution_task
    )

    return {
        "faithfulness":      faith["score"],
        "answer_relevancy":  relevancy["score"],
        "context_precision": precision["score"],
        "resolution_rate":   resolution["score"],
        "latency":           latency,
        "abstained":         abstained,
        "error":             None,
        "reasons": {
            "faithfulness":      faith.get("reason", ""),
            "answer_relevancy":  relevancy.get("reason", ""),
            "context_precision": precision.get("reason", ""),
            "resolution_rate":   resolution.get("reason", ""),
        },
    }


# ── Aggregate ─────────────────────────────────────────────────────────────────

def aggregate_scores(scored_results: list, system: str) -> dict:
    """Compute mean metrics and per-type breakdowns."""
    valid = [r for r in scored_results if r[system] and not r[system].get("error")]

    if not valid:
        return {}

    metrics = ["faithfulness", "answer_relevancy", "context_precision", "resolution_rate"]

    # Overall means
    overall = {
        m: round(sum(r[system][m] for r in valid) / len(valid), 3)
        for m in metrics
    }

    # Latency stats
    latencies = sorted([r[system]["latency"] for r in valid if r[system]["latency"] > 0])
    if latencies:
        overall["latency_avg"] = round(sum(latencies) / len(latencies), 3)
        overall["latency_p50"] = round(latencies[len(latencies)//2], 3)
        overall["latency_p95"] = round(latencies[int(len(latencies)*0.95)], 3)

    overall["abstention_rate"] = round(
        sum(1 for r in valid if r[system]["abstained"]) / len(valid), 3
    )
    overall["error_rate"] = round(
        sum(1 for r in scored_results if not r[system] or r[system].get("error"))
        / len(scored_results), 3
    )
    overall["n"] = len(valid)

    # Per question type
    all_types = set(r["question_type"] for r in valid)
    by_type   = {}
    for qtype in all_types:
        type_results = [r for r in valid if r["question_type"] == qtype]
        if type_results:
            by_type[qtype] = {
                m: round(sum(r[system][m] for r in type_results) / len(type_results), 3)
                for m in metrics
            }
            by_type[qtype]["n"] = len(type_results)

    return {"overall": overall, "by_type": by_type}


# ── Main ──────────────────────────────────────────────────────────────────────

async def main(responses_path: str):
    with open(responses_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    results    = data["results"]
    slug       = data["slug"]
    systems    = data["systems"]
    timestamp  = datetime.now().strftime("%Y%m%d_%H%M%S")

    print(f"Scoring {len(results)} responses for '{slug}'")
    print(f"Systems: {systems}")

    client    = AsyncAzureOpenAI(
        api_key=AZURE_OPENAI_API_KEY,
        azure_endpoint=AZURE_OPENAI_ENDPOINT,
        api_version=AZURE_API_VERSION,
    )
    semaphore = asyncio.Semaphore(5)   # concurrent judge calls

    # Score each result for each system
    systems_to_score = []
    if systems in ("rag", "both"):
        systems_to_score.append("rag")
    if systems in ("fin", "both"):
        systems_to_score.append("fin")

    scored_results = []

    for i, result in enumerate(results):
        print(f"\n[{i+1}/{len(results)}] {result['question'][:60]}")

        scored = {
            "question":      result["question"],
            "question_type": result["question_type"],
            "gold_answer":   result.get("gold_answer", ""),
            "source_url":    result.get("source_url", ""),
        }

        for system in systems_to_score:
            if result.get(system):
                scores = await score_single(client, result, system, semaphore)
                scored[system] = scores
                print(
                    f"  {system}: faith={scores['faithfulness']:.2f} "
                    f"rel={scores['answer_relevancy']:.2f} "
                    f"prec={scores['context_precision']:.2f} "
                    f"res={scores['resolution_rate']:.2f} "
                    f"({scores['latency']}s)"
                )
            else:
                scored[system] = None

        scored_results.append(scored)

    await client.close()

    # Aggregate
    summary = {"slug": slug, "timestamp": timestamp, "systems": {}}
    for system in systems_to_score:
        summary["systems"][system] = aggregate_scores(scored_results, system)

    # Save scores
    scores_path = OUTPUT_DIR / f"scores_{slug}_{timestamp}.json"
    with open(scores_path, "w", encoding="utf-8") as f:
        json.dump({
            "summary":        summary,
            "scored_results": scored_results,
        }, f, indent=2, ensure_ascii=False)

    # Print summary table
    print(f"\n{'='*65}")
    print(f"BENCHMARK RESULTS — {slug}")
    print(f"{'='*65}")

    metrics_display = [
        ("faithfulness",      "Faithfulness"),
        ("answer_relevancy",  "Answer Relevancy"),
        ("context_precision", "Context Precision"),
        ("resolution_rate",   "Resolution Rate"),
        ("latency_avg",       "Avg Latency (s)"),
        ("abstention_rate",   "Abstention Rate"),
    ]

    header = f"{'Metric':<25}"
    for system in systems_to_score:
        header += f"  {system.upper():>12}"
    print(header)
    print("-" * 65)

    for metric_key, metric_label in metrics_display:
        row = f"{metric_label:<25}"
        for system in systems_to_score:
            val = summary["systems"].get(system, {}).get("overall", {}).get(metric_key, "-")
            row += f"  {str(val):>12}"
        print(row)

    print(f"\nScores saved to: {scores_path}")
    print(f"Next step: python eval/plot_results.py --scores {scores_path}")

    return scores_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--responses", required=True, help="Path to responses JSON")
    args = parser.parse_args()

    asyncio.run(main(args.responses))