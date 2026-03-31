# eval/run_eval.py
"""
Runs the evaluation question set against:
  1. Your RAG system (local or deployed)
  2. Fin (via Intercom API)

Saves raw responses to eval/responses_*.json for scoring.

Run:
    # Query both systems
    python eval/run_eval.py --slug docs-codepup-ai --systems both

    # Query only your RAG
    python eval/run_eval.py --slug docs-codepup-ai --systems rag

    # Query only Fin
    python eval/run_eval.py --slug docs-codepup-ai --systems fin
"""

import asyncio
import json
import time
import argparse
import os
from pathlib import Path
from datetime import datetime

import httpx

# ── Config ────────────────────────────────────────────────────────────────────

RAG_BASE_URL  = os.getenv("RAG_BASE_URL",  "http://localhost:8000")
RAG_TOKEN     = os.getenv("RAG_TOKEN",     "eyJhbGciOiJFUzI1NiIsImtpZCI6IjQ1Mzg1OWY2LWNiYzctNDdiMS1hMzAzLTk0MmQyYzYwYmM4MiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwczovL3BhZ2hwaWRmcnduZXFqZnRpa3l3LnN1cGFiYXNlLmNvL2F1dGgvdjEiLCJzdWIiOiI1ZjJiYjg5My01YTJmLTRkOTYtOTE5NC03ZDJhZTkxNjFhNWUiLCJhdWQiOiJhdXRoZW50aWNhdGVkIiwiZXhwIjoxNzc0ODYzNzg0LCJpYXQiOjE3NzQ4NjAxODQsImVtYWlsIjoiaEBnbWFpbC5jb20iLCJwaG9uZSI6IiIsImFwcF9tZXRhZGF0YSI6eyJwcm92aWRlciI6ImVtYWlsIiwicHJvdmlkZXJzIjpbImVtYWlsIl19LCJ1c2VyX21ldGFkYXRhIjp7ImVtYWlsIjoiaEBnbWFpbC5jb20iLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwicGhvbmVfdmVyaWZpZWQiOmZhbHNlLCJzdWIiOiI1ZjJiYjg5My01YTJmLTRkOTYtOTE5NC03ZDJhZTkxNjFhNWUifSwicm9sZSI6ImF1dGhlbnRpY2F0ZWQiLCJhYWwiOiJhYWwxIiwiYW1yIjpbeyJtZXRob2QiOiJwYXNzd29yZCIsInRpbWVzdGFtcCI6MTc3NDQ5ODE0Nn1dLCJzZXNzaW9uX2lkIjoiZjI0MDNlZmQtZmYwYi00MzYxLWEyMjAtNTg4ZmQ4OWIyYWJjIiwiaXNfYW5vbnltb3VzIjpmYWxzZX0.c9_weQK2vST5qisjS3-hM5G9oD5SOIz-HEDDix_DLRY7q_lrt8_ZnZ3z4Ju-DC0ig0tJKn2lsn-uXg2qcBKJGQ")          # your JWT token
RAG_PDF_NAME  = os.getenv("RAG_PDF_NAME",  "docs-codepup-ai")

# Intercom Fin config
# Fin can be queried via Intercom's Resolution Bot API or test widget
# See: https://developers.intercom.com/docs/build-an-integration/
INTERCOM_TOKEN    = os.getenv("INTERCOM_TOKEN",    "")
INTERCOM_INBOX_ID = os.getenv("INTERCOM_INBOX_ID", "")  # your test inbox

OUTPUT_DIR = Path("eval")
OUTPUT_DIR.mkdir(exist_ok=True)

# ── RAG querier ───────────────────────────────────────────────────────────────

async def query_your_rag(
    client:   httpx.AsyncClient,
    question: str,
    pdf_name: str,
) -> dict:
    """Query your RAG system. Returns standardised response dict."""
    start = time.time()
    try:
        resp = await client.post(
            f"{RAG_BASE_URL}/query",
            headers={
                "Authorization": f"Bearer {RAG_TOKEN}",
                "Content-Type":  "application/json",
            },
            json={
                "query":    question,
                "pdf_name": pdf_name,
            },
            timeout=60,
        )
        latency = time.time() - start

        if resp.status_code == 200:
            data = resp.json()
            return {
                "answer":   data.get("answer", ""),
                "sources":  data.get("sources", []),
                "latency":  round(latency, 3),
                "error":    None,
            }
        else:
            return {
                "answer":  "",
                "sources": [],
                "latency": round(latency, 3),
                "error":   f"HTTP {resp.status_code}: {resp.text[:200]}",
            }
    except Exception as e:
        return {
            "answer":  "",
            "sources": [],
            "latency": round(time.time() - start, 3),
            "error":   str(e),
        }


# ── Fin querier ───────────────────────────────────────────────────────────────

async def query_fin(
    client:   httpx.AsyncClient,
    question: str,
) -> dict:
    """
    Query Intercom Fin via the Conversations API.

    How to set this up:
    1. Create a test workspace in Intercom with your docs connected to Fin
    2. Create an API access token: Settings → Integrations → Developer Hub
    3. Get your inbox_id from the Intercom dashboard
    4. This creates a real conversation and reads Fin's reply

    Alternative: use Fin's batch testing UI in the Intercom dashboard
    and export results as CSV, then load with load_fin_csv() below.
    """
    if not INTERCOM_TOKEN or not INTERCOM_INBOX_ID:
        return {
            "answer":  "FIN_NOT_CONFIGURED",
            "sources": [],
            "latency": 0,
            "error":   "INTERCOM_TOKEN or INTERCOM_INBOX_ID not set",
        }

    start = time.time()
    try:
        # Step 1: Create a conversation (simulates user sending a message)
        create_resp = await client.post(
            "https://api.intercom.io/conversations",
            headers={
                "Authorization":  f"Bearer {INTERCOM_TOKEN}",
                "Content-Type":   "application/json",
                "Accept":         "application/json",
                "Intercom-Version": "2.10",
            },
            json={
                "from": {
                    "type":  "user",
                    "email": "adityamadur735+test1@gmail.com",
                },
                "body": question,
            },
            timeout=30,
        )

        if create_resp.status_code not in (200, 201):
            err_body = create_resp.text[:400]
            print(f"  [Intercom API] status={create_resp.status_code} body={err_body}")
            return {
                "answer":  "",
                "sources": [],
                "latency": round(time.time() - start, 3),
                "error":   f"HTTP {create_resp.status_code}: {err_body}",
            }

        conv_id = create_resp.json().get("id")

        # Step 2: Wait for Fin to respond (poll for reply)
        fin_answer = ""
        for attempt in range(10):    # poll up to 10 times
            await asyncio.sleep(2)   # wait 2s between polls

            conv_resp = await client.get(
                f"https://api.intercom.io/conversations/{conv_id}",
                headers={
                    "Authorization":    f"Bearer {INTERCOM_TOKEN}",
                    "Accept":           "application/json",
                    "Intercom-Version": "2.10",
                },
                timeout=15,
            )

            if conv_resp.status_code == 200:
                conv_data    = conv_resp.json()
                conv_parts   = conv_data.get("conversation_parts", {}).get("conversation_parts", [])
                # Find the bot's reply (author type = bot)
                bot_replies  = [
                    p for p in conv_parts
                    if p.get("author", {}).get("type") == "bot"
                    and p.get("body")
                ]
                if bot_replies:
                    # Get the latest bot reply
                    fin_answer = bot_replies[-1].get("body", "")
                    # Strip HTML tags from Intercom response
                    import re
                    fin_answer = re.sub(r'<[^>]+>', '', fin_answer).strip()
                    break

        latency = time.time() - start
        return {
            "answer":  fin_answer or "No response from Fin",
            "sources": [],          # Fin doesn't expose source chunks via API
            "latency": round(latency, 3),
            "error":   None if fin_answer else "Fin did not reply within timeout",
        }

    except Exception as e:
        return {
            "answer":  "",
            "sources": [],
            "latency": round(time.time() - start, 3),
            "error":   str(e),
        }


def load_fin_csv(csv_path: str, questions: list) -> list:
    """
    Alternative: if you exported Fin batch test results as CSV,
    load them here instead of querying the API.

    CSV format expected (from Intercom batch test export):
        question, answer, resolution_status
    """
    import csv

    fin_responses = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            fin_responses.append({
                "answer":  row.get("answer", ""),
                "sources": [],
                "latency": 0,
                "error":   None,
            })

    print(f"Loaded {len(fin_responses)} Fin responses from CSV")
    return fin_responses


# ── Main runner ───────────────────────────────────────────────────────────────

async def run_eval(slug: str, systems: str, fin_csv: str = None):
    # Load questions
    questions_path = OUTPUT_DIR / f"questions_{slug}.json"
    if not questions_path.exists():
        print(f"ERROR: {questions_path} not found.")
        print(f"Run first: python eval/generate_questions.py --slug {slug}")
        return

    with open(questions_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    questions = data["questions"]
    print(f"Loaded {len(questions)} questions for '{slug}'")
    print(f"Systems to evaluate: {systems}")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results   = []

    async with httpx.AsyncClient() as client:
        for i, q in enumerate(questions):
            question     = q["question"]
            question_type = q["question_type"]

            print(f"\n[{i+1}/{len(questions)}] [{question_type}] {question[:70]}")

            result = {
                "question":      question,
                "question_type": question_type,
                "gold_answer":   q.get("gold_answer", ""),
                "gold_sources":  q.get("gold_sources", []),
                "source_url":    q.get("source_url", ""),
                "rag":           None,
                "fin":           None,
            }

            # Query your RAG
            if systems in ("rag", "both"):
                rag_resp = await query_your_rag(client, question, RAG_PDF_NAME)
                result["rag"] = rag_resp
                status = "✅" if not rag_resp["error"] else "❌"
                print(f"  RAG  {status} ({rag_resp['latency']}s): {rag_resp['answer'][:80]}...")

            # Query Fin (or load from CSV)
            if systems in ("fin", "both"):
                if fin_csv:
                    # Use pre-exported CSV
                    pass   # handled separately below
                else:
                    fin_resp = await query_fin(client, question)
                    result["fin"] = fin_resp
                    status = "✅" if not fin_resp["error"] else "❌"
                    if fin_resp["error"]:
                        print(f"  Fin  {status} ({fin_resp['latency']}s): ERROR → {fin_resp['error']}")
                    else:
                        print(f"  Fin  {status} ({fin_resp['latency']}s): {fin_resp['answer'][:80]}...")

            results.append(result)

            # Rate limiting — be gentle with both APIs
            await asyncio.sleep(1.0)

    # If Fin CSV provided, merge in
    if fin_csv and systems in ("fin", "both"):
        fin_responses = load_fin_csv(fin_csv, questions)
        for result, fin_resp in zip(results, fin_responses):
            result["fin"] = fin_resp

    # Save raw responses
    output = {
        "slug":      slug,
        "timestamp": timestamp,
        "systems":   systems,
        "results":   results,
    }

    output_path = OUTPUT_DIR / f"responses_{slug}_{timestamp}.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Saved {len(results)} responses to {output_path}")
    print(f"Next step: python eval/score_eval.py --responses {output_path}")

    # Quick summary
    if systems in ("rag", "both"):
        rag_errors  = sum(1 for r in results if r["rag"] and r["rag"]["error"])
        rag_latency = [r["rag"]["latency"] for r in results if r["rag"] and not r["rag"]["error"]]
        print(f"\nRAG summary:")
        print(f"  Errors  : {rag_errors}/{len(results)}")
        if rag_latency:
            print(f"  Avg latency: {sum(rag_latency)/len(rag_latency):.2f}s")
            print(f"  P50 latency: {sorted(rag_latency)[len(rag_latency)//2]:.2f}s")

    return output_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--slug",    default="docs-codepup-ai")
    parser.add_argument("--systems", choices=["rag", "fin", "both"], default="both")
    parser.add_argument("--fin-csv", default=None, help="Path to exported Fin CSV")
    args = parser.parse_args()

    asyncio.run(run_eval(args.slug, args.systems, args.fin_csv))