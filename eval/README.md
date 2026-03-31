# Eval Framework — Setup & Usage

## Folder structure
```
eval/
  generate_questions.py   ← Step 1: build question set from your docs
  run_eval.py             ← Step 2: query your RAG + Fin, save responses
  score_eval.py           ← Step 3: LLM-judge scores all metrics
  plot_results.py         ← Step 4: generate benchmark charts
  charts/                 ← output charts
  questions_*.json        ← generated question sets
  responses_*.json        ← raw system responses
  scores_*.json           ← scored results
```

## Install dependencies
```bash
pip install matplotlib numpy httpx
```

---

## Step 1 — Generate questions from your docs

```bash
python eval/generate_questions.py --slug docs-codepup-ai --count 80
```

This loads your BM25 index (built during crawl), samples chunks across all
pages, and generates 80 questions covering all types:
- 40% simple
- 20% multi_part
- 20% troubleshoot
- 10% comparison
- 10% out_of_scope

Output: `eval/questions_docs-codepup-ai.json`

**Review the questions** before running eval — delete bad ones, add your own.
Good eval sets include real questions you've seen users ask.

---

## Step 2 — Query your RAG

```bash
# Set env vars
export RAG_BASE_URL="http://localhost:8000"    # or your deployed URL
export RAG_TOKEN="your_jwt_token_here"
export RAG_PDF_NAME="docs-codepup-ai"

# Query only your RAG first
python eval/run_eval.py --slug docs-codepup-ai --systems rag
```

Output: `eval/responses_docs-codepup-ai_TIMESTAMP.json`

---

## Step 3A — Add Fin responses via API

If you have Intercom API access:

```bash
export INTERCOM_TOKEN="your_intercom_token"
export INTERCOM_INBOX_ID="your_inbox_id"

python eval/run_eval.py --slug docs-codepup-ai --systems fin
```

### Getting Intercom credentials:
1. Go to Intercom → Settings → Integrations → Developer Hub
2. Create a new app → get Access Token
3. Your inbox ID is in the URL when viewing your inbox in Intercom

---

## Step 3B — Add Fin responses via batch test export (easier)

Intercom has a built-in batch testing feature:
1. Go to Fin AI Agent → Testing → Batch Test
2. Upload a CSV of questions (one per line)
3. Run the batch test — Fin generates answers
4. Export results as CSV
5. Pass the CSV to the runner:

```bash
python eval/run_eval.py --slug docs-codepup-ai --systems both \
  --fin-csv path/to/fin_export.csv
```

The CSV format from Intercom export:
```
question,answer,resolution_status
"How do I deploy?","To deploy your project...","resolved"
```

---

## Step 3C — Manual Fin testing (simplest)

If you can't get API access, manually test 20-30 questions in Fin's
chat widget and record the answers in a CSV. Use `load_fin_csv()` in run_eval.py.

Even 20-30 manually tested questions give you a valid statistical comparison.

---

## Step 4 — Score responses

```bash
python eval/score_eval.py --responses eval/responses_docs-codepup-ai_TIMESTAMP.json
```

This uses your Azure OpenAI as an LLM judge to score:
- Faithfulness (0-1)
- Answer Relevancy (0-1)
- Context Precision (0-1)
- Resolution Rate (0-1)

Plus latency stats, abstention rate, per-question-type breakdown.

Output: `eval/scores_docs-codepup-ai_TIMESTAMP.json`

---

## Step 5 — Generate charts

```bash
python eval/plot_results.py --scores eval/scores_docs-codepup-ai_TIMESTAMP.json
```

Output:
- `eval/charts/grouped_bar_docs-codepup-ai.png` — all 4 metrics side by side
- `eval/charts/by_question_type_docs-codepup-ai.png` — resolution by question type
- `eval/charts/latency_quality_docs-codepup-ai.png` — latency vs quality scatter

---

## Getting your JWT token for RAG_TOKEN

```python
# In Python — login via Supabase and get token
from supabase import create_client
client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
result = client.auth.sign_in_with_password({"email": "you@email.com", "password": "..."})
print(result.session.access_token)   # use this as RAG_TOKEN
```

Or copy it from browser dev tools → Network tab → any /query request → Authorization header.

---

## Interpreting results

| Metric | Target | Below this = problem |
|--------|--------|----------------------|
| Faithfulness | > 0.85 | Hallucination risk |
| Answer Relevancy | > 0.80 | Off-topic answers |
| Context Precision | > 0.75 | Noisy retrieval |
| Resolution Rate | > 0.70 | Users not getting helped |
| Avg Latency | < 5s | Too slow for chat |

If your RAG scores within 0.05 of Fin on resolution rate, you're at parity.
If you're 0.10+ behind on faithfulness, check your grounding prompt.
If context precision is low, tune MIN_SIMILARITY_SCORE up or add more docs.