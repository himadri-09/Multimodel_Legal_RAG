import json, csv
from pathlib import Path

data = json.load(open("eval/questions_docs-codepup-ai.json"))
with open("eval/questions_for_fin.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["question"])
    for q in data["questions"]:
        writer.writerow([q["question"]])

print(f"Exported {len(data['questions'])} questions to eval/questions_for_fin.csv")
