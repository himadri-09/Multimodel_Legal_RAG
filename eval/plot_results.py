# eval/plot_results.py
"""
Generates benchmark graphs from scored eval results.

Charts produced:
  1. grouped_bar.png    — all 4 metrics side by side per system
  2. by_question_type.png — resolution rate breakdown by question type
  3. latency_quality.png  — scatter: latency vs quality (tradeoff curve)

Run:
    python eval/plot_results.py --scores eval/scores_docs-codepup-ai_20240101_120000.json
"""

import json
import argparse
from pathlib import Path

try:
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    import numpy as np
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False
    print("pip install matplotlib numpy")

OUTPUT_DIR = Path("eval/charts")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Colour palette — one per system
COLORS = {
    "rag": "#2563EB",   # blue
    "fin": "#16A34A",   # green
}
FALLBACK_COLORS = ["#2563EB", "#16A34A", "#DC2626", "#D97706"]


def grouped_bar_chart(summary: dict, output_path: Path):
    """Chart 1: all 4 RAGAS metrics grouped by system."""
    systems = list(summary["systems"].keys())
    metrics = [
        ("faithfulness",      "Faithfulness"),
        ("answer_relevancy",  "Answer Relevancy"),
        ("context_precision", "Context Precision"),
        ("resolution_rate",   "Resolution Rate"),
    ]

    fig, ax = plt.subplots(figsize=(11, 6))
    fig.patch.set_facecolor("#F8FAFC")
    ax.set_facecolor("#F8FAFC")

    n_metrics = len(metrics)
    n_systems = len(systems)
    x         = np.arange(n_metrics)
    width     = 0.75 / n_systems
    offsets   = np.linspace(-(n_systems-1)/2, (n_systems-1)/2, n_systems) * width

    for i, system in enumerate(systems):
        vals   = []
        labels = []
        color  = COLORS.get(system, FALLBACK_COLORS[i % len(FALLBACK_COLORS)])
        overall = summary["systems"][system].get("overall", {})

        for metric_key, metric_label in metrics:
            val = overall.get(metric_key, 0)
            vals.append(val)
            labels.append(metric_label)

        bars = ax.bar(
            x + offsets[i], vals, width,
            label=system.upper(),
            color=color,
            alpha=0.88,
            edgecolor="white",
            linewidth=1.2,
        )

        # Value labels on bars
        for bar, val in zip(bars, vals):
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 0.012,
                f"{val:.2f}",
                ha="center", va="bottom",
                fontsize=9, fontweight="bold",
                color="#1E293B",
            )

    ax.set_xticks(x)
    ax.set_xticklabels([m[1] for m in metrics], fontsize=11)
    ax.set_ylim(0, 1.12)
    ax.set_ylabel("Score (0–1)", fontsize=11)
    ax.set_title(
        f"RAG Benchmark — {summary['slug']}",
        fontsize=14, fontweight="bold", pad=16,
    )
    ax.legend(fontsize=10, framealpha=0.8)
    ax.yaxis.grid(True, alpha=0.35, linestyle="--")
    ax.set_axisbelow(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"Saved: {output_path}")


def by_question_type_chart(summary: dict, scored_results: list, output_path: Path):
    """Chart 2: resolution rate by question type, per system."""
    systems   = list(summary["systems"].keys())
    all_types = sorted(set(
        r["question_type"] for r in scored_results
        if r["question_type"] != "out_of_scope"
    ))

    fig, ax = plt.subplots(figsize=(11, 6))
    fig.patch.set_facecolor("#F8FAFC")
    ax.set_facecolor("#F8FAFC")

    n_types   = len(all_types)
    n_systems = len(systems)
    x         = np.arange(n_types)
    width     = 0.7 / n_systems
    offsets   = np.linspace(-(n_systems-1)/2, (n_systems-1)/2, n_systems) * width

    for i, system in enumerate(systems):
        vals  = []
        color = COLORS.get(system, FALLBACK_COLORS[i % len(FALLBACK_COLORS)])
        by_type = summary["systems"][system].get("by_type", {})

        for qtype in all_types:
            val = by_type.get(qtype, {}).get("resolution_rate", 0)
            vals.append(val)

        bars = ax.bar(
            x + offsets[i], vals, width,
            label=system.upper(),
            color=color,
            alpha=0.88,
            edgecolor="white",
            linewidth=1.2,
        )

        for bar, val in zip(bars, vals):
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 0.012,
                f"{val:.2f}",
                ha="center", va="bottom",
                fontsize=9, fontweight="bold",
                color="#1E293B",
            )

    ax.set_xticks(x)
    ax.set_xticklabels([t.replace("_", " ").title() for t in all_types], fontsize=11)
    ax.set_ylim(0, 1.15)
    ax.set_ylabel("Resolution Rate", fontsize=11)
    ax.set_title(
        "Resolution Rate by Question Type",
        fontsize=14, fontweight="bold", pad=16,
    )
    ax.legend(fontsize=10, framealpha=0.8)
    ax.yaxis.grid(True, alpha=0.35, linestyle="--")
    ax.set_axisbelow(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"Saved: {output_path}")


def latency_quality_scatter(summary: dict, output_path: Path):
    """Chart 3: latency vs quality scatter (tradeoff curve)."""
    systems = list(summary["systems"].keys())

    fig, ax = plt.subplots(figsize=(8, 6))
    fig.patch.set_facecolor("#F8FAFC")
    ax.set_facecolor("#F8FAFC")

    for system in systems:
        overall = summary["systems"][system].get("overall", {})
        latency = overall.get("latency_avg", 0)
        quality = (
            overall.get("faithfulness", 0) +
            overall.get("answer_relevancy", 0) +
            overall.get("context_precision", 0) +
            overall.get("resolution_rate", 0)
        ) / 4

        color = COLORS.get(system, FALLBACK_COLORS[0])

        ax.scatter(latency, quality, s=220, color=color, zorder=5, edgecolors="white", linewidth=2)
        ax.annotate(
            system.upper(),
            (latency, quality),
            textcoords="offset points",
            xytext=(12, 6),
            fontsize=11, fontweight="bold", color=color,
        )

    ax.set_xlabel("Average Latency (seconds)", fontsize=11)
    ax.set_ylabel("Overall Quality Score (0–1)", fontsize=11)
    ax.set_title("Latency vs Quality Tradeoff", fontsize=14, fontweight="bold", pad=16)
    ax.set_ylim(0, 1.05)

    # Quadrant labels
    ax.axhline(0.7, color="#94A3B8", linestyle="--", alpha=0.5, linewidth=1)
    ax.text(ax.get_xlim()[1] * 0.02, 0.72, "Target quality threshold (0.7)",
            color="#94A3B8", fontsize=8)

    ax.yaxis.grid(True, alpha=0.3, linestyle="--")
    ax.xaxis.grid(True, alpha=0.3, linestyle="--")
    ax.set_axisbelow(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"Saved: {output_path}")


def main(scores_path: str):
    if not MATPLOTLIB_AVAILABLE:
        print("Install matplotlib: pip install matplotlib numpy")
        return

    with open(scores_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    summary        = data["summary"]
    scored_results = data["scored_results"]
    slug           = summary["slug"]

    print(f"Generating charts for '{slug}'...")
    print(f"Systems: {list(summary['systems'].keys())}")

    grouped_bar_chart(
        summary,
        OUTPUT_DIR / f"grouped_bar_{slug}.png",
    )

    by_question_type_chart(
        summary, scored_results,
        OUTPUT_DIR / f"by_question_type_{slug}.png",
    )

    latency_quality_scatter(
        summary,
        OUTPUT_DIR / f"latency_quality_{slug}.png",
    )

    print(f"\nAll charts saved to: {OUTPUT_DIR}/")
    print(f"\nOverall summary:")
    for system, sys_data in summary["systems"].items():
        overall = sys_data.get("overall", {})
        print(f"\n  {system.upper()}:")
        for k, v in overall.items():
            if k not in ("n", "error_rate"):
                print(f"    {k:<25}: {v}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--scores", required=True, help="Path to scores JSON")
    args = parser.parse_args()
    main(args.scores)