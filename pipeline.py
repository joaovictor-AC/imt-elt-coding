"""
KICKZ EMPIRE — ELT Pipeline Orchestrator
==========================================
Main entry point for the ELT pipeline.

Executes sequentially:
    1. Extract  → Load CSV files into Bronze (raw, as-is)
    2. Transform → Clean Bronze → Silver (conformed)
    3. Gold      → Aggregate Silver → Gold (business-ready)

Usage:
    python pipeline.py                  # Run the full pipeline
    python pipeline.py --step extract   # Run extraction only
    python pipeline.py --step transform # Run transformation only
    python pipeline.py --step gold      # Run Gold layer only
"""

import argparse
import time

from src.extract import extract_all
from src.transform import transform_all
from src.gold import create_gold_layer


def run_pipeline(step: str = "all"):
    """
    Run the ELT pipeline.

    Args:
        step (str): Which step to execute.
            - "all"       : full pipeline
            - "extract"   : extraction only (Bronze)
            - "transform" : transformation only (Silver)
            - "gold"      : Gold layer only
    """
    print("=" * 60)
    print("  🏪 KICKZ EMPIRE — ELT Pipeline")
    print("=" * 60)

    t0 = time.time()

    if step in ("all", "extract"):
        extract_all()

    if step in ("all", "transform"):
        transform_all()

    if step in ("all", "gold"):
        create_gold_layer()

    elapsed = time.time() - t0
    print(f"\n{'=' * 60}")
    print(f"  ✅ Pipeline terminé en {elapsed:.1f}s")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="KICKZ EMPIRE ELT Pipeline")
    parser.add_argument(
        "--step",
        choices=["all", "extract", "transform", "gold"],
        default="all",
        help="Pipeline step to execute (default: all)",
    )
    args = parser.parse_args()
    run_pipeline(step=args.step)
