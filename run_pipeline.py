"""
run_pipeline.py — Single entry point to run the full ETL pipeline.

Usage:
    python run_pipeline.py              # incremental update (skips historical files)
    python run_pipeline.py --force      # re-download and reprocess everything

This script is also what you'd schedule via cron or GitHub Actions for
automatic daily updates.

Cron example (run every day at 6am):
    0 6 * * * cd /path/to/sube_analytics && python run_pipeline.py >> logs/cron.log 2>&1
"""

import argparse
import sys
import time
from pathlib import Path

from loguru import logger

# ── Configure logging ──────────────────────────────────────────────────────
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logger.remove()
logger.add(sys.stderr, level="INFO", colorize=True,
           format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}")
logger.add(LOG_DIR / "pipeline_{time:YYYY-MM-DD}.log", rotation="1 day",
           retention="30 days", level="DEBUG")


def main(force: bool = False) -> None:
    start = time.time()
    logger.info("=" * 60)
    logger.info("SUBE Analytics — ETL Pipeline")
    logger.info("=" * 60)

    # ── Step 1: Ingest ────────────────────────────────────────────────────
    logger.info("STEP 1/3 — Ingesting raw CSV files")
    from etl.ingest import download_all
    results = download_all(force=force)

    downloaded = [r for r in results if r["status"] in ("new", "updated")]
    if not downloaded and not force:
        # Check if DB already exists — if so, nothing to do
        from config import DB_PATH
        if DB_PATH.exists():
            logger.info("No new data and DB already exists. Nothing to do.")
            logger.info(f"Total time: {time.time() - start:.1f}s")
            return

    # ── Step 2: Clean ─────────────────────────────────────────────────────
    logger.info("STEP 2/3 — Cleaning and normalizing data")
    from etl.clean import clean_all
    df = clean_all()

    logger.info(
        f"Clean dataset: {len(df):,} rows | "
        f"{df['fecha'].min().date()} → {df['fecha'].max().date()} | "
        f"modes: {sorted(df['modo'].unique())}"
    )

    # ── Step 3: Load ──────────────────────────────────────────────────────
    logger.info("STEP 3/3 — Loading into DuckDB")
    from etl.load import load
    load(df)

    elapsed = time.time() - start
    logger.success(f"Pipeline complete in {elapsed:.1f}s ✓")
    logger.info("Run `streamlit run dashboard/app.py` to launch the dashboard.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SUBE ETL Pipeline")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-download and reprocessing of all files"
    )
    args = parser.parse_args()
    main(force=args.force)
