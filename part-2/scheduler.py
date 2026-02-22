from __future__ import annotations

import argparse
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import schedule

ROOT = Path(__file__).resolve().parent

# Configuration 

DEFAULT_INTERVAL_MINUTES = 5   


# Helpers

def _log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def _run(cmd: list[str], label: str) -> bool:
    """Run a subprocess. Returns True on success."""
    _log(f"Running: {label}")
    result = subprocess.run(cmd, cwd=str(ROOT))
    if result.returncode != 0:
        _log(f"ERROR: {label} failed (exit code {result.returncode})")
        return False
    return True


# Pipeline Job 

def run_pipeline_job() -> None:
    """
    One scheduled execution:
      Step 1 — Add new incremental rows to source files (simulates upstream writes)
      Step 2 — Run the full ingestion + dbt pipeline
    """
    _log("=" * 60)
    _log("SCHEDULED JOB STARTING")
    _log("=" * 60)

    # Step 1: Append new data to source files
    _log("Step 1 — Generating incremental source data...")
    ok = _run(
        [sys.executable, str(ROOT / "data" / "add_incremental_sample.py")],
        "add_incremental_sample.py",
    )
    if not ok:
        _log("Skipping pipeline run due to data generation failure.")
        return

    # Step 2: Run the full pipeline (ingest → dbt run → dbt test)
    _log("Step 2 — Running ingestion pipeline...")
    ok = _run(
        [sys.executable, "-m", "ingestion.run_pipeline"],
        "ingestion.run_pipeline",
    )

    status = "SUCCESS" if ok else "FAILED"
    _log(f"SCHEDULED JOB COMPLETE — {status}")
    _log("=" * 60)


# Entry Point

def main() -> None:
    parser = argparse.ArgumentParser(description="SnoreMD automated pipeline scheduler")
    parser.add_argument(
        "--interval",
        type=int,
        default=DEFAULT_INTERVAL_MINUTES,
        help=f"Minutes between each pipeline run (default: {DEFAULT_INTERVAL_MINUTES})",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run the pipeline once immediately and exit (no loop)",
    )
    args = parser.parse_args()

    _log("=" * 60)
    _log("SnoreMD Automated Pipeline Scheduler — Started")
    _log("=" * 60)

    if args.once:
        _log("Mode: single run (--once)")
        run_pipeline_job()
        return

    _log(f"Mode: scheduled every {args.interval} minute(s)")
    _log("Press Ctrl+C to stop.")
    _log("")

    # Run immediately on start, then on schedule
    run_pipeline_job()

    schedule.every(args.interval).minutes.do(run_pipeline_job)

    try:
        while True:
            schedule.run_pending()
            next_run = schedule.next_run()
            if next_run:
                wait_secs = (next_run - datetime.now()).total_seconds()
                _log(f"Next run in {int(wait_secs // 60)}m {int(wait_secs % 60)}s  (at {next_run.strftime('%H:%M:%S')})")
            time.sleep(30) 
    except KeyboardInterrupt:
        _log("")
        _log("Scheduler stopped by user.")


if __name__ == "__main__":
    main()
