"""
Orchestrates the full Snore MD data pipeline:

  1. Ingest all 5 source files into Snowflake RAW (Bronze)
  2. Run dbt deps   — install dbt packages
  3. Run dbt run    — build STAGING + ANALYTICS models (Silver + Gold)
  4. Run dbt test   — execute all data quality checks
  5. Print summary
"""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

from ingestion.ingest_appointments import AppointmentsIngester
from ingestion.ingest_billing import BillingIngester
from ingestion.ingest_clinician_notes import ClinicianNotesIngester
from ingestion.ingest_patients import PatientsIngester
from ingestion.ingest_sleep_studies import SleepStudiesIngester
from ingestion.logger import get_logger

log = get_logger("run_pipeline")

ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "data" / "raw"
DBT_DIR = ROOT / "dbt_project"


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _run_ingester(ingester_cls, file_name: str) -> tuple[str, int, str]:
    """Run a single ingester. Returns (source_name, rows_loaded, status)."""
    ingester = ingester_cls()
    file_path = str(RAW_DIR / file_name)
    try:
        rows = ingester.run(file_path)
        return ingester.source_name, rows, "success"
    except Exception as e:
        log.error(f"{ingester.source_name} failed: {e}")
        return ingester.source_name, 0, f"FAILED: {e}"


def _run_dbt(command: list[str]) -> bool:
    """Run a dbt command inside dbt_project/. Returns True on success."""
    # Invoke dbt's CLI entry point directly — avoids the broken .exe launcher
    # that occurs when the venv is moved/copied from another path.
    dbt_invoke = "import sys; from dbt.cli.main import cli; cli()"
    full_cmd = [sys.executable, "-c", dbt_invoke] + command
    log.info(f"Running: dbt {' '.join(command)}")
    result = subprocess.run(
        full_cmd,
        cwd=str(DBT_DIR),
        capture_output=False,   # stream output to terminal
    )
    success = result.returncode == 0
    if not success:
        log.error(f"dbt command failed (exit code {result.returncode}): {' '.join(command)}")
    return success


# ─── Pipeline ─────────────────────────────────────────────────────────────────

def run_ingestion() -> list[tuple[str, int, str]]:
    """Run all ingesters sequentially. Returns list of results."""
    log.info("━" * 60)
    log.info("PHASE 1 — Ingestion (Bronze layer)")
    log.info("━" * 60)

    jobs = [
        (PatientsIngester,       "patients.csv"),
        (AppointmentsIngester,   "appointments.json"),
        (SleepStudiesIngester,   "sleep_studies.csv"),
        (ClinicianNotesIngester, "clinician_notes.json"),
        (BillingIngester,        "billing_report.xlsx"),
    ]

    results = []
    for cls, fname in jobs:
        results.append(_run_ingester(cls, fname))

    return results


def run_dbt_pipeline() -> dict[str, bool]:
    """Run dbt deps → run → test. Returns {step: success}."""
    log.info("━" * 60)
    log.info("PHASE 2 — dbt Transformations (Silver + Gold layers)")
    log.info("━" * 60)

    steps = {
        "deps": ["deps"],
        "run":  ["run"],
        "test": ["test"],
    }
    results = {}
    for name, args in steps.items():
        log.info(f"dbt {name} ...")
        ok = _run_dbt(args)
        results[name] = ok
        if not ok and name == "run":
            log.error("dbt run failed — skipping dbt test.")
            results["test"] = False
            break
    return results


def print_summary(ingestion_results, dbt_results, elapsed: float):
    log.info("━" * 60)
    log.info("PIPELINE SUMMARY")
    log.info("━" * 60)
    log.info("Ingestion results:")
    total_rows = 0
    all_ok = True
    for source, rows, status in ingestion_results:
        icon = "✓" if status == "success" else "✗"
        log.info(f"  {icon}  {source:<35} {rows:>6} rows  [{status}]")
        total_rows += rows
        if status != "success":
            all_ok = False
    log.info(f"  Total rows loaded: {total_rows}")
    log.info("")
    log.info("dbt results:")
    for step, ok in dbt_results.items():
        icon = "✓" if ok else "✗"
        log.info(f"  {icon}  dbt {step}")
    log.info("")
    overall = all_ok and all(dbt_results.values())
    log.info(f"Overall status : {'SUCCESS' if overall else 'FAILED'}")
    log.info(f"Elapsed time   : {elapsed:.1f}s")
    log.info("━" * 60)


# ─── Entry point ──────────────────────────────────────────────────────────────

def main():
    t0 = time.time()
    log.info("=" * 60)
    log.info("Snore MD Data Pipeline — Starting")
    log.info("=" * 60)

    ingestion_results = run_ingestion()
    dbt_results = run_dbt_pipeline()
    elapsed = time.time() - t0

    print_summary(ingestion_results, dbt_results, elapsed)

    # Exit with non-zero code if any step failed (useful in CI)
    any_failed = any(s != "success" for _, _, s in ingestion_results)
    any_dbt_failed = not all(dbt_results.values())
    sys.exit(1 if (any_failed or any_dbt_failed) else 0)


if __name__ == "__main__":
    main()
