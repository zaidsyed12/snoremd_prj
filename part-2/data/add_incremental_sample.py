# for incremental watermark logic: small batch of NEW rows (with today's timestamps) to the source data files

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

RAW_DIR = Path(__file__).resolve().parent / "raw"
NOW = datetime.utcnow()
TODAY = NOW.strftime("%Y-%m-%d")
NOW_ISO = NOW.strftime("%Y-%m-%dT%H:%M:%S")

# Existing clinic IDs from reference data
CLINIC_IDS = ["CLN001", "CLN002", "CLN003", "CLN004", "CLN005"]


def add_patients(n: int = 3) -> None:
    path = RAW_DIR / "patients.csv"
    df = pd.read_csv(path)

    new_rows = []
    for i in range(n):
        pid = f"PAT-NEW-{uuid.uuid4().hex[:8].upper()}"
        new_rows.append({
            "patient_id":   pid,
            "first_name":   f"NewPatient{i+1}",
            "last_name":    "Demo",
            "date_of_birth": "1990-01-01",
            "gender":        "M",
            "email":         f"newpatient{i+1}@demo.com",
            "phone":         f"416-000-{1000 + i}",
            "clinic_id":     CLINIC_IDS[i % len(CLINIC_IDS)],
            "is_active":     True,
            "created_at":    NOW_ISO,
            "updated_at":    NOW_ISO,   # newer than existing watermark
        })

    new_df = pd.DataFrame(new_rows)
    updated = pd.concat([df, new_df], ignore_index=True)
    updated.to_csv(path, index=False)
    print(f"  Added {n} new patients to {path.name}")


def add_appointments(n: int = 5) -> None:
    path = RAW_DIR / "appointments.json"
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    # Collect existing patient IDs and clinician IDs to reference
    patients_df = pd.read_csv(RAW_DIR / "patients.csv")
    patient_ids = patients_df["patient_id"].tolist()
    # Pull real clinician IDs from existing appointments so FK joins succeed
    clinician_ids = list({row["clinician_id"] for row in data if row.get("clinician_id")})

    new_rows = []
    for i in range(n):
        new_rows.append({
            "appointment_id":   f"APT-NEW-{uuid.uuid4().hex[:8].upper()}",
            "patient_id":       patient_ids[i % len(patient_ids)],
            "clinic_id":        CLINIC_IDS[i % len(CLINIC_IDS)],
            "clinician_id":     clinician_ids[i % len(clinician_ids)],
            "appointment_date": (NOW - timedelta(days=i+1)).strftime("%Y-%m-%d"),  # past dates
            "appointment_type": "follow_up",
            "status":           "scheduled",
            "created_at":       NOW_ISO,   #  newer than existing watermark
        })

    data.extend(new_rows)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    print(f"  Added {n} new appointments to {path.name}")


def main() -> None:
    print("=" * 55)
    print("Adding incremental sample data to source files")
    print(f"Timestamp: {NOW_ISO}  (newer than existing watermarks)")
    print("=" * 55)

    add_patients(n=3)
    add_appointments(n=5)

    print()
    print("Done. Now run the pipeline to load only these new rows:")
    print("  python -m ingestion.run_pipeline")
    print()
    print("Expected output:")
    print("  ingest_patients      3 rows  [success]")
    print("  ingest_appointments  5 rows  [success]")
    print("  (all others: 0 rows — watermark covers existing data)")


if __name__ == "__main__":
    main()
