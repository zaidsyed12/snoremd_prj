# Generating all five mock data sources for the Snore MD pipeline.

import json
import os
import random
from datetime import datetime, timedelta, date
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker

fake = Faker("en_CA")
random.seed(42)
np.random.seed(42)

# Output directory 
RAW_DIR = Path(__file__).parent / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

# Reference data
CLINICS = [
    {"clinic_id": "CLN001", "clinic_name": "Snore MD Toronto", "city": "Toronto", "province": "ON"},
    {"clinic_id": "CLN002", "clinic_name": "Snore MD Vancouver", "city": "Vancouver", "province": "BC"},
    {"clinic_id": "CLN003", "clinic_name": "Snore MD Calgary", "city": "Calgary", "province": "AB"},
    {"clinic_id": "CLN004", "clinic_name": "Snore MD Ottawa", "city": "Ottawa", "province": "ON"},
    {"clinic_id": "CLN005", "clinic_name": "Snore MD Montreal", "city": "Montreal", "province": "QC"},
]

SPECIALTIES = ["Sleep Medicine", "Respirology", "Otolaryngology", "General Practice"]
INSURANCE_PROVIDERS = ["Sun Life", "Manulife", "Blue Cross", "Great-West Life", "Canada Life"]
SERVICE_CODES = {
    "A001": "Initial Sleep Consultation",
    "A002": "Follow-Up Consultation",
    "B001": "Polysomnography (In-Lab)",
    "B002": "Home Sleep Test",
    "C001": "CPAP Setup & Education",
    "C002": "Treatment Review",
}

# Helper functions

def rand_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def rand_ts(start: date, end: date) -> datetime:
    d = rand_date(start, end)
    return datetime(d.year, d.month, d.day,
                    random.randint(7, 18), random.randint(0, 59))


def days_ago(n: int) -> datetime:
    return datetime.now() - timedelta(days=n)


# 1. Clinicians (embedded in patient/appointment data) 

def build_clinicians(n_per_clinic: int = 4) -> list[dict]:
    clinicians = []
    for clinic in CLINICS:
        for _ in range(n_per_clinic):
            clinicians.append({
                "clinician_id": fake.uuid4(),
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "specialty": random.choice(SPECIALTIES),
                "clinic_id": clinic["clinic_id"],
            })
    return clinicians


CLINICIANS = build_clinicians()
CLINIC_IDS = [c["clinic_id"] for c in CLINICS]
CLINICIAN_BY_CLINIC: dict[str, list] = {}
for cl in CLINICIANS:
    CLINICIAN_BY_CLINIC.setdefault(cl["clinic_id"], []).append(cl["clinician_id"])


# 2. Patients 

def build_patients(n: int = 120) -> pd.DataFrame:
    rows = []
    start = date(2022, 1, 1)
    end = date(2024, 6, 30)
    for _ in range(n):
        clinic_id = random.choice(CLINIC_IDS)
        created = rand_ts(start, end)
        updated = created + timedelta(days=random.randint(0, 90))
        rows.append({
            "patient_id": fake.uuid4(),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=80).isoformat(),
            "gender": random.choice(["M", "F", "Other"]),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "clinic_id": clinic_id,
            "is_active": random.choices([True, False], weights=[90, 10])[0],
            "created_at": created.isoformat(),
            "updated_at": updated.isoformat(),
        })
    return pd.DataFrame(rows)


# 3. Appointments

def build_appointments(patients: pd.DataFrame, n: int = 350) -> list[dict]:
    appts = []
    patient_ids = patients["patient_id"].tolist()
    clinic_map = dict(zip(patients["patient_id"], patients["clinic_id"]))

    appt_types = ["initial_consult", "follow_up", "sleep_study_review", "cpap_followup"]
    statuses = ["completed", "completed", "completed", "cancelled", "no_show", "scheduled"]

    start = date(2023, 1, 1)
    end = date(2024, 9, 30)

    for _ in range(n):
        pid = random.choice(patient_ids)
        clinic_id = clinic_map[pid]
        clinician_id = random.choice(CLINICIAN_BY_CLINIC[clinic_id])
        appt_date = rand_date(start, end)
        created = datetime(appt_date.year, appt_date.month, appt_date.day) - timedelta(days=random.randint(1, 30))
        appts.append({
            "appointment_id": fake.uuid4(),
            "patient_id": pid,
            "clinic_id": clinic_id,
            "clinician_id": clinician_id,
            "appointment_date": appt_date.isoformat(),
            "appointment_type": random.choice(appt_types),
            "status": random.choice(statuses),
            "duration_minutes": random.choice([15, 30, 45, 60]),
            "created_at": created.isoformat(),
        })

    appts.sort(key=lambda x: (x["patient_id"], x["appointment_date"]))
    return appts


# 4. Sleep studies

def build_sleep_studies(patients: pd.DataFrame, n: int = 200) -> pd.DataFrame:
    rows = []
    patient_ids = patients["patient_id"].tolist()
    clinic_map = dict(zip(patients["patient_id"], patients["clinic_id"]))

    study_types = ["polysomnography", "home_sleep_test"]
    statuses = ["completed", "completed", "completed", "failed", "pending"]

    start = date(2023, 1, 1)
    end = date(2024, 9, 30)

    for _ in range(n):
        pid = random.choice(patient_ids)
        clinic_id = clinic_map[pid]
        clinician_id = random.choice(CLINICIAN_BY_CLINIC[clinic_id])
        study_date = rand_date(start, end)
        status = random.choice(statuses)

        ahi = round(np.random.exponential(scale=12), 1) if status == "completed" else None
        odi = round(ahi * random.uniform(0.7, 1.1), 1) if ahi else None
        spo2 = round(random.uniform(82, 96), 1) if status == "completed" else None
        report_at = (
            datetime(study_date.year, study_date.month, study_date.day)
            + timedelta(days=random.randint(3, 14))
        ).isoformat() if status == "completed" else None

        rows.append({
            "study_id": fake.uuid4(),
            "patient_id": pid,
            "clinic_id": clinic_id,
            "clinician_id": clinician_id,
            "study_date": study_date.isoformat(),
            "study_type": random.choice(study_types),
            "status": status,
            "ahi_score": ahi,
            "odi_score": odi,
            "spo2_nadir": spo2,
            "report_generated_at": report_at,
            "created_at": datetime(study_date.year, study_date.month, study_date.day).isoformat(),
        })
    return pd.DataFrame(rows)


# 5. Clinician notes

def build_clinician_notes(appointments: list[dict], n: int = 160) -> list[dict]:
    notes = []
    completed_appts = [a for a in appointments if a["status"] == "completed"]
    sample = random.sample(completed_appts, min(n, len(completed_appts)))

    note_types = ["assessment", "treatment_plan", "follow_up", "progress_note"]
    templates = [
        "Patient presents with {symptom}. Recommend {treatment}.",
        "Follow-up visit. Patient reports {outcome}. Plan to {plan}.",
        "Sleep study results reviewed. AHI indicates {severity} sleep apnea.",
        "CPAP compliance noted. Patient {compliance}. Next review in {weeks} weeks.",
    ]
    symptoms = ["excessive daytime sleepiness", "loud snoring", "morning headaches", "insomnia"]
    treatments = ["polysomnography", "CPAP therapy", "lifestyle modifications", "referral to ENT"]
    outcomes = ["improvement in sleep quality", "ongoing fatigue", "better CPAP adherence"]
    plans = ["adjust CPAP pressure", "continue current therapy", "schedule follow-up"]
    severities = ["mild", "moderate", "severe"]
    compliance = ["is compliant with CPAP", "has difficulty with CPAP", "reports good tolerance"]

    for appt in sample:
        note_date = (datetime.fromisoformat(appt["appointment_date"]) + timedelta(hours=1)).isoformat()
        template = random.choice(templates)
        content = template.format(
            symptom=random.choice(symptoms),
            treatment=random.choice(treatments),
            outcome=random.choice(outcomes),
            plan=random.choice(plans),
            severity=random.choice(severities),
            compliance=random.choice(compliance),
            weeks=random.choice([4, 6, 8, 12]),
        )
        notes.append({
            "note_id": fake.uuid4(),
            "patient_id": appt["patient_id"],
            "clinician_id": appt["clinician_id"],
            "appointment_id": appt["appointment_id"],
            "note_date": note_date,
            "note_type": random.choice(note_types),
            "content": content,
            "created_at": note_date,
        })
    return notes


# 6. Billing report

def build_billing(patients: pd.DataFrame, appointments: list[dict], n: int = 220) -> pd.DataFrame:
    rows = []
    completed_appts = [a for a in appointments if a["status"] == "completed"]
    sample = random.sample(completed_appts, min(n, len(completed_appts)))
    clinic_map = dict(zip(patients["patient_id"], patients["clinic_id"]))
    billing_statuses = ["paid", "paid", "approved", "pending", "denied"]

    for appt in sample:
        svc_code = random.choice(list(SERVICE_CODES.keys()))
        svc_date = datetime.fromisoformat(appt["appointment_date"])
        amount = round(random.uniform(75, 450), 2)
        rows.append({
            "billing_id": fake.uuid4(),
            "patient_id": appt["patient_id"],
            "clinic_id": clinic_map.get(appt["patient_id"], appt["clinic_id"]),
            "appointment_id": appt["appointment_id"],
            "service_date": svc_date.date().isoformat(),
            "service_code": svc_code,
            "service_description": SERVICE_CODES[svc_code],
            "amount": amount,
            "insurance_provider": random.choice(INSURANCE_PROVIDERS),
            "billing_status": random.choice(billing_statuses),
            "billing_month": svc_date.strftime("%Y-%m"),
            "created_at": svc_date.isoformat(),
        })
    return pd.DataFrame(rows)


# Main

def main():
    print("Generating mock data for Snore MD pipeline...")

    # Patients
    patients = build_patients(120)
    patients.to_csv(RAW_DIR / "patients.csv", index=False)
    print(f"  patients.csv            → {len(patients)} rows")

    # Appointments (JSON — to simulate REST API payload)
    appointments = build_appointments(patients, 350)
    with open(RAW_DIR / "appointments.json", "w") as f:
        json.dump(appointments, f, indent=2, default=str)
    print(f"  appointments.json       → {len(appointments)} rows")

    # Sleep studies
    studies = build_sleep_studies(patients, 200)
    studies.to_csv(RAW_DIR / "sleep_studies.csv", index=False)
    print(f"  sleep_studies.csv       → {len(studies)} rows")

    # Clinician notes (JSON)
    notes = build_clinician_notes(appointments, 160)
    with open(RAW_DIR / "clinician_notes.json", "w") as f:
        json.dump(notes, f, indent=2, default=str)
    print(f"  clinician_notes.json    → {len(notes)} rows")

    # Billing (Excel)
    billing = build_billing(patients, appointments, 220)
    billing.to_excel(RAW_DIR / "billing_report.xlsx", index=False, sheet_name="Billing")
    print(f"  billing_report.xlsx     → {len(billing)} rows")

    # Also save clinics + clinicians as CSVs (used in Snowflake setup seed load)
    pd.DataFrame(CLINICS).to_csv(RAW_DIR / "clinics.csv", index=False)
    pd.DataFrame(CLINICIANS).to_csv(RAW_DIR / "clinicians.csv", index=False)
    print(f"  clinics.csv             → {len(CLINICS)} rows")
    print(f"  clinicians.csv          → {len(CLINICIANS)} rows")

    print("\nAll mock data files written to data/raw/")


if __name__ == "__main__":
    main()
