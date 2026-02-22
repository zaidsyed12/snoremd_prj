"""
Seeds RAW.CLINICS and RAW.CLINICIANS from CSV files in data/raw/.
Run once after snowflake_setup/setup.sql (and after generating mock data).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from ingestion.base_ingester import BaseIngester

ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "data" / "raw"


class ClinicsLoader(BaseIngester):
    TABLE = "CLINICS"

    def __init__(self):
        super().__init__("load_clinics")

    def extract(self, file_path: str) -> pd.DataFrame:
        df = pd.read_csv(file_path)
        self.log.debug(f"Columns: {list(df.columns)}")
        return df

    def filter_incremental(self, df: pd.DataFrame) -> pd.DataFrame:
        self.log.info("Clinics — full refresh (small static reference table).")
        return df

    def load(self, df: pd.DataFrame) -> int:
        return self.write_to_snowflake(df, self.TABLE, overwrite=True)


class CliniciansLoader(BaseIngester):
    TABLE = "CLINICIANS"

    def __init__(self):
        super().__init__("load_clinicians")

    def extract(self, file_path: str) -> pd.DataFrame:
        df = pd.read_csv(file_path)
        self.log.debug(f"Columns: {list(df.columns)}")
        return df

    def filter_incremental(self, df: pd.DataFrame) -> pd.DataFrame:
        self.log.info("Clinicians — full refresh (small static reference table).")
        return df

    def load(self, df: pd.DataFrame) -> int:
        return self.write_to_snowflake(df, self.TABLE, overwrite=True)


def main():
    print("=" * 55)
    print("Loading reference tables into Snowflake RAW")
    print("=" * 55)
    clinics_rows = ClinicsLoader().run(str(RAW_DIR / "clinics.csv"))
    clinicians_rows = CliniciansLoader().run(str(RAW_DIR / "clinicians.csv"))
    print(f"  clinics      → {clinics_rows} rows loaded")
    print(f"  clinicians   → {clinicians_rows} rows loaded")
    print("Reference tables loaded successfully.")


if __name__ == "__main__":
    main()
