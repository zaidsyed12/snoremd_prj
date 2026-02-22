"""
Ingests patients.csv - RAW.PATIENTS
Strategy: Incremental on updated_at (load rows newer than max(UPDATED_AT) in Snowflake)
"""

from __future__ import annotations
import pandas as pd
from ingestion.base_ingester import BaseIngester


class PatientsIngester(BaseIngester):
    TABLE = "PATIENTS"

    def __init__(self):
        super().__init__("ingest_patients")

    def extract(self, file_path: str) -> pd.DataFrame:
        df = pd.read_csv(file_path, parse_dates=["created_at", "updated_at", "date_of_birth"])
        self.log.debug(f"Columns: {list(df.columns)}")
        return df

    def filter_incremental(self, df: pd.DataFrame) -> pd.DataFrame:
        watermark = self.get_max_watermark(self.TABLE, "UPDATED_AT")
        if watermark is None:
            return df  # first load — take all rows
        # Normalise timezone-naive for comparison
        df["updated_at"] = pd.to_datetime(df["updated_at"]).dt.tz_localize(None)
        watermark = pd.Timestamp(watermark).tz_localize(None)
        new_rows = df[df["updated_at"] > watermark]
        self.log.info(f"Watermark={watermark} → {len(new_rows)}/{len(df)} rows are new/updated.")
        return new_rows

    def load(self, df: pd.DataFrame) -> int:
        return self.write_to_snowflake(df, self.TABLE)


if __name__ == "__main__":
    from pathlib import Path
    PatientsIngester().run(str(Path(__file__).parent.parent / "data/raw/patients.csv"))
