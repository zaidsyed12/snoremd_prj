"""
Ingests clinician_notes.json -> RAW.CLINICIAN_NOTES
Strategy: Incremental on created_at
"""

from __future__ import annotations
import json
import pandas as pd
from ingestion.base_ingester import BaseIngester


class ClinicianNotesIngester(BaseIngester):
    TABLE = "CLINICIAN_NOTES"

    def __init__(self):
        super().__init__("ingest_clinician_notes")

    def extract(self, file_path: str) -> pd.DataFrame:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        df = pd.DataFrame(data)
        df["note_date"] = pd.to_datetime(df["note_date"])
        df["created_at"] = pd.to_datetime(df["created_at"])
        self.log.debug(f"Columns: {list(df.columns)}")
        return df

    def filter_incremental(self, df: pd.DataFrame) -> pd.DataFrame:
        watermark = self.get_max_watermark(self.TABLE, "CREATED_AT")
        if watermark is None:
            return df
        df["created_at"] = pd.to_datetime(df["created_at"]).dt.tz_localize(None)
        watermark = pd.Timestamp(watermark).tz_localize(None)
        new_rows = df[df["created_at"] > watermark]
        self.log.info(f"Watermark={watermark} → {len(new_rows)}/{len(df)} rows are new.")
        return new_rows

    def load(self, df: pd.DataFrame) -> int:
        return self.write_to_snowflake(df, self.TABLE)


if __name__ == "__main__":
    from pathlib import Path
    ClinicianNotesIngester().run(str(Path(__file__).parent.parent / "data/raw/clinician_notes.json"))
