"""
Ingests sleep_studies.csv - RAW.SLEEP_STUDIES
Strategy: Incremental on created_at
"""

from __future__ import annotations
import pandas as pd
from ingestion.base_ingester import BaseIngester


class SleepStudiesIngester(BaseIngester):
    TABLE = "SLEEP_STUDIES"

    def __init__(self):
        super().__init__("ingest_sleep_studies")

    def extract(self, file_path: str) -> pd.DataFrame:
        df = pd.read_csv(
            file_path,
            parse_dates=["study_date", "report_generated_at", "created_at"],
        )
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
    SleepStudiesIngester().run(str(Path(__file__).parent.parent / "data/raw/sleep_studies.csv"))
