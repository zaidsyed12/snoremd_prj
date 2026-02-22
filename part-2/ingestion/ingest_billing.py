# Ingests billing_report.xlsx - RAW.BILLING

from __future__ import annotations
import pandas as pd
from ingestion.base_ingester import BaseIngester


class BillingIngester(BaseIngester):
    TABLE = "BILLING"

    def __init__(self):
        super().__init__("ingest_billing")

    def extract(self, file_path: str) -> pd.DataFrame:
        df = pd.read_excel(file_path, sheet_name="Billing", engine="openpyxl")
        df["service_date"] = pd.to_datetime(df["service_date"]).dt.date
        df["created_at"] = pd.to_datetime(df["created_at"])
        self.log.debug(f"Columns: {list(df.columns)}")
        return df

    def filter_incremental(self, df: pd.DataFrame) -> pd.DataFrame:
        self.log.info("Billing uses full refresh strategy — loading all rows.")
        return df

    def load(self, df: pd.DataFrame) -> int:
        self.log.info(f"Truncating RAW.{self.TABLE} before full reload.")
        return self.write_to_snowflake(df, self.TABLE, overwrite=True)


if __name__ == "__main__":
    from pathlib import Path
    BillingIngester().run(str(Path(__file__).parent.parent / "data/raw/billing_report.xlsx"))
