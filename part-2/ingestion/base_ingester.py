"""
Base class for all Snore MD ingestion scripts.
  - Snowflake connection management
  - Incremental watermark logic (max timestamp per table)
  - DataFrame - Snowflake writer (via executemany INSERT)
  - Audit logging to RAW.INGESTION_LOG
"""

from __future__ import annotations
import math
import traceback
from abc import ABC, abstractmethod
from datetime import datetime
import pandas as pd
import snowflake.connector
from ingestion.config import get_snowflake_connection_params
from ingestion.logger import get_logger


class BaseIngester(ABC):
    """Abstract base for all source ingesters."""

    def __init__(self, source_name: str):
        self.source_name = source_name
        self.log = get_logger(source_name)
        self._conn: snowflake.connector.SnowflakeConnection | None = None

    # Connection 

    def connect(self) -> snowflake.connector.SnowflakeConnection:
        if self._conn is None or self._conn.is_closed():
            params = get_snowflake_connection_params()
            self.log.info(f"Connecting to Snowflake account={params['account']}")
            self._conn = snowflake.connector.connect(**params)
            self.log.info("Connected.")
        return self._conn

    def close(self):
        if self._conn and not self._conn.is_closed():
            self._conn.close()
            self.log.debug("Snowflake connection closed.")

    def _cursor(self):
        return self.connect().cursor()

    # Incremental watermark 

    def get_max_watermark(self, table: str, column: str) -> datetime | None:
        """
        Return the maximum value of `column` in `table` (RAW schema).
        Returns None if the table is empty, so the ingester loads everything.
        """
        sql = f"SELECT MAX({column}) FROM RAW.{table}"
        self.log.debug(f"Watermark query: {sql}")
        with self._cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
        val = row[0] if row else None
        self.log.info(f"Watermark for {table}.{column} = {val}")
        return val

    # Writer

    @staticmethod
    def _clean_value(v):
        """Convert pandas NaN/NaT to None; convert everything else to str-safe type."""
        if v is None or v is pd.NaT:
            return None
        try:
            if isinstance(v, float) and math.isnan(v):
                return None
        except TypeError:
            pass
        # pandas Timestamp / datetime → ISO string
        if hasattr(v, 'isoformat'):
            return v.isoformat()
        return v

    def write_to_snowflake(
        self,
        df: pd.DataFrame,
        table: str,
        schema: str = "RAW",
        overwrite: bool = False,
    ) -> int:
        """
        Write a DataFrame to Snowflake using executemany INSERT statements.
        Uses insecure_mode connection — avoids OCSP S3 staging issues.
        """
        if df.empty:
            self.log.info(f"No new rows to write to {schema}.{table}.")
            return 0

        df = df.copy()
        # Upper-case column names to match Snowflake DDL
        df.columns = [c.upper() for c in df.columns]

        full_table = f"{schema}.{table}"
        cols = ", ".join(df.columns)
        placeholders = ", ".join(["%s"] * len(df.columns))

        rows = [
            tuple(self._clean_value(v) for v in row)
            for row in df.itertuples(index=False)
        ]

        with self._cursor() as cur:
            if overwrite:
                self.log.info(f"Truncating {full_table} before reload.")
                cur.execute(f"TRUNCATE TABLE {full_table}")
            cur.executemany(
                f"INSERT INTO {full_table} ({cols}) VALUES ({placeholders})",
                rows,
            )

        self.log.info(f"Wrote {len(rows)} rows to {full_table}.")
        return len(rows)

    # Audit log

    def _log_result(
        self,
        file_name: str,
        rows: int,
        status: str,
        error: str | None = None,
    ):
        sql = """
            INSERT INTO RAW.INGESTION_LOG
                (source_name, file_name, rows_ingested, status, error_message)
            VALUES (%s, %s, %s, %s, %s)
        """
        with self._cursor() as cur:
            cur.execute(sql, (self.source_name, file_name, rows, status, error))
        self.log.debug(f"Audit log written: source={self.source_name} file={file_name} status={status}")

    # Template method

    def run(self, file_path: str) -> int:
        """
        Entry point. Calls `extract`, applies incremental filter, writes to Snowflake.
        Returns number of rows loaded (0 if nothing new).
        """
        self.log.info(f"=== Starting ingestion: {self.source_name} | file={file_path} ===")
        rows_loaded = 0
        try:
            self.connect()
            df = self.extract(file_path)
            self.log.info(f"Extracted {len(df)} raw rows from source.")

            df = self.filter_incremental(df)
            self.log.info(f"After incremental filter: {len(df)} rows to load.")

            rows_loaded = self.load(df)
            self._log_result(file_path, rows_loaded, "success")
            self.log.info(f"=== Finished: {rows_loaded} rows loaded ===")
        except Exception:
            err = traceback.format_exc()
            self.log.error(f"Ingestion failed:\n{err}")
            self._log_result(file_path, rows_loaded, "failed", err[:2000])
            raise
        finally:
            self.close()
        return rows_loaded

    # Abstract hooks

    @abstractmethod
    def extract(self, file_path: str) -> pd.DataFrame:
        """Read raw source data into a DataFrame."""

    @abstractmethod
    def filter_incremental(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter DataFrame to only new/changed rows."""

    @abstractmethod
    def load(self, df: pd.DataFrame) -> int:
        """Write the filtered DataFrame to Snowflake RAW. Return row count."""
