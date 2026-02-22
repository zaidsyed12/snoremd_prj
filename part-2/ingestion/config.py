# config.py — Loading Snowflake connection parameters from .env


import os
from pathlib import Path
from dotenv import load_dotenv

_root = Path(__file__).resolve().parent.parent
load_dotenv(_root / ".env")


def get_snowflake_connection_params() -> dict:
    """Return a dict of Snowflake connector parameters sourced from .env."""
    required = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
    ]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(missing)}\n"
            f"Copy .env.example - .env and fill in your Snowflake credentials."
        )

    return {
        "account":       os.environ["SNOWFLAKE_ACCOUNT"],
        "user":          os.environ["SNOWFLAKE_USER"],
        "password":      os.environ["SNOWFLAKE_PASSWORD"],
        "role":          os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        "warehouse":     os.environ["SNOWFLAKE_WAREHOUSE"],
        "database":      os.environ["SNOWFLAKE_DATABASE"],
        "schema":        os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
        "insecure_mode": True, 
    }
