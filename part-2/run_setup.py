import snowflake.connector
from dotenv import load_dotenv
import os

load_dotenv()

conn = snowflake.connector.connect(
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    role=os.getenv('SNOWFLAKE_ROLE'),
)

with open('snowflake_setup/setup.sql', 'r') as f:
    sql = f.read()

cur = conn.cursor()
for statement in sql.split(';'):
    stmt = statement.strip()
    # skip empty lines and comment-only blocks
    lines = [l for l in stmt.splitlines() if l.strip() and not l.strip().startswith('--')]
    if not lines:
        continue
    try:
        cur.execute(stmt)
        print(f"OK: {stmt[:80].strip()}")
    except Exception as e:
        print(f"SKIP ({e}): {stmt[:60].strip()}")

conn.close()
print("\nSetup complete.")
