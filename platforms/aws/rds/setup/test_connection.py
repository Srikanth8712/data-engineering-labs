import sys
from pathlib import Path

ROOT = next(p for p in Path(__file__).resolve().parents if (p / "shared").exists())
sys.path.insert(0, str(ROOT))

import psycopg2
from shared.config import (
    RDS_HOST, RDS_PORT,
    RDS_DB, RDS_USER, RDS_PASSWORD
)

print(f"\nConnecting to RDS at {RDS_HOST}...")
print(f"Database : {RDS_DB}")
print(f"User     : {RDS_USER}\n")

try:
    conn = psycopg2.connect(
        host            = RDS_HOST,
        port            = RDS_PORT,
        dbname          = RDS_DB,
        user            = RDS_USER,
        password        = RDS_PASSWORD,
        connect_timeout = 10
    )
    cur = conn.cursor()
    cur.execute("SELECT version();")
    version = cur.fetchone()
    print(f"Connected successfully!")
    print(f"Version : {version[0]}\n")
    cur.close()
    conn.close()

except psycopg2.OperationalError as e:
    print(f"Connection failed: {e}")
    print("\nCommon causes:")
    print("  1. RDS status not yet Available — wait and retry")
    print("  2. Security group missing My IP rule on port 5432")
    print("  3. Public access not set to Yes")
    print("  4. Wrong endpoint in .env file")