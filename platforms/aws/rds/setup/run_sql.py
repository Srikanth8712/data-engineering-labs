import sys
from pathlib import Path

ROOT = next(p for p in Path(__file__).resolve().parents if (p / "shared").exists())
sys.path.insert(0, str(ROOT))

import psycopg2
from shared.config import RDS_HOST, RDS_PORT, RDS_DB, RDS_USER, RDS_PASSWORD

SQL_FILE = Path(__file__).parent / "create_tables.sql"

print(f"\nRunning {SQL_FILE.name} against {RDS_DB}...\n")

conn = psycopg2.connect(
    host=RDS_HOST, port=RDS_PORT,
    dbname=RDS_DB, user=RDS_USER, password=RDS_PASSWORD
)
conn.autocommit = True
cur = conn.cursor()

sql = SQL_FILE.read_text()
cur.execute(sql)

print("Tables created successfully. Verifying...\n")

cur.execute("""
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_schema IN ('insurance', 'pharma', 'audit')
    ORDER BY table_schema, table_name;
""")

rows = cur.fetchall()
current_schema = None
for schema, table in rows:
    if schema != current_schema:
        print(f"  {schema}/")
        current_schema = schema
    print(f"    {table}")

print(f"\n{len(rows)} tables created.")
cur.close()
conn.close()