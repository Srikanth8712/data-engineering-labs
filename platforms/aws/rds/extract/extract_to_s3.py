import sys
import boto3
import psycopg2
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
from pathlib import Path
from datetime import datetime, timezone

ROOT = next(p for p in Path(__file__).resolve().parents if (p / "shared").exists())
sys.path.insert(0, str(ROOT))

from shared.config import (
    RDS_HOST, RDS_PORT, RDS_DB, RDS_USER, RDS_PASSWORD,
    AWS_REGION, RAW_BUCKET, ENV
)

s3  = boto3.client("s3", region_name=AWS_REGION)
RUN_TS = datetime.now(timezone.utc).strftime("%Y/%m/%d/%H%M%S")


# ── DB connection ─────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(
        host=RDS_HOST, port=RDS_PORT,
        dbname=RDS_DB, user=RDS_USER,
        password=RDS_PASSWORD
    )


# ── Core extract functions ────────────────────────────────────────────

def extract_table(conn, schema: str, table: str) -> pd.DataFrame:
    """Read full table into a DataFrame."""
    query = f"SELECT * FROM {schema}.{table}"
    df = pd.read_sql(query, conn)
    print(f"  Extracted {len(df):>4} rows from {schema}.{table}")
    return df


def df_to_parquet_buffer(df: pd.DataFrame) -> bytes:
    """Serialize DataFrame to Parquet bytes in memory."""
    table  = pa.Table.from_pandas(df, preserve_index=False)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    return buffer.getvalue()


def upload_to_s3(
    data: bytes,
    s3_key: str,
    domain: str,
    table: str,
    record_count: int
):
    """Upload Parquet bytes to S3 with compliance tags."""
    s3.put_object(
        Bucket=RAW_BUCKET,
        Key=s3_key,
        Body=data,
        ServerSideEncryption="AES256",
        ContentType="application/octet-stream",
        Tagging=(
            f"env={ENV}"
            f"&domain={domain}"
            f"&table={table}"
            f"&record_count={record_count}"
            f"&classification=sensitive"
            f"&format=parquet"
            f"&ingestion_ts={datetime.now(timezone.utc).date()}"
        )
    )
    size_kb = len(data) / 1024
    print(f"  Uploaded  s3://{RAW_BUCKET}/{s3_key} ({size_kb:.1f} KB)")


def log_pipeline_run(
    conn,
    pipeline_name: str,
    source_table: str,
    target_location: str,
    records_extracted: int,
    status: str,
    started_at: datetime,
    error_message: str = None
):
    """Write audit record to RDS pipeline_runs table."""
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO audit.pipeline_runs
            (pipeline_name, source_table, target_location,
             records_extracted, status, started_at,
             completed_at, error_message)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        pipeline_name,
        source_table,
        target_location,
        records_extracted,
        status,
        started_at,
        datetime.now(timezone.utc),
        error_message
    ))
    conn.commit()
    cur.close()


# ── Extract jobs ──────────────────────────────────────────────────────

def run_extract(
    conn,
    schema: str,
    table: str,
    domain: str
):
    started_at = datetime.now(timezone.utc)
    s3_key     = f"{ENV}/{domain}/{table}/{RUN_TS}/{table}.parquet"

    try:
        df      = extract_table(conn, schema, table)
        data    = df_to_parquet_buffer(df)
        upload_to_s3(data, s3_key, domain, table, len(df))
        log_pipeline_run(
            conn,
            pipeline_name     = f"rds_extract_{domain}_{table}",
            source_table      = f"{schema}.{table}",
            target_location   = f"s3://{RAW_BUCKET}/{s3_key}",
            records_extracted = len(df),
            status            = "success",
            started_at        = started_at
        )
        return len(df)

    except Exception as e:
        log_pipeline_run(
            conn,
            pipeline_name     = f"rds_extract_{domain}_{table}",
            source_table      = f"{schema}.{table}",
            target_location   = f"s3://{RAW_BUCKET}/{s3_key}",
            records_extracted = 0,
            status            = "failed",
            started_at        = started_at,
            error_message     = str(e)
        )
        raise


# ── Main ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"\n{'='*52}")
    print(f"  RDS → S3 Extract")
    print(f"  ENV    : {ENV}")
    print(f"  Bucket : {RAW_BUCKET}")
    print(f"  Run TS : {RUN_TS}")
    print(f"{'='*52}\n")

    conn = get_conn()

    extracts = [
        ("insurance", "claims",          "insurance"),
        ("insurance", "policies",        "insurance"),
        ("pharma",    "adverse_events",  "pharma"),
        ("pharma",    "drugs",           "pharma"),
    ]

    total_records = 0
    print("Extracting tables:\n")

    for schema, table, domain in extracts:
        count         = run_extract(conn, schema, table, domain)
        total_records += count

    conn.close()

    print(f"\n{'='*52}")
    print(f"  Extract complete")
    print(f"  Total records : {total_records}")
    print(f"{'='*52}\n")