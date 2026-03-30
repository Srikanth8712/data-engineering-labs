import sys
import os
import boto3
import json
from datetime import datetime, timezone
from pathlib import Path

# walk up from this file until we find the project root
# (the folder that contains both 'shared' and 'platforms')
_here = Path(__file__).resolve()
ROOT = next(p for p in _here.parents if (p / "shared").exists())
sys.path.insert(0, str(ROOT))


from shared.config import (
    AWS_REGION, RAW_BUCKET, PROCESSED_BUCKET,
    INSURANCE_RAW_PREFIX, INSURANCE_PROCESSED_PREFIX,
    INSURANCE_PII_FIELDS, ENV, validate
)
from shared.utils.pii_masker import mask_records

validate()

s3 = boto3.client("s3", region_name=AWS_REGION)
TIMESTAMP   = datetime.now(timezone.utc).strftime("%Y/%m/%d/%H%M%S")
DATA_FILE   = Path(__file__).resolve().parents[4] / "shared/sample_data/insurance_claims.json"

def upload_raw(records: list):
    key = f"{INSURANCE_RAW_PREFIX}/{TIMESTAMP}/claims_raw.json"
    s3.put_object(
        Bucket=RAW_BUCKET,
        Key=key,
        Body=json.dumps(records, indent=2),
        ServerSideEncryption="AES256",
        ContentType="application/json",
        Tagging=(
            f"env={ENV}"
            f"&domain=insurance"
            f"&classification=sensitive"
            f"&pii=true"
            f"&ingestion_ts={datetime.now(timezone.utc).date()}"
        )
    )
    print(f"[RAW]  s3://{RAW_BUCKET}/{key}")
    return key


def upload_processed(records: list):
    masked  = mask_records(records, INSURANCE_PII_FIELDS)
    key     = f"{INSURANCE_PROCESSED_PREFIX}/{TIMESTAMP}/claims_masked.json"
    s3.put_object(
        Bucket=PROCESSED_BUCKET,
        Key=key,
        Body=json.dumps(masked, indent=2),
        ServerSideEncryption="AES256",
        ContentType="application/json",
        Tagging=(
            f"env={ENV}"
            f"&domain=insurance"
            f"&classification=internal"
            f"&pii=false"
            f"&pii_masked=true"
        )
    )
    print(f"[PROC] s3://{PROCESSED_BUCKET}/{key}")
    return key


def summarise(records: list):
    total     = sum(r["amount_claimed"] for r in records)
    by_status = {}
    by_type   = {}
    for r in records:
        by_status[r["status"]]     = by_status.get(r["status"], 0) + 1
        by_type[r["claim_type"]]   = by_type.get(r["claim_type"], 0) + 1
    print("\n── Claims summary ──────────────────────")
    print(f"  Records      : {len(records)}")
    print(f"  Total exposure: ${total:,.2f}")
    print(f"  By status    : {by_status}")
    print(f"  By type      : {by_type}")
    print("────────────────────────────────────────\n")


if __name__ == "__main__":
    print(f"\nRunning in ENV={ENV}\n")
    records = json.loads(DATA_FILE.read_text())
    upload_raw(records)
    upload_processed(records)
    summarise(records)
    print("Done. Check both buckets in AWS Console.")