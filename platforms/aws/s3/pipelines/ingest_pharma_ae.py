import boto3
import json
import sys
import os
from datetime import datetime, timezone
from pathlib import Path

# add project root to path so shared/ is importable
ROOT = Path(__file__).resolve().parent.parent.parent.parent.parent
sys.path.insert(0, str(ROOT))

from shared.config import (
    AWS_REGION, RAW_BUCKET, PROCESSED_BUCKET,
    PHARMA_RAW_PREFIX, PHARMA_PROCESSED_PREFIX,
    PHARMA_PII_FIELDS, ENV, validate
)
from shared.utils.pii_masker import mask_records

validate()

s3          = boto3.client("s3", region_name=AWS_REGION)
TIMESTAMP   = datetime.now(timezone.utc).strftime("%Y/%m/%d/%H%M%S")
DATA_FILE   = Path(__file__).resolve().parents[4] / "shared/sample_data/pharma_adverse_events.json"


def upload_raw(records: list):
    key = f"{PHARMA_RAW_PREFIX}/{TIMESTAMP}/ae_raw.json"
    s3.put_object(
        Bucket=RAW_BUCKET,
        Key=key,
        Body=json.dumps(records, indent=2),
        ServerSideEncryption="AES256",
        ContentType="application/json",
        Tagging=(
            f"env={ENV}"
            f"&domain=pharma"
            f"&classification=sensitive"
            f"&compliance=hipaa-candidate"
        )
    )
    print(f"[RAW]  s3://{RAW_BUCKET}/{key}")
    return key


def upload_processed(records: list):
    masked = mask_records(records, PHARMA_PII_FIELDS)
    key    = f"{PHARMA_PROCESSED_PREFIX}/{TIMESTAMP}/ae_processed.json"
    s3.put_object(
        Bucket=PROCESSED_BUCKET,
        Key=key,
        Body=json.dumps(masked, indent=2),
        ServerSideEncryption="AES256",
        ContentType="application/json",
        Tagging=(
            f"env={ENV}"
            f"&domain=pharma"
            f"&classification=internal"
        )
    )
    print(f"[PROC] s3://{PROCESSED_BUCKET}/{key}")
    return key


def summarise(records: list):
    by_severity = {}
    by_drug     = {}
    for r in records:
        by_severity[r["severity"]] = by_severity.get(r["severity"], 0) + 1
        by_drug[r["drug_name"]]    = by_drug.get(r["drug_name"], 0) + 1
    severe = [r["event_id"] for r in records if r["severity"] == "severe"]
    print("\n── Adverse events summary ──────────────")
    print(f"  Records      : {len(records)}")
    print(f"  By severity  : {by_severity}")
    print(f"  By drug      : {by_drug}")
    if severe:
        print(f"  Severe events: {severe}")
    print("────────────────────────────────────────\n")


if __name__ == "__main__":
    print(f"\nRunning in ENV={ENV}\n")
    records = json.loads(DATA_FILE.read_text())
    upload_raw(records)
    upload_processed(records)
    summarise(records)
    print("Done. Check both buckets in AWS Console.")