import json
import boto3
import pyarrow.parquet as pq
import io
import logging
import os
from datetime import datetime, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3     = boto3.client("s3")
lmbda  = boto3.client("lambda", region_name=os.environ.get("AWS_REGION", "us-east-1"))

SCHEMA_RULES = {
    "claims": {
        "required_columns": [
            "claim_id", "policy_number", "claimant_name",
            "claim_date", "claim_type", "amount_claimed", "status"
        ],
        "min_rows": 1
    },
    "policies": {
        "required_columns": [
            "policy_number", "policy_holder", "policy_type",
            "start_date", "end_date", "premium_amount", "status"
        ],
        "min_rows": 1
    },
    "adverse_events": {
        "required_columns": [
            "event_id", "report_date", "drug_name",
            "severity", "outcome", "country"
        ],
        "min_rows": 1
    },
    "drugs": {
        "required_columns": [
            "ndc_code", "drug_name", "manufacturer", "drug_class"
        ],
        "min_rows": 1
    },
}


def get_table_name(s3_key: str) -> str:
    parts = s3_key.split("/")
    return parts[2] if len(parts) > 2 else "unknown"


def validate_parquet(bucket: str, key: str) -> dict:
    response   = s3.get_object(Bucket=bucket, Key=key)
    data       = response["Body"].read()
    table      = pq.read_table(io.BytesIO(data))
    table_name = get_table_name(key)
    rules      = SCHEMA_RULES.get(table_name, {})

    result = {
        "bucket"      : bucket,
        "key"         : key,
        "table_name"  : table_name,
        "row_count"   : table.num_rows,
        "columns"     : table.schema.names,
        "file_size_kb": round(len(data) / 1024, 2),
        "validated_at": datetime.now(timezone.utc).isoformat(),
        "validation"  : {"passed": True, "errors": []}
    }

    min_rows = rules.get("min_rows", 1)
    if table.num_rows < min_rows:
        result["validation"]["errors"].append(
            f"Row count {table.num_rows} below minimum {min_rows}"
        )

    required = rules.get("required_columns", [])
    missing  = [c for c in required if c not in table.schema.names]
    if missing:
        result["validation"]["errors"].append(
            f"Missing required columns: {missing}"
        )

    result["validation"]["passed"] = len(result["validation"]["errors"]) == 0
    return result


def invoke_trigger_glue(original_event: dict):
    """Invoke Lambda 2 with the same S3 event."""
    lmbda.invoke(
        FunctionName   = "de-labs-trigger-glue",
        InvocationType = "Event",           # async — fire and forget
        Payload        = json.dumps(original_event).encode()
    )
    logger.info("Invoked de-labs-trigger-glue")


def lambda_handler(event, context):
    logger.info(f"Event received: {json.dumps(event)}")

    results       = []
    all_passed    = True

    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key    = record["s3"]["object"]["key"]

        if not key.endswith(".parquet"):
            logger.info(f"Skipping non-parquet file: {key}")
            continue

        logger.info(f"Validating s3://{bucket}/{key}")

        try:
            result = validate_parquet(bucket, key)

            if result["validation"]["passed"]:
                logger.info(
                    f"PASSED | {result['table_name']} | "
                    f"{result['row_count']} rows | "
                    f"{result['file_size_kb']} KB"
                )
            else:
                logger.error(
                    f"FAILED | {result['table_name']} | "
                    f"Errors: {result['validation']['errors']}"
                )
                all_passed = False

            results.append(result)

        except Exception as e:
            logger.error(f"Validation error for {key}: {str(e)}")
            all_passed = False
            results.append({
                "bucket"    : bucket,
                "key"       : key,
                "error"     : str(e),
                "validation": {"passed": False, "errors": [str(e)]}
            })

    # only invoke Lambda 2 if all files passed validation
    if all_passed and results:
        invoke_trigger_glue(event)
    elif not all_passed:
        logger.error("Validation failed — Glue job will NOT be triggered")

    return {
        "statusCode": 200,
        "body"      : json.dumps(results, default=str)
    }