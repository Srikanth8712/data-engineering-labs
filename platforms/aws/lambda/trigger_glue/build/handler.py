import json
import boto3
import psycopg2
import logging
import os
from datetime import datetime, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

glue = boto3.client("glue", region_name="us-east-1")

DOMAIN_JOB_MAP = {
    "insurance" : "de-labs-glue-insurance",
    "pharma"    : "de-labs-glue-pharma",
}


def get_domain(s3_key: str) -> str:
    parts = s3_key.split("/")
    return parts[1] if len(parts) > 1 else "unknown"


def get_table(s3_key: str) -> str:
    parts = s3_key.split("/")
    return parts[2] if len(parts) > 2 else "unknown"


def log_to_rds(pipeline_name, source_table, target_location, status, error_message=None):
    try:
        conn = psycopg2.connect(
            host     = os.environ["RDS_HOST"],
            port     = int(os.environ.get("RDS_PORT", "5432")),
            dbname   = os.environ["RDS_DB"],
            user     = os.environ["RDS_USER"],
            password = os.environ["RDS_PASSWORD"]
        )
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO audit.pipeline_runs
                (pipeline_name, source_table, target_location,
                 status, started_at, completed_at, error_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            pipeline_name, source_table, target_location,
            status,
            datetime.now(timezone.utc),
            datetime.now(timezone.utc),
            error_message
        ))
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning(f"Audit log failed (non-fatal): {e}")


def lambda_handler(event, context):
    logger.info(f"Event: {json.dumps(event)}")

    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key    = record["s3"]["object"]["key"]

        if not key.endswith(".parquet"):
            logger.info(f"Skipping non-parquet: {key}")
            continue

        domain = get_domain(key)
        table  = get_table(key)
        job    = DOMAIN_JOB_MAP.get(domain)

        if not job:
            logger.warning(f"No Glue job mapped for domain: {domain}")
            continue

        logger.info(f"Starting Glue job {job} for s3://{bucket}/{key}")

        try:
            response   = glue.start_job_run(
                JobName   = job,
                Arguments = {
                    "--source_bucket" : bucket,
                    "--source_key"    : key,
                    "--domain"        : domain,
                    "--table"         : table,
                    "--env"           : os.environ.get("ENV", "dev")
                }
            )
            job_run_id = response["JobRunId"]
            logger.info(f"Glue job started — JobRunId: {job_run_id}")
            log_to_rds(
                pipeline_name   = f"glue_trigger_{domain}_{table}",
                source_table    = f"s3://{bucket}/{key}",
                target_location = f"glue://{job}/{job_run_id}",
                status          = "triggered"
            )

        except glue.exceptions.EntityNotFoundException:
            msg = f"Glue job {job} not found — create it first"
            logger.error(msg)
            log_to_rds(
                pipeline_name   = f"glue_trigger_{domain}_{table}",
                source_table    = f"s3://{bucket}/{key}",
                target_location = f"glue://{job}",
                status          = "failed",
                error_message   = msg
            )

        except Exception as e:
            logger.error(f"Failed to start Glue job: {e}")
            log_to_rds(
                pipeline_name   = f"glue_trigger_{domain}_{table}",
                source_table    = f"s3://{bucket}/{key}",
                target_location = f"glue://{job}",
                status          = "failed",
                error_message   = str(e)
            )

    return {"statusCode": 200, "body": "trigger_glue complete"}