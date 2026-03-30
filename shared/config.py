import os
from dotenv import load_dotenv

load_dotenv()

# ── AWS ──────────────────────────────────────────────
AWS_REGION       = os.getenv("AWS_REGION", "us-east-1")
RAW_BUCKET       = os.getenv("RAW_BUCKET")
PROCESSED_BUCKET = os.getenv("PROCESSED_BUCKET")

# ── Databricks ───────────────────────────────────────
DATABRICKS_HOST       = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN      = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_CLUSTER_ID = os.getenv("DATABRICKS_CLUSTER_ID")

# ── Environment ──────────────────────────────────────
ENV = os.getenv("ENV", "dev")   # dev | tst | prod

# ── S3 key prefixes (partitioned by env) ─────────────
INSURANCE_RAW_PREFIX       = f"{ENV}/insurance/claims"
INSURANCE_PROCESSED_PREFIX = f"{ENV}/insurance/claims/processed"
PHARMA_RAW_PREFIX          = f"{ENV}/pharma/adverse_events"
PHARMA_PROCESSED_PREFIX    = f"{ENV}/pharma/adverse_events/processed"

# ── PII fields per domain ────────────────────────────
INSURANCE_PII_FIELDS = ["claimant_name", "ssn_last4"]
PHARMA_PII_FIELDS    = []   # AE data has no direct PII in this dataset


def validate():
    """Fail fast if critical config is missing."""
    missing = [k for k, v in {
        "RAW_BUCKET": RAW_BUCKET,
        "PROCESSED_BUCKET": PROCESSED_BUCKET,
    }.items() if not v]
    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {missing}\n"
            f"Check your .env file."
        )


if __name__ == "__main__":
    validate()
    print(f"ENV            : {ENV}")
    print(f"RAW_BUCKET     : {RAW_BUCKET}")
    print(f"PROCESSED_BUCKET: {PROCESSED_BUCKET}")
    print(f"AWS_REGION     : {AWS_REGION}")
    print("Config OK")