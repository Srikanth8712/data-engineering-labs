import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import hashlib
from datetime import datetime, timezone

# ── Job init ─────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "source_bucket",
    "source_key",
    "domain",
    "table",
    "env"
])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_BUCKET    = args["source_bucket"]
SOURCE_KEY       = args["source_key"]
DOMAIN           = args["domain"]
TABLE            = args["table"]
ENV              = args["env"]
PROCESSED_BUCKET = SOURCE_BUCKET.replace("raw", "processed")
RUN_TS           = datetime.now(timezone.utc).strftime("%Y/%m/%d/%H%M%S")


# ── PII masking UDF ───────────────────────────────────────────────────
def mask_value(value):
    if value is None:
        return None
    return hashlib.sha256(str(value).encode()).hexdigest()[:12]

mask_udf = F.udf(mask_value, StringType())


# ── Read source parquet ───────────────────────────────────────────────
source_path = f"s3://{SOURCE_BUCKET}/{SOURCE_KEY}"
print(f"Reading from {source_path}")
df = spark.read.parquet(source_path)
print(f"Source row count: {df.count()}")


# ── Transformations per table ─────────────────────────────────────────
if TABLE == "claims":
    df = (df
        .withColumn("claimant_name",    mask_udf(F.col("claimant_name")))
        .withColumn("ssn_last4",        mask_udf(F.col("ssn_last4")))
        .withColumn("amount_claimed",   F.col("amount_claimed").cast("double"))
        .withColumn("medical_expenses", F.col("medical_expenses").cast("double"))
        .withColumn("claim_year",       F.year(F.col("claim_date")))
        .withColumn("claim_month",      F.month(F.col("claim_date")))
        .withColumn("is_high_value",    F.when(F.col("amount_claimed") > 10000, True).otherwise(False))
        .withColumn("processed_at",     F.lit(datetime.now(timezone.utc).isoformat()))
        .withColumn("pii_masked",       F.lit(True))
    )

elif TABLE == "policies":
    df = (df
        .withColumn("policy_holder",  mask_udf(F.col("policy_holder")))
        .withColumn("premium_amount", F.col("premium_amount").cast("double"))
        .withColumn("coverage_limit", F.col("coverage_limit").cast("double"))
        .withColumn("policy_duration_days",
            F.datediff(F.col("end_date"), F.col("start_date")))
        .withColumn("processed_at",   F.lit(datetime.now(timezone.utc).isoformat()))
        .withColumn("pii_masked",     F.lit(True))
    )

elif TABLE == "adverse_events":
    df = (df
        .withColumn("is_severe",
            F.when(F.col("severity") == "severe", True).otherwise(False))
        .withColumn("is_ongoing",
            F.when(F.col("outcome") == "ongoing", True).otherwise(False))
        .withColumn("report_year",  F.year(F.col("report_date")))
        .withColumn("report_month", F.month(F.col("report_date")))
        .withColumn("processed_at", F.lit(datetime.now(timezone.utc).isoformat()))
        .withColumn("pii_masked",   F.lit(False))
    )

elif TABLE == "drugs":
    df = (df
        .withColumn("drug_name",    F.upper(F.col("drug_name")))
        .withColumn("drug_class",   F.lower(F.col("drug_class")))
        .withColumn("processed_at", F.lit(datetime.now(timezone.utc).isoformat()))
        .withColumn("pii_masked",   F.lit(False))
    )


# ── Write to S3 processed ─────────────────────────────────────────────
output_path = f"s3://{PROCESSED_BUCKET}/{ENV}/{DOMAIN}/{TABLE}/{RUN_TS}/"
print(f"Writing to {output_path}")

df.write.mode("overwrite").parquet(output_path)
print(f"Written {df.count()} rows to processed bucket")


# ── Commit job ────────────────────────────────────────────────────────
job.commit()
print("Glue job complete")