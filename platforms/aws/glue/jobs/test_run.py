import boto3
import time

client   = boto3.client('glue', region_name='us-east-1')
BUCKET   = 'de-labs-sgorripati-raw'
BASE_KEY = 'dev/{domain}/{table}/2026/04/07/190838/{table}.parquet'

runs = [
    ('de-labs-glue-insurance', 'insurance', 'claims'),
    ('de-labs-glue-insurance', 'insurance', 'policies'),
    ('de-labs-glue-pharma',    'pharma',    'adverse_events'),
    ('de-labs-glue-pharma',    'pharma',    'drugs'),
]

job_runs = []

for job_name, domain, table in runs:
    key = f"dev/{domain}/{table}/2026/04/07/190838/{table}.parquet"
    response = client.start_job_run(
        JobName=job_name,
        Arguments={
            '--source_bucket': BUCKET,
            '--source_key'   : key,
            '--domain'       : domain,
            '--table'        : table,
            '--env'          : 'dev'
        }
    )
    run_id = response['JobRunId']
    job_runs.append((job_name, domain, table, run_id))
    print(f"Started {job_name} / {table} — RunId: {run_id}")

print(f"\nMonitoring {len(job_runs)} jobs...\n")

while True:
    all_done = True
    for job_name, domain, table, run_id in job_runs:
        resp   = client.get_job_run(JobName=job_name, RunId=run_id)
        status = resp['JobRun']['JobRunState']
        print(f"  {table:<25} {status}")
        if status not in ('SUCCEEDED', 'FAILED', 'STOPPED', 'TIMEOUT'):
            all_done = False
    print()
    if all_done:
        break
    time.sleep(30)

print("All jobs complete")