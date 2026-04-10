import boto3
import time

client = boto3.client('glue', region_name='us-east-1')
BUCKET = 'de-labs-sgorripati-raw'

runs = [
    ('de-labs-glue-pharma', 'pharma', 'adverse_events'),
    ('de-labs-glue-pharma', 'pharma', 'drugs'),
]

job_runs = []

for job_name, domain, table in runs:
    key      = f"dev/{domain}/{table}/2026/04/07/190838/{table}.parquet"
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
    print(f"Started {table:<25} RunId: {run_id}")
    time.sleep(2)

print(f"\nMonitoring...\n")

while True:
    all_done = True
    for job_name, domain, table, run_id in job_runs:
        resp   = client.get_job_run(JobName=job_name, RunId=run_id)
        status = resp['JobRun']['JobRunState']
        error  = resp['JobRun'].get('ErrorMessage', '')
        print(f"  {table:<25} {status}")
        if error:
            print(f"    Error: {error}")
        if status not in ('SUCCEEDED', 'FAILED', 'STOPPED', 'TIMEOUT'):
            all_done = False
    print()
    if all_done:
        break
    time.sleep(30)

print("Done")