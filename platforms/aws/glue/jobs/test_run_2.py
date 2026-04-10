import boto3
import time

client = boto3.client('glue', region_name='us-east-1')
BUCKET = 'de-labs-sgorripati-raw'

# increase concurrent runs limit on both jobs
for job_name, script in [
    ('de-labs-glue-insurance', 'insurance_transform.py'),
    ('de-labs-glue-pharma',    'pharma_transform.py')
]:
    client.update_job(
        JobName=job_name,
        JobUpdate={
            'Role': 'arn:aws:iam::480357465972:role/de-labs-glue-role',
            'Command': {
                'Name'          : 'glueetl',
                'ScriptLocation': f's3://de-labs-sgorripati-scripts/glue/jobs/{script}',
                'PythonVersion' : '3'
            },
            'ExecutionProperty': {'MaxConcurrentRuns': 4},
            'GlueVersion'      : '4.0',
            'NumberOfWorkers'  : 2,
            'WorkerType'       : 'G.1X',
            'Timeout'          : 10
        }
    )
    print(f"Updated {job_name}")

print("Concurrent runs limit updated\n")

runs = [
    ('de-labs-glue-insurance', 'insurance', 'claims'),
    ('de-labs-glue-insurance', 'insurance', 'policies'),
    ('de-labs-glue-pharma',    'pharma',    'adverse_events'),
    ('de-labs-glue-pharma',    'pharma',    'drugs'),
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

print(f"\nMonitoring {len(job_runs)} jobs...\n")

while True:
    all_done = True
    statuses = []
    for job_name, domain, table, run_id in job_runs:
        resp   = client.get_job_run(JobName=job_name, RunId=run_id)
        status = resp['JobRun']['JobRunState']
        statuses.append(f"  {table:<25} {status}")
        if status not in ('SUCCEEDED', 'FAILED', 'STOPPED', 'TIMEOUT'):
            all_done = False
    for s in statuses:
        print(s)
    print()
    if all_done:
        break
    time.sleep(30)

print("All jobs complete")