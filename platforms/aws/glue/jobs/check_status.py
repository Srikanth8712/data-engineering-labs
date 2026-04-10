import boto3
import time

client = boto3.client('glue', region_name='us-east-1')

RUN_ID   = "jr_62e241d50455433a9519970bd0f8f674471c2fd3df1b9ed75061d6535c6a6b98"
JOB_NAME = "de-labs-glue-insurance"

print(f"Monitoring {JOB_NAME} / {RUN_ID}\n")

while True:
    response = client.get_job_run(JobName=JOB_NAME, RunId=RUN_ID)
    run      = response['JobRun']
    status   = run['JobRunState']
    print(f"Status: {status}")

    if 'ErrorMessage' in run:
        print(f"Error : {run['ErrorMessage']}")

    if status in ('SUCCEEDED', 'FAILED', 'STOPPED', 'TIMEOUT'):
        break

    time.sleep(30)

print(f"\nFinal status: {status}")