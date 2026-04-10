import boto3

client   = boto3.client('glue', region_name='us-east-1')
run_id   = 'jr_9d334aa67faa91c0219824ee04327e9eb1b40cf1334ce61c72d2af3212d76681'

resp   = client.get_job_run(JobName='de-labs-glue-pharma', RunId=run_id)
run    = resp['JobRun']
print(f"Status : {run['JobRunState']}")
print(f"Error  : {run.get('ErrorMessage', 'No error message')}")