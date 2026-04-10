import boto3

client = boto3.client('glue', region_name='us-east-1')

for job_name in ['de-labs-glue-insurance', 'de-labs-glue-pharma']:
    runs = client.get_job_runs(JobName=job_name, MaxResults=3)
    print(f"\n{job_name}")
    for run in runs['JobRuns']:
        print(f"  {run['JobRunState']:<12} {str(run['StartedOn'])[:19]}  {run.get('ErrorMessage','')[:60]}")