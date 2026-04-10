import boto3

logs = boto3.client('logs', region_name='us-east-1')

for fn in ['de-labs-validate-raw', 'de-labs-trigger-glue']:
    streams = logs.describe_log_streams(
        logGroupName=f'/aws/lambda/{fn}',
        orderBy='LastEventTime',
        descending=True,
        limit=1
    )
    stream = streams['logStreams'][0]
    print(f'\n{fn}')
    print(f'  Latest stream : {stream["logStreamName"]}')
    import datetime
    ts = stream["lastEventTimestamp"] / 1000
    print(f'  Last event    : {datetime.datetime.fromtimestamp(ts)}')

print()