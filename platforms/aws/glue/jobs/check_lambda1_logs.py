import boto3

logs   = boto3.client('logs', region_name='us-east-1')
stream = '2026/04/10/[$LATEST]b5ef517b18f845e0a961023de4aea259'

events = logs.get_log_events(
    logGroupName ='/aws/lambda/de-labs-validate-raw',
    logStreamName=stream,
    limit=50
)

print()
for e in events['events']:
    msg = e['message'].strip()
    if msg:
        print(msg)
print()