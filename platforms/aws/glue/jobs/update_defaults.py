import boto3

client = boto3.client('glue', region_name='us-east-1')

default_args = {
    '--job-language'                  : 'python',
    '--enable-metrics'                : 'true',
    '--enable-continuous-cloudwatch-log': 'true',
    '--source_bucket'                 : 'de-labs-sgorripati-raw',
    '--source_key'                    : 'dev/insurance/claims/2026/04/07/190838/claims.parquet',
    '--domain'                        : 'insurance',
    '--table'                         : 'claims',
    '--env'                           : 'dev'
}

client.update_job(
    JobName='de-labs-glue-insurance',
    JobUpdate={
        'Role'            : 'arn:aws:iam::480357465972:role/de-labs-glue-role',
        'DefaultArguments': default_args,
        'Command': {
            'Name'          : 'glueetl',
            'ScriptLocation': 's3://de-labs-sgorripati-scripts/glue/jobs/insurance_transform.py',
            'PythonVersion' : '3'
        },
        'GlueVersion'    : '4.0',
        'NumberOfWorkers': 2,
        'WorkerType'     : 'G.1X',
        'Timeout'        : 10
    }
)
print('de-labs-glue-insurance updated with default arguments')

default_args['--source_key'] = 'dev/pharma/adverse_events/2026/04/07/190838/adverse_events.parquet'
default_args['--domain']     = 'pharma'
default_args['--table']      = 'adverse_events'

client.update_job(
    JobName='de-labs-glue-pharma',
    JobUpdate={
        'Role'            : 'arn:aws:iam::480357465972:role/de-labs-glue-role',
        'DefaultArguments': default_args,
        'Command': {
            'Name'          : 'glueetl',
            'ScriptLocation': 's3://de-labs-sgorripati-scripts/glue/jobs/pharma_transform.py',
            'PythonVersion' : '3'
        },
        'GlueVersion'    : '4.0',
        'NumberOfWorkers': 2,
        'WorkerType'     : 'G.1X',
        'Timeout'        : 10
    }
)
print('de-labs-glue-pharma updated with default arguments')