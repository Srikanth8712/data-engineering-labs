import boto3
import zipfile
import io

client   = boto3.client('lambda', region_name='us-east-1')
response = client.get_function(FunctionName='de-labs-validate-raw')
url      = response['Code']['Location']

import urllib.request
with urllib.request.urlopen(url) as r:
    data = r.read()

with zipfile.ZipFile(io.BytesIO(data)) as z:
    content = z.read('handler.py').decode()
    print(content)