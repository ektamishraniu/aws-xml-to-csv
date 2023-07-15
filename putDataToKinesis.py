import boto3
import time
import json

# The kinesis stream I defined in asw console
stream_name = 'testKinesis1'

k_client = boto3.client('kinesis', region_name='us-east-2')

def put_to_stream(bucket, filename):
    payload = {
                'srcBucket': str(bucket),
                'FileName': str(filename)
              }
    print (payload)
    put_response = k_client.put_record(
                        StreamName=stream_name,
                        Data=json.dumps(payload),
                        PartitionKey=bucket)
def lambda_handler(event, context):
    put_to_stream("srcBucket", "srcFile")
    time.sleep(1)
        
