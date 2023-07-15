from __future__ import print_function
import boto3
from datetime import datetime
import time
import json

def lambda_handler(event, context):
    print("event: ", event)
    print("context: ", context)
    
    my_stream_name = 'testKinesis1'
    kinesis_client = boto3.client('kinesis', region_name='us-east-2')
    response = kinesis_client.describe_stream(StreamName=my_stream_name)
    my_shard_id = response['StreamDescription']['Shards'][0]['ShardId']
    shard_iterator = kinesis_client.get_shard_iterator(StreamName=my_stream_name,
                                                      ShardId=my_shard_id,
                                                      ShardIteratorType='LATEST')

    my_shard_iterator = shard_iterator['ShardIterator']
    record_response = kinesis_client.get_records(ShardIterator=my_shard_iterator,Limit=2)
    print("record_response: ", record_response)
    while 'NextShardIterator' in record_response:
        record_response = kinesis_client.get_records(ShardIterator=record_response['NextShardIterator'],Limit=2)
        if(record_response['Records']):
            print ("record_response: ", record_response)