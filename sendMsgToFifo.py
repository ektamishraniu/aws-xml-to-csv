import json
import boto3
import ast

sqr = boto3.resource('sqs')
sqs = boto3.client('sqs')

def lambda_handler(event, context):
    srcBucket="pipedelimfiles-for-table"
    srcFile="repair_order/RO_3612514224_repair_order.csv"
    srcFile="repair_order/RO_3612514224_uptime_repair_order.csv"
    #srcFile="repair_order/RO_3612514224_downtime_repair_order.csv"
    srcFile="repair_order/20180817T170345260_330c6be4-ed12-4ae8-88b3-a3ac83815a66_RO_Create_repair_order.csv"
    #print("event: ", event)
    try:
        srcBucket=event['Records'][0]['s3']['bucket']['name']
        srcFile=event['Records'][0]['s3']['object']['key']
    except:
        print("Will be using default values from Lambda")
    print("fileName:     ", srcFile)
    print("sourceBucket: ", srcBucket)
    
    if "log_table" in srcFile:
        return {
            'statusCode': 200,
            'body': json.dumps('Log Tables will not be uploaded to Redshift')
        }        
    
    queue = sqr.get_queue_by_name(QueueName='sendToRedShiftUpload.fifo')
    response = queue.send_message(
        MessageBody= (srcBucket+":"+srcFile).strip(),
        MessageGroupId='testGrp1'
    )

    # The response is NOT a resource, but gives you a message ID and MD5
    print(response.get('MessageId'))
    print(response.get('MD5OfMessageBody'))
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }