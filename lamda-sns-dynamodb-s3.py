import boto3
from uuid import uuid4



topic_arn = "arn:aws:sns:us-east-1:278529849729:p1"
def send_sns(message, subject):
    try:
        client = boto3.client("sns")
        result = client.publish(TopicArn=topic_arn, Message=message, Subject=subject)
        if result['ResponseMetadata']['HTTPStatusCode'] == 200:
            print(result)
            print("Notification send successfully..!!!")
            return True
    except Exception as e:
        print("Error occured while publish notifications and error is : ", e)
        return True


def lambda_handler(event, context):
    s3 = boto3.client("s3")
    dynamodb = boto3.resource('dynamodb')
    for record in event['Records']:
        bucket_name = record['s3']['bucket']['name']
        object_key = record['s3']['object']['key']
        size = record['s3']['object'].get('size', -1)
        event_name = record ['eventName']
        event_time = record['eventTime']
        dynamoTable = dynamodb.Table('project01db')
        dynamoTable.put_item(
            Item={'id': str(uuid4()), 'Bucket': bucket_name, 'Object': object_key,'Size': size, 'Event': event_name, 'EventTime': event_time})

        from_path = "s3://{}/{}".format(bucket_name, object_key)
        print("from path {}".format(from_path))
        message = "The file is uploaded at S3 bucket path {}".format(from_path)
        subject = "Processes completion Notification"
        SNSResult = send_sns(message, subject)
        if SNSResult :
            print("Notification Sent..") 
            return SNSResult
        else:
            return False
