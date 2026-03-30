import boto3, json
sqs = boto3.client("sqs")
sns = boto3.client("sns")
ses = boto3.client("ses")
def lambda_handler(event, context):
    for record in event["Records"]:
        alert = json.loads(record["body"])
        sns.publish(TopicArn="arn:aws:sns:us-east-1:123456789012:icu-alerts",
                    Message=json.dumps(alert))
        ses.send_email(Source="icu@hospital.com",
                       Destination={"ToAddresses":["doctor@hospital.com"]},
                       Message={"Subject":{"Data":"ICU Alert"},
                                "Body":{"Text":{"Data":str(alert)}}})
