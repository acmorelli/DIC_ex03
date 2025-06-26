import json
import boto3
import os
import string
import re

# we assume stopwords.txt is bundled with the Lambda zip
with open("stopwords.txt", "r") as f:
    STOPWORDS = set(line.strip().lower() for line in f)

s3 = boto3.client("s3")
ddb = boto3.resource("dynamodb")
ssm = boto3.client("ssm")

def clean_text(text):
    # lowercase first
    text = text.lower()
    # extract only alphabetic words using regex
    words = re.findall(r"[a-zA-Z]+", text)
    # filter stopwords and one-letter words
    return [w for w in words if w not in STOPWORDS and len(w) > 1]

def handler(event, context):
    # parse S3 event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    # download review from S3
    obj = s3.get_object(Bucket=bucket, Key=key)
    review_data = json.loads(obj['Body'].read().decode('utf-8'))

    # fetch parameter from SSM for next bucket
    target_bucket = ssm.get_parameter(Name='/localstack/reviews/preprocessed')['Parameter']['Value']

    # clean review fields
    review_data['clean_summary'] = clean_text(review_data.get('summary', ''))
    review_data['clean_reviewText'] = clean_text(review_data.get('reviewText', ''))

    # write cleaned result to S3
    s3.put_object(
        Bucket=target_bucket,
        Key=key,
        Body=json.dumps(review_data).encode('utf-8')
    )

    return {
        'statusCode': 200,
        'body': f"Preprocessed review stored in {target_bucket}/{key}"
    }
