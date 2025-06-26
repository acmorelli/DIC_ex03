import json
import boto3
import os
from profanityfilter import ProfanityFilter

s3 = boto3.client("s3")
ssm = boto3.client("ssm")
ddb = boto3.resource("dynamodb")

# initialize profanity filter (has default bad words list)
pf = ProfanityFilter()

def handler(event, context):
    # extract bucket and object key from the triggering event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    # get the review file from S3
    obj = s3.get_object(Bucket=bucket, Key=key)
    review_data = json.loads(obj['Body'].read().decode('utf-8'))

    # concatenate summary and reviewText into a single string for analysis
    full_text = ' '.join(review_data.get('clean_summary', []) + review_data.get('clean_reviewText', []))
    has_profanity = pf.is_profane(full_text)

    # annotate result with profanity flag
    review_data['has_profanity'] = has_profanity

    # upload the annotated review to the next S3 bucket (from SSM)
    target_bucket = ssm.get_parameter(Name='/localstack/reviews/profanity_checked')['Parameter']['Value']
    s3.put_object(
        Bucket=target_bucket,
        Key=key,
        Body=json.dumps(review_data).encode('utf-8')
    )

    # if profane, update DynamoDB table (name from SSM)
    if has_profanity:
        table_name = ssm.get_parameter(Name='/localstack/reviews/profanity_table')['Parameter']['Value']
        table = ddb.Table(table_name)

        user_id = review_data.get('reviewerID', 'unknown')
        # increment the counter (or create the entry)
        table.update_item(
            Key={'reviewerID': user_id},
            UpdateExpression="ADD bad_review_count :inc",
            ExpressionAttributeValues={':inc': 1}
        )

    return {
        'statusCode': 200,
        'body': f"Profanity check complete. Stored in {target_bucket}/{key}."
    }
