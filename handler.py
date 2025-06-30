import json
import boto3
import os
import nltk  # added!
nltk.data.path.append("nltk_data")  # this tells nltk where to look

from nltk.sentiment import SentimentIntensityAnalyzer


s3 = boto3.client("s3")
ssm = boto3.client("ssm")
ddb = boto3.resource("dynamodb")

# load VADER analyzer (make sure nltk data is zipped with the Lambda)
analyzer = SentimentIntensityAnalyzer()

def label_sentiment(text):
    score = analyzer.polarity_scores(text)['compound']
    if score >= 0.05:
        return 'positive'
    elif score <= -0.05:
        return 'negative'
    else:
        return 'neutral'

def handler(event, context):
    # get S3 source info from event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    # download file from S3
    obj = s3.get_object(Bucket=bucket, Key=key)
    review_data = json.loads(obj['Body'].read().decode('utf-8'))

    # concatenate cleaned fields for analysis
    tokens = review_data.get('clean_summary', []) + review_data.get('clean_reviewText', [])
    full_text = ' '.join(tokens)

    # determine sentiment
    review_data['sentiment'] = label_sentiment(full_text)

    # store the annotated file to another S3 bucket
    target_bucket = ssm.get_parameter(Name='/localstack/reviews/sentiment_analyzed')['Parameter']['Value']
    s3.put_object(
        Bucket=target_bucket,
        Key=key,
        Body=json.dumps(review_data).encode('utf-8')
    )

    # also store summary in DynamoDB (optional, for querying/reporting)
    table_name = ssm.get_parameter(Name='/localstack/reviews/sentiment_table')['Parameter']['Value']
    table = ddb.Table(table_name)

    table.put_item(
        Item={
            'reviewerID': review_data.get('reviewerID', 'unknown'),
            'asin': review_data.get('asin', 'unknown'),
            'sentiment': review_data['sentiment'],
            'has_profanity': review_data.get('has_profanity', False),
            'timestamp': review_data.get('unixReviewTime', 0),
            'overall': float(review_data.get('overall', 0.0))
        }
    )

    return {
        'statusCode': 200,
        'body': f"Sentiment '{review_data['sentiment']}' stored in {target_bucket}/{key}"
    }
