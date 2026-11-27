import json
import os
import time
import boto3
from decimal import Decimal
import requests
from botocore.exceptions import ClientError

# --- AWS Clients and Configuration ---
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['DYNAMODB_TABLE'])

# --- Token Bucket Configuration ---
BUCKET_PK_NAME = 'clay_rate_limit_for_people_table_apollo'
BUCKET_ID = 'clay_rate_limit'
REFILL_RATE = Decimal('6') / Decimal('60')  # 6 requests/min = 0.1 tokens/sec
BUCKET_CAPACITY = Decimal('6')

# --- Downstream API Configuration ---
CLAY_ENDPOINT = os.environ['CLAY_ENDPOINT']
CLAY_API_KEY = os.environ['CLAY_API_KEY']


def lambda_handler(event, context):
    """
    Processes messages from SQS, rate-limited by a DynamoDB token bucket.
    Relies on native SQS-Lambda integration for message deletion and retries.
    """
    batch_item_failures = []

    for record in event['Records']:
        message_id = record['messageId']
        
        try:
            # 1. ATOMICALLY ATTEMPT TO CONSUME A TOKEN
            if not consume_token():
                print(f"Rate limit hit for MessageID={message_id}. Marking for retry.")
                batch_item_failures.append({"itemIdentifier": message_id})
                continue

            # 2. IF TOKEN WAS CONSUMED, PROCESS THE MESSAGE
            print(f"Token acquired. Processing MessageID={message_id}.")
            webhook_data = json.loads(record['body'])

            response = requests.post(
                CLAY_ENDPOINT,
                json=webhook_data,
                headers={'Authorization': f'Bearer {CLAY_API_KEY}', 'Content-Type': 'application/json'},
                timeout=10
            )
            
            response.raise_for_status()

            print(f"Successfully sent MessageID={message_id} to Clay.")

        except Exception as e:
            print(f"Error processing MessageID={message_id}: {e}. Marking for retry.")
            batch_item_failures.append({"itemIdentifier": message_id})

    return {"batchItemFailures": batch_item_failures}


def consume_token():
    """
    Atomically consumes a token using an optimistic locking pattern.
    This is immune to race conditions.

    Returns:
        bool: True if a token was consumed, False otherwise.
    """
    try:
        current_time = Decimal(str(time.time()))

        # STEP 1: Read the current state of the token bucket.
        response = table.get_item(Key={BUCKET_PK_NAME: BUCKET_ID})

        # Initialize the bucket if it doesn't exist
        if 'Item' not in response:
            last_tokens = BUCKET_CAPACITY
            last_timestamp = current_time
        else:
            last_tokens = response['Item'].get('Tokens', BUCKET_CAPACITY)
            last_timestamp = response['Item'].get('LastActiveTimestamp', current_time)

        # STEP 2: Perform calculations in the Lambda code.
        time_elapsed = current_time - last_timestamp
        tokens_to_add = time_elapsed * REFILL_RATE
        
        # Refill tokens, capped at the bucket capacity
        refilled_tokens = min(BUCKET_CAPACITY, last_tokens + tokens_to_add)

        # STEP 3: Check if a token is available for consumption.
        if refilled_tokens < 1:
            print("Insufficient tokens after refill.")
            return False

        # Calculate the new token count after consumption.
        new_token_count = refilled_tokens - 1
        
        # STEP 4: Atomically update the item using a ConditionExpression.
        # This update will only succeed if the LastActiveTimestamp has not changed
        # since we read it in Step 1. This prevents race conditions.
        table.update_item(
            Key={BUCKET_PK_NAME: BUCKET_ID},
            UpdateExpression="SET Tokens = :new_tokens, LastActiveTimestamp = :now",
            ConditionExpression="LastActiveTimestamp = :expected_ts",
            ExpressionAttributeValues={
                ':new_tokens': new_token_count,
                ':now': current_time,
                ':expected_ts': last_timestamp # The timestamp from our initial read
            }
        )
        print(f"Token successfully consumed. Remaining (approx): {new_token_count:.2f}")
        return True

    except ClientError as e:
        # This is the expected outcome of a lost race condition.
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            print("ConditionalCheckFailed: Lost race condition. Another process updated the item.")
        else:
            print(f"An unexpected DynamoDB error occurred: {e}")
        return False # Deny the token
        
    except Exception as e:
        print(f"A general error occurred in consume_token: {e}")
        return False # Deny the token on any other error