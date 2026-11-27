import json
import boto3
import os

def lambda_handler(event, context):
    print("PDWebhookReceiver started")
    try: 
        sqs_queue_url = os.environ.get('SQS_QUEUE_URL')
        if not sqs_queue_url:
            raise ValueError("SQS_QUEUE_URL environment variable not set.")
            
        sqs = boto3.client('sqs')

        # Get the body from the API Gateway event
        body_str = event.get('body')
        if not body_str:
            print("Error: Received an empty request body.")
            return {"statusCode": 400, "body": json.dumps({"message": "Empty request body"})}

        # Parse the JSON data from the body
        webhook_data = json.loads(body_str)

        # ✅ FIX: Wrap the received data in a 'data' key for the next Lambda
        payload_to_queue = {'data': webhook_data}
        
        # Send the newly formatted payload to SQS
        print("Sending payload to SQS:", json.dumps(payload_to_queue))
        response = sqs.send_message(
            QueueUrl=sqs_queue_url,
            MessageBody=json.dumps(payload_to_queue)
        )
        
        print(f"Successfully queued message. SQS MessageID={response.get('MessageId')}")
        
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Webhook queued successfully"})
        }

    except json.JSONDecodeError as e:
        print(f"Error: Could not decode JSON from request body. Error: {str(e)}")
        return {"statusCode": 400, "body": json.dumps({"message": f"Invalid JSON format: {str(e)}"})}

    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"message": f"Failed to queue webhook: {str(e)}"})
        }