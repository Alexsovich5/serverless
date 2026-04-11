"""
API Handler Lambda
IT Operations Specialist - ACORIA (2018)

REST API endpoints for event submission and retrieval.
"""

import json
import uuid
import logging
import boto3
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('acoria-event-platform-events-production')
sqs = boto3.client('sqs')
QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/123456789/acoria-event-platform-events-production'


def handler(event, context):
    """API Gateway handler - routes based on HTTP method."""
    method = event['httpMethod']
    path = event.get('pathParameters') or {}

    if method == 'POST':
        return create_event(event)
    elif method == 'GET' and 'id' in path:
        return get_event(path['id'])
    elif method == 'GET':
        return list_events(event)
    else:
        return response(405, {'error': 'Method not allowed'})


def create_event(event):
    """Submit a new event to the processing queue."""
    try:
        body = json.loads(event['body'])
        event_id = str(uuid.uuid4())

        sqs.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=json.dumps(body),
            MessageAttributes={
                'eventId': {'DataType': 'String', 'StringValue': event_id}
            }
        )

        return response(202, {
            'eventId': event_id,
            'status': 'queued',
            'message': 'Event submitted for processing'
        })
    except Exception as e:
        logger.error("Create event error: %s", str(e))
        return response(500, {'error': str(e)})


def get_event(event_id):
    """Retrieve a specific event by ID."""
    try:
        result = table.get_item(Key={'eventId': event_id})
        item = result.get('Item')
        if not item:
            return response(404, {'error': 'Event not found'})
        return response(200, item)
    except Exception as e:
        return response(500, {'error': str(e)})


def list_events(event):
    """List recent events with optional filtering."""
    try:
        params = event.get('queryStringParameters') or {}
        limit = int(params.get('limit', 50))
        result = table.scan(Limit=limit)
        return response(200, {
            'events': result.get('Items', []),
            'count': result.get('Count', 0)
        })
    except Exception as e:
        return response(500, {'error': str(e)})


def response(status_code, body):
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps(body, default=str)
    }
