"""
Event Processor Lambda

Processes events from SQS queue, validates and enriches them,
then stores in DynamoDB.
"""

import json
import uuid
import logging
import boto3
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('event-platform-events-production')


def handler(event, context):
    """SQS trigger handler - processes batched events."""
    processed = 0
    errors = 0

    for record in event.get('Records', []):
        try:
            body = json.loads(record['body'])
            process_event(body)
            processed += 1
        except Exception as e:
            logger.error("Failed to process event: %s", str(e))
            errors += 1

    logger.info("Processed: %d, Errors: %d", processed, errors)
    return {'processed': processed, 'errors': errors}


def process_event(event_data):
    """Validate, enrich, and store a single event."""
    # Validate required fields
    required = ['source', 'type', 'payload']
    for field in required:
        if field not in event_data:
            raise ValueError(f"Missing required field: {field}")

    # Enrich event
    enriched = {
        'eventId': str(uuid.uuid4()),
        'timestamp': datetime.utcnow().isoformat(),
        'source': event_data['source'],
        'type': event_data['type'],
        'payload': json.dumps(event_data['payload']),
        'status': 'processed',
        'metadata': json.dumps(event_data.get('metadata', {}))
    }

    # Store in DynamoDB
    table.put_item(Item=enriched)
    logger.info("Stored event: %s (type=%s)", enriched['eventId'], enriched['type'])
    return enriched
