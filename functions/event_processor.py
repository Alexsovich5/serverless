"""
Event Processor Lambda

Consumes ingest messages delivered by SNS, validates and enriches them,
stores them idempotently in DynamoDB and dead-letters failures to SQS.
"""

import json
import logging

from botocore.exceptions import ClientError

from functions import common

logger = logging.getLogger()
logger.setLevel(logging.INFO)

REQUIRED_FIELDS = ('source', 'type', 'payload')
STRING_FIELDS = ('source', 'type')


def handler(event, context):
    """SNS trigger handler; returns processed, duplicate and failed counts."""
    counts = {'processed': 0, 'duplicates': 0, 'failed': 0}

    for record in event.get('Records', []):
        message_str = record['Sns']['Message']
        outcome = process_message(message_str)
        counts[outcome] += 1

    logger.info('Processed: %(processed)d, Duplicates: %(duplicates)d, Failed: %(failed)d',
                counts)
    return counts


def process_message(message_str):
    """Handle one SNS message string; returns the name of the counter to bump."""
    try:
        message = json.loads(message_str)
    except ValueError as exc:
        dead_letter(message_str, None, 'Message is not valid JSON: {}'.format(exc))
        return 'failed'

    if not isinstance(message, dict):
        dead_letter(message_str, None, 'Message must be a JSON object')
        return 'failed'

    event_id = message.get('eventId')
    try:
        if not isinstance(event_id, str) or not event_id:
            raise ValueError("Field 'eventId' must be a non-empty string")
        validate(message.get('event'))
    except ValueError as exc:
        dead_letter(message_str, event_id if isinstance(event_id, str) else None, str(exc))
        return 'failed'

    if store(enrich(message)):
        return 'processed'
    return 'duplicates'


def validate(event):
    """Raise ValueError describing the first problem with an ingest event."""
    if not isinstance(event, dict):
        raise ValueError("Field 'event' must be an object")
    for field in REQUIRED_FIELDS:
        if field not in event:
            raise ValueError('Missing required field: {}'.format(field))
    for field in STRING_FIELDS:
        value = event[field]
        if not isinstance(value, str) or not value:
            raise ValueError("Field '{}' must be a non-empty string".format(field))
    if not isinstance(event['payload'], dict):
        raise ValueError("Field 'payload' must be an object")


def enrich(message):
    """Build the events-table item from a validated ingest message."""
    event = message['event']
    return {
        'eventId': message['eventId'],
        'receivedAt': message.get('receivedAt') or common.utc_now_iso(),
        'timestamp': common.utc_now_iso(),
        'source': event['source'],
        'type': event['type'],
        'payload': json.dumps(event['payload']),
        'metadata': json.dumps(event.get('metadata') or {}),
        'status': 'processed',
    }


def store(item):
    """Write the item unless its eventId exists; returns False for a duplicate."""
    table = common.resource('dynamodb').Table(common.get_config()['events_table'])
    try:
        table.put_item(Item=item, ConditionExpression='attribute_not_exists(eventId)')
    except ClientError as exc:
        if exc.response.get('Error', {}).get('Code') == 'ConditionalCheckFailedException':
            logger.info('Duplicate event ignored: %s', item['eventId'])
            return False
        raise
    logger.info('Stored event: %s (type=%s)', item['eventId'], item['type'])
    return True


def dead_letter(message_str, event_id, reason):
    """Send the original message and the failure reason to the failed queue."""
    logger.warning('Dead-lettering event %s: %s', event_id, reason)
    common.client('sqs').send_message(
        QueueUrl=common.get_config()['failed_queue_url'],
        MessageBody=json.dumps({
            'eventId': event_id,
            'reason': reason,
            'message': message_str,
        }),
    )
