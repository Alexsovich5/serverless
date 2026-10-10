"""
Data Transformer Lambda

Consumes DynamoDB stream records from the events table. Each INSERT image is
flattened into a row of the transformed-events table and announced on the
notifications topic. MODIFY and REMOVE records are ignored.
"""

import json
import logging

from boto3.dynamodb.types import TypeDeserializer

from functions import common

logger = logging.getLogger()
logger.setLevel(logging.INFO)

REQUIRED_ATTRIBUTES = ('eventId', 'source', 'type', 'timestamp', 'payload')
NOTIFICATION_SUBJECT = 'event.transformed'
# DynamoDB items are limited to 400 KB; leave room for the other attributes.
MAX_FIELDS_BYTES = 350 * 1024

_deserializer = TypeDeserializer()


def _leaf(value):
    if value is True or value is False or value is None:
        return json.dumps(value)
    return str(value)


def flatten(obj, prefix=''):
    """Flatten nested dicts and lists into dotted keys with string leaves."""
    if isinstance(obj, dict):
        items = ((str(key), value) for key, value in obj.items())
    elif isinstance(obj, (list, tuple)):
        items = ((str(index), value) for index, value in enumerate(obj))
    else:
        return {prefix: _leaf(obj)}

    flat = {}
    for key, value in items:
        path = '{}.{}'.format(prefix, key) if prefix else key
        flat.update(flatten(value, path))
    return flat


def transform(item):
    """Build the transformed-table row for a deserialized events-table image."""
    missing = [name for name in REQUIRED_ATTRIBUTES if name not in item]
    if missing:
        raise ValueError('Missing required attribute: {}'.format(', '.join(missing)))

    try:
        payload = json.loads(item['payload'])
    except (TypeError, ValueError, RecursionError) as exc:
        raise ValueError('Attribute payload is not valid JSON: {}'.format(exc))
    if not isinstance(payload, (dict, list)):
        raise ValueError('Attribute payload must decode to an object or a list')
    if common.json_depth(payload, limit=common.MAX_JSON_DEPTH) > common.MAX_JSON_DEPTH:
        raise ValueError('Attribute payload is nested more than {} levels deep'.format(
            common.MAX_JSON_DEPTH))

    fields = flatten(payload)
    fields_json = json.dumps(fields, sort_keys=True)
    if len(fields_json.encode('utf-8')) > MAX_FIELDS_BYTES:
        raise ValueError('flattened fields exceed {} bytes'.format(MAX_FIELDS_BYTES))
    return {
        'eventId': item['eventId'],
        'source': str(item['source']).lower(),
        'type': str(item['type']).lower(),
        'day': str(item['timestamp'])[:10],
        'fields': fields_json,
        'fieldCount': len(fields),
        'transformedAt': common.utc_now_iso(),
    }


def deserialize(image):
    """Convert a DynamoDB-typed stream image into plain Python values."""
    return {key: _deserializer.deserialize(value) for key, value in image.items()}


def notification(row):
    """Notification body published for a transformed row."""
    return {
        'eventId': row['eventId'],
        'source': row['source'],
        'type': row['type'],
        'day': row['day'],
        'fieldCount': row['fieldCount'],
    }


def handler(event, context):
    """DynamoDB stream trigger handler; returns transformed and skipped counts."""
    counts = {'transformed': 0, 'skipped': 0}
    records = event.get('Records', [])
    if not records:
        return counts

    config = common.get_config()
    table = common.resource('dynamodb').Table(config['transformed_table'])
    sns = common.client('sns')

    for record in records:
        if record.get('eventName') != 'INSERT':
            counts['skipped'] += 1
            continue

        image = deserialize(record.get('dynamodb', {}).get('NewImage', {}))
        try:
            row = transform(image)
        except ValueError as exc:
            logger.warning('Skipping stream record %s: %s', record.get('eventID'), exc)
            counts['skipped'] += 1
            continue

        table.put_item(Item=row)
        sns.publish(
            TopicArn=config['notify_topic_arn'],
            Subject=NOTIFICATION_SUBJECT,
            Message=json.dumps(notification(row)),
        )
        logger.info('Transformed event: %s (%d fields)', row['eventId'], row['fieldCount'])
        counts['transformed'] += 1

    logger.info('Transformed: %(transformed)d, Skipped: %(skipped)d', counts)
    return counts
