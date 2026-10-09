"""
API Handler Lambda

REST API endpoints for event submission and retrieval.
POST publishes to the SNS ingest topic; GET reads the events table.
"""

import json
import logging
import uuid

from boto3.dynamodb.conditions import Key

from functions import common
from functions.common import response

logger = logging.getLogger()
logger.setLevel(logging.INFO)

EVENT_PATH = '/events/{id}'
LIST_PATH = '/events'
TYPE_INDEX = 'type-timestamp-index'
DEFAULT_LIMIT = 50
MAX_LIMIT = 100
JSON_FIELDS = ('payload', 'metadata')


def handler(event, context):
    """API Gateway handler - routes based on HTTP method and resource path."""
    method = event.get('httpMethod')
    path = event.get('resource') or event.get('path')
    params = event.get('pathParameters') or {}

    try:
        if method == 'POST' and path == LIST_PATH:
            return create_event(event)
        if method == 'GET' and path == EVENT_PATH and 'id' in params:
            return get_event(params['id'])
        if method == 'GET' and path == LIST_PATH:
            return list_events(event)
        return response(405, {'error': 'Method not allowed'})
    except Exception as exc:
        logger.exception('Request failed: %s %s', method, path)
        return response(500, {'error': str(exc)})


def _parse_body(raw):
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except ValueError:
        return None


def create_event(event):
    """Assign an eventId and publish the event to the ingest topic."""
    body = _parse_body(event.get('body'))
    if not isinstance(body, dict):
        return response(400, {'error': 'body must be a JSON object'})

    config = common.get_config()
    event_id = str(uuid.uuid4())
    message = {
        'eventId': event_id,
        'receivedAt': common.utc_now_iso(),
        'event': body,
    }
    attributes = {
        'eventId': {'DataType': 'String', 'StringValue': event_id},
    }
    event_type = body.get('type')
    if isinstance(event_type, str) and event_type:
        attributes['type'] = {'DataType': 'String', 'StringValue': event_type}

    common.client('sns').publish(
        TopicArn=config['ingest_topic_arn'],
        Message=json.dumps(message),
        MessageAttributes=attributes,
    )
    logger.info('Queued event %s', event_id)
    return response(202, {'eventId': event_id, 'status': 'queued'})


def _events_table():
    return common.resource('dynamodb').Table(common.get_config()['events_table'])


def _decode(item):
    for field in JSON_FIELDS:
        value = item.get(field)
        if isinstance(value, str):
            try:
                item[field] = json.loads(value)
            except ValueError:
                pass
    return item


def get_event(event_id):
    """Return one stored event by its eventId."""
    item = _events_table().get_item(Key={'eventId': event_id}).get('Item')
    if not item:
        return response(404, {'error': 'Event not found'})
    return response(200, _decode(item))


def _parse_limit(params):
    raw = params.get('limit')
    if raw is None:
        return DEFAULT_LIMIT
    try:
        limit = int(raw)
    except (TypeError, ValueError):
        return None
    if limit < 1 or limit > MAX_LIMIT:
        return None
    return limit


def list_events(event):
    """List recent events, newest first when filtered by type."""
    params = event.get('queryStringParameters') or {}
    limit = _parse_limit(params)
    if limit is None:
        return response(400, {'error': 'limit must be an integer between 1 and 100'})

    table = _events_table()
    event_type = params.get('type')
    if event_type:
        result = table.query(
            IndexName=TYPE_INDEX,
            KeyConditionExpression=Key('type').eq(event_type),
            ScanIndexForward=False,
            Limit=limit,
        )
    else:
        result = table.scan(Limit=limit)

    items = [_decode(item) for item in result.get('Items', [])]
    return response(200, {'events': items, 'count': len(items)})
