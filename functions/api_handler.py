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
# SNS rejects messages whose body plus attributes exceed 256 KiB.
MAX_MESSAGE_BYTES = 256 * 1024
JSON_CONTENT_TYPE = 'application/json'


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
    except Exception:
        # The traceback goes to the function log only; clients get no detail.
        logger.exception('Request failed: %s %s', method, path)
        return response(500, {'error': 'Internal server error'})


def _content_type(event):
    for name, value in (event.get('headers') or {}).items():
        if name.lower() == 'content-type' and isinstance(value, str):
            return value.split(';', 1)[0].strip().lower()
    return None


def _too_deep():
    return response(400, {
        'error': 'body is nested more than {} levels deep'.format(common.MAX_JSON_DEPTH)})


def _message_size(message_json, attributes):
    size = len(message_json.encode('utf-8'))
    for name, attribute in attributes.items():
        size += len(name.encode('utf-8'))
        size += len(attribute['DataType'].encode('utf-8'))
        size += len(attribute['StringValue'].encode('utf-8'))
    return size


def create_event(event):
    """Assign an eventId and publish the event to the ingest topic."""
    # Requiring a JSON content type keeps browsers from sending cross-site
    # "simple" form or text/plain posts without a CORS preflight.
    if _content_type(event) != JSON_CONTENT_TYPE:
        return response(415, {'error': 'Content-Type must be application/json'})

    raw = event.get('body')
    try:
        body = json.loads(raw) if raw is not None else None
    except RecursionError:
        return _too_deep()
    except ValueError:
        body = None
    if not isinstance(body, dict):
        return response(400, {'error': 'body must be a JSON object'})
    if common.json_depth(body, limit=common.MAX_JSON_DEPTH) > common.MAX_JSON_DEPTH:
        return _too_deep()

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

    message_json = json.dumps(message)
    if _message_size(message_json, attributes) > MAX_MESSAGE_BYTES:
        return response(413, {
            'error': 'event is larger than {} bytes'.format(MAX_MESSAGE_BYTES)})

    common.client('sns').publish(
        TopicArn=config['ingest_topic_arn'],
        Message=message_json,
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
