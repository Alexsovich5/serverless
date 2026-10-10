"""Run the whole pipeline over HTTP against LocalStack through the local runner."""

import json
import time
import urllib.error
import urllib.request
import uuid

from functions import event_processor
from functions.common import resource
from local.runner import LambdaContext, sns_record
from tests.integration.conftest import drain, wait_for

PIPELINE_TIMEOUT = 60


def request(base_url, path, method='GET', body=None):
    data = json.dumps(body).encode('utf-8') if body is not None else None
    req = urllib.request.Request(base_url + path, data=data, method=method,
                                 headers={'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read().decode('utf-8'))
    except urllib.error.HTTPError as err:
        return err.code, json.loads(err.read().decode('utf-8'))


def post_event(base_url, body):
    status, result = request(base_url, '/events', 'POST', body)
    assert status == 202, result
    return result['eventId']


def wait_until_processed(base_url, event_id):
    def processed():
        status, item = request(base_url, '/events/' + event_id)
        return item if status == 200 and item.get('status') == 'processed' else None
    return wait_for(processed, timeout=PIPELINE_TIMEOUT)


def transformed_row(runner, event_id):
    table = resource('dynamodb').Table(runner.resources['TRANSFORMED_TABLE'])
    return table.get_item(Key={'eventId': event_id}).get('Item')


def notifications_for(bodies, event_id):
    found = []
    for body in bodies:
        envelope = json.loads(body)
        message = json.loads(envelope['Message'])
        if message.get('eventId') == event_id:
            found.append(envelope)
    return found


def wait_for_messages(queue_url, predicate, seen):
    """Drain a queue into `seen` until predicate(seen) holds."""
    def check():
        seen.extend(drain(queue_url))
        return predicate(seen)
    return wait_for(check, timeout=PIPELINE_TIMEOUT)


def unique(prefix):
    return '{}-{}'.format(prefix, uuid.uuid4().hex[:12])


def test_posted_event_is_processed_transformed_and_notified(runner, base_url):
    event_id = post_event(base_url, {
        'source': 'Billing',
        'type': 'Invoice.Created',
        'payload': {'customer': {'id': 42}, 'amount': 19.99},
    })

    item = wait_until_processed(base_url, event_id)
    assert item['eventId'] == event_id
    assert item['payload'] == {'customer': {'id': 42}, 'amount': 19.99}

    row = wait_for(lambda: transformed_row(runner, event_id), timeout=PIPELINE_TIMEOUT)
    assert row['source'] == 'billing'
    assert row['type'] == 'invoice.created'
    assert json.loads(row['fields']) == {'customer.id': '42', 'amount': '19.99'}
    assert row['fieldCount'] == 2

    seen = []
    wait_for_messages(runner.resources['NOTIFY_SUB_QUEUE_URL'],
                      lambda bodies: notifications_for(bodies, event_id), seen)
    envelope = notifications_for(seen, event_id)[0]
    assert envelope['Subject'] == 'event.transformed'
    assert json.loads(envelope['Message'])['type'] == 'invoice.created'


def failed_ids(bodies):
    return {json.loads(body)['eventId'] for body in bodies}


def test_invalid_events_are_dead_lettered(runner, base_url):
    missing_payload = post_event(base_url, {'source': 'billing', 'type': 'invoice'})
    empty = post_event(base_url, {})

    seen = []
    wait_for_messages(runner.resources['FAILED_QUEUE_URL'],
                      lambda bodies: {missing_payload, empty} <= failed_ids(bodies), seen)

    reasons = {json.loads(body)['eventId']: json.loads(body)['reason'] for body in seen}
    assert reasons[missing_payload] == 'Missing required field: payload'
    assert reasons[empty] == 'Missing required field: source'
    for event_id in (missing_payload, empty):
        status, body = request(base_url, '/events/' + event_id)
        assert status == 404, body


def test_duplicate_delivery_stores_and_transforms_once(runner):
    event_id = unique('dup')
    message = json.dumps({
        'eventId': event_id,
        'receivedAt': '2018-05-01T12:00:00.000000Z',
        'event': {'source': 'orders', 'type': 'order.placed', 'payload': {'n': 1}},
    })

    first = event_processor.handler(sns_record(message), LambdaContext('eventProcessor'))
    second = event_processor.handler(sns_record(message), LambdaContext('eventProcessor'))

    assert first['processed'] == 1
    assert second == {'processed': 0, 'duplicates': 1, 'failed': 0}

    events = resource('dynamodb').Table(runner.resources['EVENTS_TABLE'])
    assert events.get_item(Key={'eventId': event_id})['Item']['status'] == 'processed'

    wait_for(lambda: transformed_row(runner, event_id), timeout=PIPELINE_TIMEOUT)
    time.sleep(15)

    transformed = resource('dynamodb').Table(runner.resources['TRANSFORMED_TABLE'])
    rows = [row for row in transformed.scan()['Items'] if row['eventId'] == event_id]
    assert len(rows) == 1
    bodies = drain(runner.resources['NOTIFY_SUB_QUEUE_URL'])
    assert len(notifications_for(bodies, event_id)) == 1


def test_list_by_type_returns_only_that_type(runner, base_url):
    event_type = unique('type')
    event_id = post_event(base_url, {'source': 'web', 'type': event_type, 'payload': {}})
    other_id = post_event(base_url, {'source': 'web', 'type': unique('type'), 'payload': {}})
    wait_until_processed(base_url, event_id)
    wait_until_processed(base_url, other_id)

    status, body = request(base_url, '/events?type=' + event_type)

    assert status == 200
    assert [item['eventId'] for item in body['events']] == [event_id]
    assert body['count'] == 1
