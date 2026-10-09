import json
import uuid

import boto3
import mock
import pytest

from functions import api_handler, common
from tests.unit.conftest import apigw_event


def body_of(result):
    return json.loads(result['body'])


@pytest.fixture
def published(pipeline):
    """Record every SNS publish call while still delivering it through moto."""
    calls = []
    real_client = common.client

    def spying_client(service):
        created = real_client(service)
        if service == 'sns':
            original = created.publish

            def publish(**kwargs):
                calls.append(kwargs)
                return original(**kwargs)

            created.publish = publish
        return created

    with mock.patch.object(common, 'client', side_effect=spying_client):
        yield calls


@pytest.fixture
def events_table(pipeline):
    return boto3.resource('dynamodb').Table(pipeline['EVENTS_TABLE'])


def seed(table, event_id, event_type, timestamp, payload=None):
    table.put_item(Item={
        'eventId': event_id,
        'timestamp': timestamp,
        'receivedAt': timestamp,
        'source': 'billing',
        'type': event_type,
        'payload': json.dumps(payload or {'amount': 1}),
        'metadata': json.dumps({'origin': 'test'}),
        'status': 'processed',
    })


def receive_envelopes(queue_url):
    sqs = boto3.client('sqs')
    messages = sqs.receive_message(
        QueueUrl=queue_url, MaxNumberOfMessages=10).get('Messages', [])
    return [json.loads(message['Body']) for message in messages]


def test_post_returns_202_and_publishes_event_to_ingest_topic(pipeline):
    event = {
        'source': 'billing',
        'type': 'invoice.created',
        'payload': {'amount': 19.99},
        'metadata': {'origin': 'test'},
    }
    result = api_handler.handler(apigw_event('POST', body=event), None)

    assert result['statusCode'] == 202
    body = body_of(result)
    assert body['status'] == 'queued'
    assert str(uuid.UUID(body['eventId'], version=4)) == body['eventId']

    envelopes = receive_envelopes(pipeline['INGEST_SUB_QUEUE_URL'])
    assert len(envelopes) == 1
    assert envelopes[0]['TopicArn'] == pipeline['INGEST_TOPIC_ARN']
    message = json.loads(envelopes[0]['Message'])
    assert message['eventId'] == body['eventId']
    assert message['event'] == event
    assert message['receivedAt'].endswith('Z')


def test_post_sets_event_id_and_type_attributes(published):
    result = api_handler.handler(
        apigw_event('POST', body={'source': 's', 'type': 'a', 'payload': {}}), None)

    assert result['statusCode'] == 202
    assert len(published) == 1
    attributes = published[0]['MessageAttributes']
    assert attributes['eventId'] == {
        'DataType': 'String', 'StringValue': body_of(result)['eventId']}
    assert attributes['type'] == {'DataType': 'String', 'StringValue': 'a'}


@pytest.mark.parametrize('body', ['not json', '[1, 2]', None])
def test_post_rejects_body_that_is_not_a_json_object(pipeline, body):
    result = api_handler.handler(apigw_event('POST', body=body), None)

    assert result['statusCode'] == 400
    assert body_of(result) == {'error': 'body must be a JSON object'}
    assert receive_envelopes(pipeline['INGEST_SUB_QUEUE_URL']) == []


@pytest.mark.parametrize('body', [{}, {'type': 5}, {'type': ''}])
def test_post_accepts_object_without_valid_type_and_omits_type_attribute(
        published, pipeline, body):
    result = api_handler.handler(apigw_event('POST', body=body), None)

    assert result['statusCode'] == 202
    assert len(published) == 1
    attributes = published[0]['MessageAttributes']
    assert attributes['eventId']['StringValue'] == body_of(result)['eventId']
    assert 'type' not in attributes
    message = json.loads(published[0]['Message'])
    assert message['event'] == body
    assert len(receive_envelopes(pipeline['INGEST_SUB_QUEUE_URL'])) == 1


def test_get_event_returns_item_with_decoded_payload(events_table):
    seed(events_table, 'abc-1', 'a', '2018-05-01T12:00:00.000000Z',
         payload={'customer': {'id': 42}})

    result = api_handler.handler(apigw_event('GET', path_params={'id': 'abc-1'}), None)

    assert result['statusCode'] == 200
    item = body_of(result)
    assert item['eventId'] == 'abc-1'
    assert item['payload'] == {'customer': {'id': 42}}
    assert item['metadata'] == {'origin': 'test'}


def test_get_missing_event_returns_404(pipeline):
    result = api_handler.handler(apigw_event('GET', path_params={'id': 'nope'}), None)

    assert result['statusCode'] == 404
    assert body_of(result) == {'error': 'Event not found'}


def test_list_by_type_returns_only_that_type_newest_first(events_table):
    seed(events_table, 'a-old', 'a', '2018-05-01T10:00:00.000000Z')
    seed(events_table, 'b-mid', 'b', '2018-05-01T11:00:00.000000Z')
    seed(events_table, 'a-new', 'a', '2018-05-01T12:00:00.000000Z')
    seed(events_table, 'a-mid', 'a', '2018-05-01T11:30:00.000000Z')

    result = api_handler.handler(apigw_event('GET', qs={'type': 'a'}), None)

    assert result['statusCode'] == 200
    body = body_of(result)
    assert [item['eventId'] for item in body['events']] == ['a-new', 'a-mid', 'a-old']
    assert body['count'] == 3
    assert body['events'][0]['payload'] == {'amount': 1}


def test_list_by_type_honours_limit(events_table):
    for hour in range(10, 14):
        seed(events_table, 'a-{}'.format(hour), 'a',
             '2018-05-01T{}:00:00.000000Z'.format(hour))

    result = api_handler.handler(
        apigw_event('GET', qs={'type': 'a', 'limit': '2'}), None)

    body = body_of(result)
    assert [item['eventId'] for item in body['events']] == ['a-13', 'a-12']
    assert body['count'] == 2


def test_list_without_type_scans_with_limit(events_table):
    for index in range(5):
        seed(events_table, 'e-{}'.format(index), 'a' if index % 2 else 'b',
             '2018-05-01T12:00:0{}.000000Z'.format(index))

    everything = body_of(api_handler.handler(apigw_event('GET'), None))
    limited = body_of(api_handler.handler(apigw_event('GET', qs={'limit': '3'}), None))

    assert everything['count'] == 5
    assert sorted(item['eventId'] for item in everything['events']) == [
        'e-0', 'e-1', 'e-2', 'e-3', 'e-4']
    assert limited['count'] == 3
    assert len(limited['events']) == 3


@pytest.mark.parametrize('limit', ['0', '101', 'x', '-1', '2.5'])
def test_list_rejects_limit_out_of_range(pipeline, limit):
    result = api_handler.handler(apigw_event('GET', qs={'limit': limit}), None)

    assert result['statusCode'] == 400
    assert body_of(result) == {'error': 'limit must be an integer between 1 and 100'}


@pytest.mark.parametrize('limit', ['1', '100'])
def test_list_accepts_limit_bounds(pipeline, limit):
    result = api_handler.handler(apigw_event('GET', qs={'limit': limit}), None)

    assert result['statusCode'] == 200
    assert body_of(result) == {'events': [], 'count': 0}


def test_delete_returns_405(pipeline):
    result = api_handler.handler(apigw_event('DELETE', path_params={'id': 'x'}), None)

    assert result['statusCode'] == 405
    assert body_of(result) == {'error': 'Method not allowed'}


def test_get_on_unknown_path_returns_405(pipeline):
    event = apigw_event('GET')
    event['resource'] = event['path'] = '/other'

    result = api_handler.handler(event, None)

    assert result['statusCode'] == 405


def test_responses_carry_cors_header(pipeline):
    result = api_handler.handler(apigw_event('GET', path_params={'id': 'nope'}), None)

    assert result['headers']['Access-Control-Allow-Origin'] == '*'
