import json
from datetime import datetime

import boto3
import mock
import pytest

from functions import event_processor
from tests.unit.conftest import sns_event

RECEIVED_AT = '2018-05-01T12:00:00.000000Z'


def ingest_message(event_id, event):
    return {'eventId': event_id, 'receivedAt': RECEIVED_AT, 'event': event}


def valid_event(**overrides):
    event = {
        'source': 'billing',
        'type': 'invoice.paid',
        'payload': {'amount': 19.99, 'customer': {'id': 42}},
        'metadata': {'origin': 'unit'},
    }
    event.update(overrides)
    return event


@pytest.fixture
def events_table(pipeline):
    return boto3.resource('dynamodb').Table(pipeline['EVENTS_TABLE'])


def all_items(table):
    return table.scan()['Items']


def failed_messages(pipeline):
    sqs = boto3.client('sqs')
    messages = sqs.receive_message(
        QueueUrl=pipeline['FAILED_QUEUE_URL'], MaxNumberOfMessages=10).get('Messages', [])
    return [json.loads(message['Body']) for message in messages]


def test_valid_record_is_stored_with_api_event_id(pipeline, events_table):
    message = ingest_message('id-valid', valid_event())

    result = event_processor.handler(sns_event([message]), None)

    assert result == {'processed': 1, 'duplicates': 0, 'failed': 0}
    item = events_table.get_item(Key={'eventId': 'id-valid'})['Item']
    assert item['status'] == 'processed'
    assert item['receivedAt'] == RECEIVED_AT
    assert item['source'] == 'billing'
    assert item['type'] == 'invoice.paid'
    datetime.strptime(item['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
    assert json.loads(item['payload']) == {'amount': 19.99, 'customer': {'id': 42}}
    assert json.loads(item['metadata']) == {'origin': 'unit'}
    assert failed_messages(pipeline) == []


def test_missing_payload_is_dead_lettered_and_not_stored(pipeline, events_table):
    event = valid_event()
    del event['payload']
    message = ingest_message('id-missing', event)

    result = event_processor.handler(sns_event([message]), None)

    assert result['failed'] == 1
    assert result['processed'] == 0
    failed = failed_messages(pipeline)
    assert len(failed) == 1
    assert failed[0]['reason'] == 'Missing required field: payload'
    assert failed[0]['eventId'] == 'id-missing'
    assert json.loads(failed[0]['message']) == message
    assert all_items(events_table) == []


def test_string_payload_is_rejected(pipeline, events_table):
    message = ingest_message('id-str', valid_event(payload='str'))

    result = event_processor.handler(sns_event([message]), None)

    assert result == {'processed': 0, 'duplicates': 0, 'failed': 1}
    failed = failed_messages(pipeline)
    assert [entry['reason'] for entry in failed] == ["Field 'payload' must be an object"]
    assert all_items(events_table) == []


@pytest.mark.parametrize('event, reason', [
    ({'type': 't', 'payload': {}}, 'Missing required field: source'),
    ({'source': 's', 'payload': {}}, 'Missing required field: type'),
    ({'source': '', 'type': 't', 'payload': {}}, "Field 'source' must be a non-empty string"),
    ({'source': 5, 'type': 't', 'payload': {}}, "Field 'source' must be a non-empty string"),
    ({'source': 's', 'type': '', 'payload': {}}, "Field 'type' must be a non-empty string"),
    ({'source': 's', 'type': 't', 'payload': [1]}, "Field 'payload' must be an object"),
])
def test_validate_messages(event, reason):
    with pytest.raises(ValueError) as excinfo:
        event_processor.validate(event)
    assert str(excinfo.value) == reason


def test_validate_accepts_valid_event():
    event_processor.validate(valid_event())


def test_enrich_keeps_ids_and_serializes_json_fields():
    message = ingest_message('id-enrich', valid_event(metadata=None))
    del message['event']['metadata']

    item = event_processor.enrich(message)

    assert item['eventId'] == 'id-enrich'
    assert item['receivedAt'] == RECEIVED_AT
    assert item['status'] == 'processed'
    assert item['timestamp'].endswith('Z')
    assert json.loads(item['payload']) == {'amount': 19.99, 'customer': {'id': 42}}
    assert json.loads(item['metadata']) == {}


def test_mixed_batch_counts(pipeline, events_table):
    messages = [
        ingest_message('id-a', valid_event()),
        ingest_message('id-b', valid_event(source='')),
        ingest_message('id-c', valid_event(type='other')),
    ]

    result = event_processor.handler(sns_event(messages), None)

    assert result == {'processed': 2, 'duplicates': 0, 'failed': 1}
    assert sorted(item['eventId'] for item in all_items(events_table)) == ['id-a', 'id-c']
    assert [entry['eventId'] for entry in failed_messages(pipeline)] == ['id-b']


def test_redelivered_record_is_counted_as_duplicate(pipeline, events_table):
    event = sns_event([ingest_message('id-dup', valid_event())])

    first = event_processor.handler(event, None)
    stored = events_table.get_item(Key={'eventId': 'id-dup'})['Item']
    second = event_processor.handler(event, None)

    assert first == {'processed': 1, 'duplicates': 0, 'failed': 0}
    assert second == {'processed': 0, 'duplicates': 1, 'failed': 0}
    items = all_items(events_table)
    assert len(items) == 1
    assert items[0]['timestamp'] == stored['timestamp']
    assert failed_messages(pipeline) == []


def test_non_json_message_is_dead_lettered_with_null_id(pipeline, events_table):
    result = event_processor.handler(sns_event(['not json at all']), None)

    assert result == {'processed': 0, 'duplicates': 0, 'failed': 1}
    failed = failed_messages(pipeline)
    assert len(failed) == 1
    assert failed[0]['eventId'] is None
    assert failed[0]['message'] == 'not json at all'
    assert failed[0]['reason']
    assert all_items(events_table) == []


def test_message_without_event_object_is_dead_lettered(pipeline, events_table):
    result = event_processor.handler(sns_event([{'eventId': 'id-x', 'event': [1]}]), None)

    assert result['failed'] == 1
    failed = failed_messages(pipeline)
    assert failed[0]['eventId'] == 'id-x'
    assert all_items(events_table) == []


def test_dead_letter_sends_spec_shape(pipeline):
    event_processor.dead_letter('raw', 'id-dl', 'some reason')

    assert failed_messages(pipeline) == [
        {'eventId': 'id-dl', 'reason': 'some reason', 'message': 'raw'}]


def test_empty_records_returns_zeros(pipeline):
    assert event_processor.handler({'Records': []}, None) == {
        'processed': 0, 'duplicates': 0, 'failed': 0}


def deep_payload(depth):
    value = {}
    for _ in range(depth - 1):
        value = {'a': value}
    return value


def test_event_nested_too_deeply_is_dead_lettered(pipeline, events_table):
    from functions import common
    message = ingest_message('id-deep', valid_event(payload=deep_payload(common.MAX_JSON_DEPTH)))

    result = event_processor.handler(sns_event([message]), None)

    assert result == {'processed': 0, 'duplicates': 0, 'failed': 1}
    failed = failed_messages(pipeline)
    assert failed[0]['eventId'] == 'id-deep'
    assert failed[0]['reason'] == "Field 'event' is nested more than {} levels deep".format(
        common.MAX_JSON_DEPTH)
    assert all_items(events_table) == []


def test_message_beyond_recursion_limit_is_dead_lettered_not_raised(pipeline, events_table):
    raw = '{"eventId": "x", "event": ' + '[' * 100000 + ']' * 100000 + '}'
    good = ingest_message('id-after', valid_event())

    result = event_processor.handler(sns_event([raw, good]), None)

    assert result == {'processed': 1, 'duplicates': 0, 'failed': 1}
    failed = failed_messages(pipeline)
    assert failed[0]['eventId'] is None
    assert failed[0]['reason'].startswith('Message is not valid JSON')
    assert [item['eventId'] for item in all_items(events_table)] == ['id-after']


def test_dead_letter_of_a_maximum_size_message_fits_in_one_sqs_message(pipeline):
    raw = 'é' * (256 * 1024 // 2 - 10)
    sent = []
    real_client = event_processor.common.client

    def recording_client(service):
        created = real_client(service)
        original = created.send_message

        def send_message(**kwargs):
            sent.append(kwargs['MessageBody'])
            return original(**kwargs)

        created.send_message = send_message
        return created

    with mock.patch.object(event_processor.common, 'client', side_effect=recording_client):
        event_processor.dead_letter(raw, 'id-big', 'some reason')

    assert len(sent[0].encode('utf-8')) <= event_processor.MAX_DEAD_LETTER_BYTES
    failed = failed_messages(pipeline)
    assert failed[0]['eventId'] == 'id-big'
    assert failed[0]['reason'] == 'some reason'
    assert failed[0]['truncated'] is True
    assert raw.startswith(failed[0]['message'])
