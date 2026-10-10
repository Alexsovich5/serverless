import json

import boto3
import pytest
from boto3.dynamodb.types import TypeSerializer

from functions import data_transformer

TIMESTAMP = '2018-05-01T12:00:00.123456Z'


def stored_item(**overrides):
    item = {
        'eventId': 'id-1',
        'receivedAt': '2018-05-01T11:59:59.000000Z',
        'timestamp': TIMESTAMP,
        'source': 'Billing',
        'type': 'Invoice.Paid',
        'payload': json.dumps({'amount': 19.99, 'customer': {'id': 42}, 'tags': ['a', 'b']}),
        'metadata': json.dumps({'origin': 'unit'}),
        'status': 'processed',
    }
    item.update(overrides)
    return item


def stream_record(event_name, image=None, keys=None):
    serializer = TypeSerializer()
    dynamodb = {
        'Keys': {k: serializer.serialize(v) for k, v in (keys or {'eventId': 'id-1'}).items()},
        'StreamViewType': 'NEW_IMAGE',
        'SequenceNumber': '100',
        'SizeBytes': 128,
    }
    if image is not None:
        dynamodb['NewImage'] = {k: serializer.serialize(v) for k, v in image.items()}
    return {
        'eventID': 'record-1',
        'eventName': event_name,
        'eventVersion': '1.1',
        'eventSource': 'aws:dynamodb',
        'awsRegion': 'us-east-1',
        'dynamodb': dynamodb,
    }


@pytest.fixture
def transformed_table(pipeline):
    return boto3.resource('dynamodb').Table(pipeline['TRANSFORMED_TABLE'])


def notifications(pipeline):
    sqs = boto3.client('sqs')
    messages = sqs.receive_message(
        QueueUrl=pipeline['NOTIFY_SUB_QUEUE_URL'], MaxNumberOfMessages=10).get('Messages', [])
    return [json.loads(message['Body']) for message in messages]


def test_flatten_nested_dicts_lists_and_literals():
    assert data_transformer.flatten({'a': {'b': 1}, 'c': [True, None]}) == {
        'a.b': '1', 'c.0': 'true', 'c.1': 'null'}


def test_flatten_keeps_strings_and_false_and_floats():
    assert data_transformer.flatten({'s': 'x', 'f': False, 'n': 19.99}) == {
        's': 'x', 'f': 'false', 'n': '19.99'}


def test_flatten_nested_list_of_dicts():
    assert data_transformer.flatten({'items': [{'sku': 'A'}, {'sku': 'B'}]}) == {
        'items.0.sku': 'A', 'items.1.sku': 'B'}


def test_transform_lowercases_and_derives_day_and_field_count():
    result = data_transformer.transform(stored_item())

    assert result['eventId'] == 'id-1'
    assert result['source'] == 'billing'
    assert result['type'] == 'invoice.paid'
    assert result['day'] == '2018-05-01'
    fields = json.loads(result['fields'])
    assert fields == {'amount': '19.99', 'customer.id': '42', 'tags.0': 'a', 'tags.1': 'b'}
    assert result['fieldCount'] == 4
    assert result['transformedAt'].endswith('Z')


def test_transform_decodes_json_string_payload_before_flattening():
    result = data_transformer.transform(stored_item(payload='{"customer": {"id": 42}}'))

    assert json.loads(result['fields']) == {'customer.id': '42'}
    assert result['fieldCount'] == 1


@pytest.mark.parametrize('missing', ['eventId', 'source', 'type', 'timestamp', 'payload'])
def test_transform_rejects_missing_attribute(missing):
    item = stored_item()
    del item[missing]

    with pytest.raises(ValueError) as excinfo:
        data_transformer.transform(item)
    assert missing in str(excinfo.value)


def test_transform_rejects_payload_that_is_not_json():
    with pytest.raises(ValueError):
        data_transformer.transform(stored_item(payload='not json'))


def test_insert_record_writes_row_and_publishes_notification(pipeline, transformed_table):
    event = {'Records': [stream_record('INSERT', stored_item())]}

    result = data_transformer.handler(event, None)

    assert result == {'transformed': 1, 'skipped': 0}
    rows = transformed_table.scan()['Items']
    assert len(rows) == 1
    row = rows[0]
    assert row['eventId'] == 'id-1'
    assert row['source'] == 'billing'
    assert row['type'] == 'invoice.paid'
    assert row['day'] == '2018-05-01'
    assert row['fieldCount'] == 4
    assert json.loads(row['fields'])['customer.id'] == '42'

    received = notifications(pipeline)
    assert len(received) == 1
    assert received[0]['Subject'] == 'event.transformed'
    body = json.loads(received[0]['Message'])
    assert body == {'eventId': 'id-1', 'source': 'billing', 'type': 'invoice.paid',
                    'day': '2018-05-01', 'fieldCount': 4}


def test_incomplete_insert_image_is_skipped_without_raising(pipeline, transformed_table):
    image = {'eventId': 'id-2', 'timestamp': TIMESTAMP, 'status': 'processed'}
    event = {'Records': [stream_record('INSERT', image, keys={'eventId': 'id-2'})]}

    result = data_transformer.handler(event, None)

    assert result == {'transformed': 0, 'skipped': 1}
    assert transformed_table.scan()['Items'] == []
    assert notifications(pipeline) == []


@pytest.mark.parametrize('event_name', ['MODIFY', 'REMOVE'])
def test_modify_and_remove_records_are_skipped(pipeline, transformed_table, event_name):
    image = stored_item() if event_name == 'MODIFY' else None
    event = {'Records': [stream_record(event_name, image)]}

    result = data_transformer.handler(event, None)

    assert result == {'transformed': 0, 'skipped': 1}
    assert transformed_table.scan()['Items'] == []
    assert notifications(pipeline) == []


def test_mixed_batch_counts_each_record(pipeline, transformed_table):
    event = {'Records': [
        stream_record('INSERT', stored_item(eventId='id-a'), keys={'eventId': 'id-a'}),
        stream_record('MODIFY', stored_item(eventId='id-a'), keys={'eventId': 'id-a'}),
        stream_record('INSERT', stored_item(eventId='id-b'), keys={'eventId': 'id-b'}),
    ]}

    result = data_transformer.handler(event, None)

    assert result == {'transformed': 2, 'skipped': 1}
    ids = sorted(row['eventId'] for row in transformed_table.scan()['Items'])
    assert ids == ['id-a', 'id-b']


def test_empty_records_returns_zeros(pipeline):
    assert data_transformer.handler({'Records': []}, None) == {'transformed': 0, 'skipped': 0}


def test_transform_rejects_payload_nested_too_deeply():
    from functions import common
    depth = common.MAX_JSON_DEPTH + 1
    payload = '{"a": ' * depth + '1' + '}' * depth

    with pytest.raises(ValueError) as excinfo:
        data_transformer.transform(stored_item(payload=payload))
    assert 'nested more than' in str(excinfo.value)


def test_transform_accepts_payload_at_maximum_depth():
    from functions import common
    depth = common.MAX_JSON_DEPTH
    payload = '{"a": ' * depth + '1' + '}' * depth

    row = data_transformer.transform(stored_item(payload=payload))

    assert row['fieldCount'] == 1


def test_transform_rejects_payload_beyond_recursion_limit():
    payload = '[' * 100000 + ']' * 100000

    with pytest.raises(ValueError):
        data_transformer.transform(stored_item(payload=payload))


def test_transform_rejects_fields_larger_than_an_item_can_hold():
    payload = json.dumps({'k{}'.format(index): 'x' * 1000 for index in range(400)})

    with pytest.raises(ValueError) as excinfo:
        data_transformer.transform(stored_item(payload=payload))
    assert 'flattened fields exceed' in str(excinfo.value)


def test_poison_records_are_skipped_and_the_batch_continues(pipeline, transformed_table):
    records = [
        stream_record('INSERT', stored_item(eventId='deep', payload='[' * 100000 + ']' * 100000),
                      keys={'eventId': 'deep'}),
        stream_record('INSERT', stored_item(eventId='good'), keys={'eventId': 'good'}),
    ]

    result = data_transformer.handler({'Records': records}, None)

    assert result == {'transformed': 1, 'skipped': 1}
    assert [item['eventId'] for item in transformed_table.scan()['Items']] == ['good']
