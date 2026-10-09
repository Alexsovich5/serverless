"""Check that LocalStack behaves the way the pipeline relies on."""

import json
import time

import pytest
from botocore.exceptions import ClientError

from functions.common import client
from local import bootstrap
from tests.integration.conftest import STAGE, drain, stream_arn, wait_for

EVENTS_TABLE = 'event-platform-events-{}'.format(STAGE)


def _receive_one(queue_url, timeout=30):
    sqs = client('sqs')

    def poll():
        received = sqs.receive_message(
            QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=1)
        return received.get('Messages')

    return wait_for(poll, timeout=timeout, interval=0)


def _create_scratch_table(name, stream=False):
    dynamodb = client('dynamodb')
    request = {
        'TableName': name,
        'KeySchema': [{'AttributeName': 'eventId', 'KeyType': 'HASH'}],
        'AttributeDefinitions': [{'AttributeName': 'eventId', 'AttributeType': 'S'}],
        'ProvisionedThroughput': {'ReadCapacityUnits': 1, 'WriteCapacityUnits': 1},
    }
    if stream:
        request['StreamSpecification'] = {
            'StreamEnabled': True, 'StreamViewType': 'NEW_IMAGE'}
    dynamodb.create_table(**request)
    dynamodb.get_waiter('table_exists').wait(TableName=name)


def _delete_table(name):
    dynamodb = client('dynamodb')
    dynamodb.delete_table(TableName=name)
    dynamodb.get_waiter('table_not_exists').wait(TableName=name)


@pytest.fixture
def scratch_table():
    name = 'it-scratch'
    _create_scratch_table(name)
    yield name
    _delete_table(name)


@pytest.fixture
def scratch_stream_table():
    name = 'it-scratch-stream'
    _create_scratch_table(name, stream=True)
    yield name
    _delete_table(name)


def test_bootstrap_is_idempotent(resources):
    again = bootstrap.bootstrap(stage=STAGE)
    assert again == resources
    assert all(again.values())


def test_single_publish_delivers_exactly_one_message(resources):
    bootstrap.bootstrap(stage=STAGE)
    queue_url = resources['INGEST_SUB_QUEUE_URL']
    drain(queue_url)

    client('sns').publish(
        TopicArn=resources['INGEST_TOPIC_ARN'],
        Message=json.dumps({'eventId': 'one-delivery'}))

    messages = _receive_one(queue_url)
    assert len(messages) == 1
    envelope = json.loads(messages[0]['Body'])
    assert 'Message' in envelope
    assert json.loads(envelope['Message']) == {'eventId': 'one-delivery'}

    sqs = client('sqs')
    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=messages[0]['ReceiptHandle'])
    time.sleep(1)
    again = sqs.receive_message(
        QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=2)
    assert again.get('Messages', []) == []


def test_events_table_stream_has_a_shard(resources):
    arn = stream_arn(resources['EVENTS_TABLE'])
    assert arn
    description = client('dynamodbstreams').describe_stream(StreamArn=arn)
    assert len(description['StreamDescription']['Shards']) >= 1


def test_stream_arn_filters_by_table_name(resources, scratch_stream_table):
    assert stream_arn(scratch_stream_table)
    arn = stream_arn(resources['EVENTS_TABLE'])
    assert ':table/{}/'.format(EVENTS_TABLE) in arn
    with pytest.raises(LookupError) as excinfo:
        stream_arn('no-such-table')
    assert 'no-such-table' in str(excinfo.value)


def test_conditional_put_rejects_second_write(resources, scratch_table):
    dynamodb = client('dynamodb')
    put = dict(
        TableName=scratch_table,
        Item={'eventId': {'S': 'dup-1'}},
        ConditionExpression='attribute_not_exists(eventId)')
    dynamodb.put_item(**put)
    with pytest.raises(ClientError) as excinfo:
        dynamodb.put_item(**put)
    assert excinfo.value.response['Error']['Code'] == 'ConditionalCheckFailedException'
