import boto3
import mock
import pytest
from moto import mock_dynamodb2, mock_sns, mock_sqs

from local import bootstrap

ENDPOINT_VARIABLES = (
    'DYNAMODB_ENDPOINT_URL',
    'SNS_ENDPOINT_URL',
    'SQS_ENDPOINT_URL',
)

EXPECTED_KEYS = (
    'EVENTS_TABLE',
    'TRANSFORMED_TABLE',
    'INGEST_TOPIC_ARN',
    'NOTIFY_TOPIC_ARN',
    'FAILED_QUEUE_URL',
    'INGEST_SUB_QUEUE_URL',
    'NOTIFY_SUB_QUEUE_URL',
)


@pytest.fixture(autouse=True)
def no_endpoint_overrides(monkeypatch):
    for name in ENDPOINT_VARIABLES:
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def aws():
    with mock_dynamodb2(), mock_sns(), mock_sqs():
        yield


def test_specs_load_with_event_id_hash_key():
    specs = bootstrap.load_table_specs()
    assert [spec['nameSuffix'] for spec in specs] == ['events', 'transformed']
    for spec in specs:
        assert spec['keySchema'] == [
            {'AttributeName': 'eventId', 'KeyType': 'HASH'}
        ]


def test_table_name_uses_suffix_and_stage():
    spec = {'nameSuffix': 'events'}
    assert bootstrap.table_name(spec, 'it') == 'event-platform-events-it'


def test_create_tables_builds_events_table_with_gsi(aws):
    dynamodb = boto3.client('dynamodb')
    bootstrap.create_tables(dynamodb, bootstrap.load_table_specs(), 'local')

    names = sorted(dynamodb.list_tables()['TableNames'])
    assert names == [
        'event-platform-events-local',
        'event-platform-transformed-local',
    ]
    events = dynamodb.describe_table(
        TableName='event-platform-events-local')['Table']
    index_names = [index['IndexName']
                   for index in events.get('GlobalSecondaryIndexes', [])]
    assert index_names == ['type-timestamp-index']
    assert events['KeySchema'] == [
        {'AttributeName': 'eventId', 'KeyType': 'HASH'}
    ]


def test_bootstrap_twice_is_idempotent(aws):
    bootstrap.bootstrap()
    result = bootstrap.bootstrap()

    dynamodb = boto3.client('dynamodb')
    sns = boto3.client('sns')
    sqs = boto3.client('sqs')

    assert len(dynamodb.list_tables()['TableNames']) == 2
    assert len(sns.list_topics()['Topics']) == 2
    assert len(sqs.list_queues()['QueueUrls']) == 3
    for topic_key in ('INGEST_TOPIC_ARN', 'NOTIFY_TOPIC_ARN'):
        subscriptions = sns.list_subscriptions_by_topic(
            TopicArn=result[topic_key])['Subscriptions']
        assert len(subscriptions) == 1


def test_create_messaging_skips_subscribe_when_queue_already_subscribed(aws):
    # LocalStack appends a new subscription on every Subscribe call, so the
    # bootstrap must not call it again for a queue that is already subscribed.
    sns = boto3.client('sns')
    sqs = boto3.client('sqs')
    with mock.patch.object(sns, 'subscribe', wraps=sns.subscribe) as spy:
        bootstrap.create_messaging(sns, sqs, 'local')
        bootstrap.create_messaging(sns, sqs, 'local')

    assert spy.call_count == 2
    protocols = sorted(call[1]['Protocol'] for call in spy.call_args_list)
    assert protocols == ['sqs', 'sqs']


def test_ingest_topic_has_sqs_subscription_to_sub_queue(aws):
    result = bootstrap.bootstrap()

    sqs = boto3.client('sqs')
    queue_arn = sqs.get_queue_attributes(
        QueueUrl=result['INGEST_SUB_QUEUE_URL'],
        AttributeNames=['QueueArn'])['Attributes']['QueueArn']
    subscriptions = boto3.client('sns').list_subscriptions_by_topic(
        TopicArn=result['INGEST_TOPIC_ARN'])['Subscriptions']
    assert [(s['Protocol'], s['Endpoint']) for s in subscriptions] == [
        ('sqs', queue_arn)
    ]


def test_bootstrap_returns_every_key_non_empty(aws):
    result = bootstrap.bootstrap(stage='it')

    assert sorted(result) == sorted(EXPECTED_KEYS)
    for key in EXPECTED_KEYS:
        assert result[key], key
    assert result['EVENTS_TABLE'] == 'event-platform-events-it'
    assert result['TRANSFORMED_TABLE'] == 'event-platform-transformed-it'
    assert result['INGEST_TOPIC_ARN'].endswith(':event-platform-ingest-it')
    assert result['NOTIFY_TOPIC_ARN'].endswith(
        ':event-platform-notifications-it')
    assert result['FAILED_QUEUE_URL'].endswith('/event-platform-failed-it')


def test_main_prints_and_writes_env_file(monkeypatch, tmpdir, capsys):
    settings = {'EVENTS_TABLE': 'event-platform-events-x',
                'FAILED_QUEUE_URL': 'http://queue/failed'}
    monkeypatch.setattr(bootstrap, 'PROJECT_ROOT', str(tmpdir))
    monkeypatch.setattr(bootstrap, 'bootstrap', lambda stage: dict(
        settings, STAGE=stage))

    bootstrap.main(['--stage', 'x'])

    expected = ('EVENTS_TABLE=event-platform-events-x\n'
                'FAILED_QUEUE_URL=http://queue/failed\n'
                'STAGE=x\n')
    assert capsys.readouterr().out == expected
    assert tmpdir.join('var', 'local.env').read() == expected
