import json
from datetime import datetime
from decimal import Decimal

import pytest

from functions import common

REQUIRED = {
    'EVENTS_TABLE': 'event-platform-events-local',
    'TRANSFORMED_TABLE': 'event-platform-transformed-local',
    'INGEST_TOPIC_ARN': 'arn:aws:sns:us-east-1:000000000000:ingest',
    'NOTIFY_TOPIC_ARN': 'arn:aws:sns:us-east-1:000000000000:notify',
    'FAILED_QUEUE_URL': 'http://localstack:4576/queue/failed',
}


@pytest.fixture
def full_env(monkeypatch):
    for name, value in REQUIRED.items():
        monkeypatch.setenv(name, value)
    return monkeypatch


def test_get_config_lists_every_missing_variable(full_env):
    full_env.delenv('EVENTS_TABLE')
    full_env.delenv('INGEST_TOPIC_ARN')
    with pytest.raises(common.ConfigError) as excinfo:
        common.get_config()
    message = str(excinfo.value)
    assert 'EVENTS_TABLE' in message
    assert 'INGEST_TOPIC_ARN' in message
    assert 'TRANSFORMED_TABLE' not in message


def test_get_config_returns_lower_case_keys(full_env):
    config = common.get_config()
    assert config == {name.lower(): value for name, value in REQUIRED.items()}


def test_client_uses_service_endpoint_override(monkeypatch):
    monkeypatch.setenv('SQS_ENDPOINT_URL', 'http://localstack:4576')
    sqs = common.client('sqs')
    assert sqs._endpoint.host == 'http://localstack:4576'


def test_client_uses_default_endpoint_when_unset(monkeypatch):
    monkeypatch.delenv('SQS_ENDPOINT_URL', raising=False)
    sqs = common.client('sqs')
    assert sqs._endpoint.host == 'https://queue.amazonaws.com'


def test_empty_endpoint_override_counts_as_unset(monkeypatch):
    monkeypatch.setenv('SQS_ENDPOINT_URL', '')
    assert common._endpoint_url('sqs') is None
    sqs = common.client('sqs')
    assert sqs._endpoint.host == 'https://queue.amazonaws.com'


def test_endpoint_variable_strips_non_alphanumerics(monkeypatch):
    monkeypatch.setenv('DYNAMODBSTREAMS_ENDPOINT_URL', 'http://localstack:4570')
    streams = common.client('dynamodbstreams')
    assert streams._endpoint.host == 'http://localstack:4570'
    assert common.endpoint_env_var('dynamodb-streams') == 'DYNAMODBSTREAMS_ENDPOINT_URL'


def test_resource_uses_service_endpoint_override(monkeypatch):
    monkeypatch.setenv('DYNAMODB_ENDPOINT_URL', 'http://localstack:4569')
    dynamodb = common.resource('dynamodb')
    assert dynamodb.meta.client._endpoint.host == 'http://localstack:4569'


def test_decimal_encoder_keeps_integers_and_floats():
    encoded = json.dumps({'a': Decimal('3'), 'b': Decimal('1.5')}, cls=common.DecimalEncoder)
    assert json.loads(encoded) == {'a': 3, 'b': 1.5}
    assert '"a": 3,' in encoded
    assert isinstance(json.loads(encoded)['a'], int)


def test_decimal_encoder_rejects_unknown_types():
    with pytest.raises(TypeError):
        json.dumps({'a': object()}, cls=common.DecimalEncoder)


def test_response_shape():
    result = common.response(201, {'id': 'x', 'n': Decimal('2')})
    assert result['statusCode'] == 201
    assert result['headers']['Content-Type'] == 'application/json'
    assert result['headers']['Access-Control-Allow-Origin'] == '*'
    assert json.loads(result['body']) == {'id': 'x', 'n': 2}


def test_utc_now_iso_format():
    value = common.utc_now_iso()
    assert value.endswith('Z')
    datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%fZ')
