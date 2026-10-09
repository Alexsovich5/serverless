"""Shared unit-test fixtures: environment, moto-backed pipeline and event builders."""

import json

import pytest
from moto import mock_dynamodb2, mock_sns, mock_sqs

from local import bootstrap

ENDPOINT_VARIABLES = (
    'DYNAMODB_ENDPOINT_URL',
    'DYNAMODBSTREAMS_ENDPOINT_URL',
    'SNS_ENDPOINT_URL',
    'SQS_ENDPOINT_URL',
)

DEFAULT_ENV = {
    'EVENTS_TABLE': 'event-platform-events-test',
    'TRANSFORMED_TABLE': 'event-platform-transformed-test',
    'INGEST_TOPIC_ARN': 'arn:aws:sns:us-east-1:123456789012:event-platform-ingest-test',
    'NOTIFY_TOPIC_ARN': 'arn:aws:sns:us-east-1:123456789012:event-platform-notifications-test',
    'FAILED_QUEUE_URL': 'https://queue.amazonaws.com/123456789012/event-platform-failed-test',
    'AWS_DEFAULT_REGION': 'us-east-1',
    'AWS_ACCESS_KEY_ID': 'testing',
    'AWS_SECRET_ACCESS_KEY': 'testing',
}


@pytest.fixture
def aws_env(monkeypatch):
    """Set the required variables and clear endpoint overrides so moto applies."""
    for name, value in DEFAULT_ENV.items():
        monkeypatch.setenv(name, value)
    for name in ENDPOINT_VARIABLES:
        monkeypatch.delenv(name, raising=False)
    return monkeypatch


@pytest.fixture
def pipeline(aws_env):
    """Start moto mocks, create every resource and export their names."""
    with mock_dynamodb2(), mock_sns(), mock_sqs():
        resources = bootstrap.bootstrap('test')
        for name, value in resources.items():
            aws_env.setenv(name, value)
        yield resources


def apigw_event(method, path_params=None, qs=None, body=None):
    """API Gateway Lambda-proxy event; a non-string body is JSON-encoded."""
    if body is not None and not isinstance(body, str):
        body = json.dumps(body)
    if path_params and 'id' in path_params:
        path = '/events/{}'.format(path_params['id'])
        resource = '/events/{id}'
    else:
        path = '/events'
        resource = '/events'
    return {
        'httpMethod': method,
        'path': path,
        'resource': resource,
        'pathParameters': path_params,
        'queryStringParameters': qs,
        'headers': {'Content-Type': 'application/json'},
        'body': body,
        'isBase64Encoded': False,
    }


def sns_event(messages):
    """Lambda SNS event with one record per message (dicts are JSON-encoded)."""
    records = []
    for index, message in enumerate(messages):
        if not isinstance(message, str):
            message = json.dumps(message)
        records.append({
            'EventSource': 'aws:sns',
            'EventVersion': '1.0',
            'EventSubscriptionArn': DEFAULT_ENV['INGEST_TOPIC_ARN'] + ':sub',
            'Sns': {
                'Type': 'Notification',
                'MessageId': 'message-{}'.format(index),
                'TopicArn': DEFAULT_ENV['INGEST_TOPIC_ARN'],
                'Subject': None,
                'Message': message,
                'Timestamp': '2018-05-01T12:00:00.000Z',
                'MessageAttributes': {},
            },
        })
    return {'Records': records}
