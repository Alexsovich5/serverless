"""Checks on the CloudFormation template produced by `serverless package`."""

import json
import os

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
TEMPLATE = os.path.join(ROOT, '.serverless', 'cloudformation-template-update-stack.json')


@pytest.fixture(scope='module')
def resources():
    with open(TEMPLATE) as handle:
        return json.load(handle)['Resources']


def _of_type(resources, kind):
    return {name: res for name, res in resources.items() if res['Type'] == kind}


def test_three_python36_functions(resources):
    functions = _of_type(resources, 'AWS::Lambda::Function')
    assert len(functions) == 3
    for function in functions.values():
        assert function['Properties']['Runtime'] == 'python3.6'


def test_three_api_methods_besides_options(resources):
    methods = [
        res['Properties']['HttpMethod']
        for res in _of_type(resources, 'AWS::ApiGateway::Method').values()
        if res['Properties']['HttpMethod'] != 'OPTIONS'
    ]
    assert sorted(methods) == ['GET', 'GET', 'POST']


def test_stream_event_source_mapping(resources):
    mappings = list(_of_type(resources, 'AWS::Lambda::EventSourceMapping').values())
    assert len(mappings) == 1
    props = mappings[0]['Properties']
    assert props['EventSourceArn'] == {'Fn::GetAtt': ['EventsTable', 'StreamArn']}
    assert props['BatchSize'] == 25
    assert props['StartingPosition'] == 'TRIM_HORIZON'


def test_ingest_topic_has_a_lambda_subscription(resources):
    topics = _of_type(resources, 'AWS::SNS::Topic')
    ingest = [res for res in topics.values()
              if res['Properties'].get('TopicName') == 'event-platform-ingest-dev']
    assert len(ingest) == 1
    subscriptions = ingest[0]['Properties'].get('Subscription', [])
    assert [sub['Protocol'] for sub in subscriptions] == ['lambda']
    assert 'NotificationsTopic' in topics


def test_tables_have_provisioned_throughput(resources):
    tables = _of_type(resources, 'AWS::DynamoDB::Table')
    assert set(tables) == {'EventsTable', 'TransformedTable'}
    for table in tables.values():
        props = table['Properties']
        assert 'BillingMode' not in props
        assert props['ProvisionedThroughput'] == {
            'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
    events = tables['EventsTable']['Properties']
    assert events['TableName'] == 'event-platform-events-dev'
    assert [index['IndexName'] for index in events['GlobalSecondaryIndexes']] == [
        'type-timestamp-index']
    assert events['StreamSpecification'] == {'StreamViewType': 'NEW_IMAGE'}


def test_failed_events_queue(resources):
    queues = _of_type(resources, 'AWS::SQS::Queue')
    names = [res['Properties']['QueueName'] for res in queues.values()]
    assert names == ['event-platform-failed-dev']
