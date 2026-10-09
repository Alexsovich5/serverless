"""serverless.yml agrees with the handler modules, the table config and common.get_config()."""

import importlib
import json
import os

import pytest
import yaml

from functions import common

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture(scope='module')
def config():
    with open(os.path.join(ROOT, 'serverless.yml')) as handle:
        return yaml.safe_load(handle)


@pytest.fixture(scope='module')
def table_specs():
    with open(os.path.join(ROOT, 'config', 'dynamodb_tables.json')) as handle:
        return {spec['logicalId']: spec for spec in json.load(handle)['tables']}


def _events(config, kind):
    found = []
    for name, function in config['functions'].items():
        for event in function.get('events', []):
            if kind in event:
                found.append((name, event[kind]))
    return found


def test_every_handler_resolves_to_a_callable(config):
    assert len(config['functions']) == 3
    for function in config['functions'].values():
        module_path, attribute = function['handler'].rsplit('.', 1)
        module = importlib.import_module(module_path.replace('/', '.'))
        assert callable(getattr(module, attribute))


def test_http_events_cover_the_api_routes(config):
    routes = set()
    for name, http in _events(config, 'http'):
        assert name == 'apiHandler'
        assert http.get('cors') is True
        routes.add((http['method'].upper(), '/' + http['path'].lstrip('/')))
    assert routes == {('POST', '/events'), ('GET', '/events'), ('GET', '/events/{id}')}


def test_no_function_uses_an_sqs_event(config):
    assert _events(config, 'sqs') == []


def test_event_processor_is_triggered_by_the_ingest_topic(config):
    assert _events(config, 'sns') == [
        ('eventProcessor', '${self:service}-ingest-${self:provider.stage}')]


def test_data_transformer_reads_the_events_stream(config):
    streams = _events(config, 'stream')
    assert len(streams) == 1
    name, stream = streams[0]
    assert name == 'dataTransformer'
    assert stream['type'] == 'dynamodb'
    assert stream['arn'] == {'Fn::GetAtt': ['EventsTable', 'StreamArn']}
    assert stream['batchSize'] == 25
    assert stream['startingPosition'] == 'TRIM_HORIZON'


def test_billing_mode_is_absent(config):
    assert 'BillingMode' not in json.dumps(config)


def test_events_table_matches_the_table_config(config, table_specs):
    spec = table_specs['EventsTable']
    props = config['resources']['Resources']['EventsTable']['Properties']
    assert props['KeySchema'] == spec['keySchema']
    assert props['AttributeDefinitions'] == spec['attributes']
    indexes = [
        {key: index[key] for key in ('IndexName', 'KeySchema', 'Projection')}
        for index in props['GlobalSecondaryIndexes']
    ]
    assert indexes == spec['globalSecondaryIndexes']
    assert props['StreamSpecification'] == {'StreamViewType': spec['stream']}
    assert props['ProvisionedThroughput'] == {
        'ReadCapacityUnits': spec['read'], 'WriteCapacityUnits': spec['write']}


def test_transformed_table_matches_the_table_config(config, table_specs):
    spec = table_specs['TransformedTable']
    props = config['resources']['Resources']['TransformedTable']['Properties']
    assert props['KeySchema'] == spec['keySchema']
    assert props['AttributeDefinitions'] == spec['attributes']
    assert props['ProvisionedThroughput'] == {
        'ReadCapacityUnits': spec['read'], 'WriteCapacityUnits': spec['write']}


def test_environment_keys_equal_the_required_config_keys(config):
    assert set(config['provider']['environment']) == set(common.REQUIRED_VARIABLES)


def test_environment_satisfies_get_config(config, monkeypatch):
    for name in common.REQUIRED_VARIABLES:
        monkeypatch.delenv(name, raising=False)
    for name in config['provider']['environment']:
        monkeypatch.setenv(name, 'value')
    assert set(common.get_config()) == {name.lower() for name in common.REQUIRED_VARIABLES}


def test_package_excludes_non_runtime_files(config):
    excluded = set(config['package']['exclude'])
    assert {'tests/**', 'local/**', 'docs/**', 'node_modules/**', 'var/**',
            'requirements-dev.txt'} <= excluded
