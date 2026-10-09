"""Create the tables, topics and queues the pipeline needs on a boto3 endpoint.

Every step is idempotent, so the bootstrap can run against an endpoint that
already holds some or all of the resources.
"""

import argparse
import json
import os

from functions.common import client

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_SPECS = 'config/dynamodb_tables.json'
ENV_FILE = os.path.join('var', 'local.env')
NAME_PREFIX = 'event-platform'


def _project_path(path):
    if os.path.isabs(path):
        return path
    return os.path.join(PROJECT_ROOT, path)


def load_table_specs(path=DEFAULT_SPECS):
    """Return the list of table specs from the JSON definitions file."""
    with open(_project_path(path)) as handle:
        return json.load(handle)['tables']


def table_name(spec, stage):
    """Physical table name for a spec in a stage."""
    return '{}-{}-{}'.format(NAME_PREFIX, spec['nameSuffix'], stage)


def _throughput(spec):
    return {
        'ReadCapacityUnits': spec['read'],
        'WriteCapacityUnits': spec['write'],
    }


def _create_table_request(spec, stage):
    request = {
        'TableName': table_name(spec, stage),
        'KeySchema': spec['keySchema'],
        'AttributeDefinitions': spec['attributes'],
        'ProvisionedThroughput': _throughput(spec),
    }
    indexes = spec.get('globalSecondaryIndexes')
    if indexes:
        request['GlobalSecondaryIndexes'] = [
            dict(index, ProvisionedThroughput=_throughput(spec))
            for index in indexes
        ]
    if spec.get('stream'):
        request['StreamSpecification'] = {
            'StreamEnabled': True,
            'StreamViewType': spec['stream'],
        }
    return request


def create_tables(dynamodb_client, specs, stage):
    """Create each table that does not exist yet and wait until it does."""
    existing = set(dynamodb_client.list_tables()['TableNames'])
    for spec in specs:
        name = table_name(spec, stage)
        if name not in existing:
            dynamodb_client.create_table(**_create_table_request(spec, stage))
        dynamodb_client.get_waiter('table_exists').wait(TableName=name)


def _queue_arn(sqs_client, queue_url):
    attributes = sqs_client.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=['QueueArn'])['Attributes']
    return attributes['QueueArn']


def _subscribe_queue(sns_client, sqs_client, topic_arn, queue_url):
    """Subscribe a queue to a topic unless an SQS subscription already exists.

    Some emulators append a duplicate subscription on every Subscribe call,
    so the existing subscriptions are checked first.
    """
    queue_arn = _queue_arn(sqs_client, queue_url)
    subscriptions = sns_client.list_subscriptions_by_topic(
        TopicArn=topic_arn)['Subscriptions']
    for subscription in subscriptions:
        if (subscription.get('Protocol') == 'sqs'
                and subscription.get('Endpoint') == queue_arn):
            return
    sns_client.subscribe(
        TopicArn=topic_arn, Protocol='sqs', Endpoint=queue_arn)


def create_messaging(sns_client, sqs_client, stage):
    """Create topics, queues and the local topic-to-queue subscriptions."""
    def topic(suffix):
        name = '{}-{}-{}'.format(NAME_PREFIX, suffix, stage)
        return sns_client.create_topic(Name=name)['TopicArn']

    def queue(name):
        return sqs_client.create_queue(QueueName=name)['QueueUrl']

    ingest_topic = topic('ingest')
    notify_topic = topic('notifications')
    failed_queue = queue('{}-failed-{}'.format(NAME_PREFIX, stage))
    ingest_sub_queue = queue('local-ingest-sub-{}'.format(stage))
    notify_sub_queue = queue('local-notify-sub-{}'.format(stage))

    _subscribe_queue(sns_client, sqs_client, ingest_topic, ingest_sub_queue)
    _subscribe_queue(sns_client, sqs_client, notify_topic, notify_sub_queue)

    return {
        'INGEST_TOPIC_ARN': ingest_topic,
        'NOTIFY_TOPIC_ARN': notify_topic,
        'FAILED_QUEUE_URL': failed_queue,
        'INGEST_SUB_QUEUE_URL': ingest_sub_queue,
        'NOTIFY_SUB_QUEUE_URL': notify_sub_queue,
    }


def bootstrap(stage='local'):
    """Create every resource for a stage and return env-style settings."""
    specs = load_table_specs()
    create_tables(client('dynamodb'), specs, stage)
    names = {spec['nameSuffix']: table_name(spec, stage) for spec in specs}
    result = {
        'EVENTS_TABLE': names['events'],
        'TRANSFORMED_TABLE': names['transformed'],
    }
    result.update(create_messaging(client('sns'), client('sqs'), stage))
    return result


def format_env(settings):
    return ''.join('{}={}\n'.format(key, settings[key])
                   for key in sorted(settings))


def main(argv=None):
    parser = argparse.ArgumentParser(
        description='Create the local tables, topics and queues.')
    parser.add_argument('--stage', default='local')
    args = parser.parse_args(argv)

    settings = bootstrap(stage=args.stage)
    text = format_env(settings)
    print(text, end='')

    env_path = _project_path(ENV_FILE)
    os.makedirs(os.path.dirname(env_path), exist_ok=True)
    with open(env_path, 'w') as handle:
        handle.write(text)


if __name__ == '__main__':
    main()
