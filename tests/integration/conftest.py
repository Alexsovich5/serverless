"""Integration fixtures: wait for LocalStack, bootstrap resources, queue and stream helpers."""

import os
import time

import boto3
import pytest

from functions.common import client
from local import bootstrap

KINESIS_ENDPOINT_URL = os.environ.get('KINESIS_ENDPOINT_URL') or 'http://localstack:4568'
STAGE = 'it'


def wait_for(fn, timeout=120, interval=1):
    """Call fn until it returns a truthy value without raising; return that value.

    The last exception (or a TimeoutError) is raised once the timeout passes.
    """
    deadline = time.time() + timeout
    last_error = None
    while True:
        try:
            result = fn()
            if result:
                return result
            last_error = None
        except Exception as error:  # any failure means "not ready yet"
            last_error = error
        if time.time() >= deadline:
            if last_error is not None:
                raise last_error
            raise TimeoutError('condition not met within {} s'.format(timeout))
        time.sleep(interval)


def _services_ready():
    client('dynamodb').list_tables()
    boto3.client('kinesis', endpoint_url=KINESIS_ENDPOINT_URL).list_streams()
    client('sns').list_topics()
    client('sqs').list_queues()
    return True


@pytest.fixture(scope='session')
def localstack():
    """Block until every emulated service the pipeline uses answers."""
    wait_for(_services_ready, timeout=180, interval=2)


@pytest.fixture(scope='session')
def resources(localstack):
    """Bootstrap the integration stage and export its settings to os.environ."""
    settings = bootstrap.bootstrap(stage=STAGE)
    os.environ.update(settings)
    return settings


def drain(queue_url):
    """Delete every message currently visible on a queue; return their bodies."""
    sqs = client('sqs')
    bodies = []
    while True:
        received = sqs.receive_message(
            QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=1)
        messages = received.get('Messages', [])
        if not messages:
            return bodies
        for message in messages:
            bodies.append(message['Body'])
            sqs.delete_message(
                QueueUrl=queue_url, ReceiptHandle=message['ReceiptHandle'])


def stream_arn(table_name):
    """Stream ARN for a table, filtered on the client side.

    LocalStack's ListStreams ignores the TableName filter and returns every
    stream-enabled table, so the first entry is not necessarily the right one.
    """
    resp = client('dynamodbstreams').list_streams()
    try:
        return next(s['StreamArn'] for s in resp['Streams']
                    if s['TableName'] == table_name)
    except StopIteration:
        raise LookupError('no stream found for table {}'.format(table_name))
