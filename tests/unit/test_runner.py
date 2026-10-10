import http.client
import json
import logging
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

import boto3
import pytest
from botocore.stub import Stubber

from local import runner


def test_event_path_maps_id_to_path_parameters():
    event = runner.http_to_apigw_event('GET', '/events/abc', '', None)

    assert event['httpMethod'] == 'GET'
    assert event['path'] == '/events/abc'
    assert event['resource'] == '/events/{id}'
    assert event['pathParameters'] == {'id': 'abc'}
    assert event['queryStringParameters'] is None
    assert event['body'] is None
    assert isinstance(event['headers'], dict)
    assert event['requestContext']['httpMethod'] == 'GET'
    assert event['requestContext']['resourcePath'] == '/events/{id}'


def test_list_path_parses_query_string():
    event = runner.http_to_apigw_event('GET', '/events', 'limit=5&type=x', None)

    assert event['resource'] == '/events'
    assert event['pathParameters'] is None
    assert event['queryStringParameters'] == {'limit': '5', 'type': 'x'}


def test_post_keeps_body_text():
    event = runner.http_to_apigw_event('POST', '/events/', '', '{"type": "a"}')

    assert event['resource'] == '/events'
    assert event['body'] == '{"type": "a"}'


@pytest.mark.parametrize('path', ['/nope', '/events/a/b', '/', '/eventsx'])
def test_unmatched_path_is_reported(path):
    assert runner.http_to_apigw_event('GET', path, '', None) is None


def test_lambda_context_stub():
    context = runner.LambdaContext('api')

    assert context.function_name == 'api'
    assert context.aws_request_id
    assert context.aws_request_id != runner.LambdaContext('api').aws_request_id
    assert context.get_remaining_time_in_millis() > 0


@pytest.fixture
def gateway(pipeline):
    server = runner.Gateway(0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield 'http://127.0.0.1:{}'.format(server.server_address[1])
    server.shutdown()
    server.server_close()
    thread.join(5)


def request(url, method='GET', body=None):
    data = body.encode('utf-8') if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, dict(resp.headers), json.loads(resp.read().decode('utf-8'))
    except urllib.error.HTTPError as err:
        return err.code, dict(err.headers), json.loads(err.read().decode('utf-8'))


def test_gateway_post_returns_202(gateway):
    status, headers, body = request(gateway + '/events', 'POST',
                                    json.dumps({'type': 'click', 'payload': {'x': 1}}))

    assert status == 202
    assert headers['Content-Type'] == 'application/json'
    assert body['status'] == 'queued'
    assert body['eventId']


def test_gateway_get_missing_event_returns_404(gateway):
    status, _, body = request(gateway + '/events/does-not-exist')

    assert status == 404
    assert body == {'error': 'Event not found'}


def test_gateway_list_passes_query_through(gateway):
    status, _, body = request(gateway + '/events?limit=500')

    assert status == 400
    assert 'limit' in body['error']


def test_gateway_unmatched_path_returns_404_json(gateway):
    status, headers, body = request(gateway + '/nope')

    assert status == 404
    assert headers['Content-Type'] == 'application/json'
    assert 'error' in body


def raw_request(base_url, method, path, headers, body=b''):
    parts = urllib.parse.urlsplit(base_url)
    conn = http.client.HTTPConnection(parts.hostname, parts.port, timeout=10)
    try:
        conn.putrequest(method, path)
        for name, value in headers.items():
            conn.putheader(name, value)
        conn.endheaders()
        if body:
            conn.send(body)
        resp = conn.getresponse()
        return resp.status, json.loads(resp.read().decode('utf-8'))
    finally:
        conn.close()


def test_gateway_listens_on_loopback_by_default(pipeline):
    server = runner.Gateway(0)
    try:
        assert server.server_address[0] == '127.0.0.1'
    finally:
        server.server_close()


def test_main_defaults_to_loopback_host():
    assert runner.parse_args([]).host == '127.0.0.1'
    assert runner.parse_args(['--host', '0.0.0.0']).host == '0.0.0.0'


def test_gateway_rejects_body_above_limit_without_reading_it(gateway):
    status, body = raw_request(gateway, 'POST', '/events', {
        'Content-Type': 'application/json',
        'Content-Length': str(runner.MAX_BODY_BYTES + 1),
    })

    assert status == 413
    assert body == {'error': 'Request body too large'}


@pytest.mark.parametrize('length', ['abc', '-5'])
def test_gateway_rejects_invalid_content_length(gateway, length):
    status, body = raw_request(gateway, 'POST', '/events', {
        'Content-Type': 'application/json', 'Content-Length': length})

    assert status == 400
    assert body == {'error': 'Invalid Content-Length'}


def test_gateway_rejects_text_plain_post(gateway):
    payload = b'{"type": "click", "payload": {}}'
    status, body = raw_request(gateway, 'POST', '/events', {
        'Content-Type': 'text/plain', 'Content-Length': str(len(payload))}, payload)

    assert status == 415


def test_sns_record_is_accepted_by_event_processor(pipeline):
    from functions import event_processor

    message = json.dumps({
        'eventId': 'sns-record-1',
        'receivedAt': '2018-05-01T12:00:00.000000Z',
        'event': {'source': 'billing', 'type': 'invoice', 'payload': {'n': 1}},
    })
    record = runner.sns_record(message)

    assert record['Records'][0]['EventSource'] == 'aws:sns'
    assert record['Records'][0]['Sns']['Message'] == message
    assert event_processor.handler(record, runner.LambdaContext('eventProcessor')) == {
        'processed': 1, 'duplicates': 0, 'failed': 0}


def _wait(condition, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if condition():
            return True
        time.sleep(0.05)
    return condition()


def test_poller_logs_handler_error_and_delivers_next_batch(pipeline, caplog):
    from functions.common import client

    queue_url = pipeline['INGEST_SUB_QUEUE_URL']
    sqs = client('sqs')
    for index in (1, 2):
        envelope = {'Type': 'Notification', 'Message': 'message-{}'.format(index)}
        sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(envelope))

    delivered = []

    def handler(event, context):
        delivered.append(event['Records'][0]['Sns']['Message'])
        if len(delivered) == 1:
            raise RuntimeError('first batch fails')
        return {}

    caplog.set_level(logging.ERROR, logger='local.runner')
    poller = runner.IngestPoller(queue_url, handler=handler, poll_interval=0.05)
    poller.start()
    try:
        assert _wait(lambda: len(delivered) >= 2)
        assert poller.is_alive()
    finally:
        poller.stop()
        poller.join(5)

    assert sorted(delivered) == ['message-1', 'message-2']
    assert 'first batch fails' in caplog.text
    assert not poller.is_alive()


def _stream(table, label):
    return {
        'StreamArn': 'arn:aws:dynamodb:us-east-1:000000000000:table/{}/stream/{}'.format(
            table, label),
        'TableName': table,
        'StreamLabel': label,
    }


def _stubbed_streams_client(streams):
    streams_client = boto3.client('dynamodbstreams', region_name='us-east-1')
    stubber = Stubber(streams_client)
    stubber.add_response('list_streams', {'Streams': streams})
    stubber.activate()
    return streams_client, stubber


def test_stream_poller_matches_table_name_not_first_stream():
    other = _stream('event-platform-events-local', 'a')
    wanted = _stream('event-platform-events-it', 'b')
    streams_client, stubber = _stubbed_streams_client([other, wanted])

    poller = runner.StreamPoller('event-platform-events-it', streams_client=streams_client)

    assert poller.resolve_stream_arn() == wanted['StreamArn']
    stubber.assert_no_pending_responses()


def test_stream_poller_start_raises_when_no_stream_matches():
    streams_client, _ = _stubbed_streams_client([_stream('other-table', 'a')])
    poller = runner.StreamPoller('event-platform-events-it', streams_client=streams_client)

    with pytest.raises(LookupError) as excinfo:
        poller.start()

    assert 'event-platform-events-it' in str(excinfo.value)
    assert not poller.is_alive()


SHARD_ID = 'shardId-00000001527000000000-00000001'


def test_stream_poller_follows_iterators_and_survives_handler_error(caplog):
    wanted = _stream('events', 'a')
    streams_client = boto3.client('dynamodbstreams', region_name='us-east-1')
    stubber = Stubber(streams_client)
    stubber.add_response('list_streams', {'Streams': [wanted]})
    stubber.add_response(
        'describe_stream',
        {'StreamDescription': {'StreamArn': wanted['StreamArn'],
                               'Shards': [{'ShardId': SHARD_ID}]}},
        {'StreamArn': wanted['StreamArn']})
    stubber.add_response(
        'get_shard_iterator', {'ShardIterator': 'it-0'},
        {'StreamArn': wanted['StreamArn'], 'ShardId': SHARD_ID,
         'ShardIteratorType': 'TRIM_HORIZON'})
    stubber.add_response(
        'get_records',
        {'Records': [{'eventID': '1', 'eventName': 'INSERT'}], 'NextShardIterator': 'it-1'},
        {'ShardIterator': 'it-0', 'Limit': 25})
    stubber.add_response(
        'get_records',
        {'Records': [{'eventID': '2', 'eventName': 'INSERT'}], 'NextShardIterator': 'it-2'},
        {'ShardIterator': 'it-1', 'Limit': 25})
    stubber.activate()

    batches = []

    def handler(event, context):
        batches.append([record['eventID'] for record in event['Records']])
        if len(batches) == 1:
            raise ValueError('bad stream record')
        return {}

    caplog.set_level(logging.ERROR, logger='local.runner')
    poller = runner.StreamPoller('events', handler=handler, streams_client=streams_client)
    poller.open()
    poller.poll_once()
    poller.poll_once()

    assert batches == [['1'], ['2']]
    assert 'bad stream record' in caplog.text
    stubber.assert_no_pending_responses()


def _record(sequence):
    return {'eventID': sequence, 'eventName': 'INSERT',
            'dynamodb': {'SequenceNumber': sequence.zfill(21)}}


def test_stream_poller_reopens_shard_without_next_iterator_and_skips_seen_records():
    # LocalStack's GetRecords returns no NextShardIterator, so the poller
    # reopens the shard each round and drops records it already delivered.
    wanted = _stream('events', 'a')
    arn = wanted['StreamArn']
    iterator_params = {'StreamArn': arn, 'ShardId': SHARD_ID,
                       'ShardIteratorType': 'TRIM_HORIZON'}
    streams_client = boto3.client('dynamodbstreams', region_name='us-east-1')
    stubber = Stubber(streams_client)
    stubber.add_response('list_streams', {'Streams': [wanted]})
    stubber.add_response(
        'describe_stream',
        {'StreamDescription': {'StreamArn': arn, 'Shards': [{'ShardId': SHARD_ID}]}},
        {'StreamArn': arn})
    stubber.add_response('get_shard_iterator', {'ShardIterator': 'it-0'}, iterator_params)
    stubber.add_response('get_records', {'Records': [_record('1'), _record('2')]},
                         {'ShardIterator': 'it-0', 'Limit': 25})
    stubber.add_response('get_shard_iterator', {'ShardIterator': 'it-1'}, iterator_params)
    stubber.add_response('get_records',
                         {'Records': [_record('1'), _record('2'), _record('3')]},
                         {'ShardIterator': 'it-1'})
    stubber.add_response('get_shard_iterator', {'ShardIterator': 'it-2'}, iterator_params)
    stubber.add_response('get_records',
                         {'Records': [_record('1'), _record('2'), _record('3')]},
                         {'ShardIterator': 'it-2'})
    stubber.activate()

    batches = []
    poller = runner.StreamPoller(
        'events', handler=lambda event, context: batches.append(
            [record['eventID'] for record in event['Records']]),
        streams_client=streams_client)
    poller.open()

    assert poller.poll_once() is True
    assert poller.poll_once() is True
    assert poller.poll_once() is False

    assert batches == [['1', '2'], ['3']]
    stubber.assert_no_pending_responses()
