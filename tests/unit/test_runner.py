import json
import threading
import urllib.error
import urllib.request

import pytest

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
