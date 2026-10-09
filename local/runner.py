"""Local stand-ins for the Lambda runtime and API Gateway.

Requests on /events and /events/{id} are turned into API Gateway
Lambda-proxy events and passed to functions.api_handler in-process.
"""

import json
import logging
import re
import socketserver
import time
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qsl, unquote, urlsplit

from functions import api_handler

logger = logging.getLogger(__name__)

STAGE = 'local'
LIST_RESOURCE = '/events'
EVENT_RESOURCE = '/events/{id}'
EVENT_PATH = re.compile(r'^/events/([^/]+)/?$')
LIST_PATH = re.compile(r'^/events/?$')
DEFAULT_TIMEOUT_MS = 30000


def _match(raw_path):
    """Return (resource, pathParameters) for a known path, or None."""
    if LIST_PATH.match(raw_path):
        return LIST_RESOURCE, None
    match = EVENT_PATH.match(raw_path)
    if match:
        return EVENT_RESOURCE, {'id': unquote(match.group(1))}
    return None


def http_to_apigw_event(method, raw_path, query, body, headers=None):
    """Build an API Gateway Lambda-proxy event, or None when the path is unknown."""
    matched = _match(raw_path)
    if matched is None:
        return None
    resource, path_params = matched
    params = dict(parse_qsl(query or '', keep_blank_values=True))
    return {
        'httpMethod': method,
        'path': raw_path,
        'resource': resource,
        'pathParameters': path_params,
        'queryStringParameters': params or None,
        'headers': dict(headers or {}),
        'body': body,
        'isBase64Encoded': False,
        'stageVariables': None,
        'requestContext': {
            'httpMethod': method,
            'path': raw_path,
            'resourcePath': resource,
            'stage': STAGE,
            'requestId': str(uuid.uuid4()),
            'identity': {'sourceIp': '127.0.0.1'},
        },
    }


class LambdaContext(object):
    """Minimal stand-in for the context object Lambda passes to handlers."""

    def __init__(self, function_name, timeout_ms=DEFAULT_TIMEOUT_MS):
        self.function_name = function_name
        self.aws_request_id = str(uuid.uuid4())
        self._deadline = time.time() + timeout_ms / 1000.0

    def get_remaining_time_in_millis(self):
        return max(0, int((self._deadline - time.time()) * 1000))


class _GatewayHandler(BaseHTTPRequestHandler):
    """Translate each HTTP request into a proxy event and write back the result."""

    def _dispatch(self):
        parts = urlsplit(self.path)
        length = int(self.headers.get('Content-Length') or 0)
        body = self.rfile.read(length).decode('utf-8') if length else None
        event = http_to_apigw_event(self.command, parts.path, parts.query, body,
                                    dict(self.headers.items()))
        if event is None:
            result = {
                'statusCode': 404,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({'error': 'Not found'}),
            }
        else:
            result = api_handler.handler(event, LambdaContext('api'))
        self._write(result)

    def _write(self, result):
        payload = (result.get('body') or '').encode('utf-8')
        self.send_response(result.get('statusCode', 200))
        for name, value in (result.get('headers') or {}).items():
            self.send_header(name, value)
        self.send_header('Content-Length', str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    do_GET = _dispatch
    do_POST = _dispatch
    do_PUT = _dispatch
    do_DELETE = _dispatch
    do_PATCH = _dispatch

    def log_message(self, fmt, *args):
        logger.info('%s %s', self.address_string(), fmt % args)


class Gateway(socketserver.ThreadingMixIn, HTTPServer):
    """Threaded HTTP server that serves api_handler like API Gateway would."""

    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, port, host='0.0.0.0'):
        HTTPServer.__init__(self, (host, port), _GatewayHandler)
