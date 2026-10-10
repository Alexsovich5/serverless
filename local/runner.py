"""Local stand-ins for the Lambda runtime, API Gateway and the Lambda triggers.

Requests on /events and /events/{id} are turned into API Gateway
Lambda-proxy events and passed to functions.api_handler in-process.
IngestPoller delivers SNS messages (through their SQS subscription queue)
to functions.event_processor, and StreamPoller delivers DynamoDB stream
records of the events table to functions.data_transformer.
"""

import argparse
import json
import logging
import os
import re
import signal
import socketserver
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qsl, unquote, urlsplit

from functions import api_handler, data_transformer, event_processor
from functions.common import client
from local import bootstrap

logger = logging.getLogger(__name__)

STAGE = 'local'
LIST_RESOURCE = '/events'
EVENT_RESOURCE = '/events/{id}'
EVENT_PATH = re.compile(r'^/events/([^/]+)/?$')
LIST_PATH = re.compile(r'^/events/?$')
DEFAULT_TIMEOUT_MS = 30000
DEFAULT_HOST = '127.0.0.1'
MAX_BODY_BYTES = 1024 * 1024


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

    def _error(self, status, message):
        self.close_connection = True
        self._write({
            'statusCode': status,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': message}),
        })

    def _content_length(self):
        raw = self.headers.get('Content-Length') or '0'
        try:
            length = int(raw)
        except ValueError:
            return None
        return length if length >= 0 else None

    def _dispatch(self):
        parts = urlsplit(self.path)
        length = self._content_length()
        if length is None:
            return self._error(400, 'Invalid Content-Length')
        if length > MAX_BODY_BYTES:
            return self._error(413, 'Request body too large')
        raw = self.rfile.read(length) if length else b''
        try:
            body = raw.decode('utf-8') if length else None
        except UnicodeDecodeError:
            return self._error(400, 'Request body is not UTF-8')
        event = http_to_apigw_event(self.command, parts.path, parts.query, body,
                                    dict(self.headers.items()))
        if event is None:
            return self._error(404, 'Not found')
        self._write(api_handler.handler(event, LambdaContext('api')))

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

    def __init__(self, port, host=DEFAULT_HOST):
        HTTPServer.__init__(self, (host, port), _GatewayHandler)


def sns_record(message_str):
    """Wrap an SNS message string as the event SNS passes to a Lambda function."""
    return {
        'Records': [{
            'EventSource': 'aws:sns',
            'EventVersion': '1.0',
            'Sns': {
                'Type': 'Notification',
                'MessageId': str(uuid.uuid4()),
                'Message': message_str,
                'MessageAttributes': {},
            },
        }],
    }


class _Poller(threading.Thread):
    """Background thread that repeatedly calls poll_once until stopped.

    Any exception raised while handling a batch is logged and the loop
    carries on, so one bad record cannot stop the thread.
    """

    def __init__(self, name, poll_interval):
        super(_Poller, self).__init__(name=name, daemon=True)
        self.poll_interval = poll_interval
        self._stopped = threading.Event()

    def poll_once(self):
        """Fetch and deliver one round of work; return True if anything was found."""
        raise NotImplementedError

    def run(self):
        while not self._stopped.is_set():
            try:
                found = self.poll_once()
            except Exception:
                logger.exception('%s: polling failed', self.name)
                found = False
            if not found:
                self._stopped.wait(self.poll_interval)

    def stop(self):
        self._stopped.set()


class IngestPoller(_Poller):
    """Deliver messages from the ingest subscription queue as SNS-triggered invocations."""

    def __init__(self, queue_url, handler=None, poll_interval=1.0):
        super(IngestPoller, self).__init__('ingest-poller', poll_interval)
        self.queue_url = queue_url
        self.handler = handler or event_processor.handler
        self._sqs = client('sqs')

    def poll_once(self):
        received = self._sqs.receive_message(
            QueueUrl=self.queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=1)
        messages = received.get('Messages', [])
        for message in messages:
            self._deliver(message['Body'])
            # The local runner does not retry failed invocations, so the
            # message is removed whether or not the handler succeeded.
            self._sqs.delete_message(
                QueueUrl=self.queue_url, ReceiptHandle=message['ReceiptHandle'])
        return bool(messages)

    def _deliver(self, body):
        try:
            message_str = json.loads(body)['Message']
            self.handler(sns_record(message_str), LambdaContext('eventProcessor'))
        except Exception:
            logger.exception('%s: handler failed for message %s', self.name, body[:200])


class StreamPoller(_Poller):
    """Deliver DynamoDB stream records of one table in batches to a handler."""

    def __init__(self, table_name, iterator_type='TRIM_HORIZON', handler=None,
                 poll_interval=1.0, batch_size=25, streams_client=None):
        super(StreamPoller, self).__init__('stream-poller', poll_interval)
        self.table_name = table_name
        self.iterator_type = iterator_type
        self.handler = handler or data_transformer.handler
        self.batch_size = batch_size
        self._streams = streams_client or client('dynamodbstreams')
        self.stream_arn = None
        self._iterators = {}
        self._last_sequence = {}

    def resolve_stream_arn(self):
        """Stream ARN for the table, matched on TableName on the client side.

        LocalStack's ListStreams ignores its TableName filter and lists every
        stream-enabled table, so the first entry is not necessarily ours.
        """
        resp = self._streams.list_streams()
        try:
            return next(s['StreamArn'] for s in resp['Streams']
                        if s['TableName'] == self.table_name)
        except StopIteration:
            raise LookupError('no stream found for table {}'.format(self.table_name))

    def _shard_ids(self):
        shard_ids = []
        kwargs = {'StreamArn': self.stream_arn}
        while True:
            description = self._streams.describe_stream(**kwargs)['StreamDescription']
            shard_ids.extend(shard['ShardId'] for shard in description.get('Shards', []))
            last = description.get('LastEvaluatedShardId')
            if not last:
                return shard_ids
            kwargs['ExclusiveStartShardId'] = last

    def _new_iterator(self, shard_id):
        return self._streams.get_shard_iterator(
            StreamArn=self.stream_arn, ShardId=shard_id,
            ShardIteratorType=self.iterator_type)['ShardIterator']

    def open(self):
        """Resolve the stream and get an iterator for every shard."""
        self.stream_arn = self.resolve_stream_arn()
        self._iterators = {}
        self._last_sequence = {}
        for shard_id in self._shard_ids():
            self._iterators[shard_id] = self._new_iterator(shard_id)
            self._last_sequence[shard_id] = -1

    def start(self):
        self.open()
        super(StreamPoller, self).start()

    def _read(self, shard_id):
        """Return (records, next iterator) for one shard.

        The iterator is followed while the service returns NextShardIterator.
        LocalStack's GetRecords never returns one, and its GetShardIterator
        ignores sequence numbers, so in that case the shard is reopened with
        the configured iterator type on every round and read without a limit.
        """
        iterator = self._iterators[shard_id]
        if iterator is None:
            resp = self._streams.get_records(ShardIterator=self._new_iterator(shard_id))
        else:
            resp = self._streams.get_records(ShardIterator=iterator, Limit=self.batch_size)
        return resp.get('Records', []), resp.get('NextShardIterator')

    def _unseen(self, shard_id, records):
        """Drop records at or below the last sequence number delivered for the shard."""
        last = self._last_sequence[shard_id]
        fresh = []
        for record in records:
            sequence = record.get('dynamodb', {}).get('SequenceNumber')
            if sequence is not None:
                if int(sequence) <= last:
                    continue
                last = int(sequence)
            fresh.append(record)
        self._last_sequence[shard_id] = last
        return fresh

    def poll_once(self):
        found = False
        for shard_id in list(self._iterators):
            records, next_iterator = self._read(shard_id)
            self._iterators[shard_id] = next_iterator
            records = self._unseen(shard_id, records)
            for index in range(0, len(records), self.batch_size):
                found = True
                self._deliver(records[index:index + self.batch_size])
        return found

    def _deliver(self, records):
        try:
            self.handler({'Records': records}, LambdaContext('dataTransformer'))
        except Exception:
            logger.exception('%s: handler failed for a batch of %d records',
                             self.name, len(records))


class Runner(object):
    """Bootstrap a stage and serve the API with both trigger pollers."""

    def __init__(self, stage='local', port=3000, poll_interval=1.0, host=DEFAULT_HOST):
        self.stage = stage
        self.host = host
        self.requested_port = port
        self.poll_interval = poll_interval
        self.resources = None
        self.port = None
        self._gateway = None
        self._threads = []

    def start(self):
        self.resources = bootstrap.bootstrap(stage=self.stage)
        os.environ.update(self.resources)

        stream_poller = StreamPoller(self.resources['EVENTS_TABLE'],
                                     poll_interval=self.poll_interval)
        ingest_poller = IngestPoller(self.resources['INGEST_SUB_QUEUE_URL'],
                                     poll_interval=self.poll_interval)
        self._gateway = Gateway(self.requested_port, host=self.host)
        self.port = self._gateway.server_address[1]
        gateway_thread = threading.Thread(
            target=self._gateway.serve_forever, name='gateway', daemon=True)

        try:
            stream_poller.start()
        except Exception:
            self._gateway.server_close()
            self._gateway = None
            raise
        ingest_poller.start()
        gateway_thread.start()
        self._threads = [gateway_thread, ingest_poller, stream_poller]
        logger.info('Runner for stage %s listening on port %d', self.stage, self.port)
        return self

    def stop(self):
        if self._gateway is not None:
            self._gateway.shutdown()
            self._gateway.server_close()
        for thread in self._threads:
            if isinstance(thread, _Poller):
                thread.stop()
        for thread in self._threads:
            thread.join(10)
        self._threads = []
        self._gateway = None


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description='Serve the API locally and drive the processor and transformer.')
    parser.add_argument('--stage', default='local')
    parser.add_argument('--host', default=DEFAULT_HOST,
                        help='address the gateway listens on (default: %(default)s)')
    parser.add_argument('--port', type=int, default=3000)
    parser.add_argument('--poll-interval', type=float, default=1.0)
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(name)s: %(message)s')
    stopping = threading.Event()
    signal.signal(signal.SIGTERM, lambda signum, frame: stopping.set())

    runner = Runner(stage=args.stage, port=args.port, poll_interval=args.poll_interval,
                    host=args.host)
    runner.start()
    for key in sorted(runner.resources):
        print('{}={}'.format(key, runner.resources[key]), flush=True)
    try:
        while not stopping.wait(1):
            pass
    except KeyboardInterrupt:
        pass
    finally:
        runner.stop()


if __name__ == '__main__':
    main()
