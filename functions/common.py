"""Shared runtime helpers: environment config, boto3 factories and API responses."""

import json
import os
import re
from datetime import datetime
from decimal import Decimal

import boto3

REQUIRED_VARIABLES = (
    'EVENTS_TABLE',
    'TRANSFORMED_TABLE',
    'INGEST_TOPIC_ARN',
    'NOTIFY_TOPIC_ARN',
    'FAILED_QUEUE_URL',
)


MAX_JSON_DEPTH = 32


class ConfigError(Exception):
    """Raised when required environment variables are missing."""


def get_config():
    """Return the required settings keyed by lower-case variable name."""
    missing = [name for name in REQUIRED_VARIABLES if not os.environ.get(name)]
    if missing:
        raise ConfigError('Missing required environment variables: ' + ', '.join(missing))
    return {name.lower(): os.environ[name] for name in REQUIRED_VARIABLES}


def endpoint_env_var(service):
    """Name of the variable that overrides the endpoint for a service."""
    return re.sub(r'[^A-Z0-9]', '', service.upper()) + '_ENDPOINT_URL'


def _endpoint_url(service):
    return os.environ.get(endpoint_env_var(service)) or None


def client(service):
    """boto3 client honouring <SERVICE>_ENDPOINT_URL when it is set."""
    return boto3.client(service, endpoint_url=_endpoint_url(service))


def resource(service):
    """boto3 resource honouring <SERVICE>_ENDPOINT_URL when it is set."""
    return boto3.resource(service, endpoint_url=_endpoint_url(service))


class DecimalEncoder(json.JSONEncoder):
    """Serialize DynamoDB Decimals as int when integral, float otherwise."""

    def default(self, o):
        if isinstance(o, Decimal):
            if o == o.to_integral_value():
                return int(o)
            return float(o)
        return super(DecimalEncoder, self).default(o)


def response(status, body):
    """API Gateway Lambda-proxy response with a JSON body."""
    return {
        'statusCode': status,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
        },
        'body': json.dumps(body, cls=DecimalEncoder),
    }


def json_depth(value, limit=None):
    """Container nesting depth of a decoded JSON value (scalars are 0, {} and [] are 1).

    Iterative, so values deeper than the interpreter's recursion limit are
    measured too. With a limit, counting stops once the depth exceeds it.
    """
    deepest = 0
    stack = [(value, 1)]
    while stack:
        current, level = stack.pop()
        if isinstance(current, dict):
            children = current.values()
        elif isinstance(current, (list, tuple)):
            children = current
        else:
            continue
        deepest = max(deepest, level)
        if limit is not None and deepest > limit:
            return deepest
        stack.extend((child, level + 1) for child in children)
    return deepest


def utc_now_iso():
    """Current UTC time as ISO 8601 with microseconds and a Z suffix."""
    return datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
