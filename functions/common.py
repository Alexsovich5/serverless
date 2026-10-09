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


def utc_now_iso():
    """Current UTC time as ISO 8601 with microseconds and a Z suffix."""
    return datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
