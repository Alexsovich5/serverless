# serverless: Implementation Plan

Each task below is **one commit** (13 tasks: T1 to T8, T9a, T9b, T10, T11, T12). Tasks are in dependency order, and every task leaves `make test` green. The specification is in `docs/SPEC.md`, and section numbers below (§) refer to it.

## Conventions

- **Docker only.** Everything runs in Docker, and the host needs only Docker, Compose and make. The Makefile exports `DOCKER_DEFAULT_PLATFORM=linux/amd64` and `COMPOSE_PROJECT_NAME ?= serverless`, and uses `COMPOSE ?= docker compose`. Any host port published by a later task is bound to `127.0.0.1` and mapped from the range 20700 to 20799.
- **Compose services:**
  - `tests`: built from `Dockerfile`, `FROM python:3.6.5-stretch`, with the repo mounted at `/app`.
  - `sls`: `Dockerfile.sls`, `FROM node:8.11.2`. Added in T7.
  - `localstack`: `localstack/localstack:0.8.6`. Added in T8.
  - `app`: the same image as `tests`, running `python -m local.runner`. Added in T10.
- **Make targets:**
  - `make lint` runs flake8 over `functions` and `tests`.
  - `make unit` runs `pytest tests/unit` with `--no-deps`, so no network is needed.
  - `make package` (from T7) runs `serverless package`, then `pytest tests/package`.
  - `make integration` (from T8) starts LocalStack and runs `pytest tests/integration`.
  - `make test` runs every target that exists at that point, in the order lint, unit, package, integration.
  - `ARGS="..."` passes arguments through to pytest.
- **Python 3.6 syntax.** f-strings are allowed.
- **Environment.** `PYTHONPATH=/app`, so `functions`, `local` and `tests` import as packages. Every container gets `AWS_DEFAULT_REGION=us-east-1` and `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY=testing`.
- **Tests first.** Each task writes its tests before the code. The acceptance command gates the commit.

---

## T1: Scaffold Docker image, Makefile, pinned dependencies and pytest harness

**Goal:** `make test` works from the first commit. It builds the period Python image with exact pins and runs lint and unit tests inside it.

**Files:**
- create `Dockerfile`:
  - `FROM python:3.6.5-stretch`
  - `ENV PYTHONPATH=/app PYTHONDONTWRITEBYTECODE=1 AWS_DEFAULT_REGION=us-east-1`
  - `WORKDIR /app`
  - copies `requirements*.txt`, then runs `pip install --no-cache-dir -r requirements.txt -r requirements-dev.txt`, then `COPY . .`
- create `docker-compose.yml`, no `version` key (the Compose Specification, which keeps `depends_on` conditions and `platform:`) and a top-level `name: serverless`, with one service `tests`: `build: .`, `image: serverless:tests`, `volumes: ['.:/app']`, and env for the region and dummy credentials. The default network is pinned to the subnet `172.47.0.0/24` (`networks.default.ipam.config`), so the stack does not collide with other Compose projects on a shared Docker host.
- create `Makefile` with targets `build`, `lint` (`flake8 tests`), `unit`, `test` (`lint unit`) and `shell`.
- create `requirements-dev.txt` with the exact pins from §6.2: pytest 3.5.1 and its dependencies, flake8 3.5.0 and its dependencies, moto 1.3.3 and the full transitive list.
- modify `requirements.txt` to the exact pins from §6.1 (`boto3==1.7.28`, `botocore==1.10.28`, `s3transfer==0.1.13`, `jmespath==0.9.3`, `python-dateutil==2.6.1`, `docutils==0.14`, `six==1.11.0`).
- create `setup.cfg`:
  - `[tool:pytest] testpaths = tests`, `addopts = -ra`
  - `[flake8] max-line-length = 100`, `exclude = .git,__pycache__,node_modules,.serverless,var`
- create `.gitignore` (`__pycache__/`, `*.pyc`, `.cache/`, `.pytest_cache/`, `node_modules/`, `.serverless/`, `var/`) and `.dockerignore` (the same entries plus `.git`).
- create `functions/__init__.py` (empty), `tests/__init__.py` and `tests/unit/__init__.py`.

**Tests to write first:**
- `tests/unit/test_environment.py` checks that:
  - `sys.version_info[:3] == (3, 6, 5)`;
  - every `name==version` line in `requirements.txt` and `requirements-dev.txt` equals `pkg_resources.get_distribution(name).version`;
  - `import boto3, moto` succeeds;
  - `boto3.__version__ == '1.7.28'`.

**Acceptance command:** `make test`

**Commit message:**
```
Add Docker image, Makefile and pinned pytest harness

Pin Python 3.6.5, boto3 1.7.28, moto 1.3.3 and pytest 3.5.1 with every
transitive dependency; make test runs flake8 and unit tests in-container.
```

---

## T2: Add shared runtime module for config, AWS clients and responses

**Goal:** there is one place that reads environment configuration, builds boto3 clients with optional endpoint overrides and formats API Gateway responses. The handlers adopt it in T4 to T6.

**Files:**
- create `functions/common.py` (§5.2):
  - `ConfigError` and `get_config()`, which checks for the required variables from §5.1 and lists every missing one in the message.
  - `client(service)` and `resource(service)`. Each reads `<SERVICE>_ENDPOINT_URL`, upper-cased with non-alphanumerics removed, so `dynamodbstreams` reads `DYNAMODBSTREAMS_ENDPOINT_URL`.
  - `response(status, body)`, moved here from `api_handler.response`: same headers, serialized with `DecimalEncoder`.
  - `DecimalEncoder` and `utc_now_iso()`.
- modify `functions/api_handler.py` to remove the unused `datetime` import, so the whole `functions/` package lints clean. There is no behaviour change.
- modify `Makefile`: `lint` runs `flake8 functions tests`.

**Tests to write first:**
- `tests/unit/test_common.py` checks that:
  - `get_config()` raises `ConfigError` listing `EVENTS_TABLE` and `INGEST_TOPIC_ARN` when they are unset (use `monkeypatch`);
  - with all variables set, it returns the lower-case keys;
  - `client('sqs')` gets `endpoint_url` from `SQS_ENDPOINT_URL`, and `None` when it is unset (inspect `client._endpoint.host`);
  - `DecimalEncoder` turns `Decimal('3')` into `3` and `Decimal('1.5')` into `1.5`;
  - `response(201, {...})` has `statusCode` 201, `Content-Type: application/json`, the CORS header, and a JSON body;
  - `utc_now_iso()` ends with `Z` and parses with `datetime.strptime(..., '%Y-%m-%dT%H:%M:%S.%fZ')`.

**Acceptance command:** `make test`

**Commit message:**
```
Add shared module for env config, boto3 factories and responses

Handlers will read table, topic and queue names from the environment and
honour per-service endpoint overrides so the same code runs on LocalStack.
```

---

## T3: Add DynamoDB table definitions and idempotent local bootstrap

**Goal:** `config/dynamodb_tables.json` becomes the single source of table schemas. `local/bootstrap.py` creates every resource the pipeline needs on any boto3 endpoint: the tables, the ingest and notification topics, the failed queue, and the local SNS-to-SQS subscription queues.

**Files:**
- create `config/dynamodb_tables.json` exactly as in §5.9.
- create `local/__init__.py`.
- create `local/bootstrap.py` with:
  - `load_table_specs(path='config/dynamodb_tables.json') -> list`
  - `table_name(spec, stage) -> str`
  - `create_tables(dynamodb_client, specs, stage)`, which creates a table only if it is missing and waits for `table_exists`
  - `create_messaging(sns_client, sqs_client, stage) -> dict`, which creates:
    - the topics `event-platform-ingest-<stage>` and `event-platform-notifications-<stage>`;
    - the queues `event-platform-failed-<stage>`, `local-ingest-sub-<stage>` and `local-notify-sub-<stage>`;
    - SQS subscriptions from ingest to `local-ingest-sub` and from notifications to `local-notify-sub`. Before each `subscribe`, it calls `list_subscriptions_by_topic` and skips the call when an `sqs` subscription to that queue ARN already exists, because LocalStack 0.8.6 appends a duplicate subscription on every `Subscribe` (SPEC §11).
  - `bootstrap(stage='local') -> dict`, returning env-style keys (`EVENTS_TABLE`, `TRANSFORMED_TABLE`, `INGEST_TOPIC_ARN`, `NOTIFY_TOPIC_ARN`, `FAILED_QUEUE_URL`, `INGEST_SUB_QUEUE_URL`, `NOTIFY_SUB_QUEUE_URL`)
  - `main()`, which prints `KEY=value` lines and writes `var/local.env`

  All clients come from `functions.common.client`.
- modify `Makefile`: `lint` covers `functions local tests`.

**Tests to write first:**
- `tests/unit/test_bootstrap.py`, using `@mock_dynamodb2 @mock_sns @mock_sqs`, checks that:
  - the specs load, and both tables have a HASH `eventId`;
  - `create_tables` creates `event-platform-events-local` with the GSI `type-timestamp-index` and `event-platform-transformed-local`;
  - running `bootstrap()` twice raises nothing and still leaves exactly 2 tables, 2 topics, 3 queues and exactly one subscription per topic;
  - `list_subscriptions_by_topic` shows the SQS endpoint subscribed to the ingest topic;
  - every returned value is non-empty.

**Acceptance command:** `make test`

**Commit message:**
```
Add DynamoDB table definitions and idempotent local bootstrap

Table schemas live in config/dynamodb_tables.json; bootstrap creates the
tables, SNS topics and SQS queues on any endpoint and emits their names.
```

---

## T4: Rework API handler to publish to SNS and serve filtered reads

**Goal:** `api_handler` implements §5.3 exactly, with no hard-coded resource names.

**Files:**
- modify `functions/api_handler.py`:
  - keep `handler()` routing, now with an explicit `path == /events/{id}` check;
  - `create_event` parses the body, returns `400` unless the body is a dict, assigns `uuid4`, and publishes the §5.4 message to `INGEST_TOPIC_ARN` through `common.client('sns')`. The JSON body goes in the message; `MessageAttributes` always carry `eventId`, and carry `type` only when `body.get('type')` is a non-empty string (otherwise SNS rejects the attribute and the API would return 500);
  - `get_event` does `get_item(Key={'eventId': id})` and decodes `payload`/`metadata` with `json.loads`;
  - `list_events` validates `limit` (1 to 100, default 50, otherwise `400`). With `type` it runs `query(IndexName='type-timestamp-index', KeyConditionExpression=Key('type').eq(t), ScanIndexForward=False, Limit=limit)`; otherwise `scan(Limit=limit)`;
  - `response` is imported from `common`;
  - clients are obtained on each invocation, so moto decorators apply.

**Tests to write first:**
- `tests/unit/conftest.py` adds:
  - an `aws_env` fixture that sets the §5.1 variables;
  - a `pipeline` fixture that starts moto mocks and runs `local.bootstrap.bootstrap('test')`;
  - `apigw_event(method, path_params=None, qs=None, body=None)` and `sns_event(messages)` builders.
- `tests/unit/test_api_handler.py` checks that:
  - `POST` returns 202 with a UUID, and the SQS subscription queue receives an SNS envelope whose `Message` contains the same `eventId` and the original event;
  - `POST` with body `"not json"`, `[1,2]` or `None` returns 400;
  - `POST {}` and `POST {"type": 5}` return 202, and the published message has an `eventId` attribute and no `type` attribute;
  - `GET /events/{id}` on a seeded item returns 200 with `payload` as a dict, and a missing ID returns 404;
  - `GET /events?type=a` returns only type `a`, newest first;
  - `limit=0`, `limit=101` and `limit=x` each return 400;
  - `DELETE` returns 405.

**Acceptance command:** `make test`

**Commit message:**
```
Route API submissions through SNS and add filtered event listing

POST publishes to the ingest topic with the assigned eventId; GET by id
uses the correct key, and listing supports limit bounds and a type GSI.
```

---

## T5: Rework event processor to consume SNS, keep IDs and dead-letter

**Goal:** `event_processor` implements §2 item 3 and the §5.4 and §5.7 contracts.

**Files:**
- modify `functions/event_processor.py`:
  - `handler` iterates `Records[].Sns.Message` and returns `{'processed', 'duplicates', 'failed'}`;
  - `validate(event)` raises `ValueError` with the exact messages `Missing required field: <f>`, `Field 'source' must be a non-empty string` and `Field 'payload' must be an object`;
  - `enrich(message)` keeps `eventId` and `receivedAt` and adds `timestamp=utc_now_iso()` and `status='processed'`. `payload` and `metadata` are stored as JSON strings, as the original code did;
  - `store(item)` does `put_item(..., ConditionExpression='attribute_not_exists(eventId)')`. A `ClientError` with code `ConditionalCheckFailedException` counts as a duplicate;
  - `dead_letter(message_str, event_id, reason)` does `sqs.send_message` to `FAILED_QUEUE_URL`;
  - a message that is not JSON at all is dead-lettered with `eventId: null`.

**Tests to write first:**
- `tests/unit/test_event_processor.py` checks that:
  - a valid record is stored with the same `eventId`, `status == 'processed'` and a `timestamp`;
  - a record missing `payload` gives `failed == 1`, one message in the failed queue with `reason == 'Missing required field: payload'`, and no item in the table;
  - a record with `payload: "str"` is rejected;
  - a batch of 3 records (2 valid, 1 invalid) gives counts 2/0/1;
  - a duplicate is counted, directly under moto: the same SNS record is passed to `handler` twice; the second call returns `processed == 0, duplicates == 1`, and the table holds a single item. moto 1.3.3's `put_item` maps `attribute_not_exists(x)` to `Expected` and raises `ConditionalCheckFailedException`, so no Stubber is needed. The same behaviour is checked against LocalStack in T8 and T9b;
  - a non-JSON message is dead-lettered.

**Acceptance command:** `make test`

**Commit message:**
```
Consume SNS in event processor and dead-letter invalid events

Keep the eventId assigned by the API, write idempotently with a
conditional put, and send validation failures to the SQS failed queue.
```

---

## T6: Add data transformer for DynamoDB stream inserts

**Goal:** create the `dataTransformer` function that `serverless.yml` already referenced but that did not exist (§2 item 4, §5.6, §5.8).

**Files:**
- create `functions/data_transformer.py`:
  - `flatten(obj, prefix='') -> dict`: nested dicts become dotted keys, lists become `key.N`, and leaves are `str()`, with `True`/`False`/`None` rendered as JSON literals;
  - `transform(item) -> dict`: pure, following the §5.6 shape. It first decodes `item['payload']` (a JSON string, §5.5) with `json.loads` and flattens the resulting object. It raises `ValueError` when `eventId`, `source`, `type`, `timestamp` or `payload` is missing;
  - `handler(event, context)`: for each record with `eventName == 'INSERT'`, deserialize `dynamodb.NewImage` with `boto3.dynamodb.types.TypeDeserializer`, `put_item` into `TRANSFORMED_TABLE`, and `sns.publish(TopicArn=NOTIFY_TOPIC_ARN, Subject='event.transformed', Message=...)`. A `ValueError` from `transform` is logged and the record counted as skipped. It returns `{'transformed': n, 'skipped': n}`.

**Tests to write first:**
- `tests/unit/test_data_transformer.py` checks that:
  - `flatten({'a': {'b': 1}, 'c': [True, None]}) == {'a.b': '1', 'c.0': 'true', 'c.1': 'null'}`;
  - `transform` lower-cases `source` and `type`, sets `day` from the timestamp, and computes `fieldCount`;
  - `transform` given an item whose `payload` is the JSON string `'{"customer": {"id": 42}}'` yields `fields == {"customer.id": "42"}` (not a flattening of the string);
  - an INSERT image without `source`/`type`/`payload` is skipped by the handler (`skipped == 1`), nothing is written and no exception escapes;
  - in the handler test (moto pipeline), an INSERT record built with `TypeSerializer` writes one transformed row, and the notify subscription queue receives a message with subject `event.transformed` and the matching `eventId`;
  - `MODIFY` and `REMOVE` records are skipped and nothing is written;
  - an empty `Records` list returns zeros.

**Acceptance command:** `make test`

**Commit message:**
```
Add data transformer for DynamoDB stream inserts

Flatten each new event's payload into a transformed table row and publish
an event.transformed notification to SNS; ignore modify and remove records.
```

---

## T7: Rewrite serverless.yml for Serverless 1.27 and verify packaging

**Goal:** `serverless.yml` uses only features that existed by May 2018 and matches §4.1. `serverless package` succeeds in `node:8.11.2`, and the generated CloudFormation template is checked by tests.

**Files:**
- modify `serverless.yml`:
  - Keep `service: event-platform`, `runtime: python3.6` and `stage: ${opt:stage, 'dev'}`.
  - Add a `provider.environment` block (§5.1).
  - Scope `iamRoleStatements` to the table, GSI, topic and queue ARNs. Include `dynamodb:Query` on `.../index/*`, plus `sns:Publish` and `sqs:SendMessage`.
  - `eventProcessor`: `sns: ${self:service}-ingest-${self:provider.stage}`. This replaces the `sqs` event.
  - `apiHandler`: three `http` events, each with `cors: true`.
  - `dataTransformer`: a `stream` event with `type: dynamodb`, `arn: Fn::GetAtt [EventsTable, StreamArn]`, `batchSize: 25` and `startingPosition: TRIM_HORIZON`.
  - Resources: `EventsTable` (HASH `eventId`, the GSI, `ProvisionedThroughput` 5/5, `StreamViewType: NEW_IMAGE`), `TransformedTable`, `NotificationsTopic` and `FailedEventsQueue`. Remove `BillingMode`.
  - Add `package.exclude` entries for `tests/**`, `local/**`, `docs/**`, `node_modules/**`, `var/**` and `requirements-dev.txt`.
- create `package.json`: `{"name": "event-platform", "private": true, "devDependencies": {"serverless": "1.27.3"}}`.
- create `Dockerfile.sls`:
  - `FROM node:8.11.2`
  - `RUN npm install -g npm@6.1.0`
  - `ENV PATH=/app/node_modules/.bin:$PATH SLS_TRACKING_DISABLED=1` (before the install, so serverless's postinstall script already runs with tracking disabled)
  - `WORKDIR /app`
  - `COPY package.json package-lock.json ./`
  - `RUN npm ci`
- create `package-lock.json`, generated once and committed: `docker run --rm -v "$PWD":/app -w /app -e SLS_TRACKING_DISABLED=1 node:8.11.2 sh -c 'npm install -g npm@6.14.18 && npm install --before=2018-05-31 --package-lock-only --no-audit'`. npm 6.1.0 has no `--before` option (it silently resolves current versions), so the lockfile is resolved with a later npm 6 release; it is still lockfileVersion 1 and the image installs it with npm 6.1.0 `npm ci`. Every entry in the resulting lockfile was checked against the registry `time` field to be published on or before 2018-05-31.
- modify `docker-compose.yml`: add an `sls` service. It builds `Dockerfile.sls`, mounts `.:/app` with an anonymous volume on `/app/node_modules`, and sets dummy AWS credentials.
- modify `Makefile`: `package` runs `$(COMPOSE) run --rm sls serverless package --stage dev` followed by `$(COMPOSE) run --rm --no-deps tests pytest tests/package`, and `test` becomes `lint unit package`.
- create `tests/package/__init__.py`.

**Tests to write first:**
- `tests/unit/test_serverless_config.py` loads `serverless.yml` with `yaml.safe_load` and checks that:
  - every `functions.*.handler` resolves through `importlib` to a callable;
  - the http events cover POST `/events`, GET `/events` and GET `/events/{id}`;
  - no function has an `sqs` event;
  - `BillingMode` is absent;
  - the `EventsTable` key schema, attribute definitions and GSI equal `config/dynamodb_tables.json`;
  - the environment keys equal the required keys of `common.get_config()`.
- `tests/package/test_template.py` loads `.serverless/cloudformation-template-update-stack.json` and checks that:
  - there are 3 `AWS::Lambda::Function` resources with `Runtime == 'python3.6'`;
  - there are 3 `AWS::ApiGateway::Method` resources (excluding OPTIONS);
  - there is 1 `AWS::Lambda::EventSourceMapping` whose `EventSourceArn` references `EventsTable.StreamArn`;
  - there is an `AWS::SNS::Topic` with a Lambda subscription;
  - both tables exist with `ProvisionedThroughput`;
  - there is an `AWS::SQS::Queue` named `event-platform-failed-dev`.

**Acceptance command:** `make test`

**Commit message:**
```
Rewrite serverless.yml for Serverless 1.27 with SNS routing

Replace the SQS trigger and on-demand billing (both newer than the
framework) with an SNS trigger and provisioned tables; test the package.
```

---

## T8: Add LocalStack stack and integration harness

**Goal:** the compose stack runs LocalStack 0.8.6. Integration tests bootstrap every resource against it and prove that the emulated services behave as the pipeline needs.

**Files:**
- modify `docker-compose.yml`:
  - add a `localstack` service: `image: localstack/localstack:0.8.6`, `platform: linux/amd64`, `SERVICES=dynamodb,dynamodbstreams,kinesis,sns,sqs` (`kinesis` is required: 0.8.6 backs DynamoDB Streams with Kinesis and does not auto-start it, SPEC §4.2), `HOSTNAME_EXTERNAL=localstack`, `DEFAULT_REGION=us-east-1`, ports `4568`, `4569`, `4570`, `4575`, `4576` (published on `127.0.0.1` as `20768`, `20769`, `20770`, `20775`, `20776`), and a healthcheck that needs no curl: `python -c "import socket;[socket.create_connection(('localhost',p),2) for p in (4568,4569,4570,4575,4576)]"` (`interval: 5s`, `retries: 60`). When Docker disk space allows, confirm the image's Python with `docker run --rm --entrypoint sh localstack/localstack:0.8.6 -c 'which python'`.
  - `tests` gets the environment `DYNAMODB_ENDPOINT_URL=http://localstack:4569`, `DYNAMODBSTREAMS_ENDPOINT_URL=http://localstack:4570`, `SNS_ENDPOINT_URL=http://localstack:4575` and `SQS_ENDPOINT_URL=http://localstack:4576`.
  - `tests` depends on `localstack` with `condition: service_healthy`.
- modify `Makefile`:
  - `integration` runs `$(COMPOSE) rm -sfv localstack`, then `$(COMPOSE) up -d localstack`, then `$(COMPOSE) run --rm tests pytest tests/integration`, so every run starts from an empty LocalStack;
  - `unit` explicitly unsets the `*_ENDPOINT_URL` variables (`-e VAR=`) so moto intercepts;
  - `test` becomes `lint unit package integration`;
  - `down` runs `$(COMPOSE) down -v`.
- modify `functions/common.py`: treat an empty `*_ENDPOINT_URL` as unset.
- create `tests/integration/__init__.py` and `tests/integration/conftest.py`:
  - `wait_for(fn, timeout=120, interval=1)`;
  - a session fixture `localstack` that waits until `dynamodb.list_tables`, `kinesis.list_streams` (endpoint `http://localstack:4568`), `sns.list_topics` and `sqs.list_queues` all succeed;
  - a session fixture `resources` that runs `bootstrap(stage='it')` and exports the env keys through `os.environ`;
  - a `drain(queue_url)` helper;
  - a `stream_arn(table_name)` helper that calls `dynamodbstreams.list_streams()` and returns `next(s['StreamArn'] for s in resp['Streams'] if s['TableName'] == table_name)`, raising `LookupError` naming the table when no stream matches. It never takes `Streams[0]`: LocalStack 0.8.6 ignores the `TableName` filter and lists every stream-enabled table in the container (SPEC §11). `LatestStreamArn` from `DescribeTable` is not used either, because LocalStack 0.8.6 returns DynamoDB Local's ARN, which its `DescribeStream` rejects (SPEC §11).

**Tests to write first:**
- `tests/integration/test_localstack.py` checks that:
  - the bootstrap is idempotent against LocalStack (calling it twice gives the same ARNs and URLs);
  - after those two bootstraps, publishing once to the ingest topic delivers exactly one message to `local-ingest-sub` within 30 s (its body envelope has a `Message` field), and after deleting it a further receive with `WaitTimeSeconds=2` returns nothing;
  - `stream_arn(events_table)` is set and `dynamodbstreams.describe_stream(StreamArn=...)` returns at least one shard;
  - with a second stream-enabled scratch table `it-scratch-stream` created by the test (and deleted afterwards), `stream_arn(events_table)` still returns an ARN containing `:table/event-platform-events-it/`, and `stream_arn('no-such-table')` raises `LookupError`;
  - a conditional `put_item` with `attribute_not_exists(eventId)` raises `ConditionalCheckFailedException` the second time. This runs against a scratch table `it-scratch` created and deleted by the test, never the events table, so no incomplete item enters the events stream.
- `tests/unit/test_common.py` adds a check that an empty override string gives `endpoint_url=None`.

**Acceptance command:** `make test`

**Commit message:**
```
Add LocalStack 0.8.6 compose stack and integration harness

Bootstrap tables, topics and queues against the emulator and check the
stream, single SNS-to-SQS delivery and conditional-write behaviour the
pipeline relies on.
```

---

## T9a: Add API Gateway event mapping and HTTP gateway for local runs

**Goal:** a local HTTP server turns requests into API Gateway proxy events for `api_handler` (§8).

**Files:**
- create `local/runner.py` with:
  - `http_to_apigw_event(method, raw_path, query, body) -> dict`. It matches the `/events` and `/events/{id}` resources and fills `httpMethod`, `path`, `resource`, `pathParameters`, `queryStringParameters` (None when empty), `body`, `headers` and `requestContext`. Anything that does not match gives 404 JSON.
  - `LambdaContext` stub with `function_name`, `aws_request_id` and `get_remaining_time_in_millis()`.
  - `Gateway(port)`, a threading HTTP server built from `socketserver.ThreadingMixIn` and `http.server.HTTPServer` (Python 3.6 has no `ThreadingHTTPServer`). It calls `api_handler.handler` and writes back its status, headers and body.

**Tests to write first:**
- `tests/unit/test_runner.py` checks that:
  - `http_to_apigw_event('GET', '/events/abc', '', None)` has `pathParameters == {'id': 'abc'}`;
  - `GET /events?limit=5&type=x` parses into `queryStringParameters`;
  - `/nope` is reported as unmatched;
  - a `Gateway(0)` started in a thread under the moto pipeline answers `POST /events` with 202 and `GET /events/<missing>` with 404 over `urllib.request`.

**Acceptance command:** `make test`

**Commit message:**
```
Add local HTTP gateway that builds API Gateway proxy events

Map requests on /events and /events/{id} to Lambda proxy events and
return the handler's status, headers and body.
```

---

## T9b: Add trigger pollers, runner lifecycle and end-to-end tests

**Goal:** `local/runner.py` also stands in for the SNS and DynamoDB stream triggers (§8). An end-to-end integration test runs the full pipeline over HTTP.

**Files:**
- modify `local/runner.py`, adding:
  - `sns_record(message_str) -> dict`, which wraps a message as `{'Records':[{'EventSource':'aws:sns','Sns':{'Message':...}}]}`.
  - `IngestPoller`, which uses long-polling `receive_message(WaitTimeSeconds=1)`, wraps each SNS envelope's `Message` with `sns_record`, calls `event_processor.handler` and deletes the message.
  - `StreamPoller(table_name, iterator_type='TRIM_HORIZON')`, which takes the stream ARN from `dynamodbstreams.list_streams()` by matching `TableName` on the client side (`next(s['StreamArn'] for s in resp['Streams'] if s['TableName'] == table_name)`) and raises `LookupError` on start when none matches; never `Streams[0]` and never `LatestStreamArn` (SPEC §11). It gets an iterator of `iterator_type` for every shard on start, follows `NextShardIterator`, and passes batches of records to `data_transformer.handler`. LocalStack 0.8.6's `GetRecords` never returns `NextShardIterator` and its `GetShardIterator` ignores sequence numbers (SPEC §11), so when a response has no `NextShardIterator` the poller reopens the shard with `iterator_type` on the next round, reads it without a `Limit`, and drops records whose `SequenceNumber` is not above the last one it delivered for that shard.
  - Both pollers catch and log any exception per batch and keep running, so one bad record cannot stop the thread or hang a test.
  - `Runner(stage='local', port=3000, poll_interval=1.0)`: `start()` runs `bootstrap(stage)`, sets the env from its result, starts the three threads and exposes `resources` (the bootstrap dict) and the bound `port`; `stop()` shuts them down.
  - `main()` with the `--stage` (default `local`), `--port` and `--poll-interval` options.
- modify `Makefile`: `integration` runs `$(COMPOSE) stop app` (ignoring errors) before the tests, so a demo runner never competes for the `it` resources.

**Tests to write first:**
- `tests/unit/test_runner.py` adds checks that:
  - `sns_record(...)` produces a record that `event_processor.handler` accepts (moto pipeline);
  - a poller whose handler raises on the first batch logs the error and still delivers the second batch.
  - `StreamPoller` follows `NextShardIterator` and keeps delivering after a handler error, and, when a response has no `NextShardIterator`, reopens the shard and delivers only records it has not delivered before (stubbed client);
  - `StreamPoller` picks the ARN whose `TableName` matches when a stubbed `list_streams` returns two streams with the other table first, and raises `LookupError` when none matches.
- `tests/integration/conftest.py` adds a module fixture that starts `Runner(stage='it', port=0)` in-process and reads every table, queue and topic name from `runner.resources`.
- `tests/integration/test_pipeline.py` checks that:
  - POST `/events` over `urllib.request` returns 202, and within 60 s `GET /events/{id}` returns 200 with `status == 'processed'`;
  - the transformed table has a row for that `eventId` with the flattened `fields`;
  - `local-notify-sub-it` receives an `event.transformed` notification for it;
  - an invalid event (no `payload`) and `POST {}` each appear in the failed queue, and GET returns 404;
  - duplicates: the test calls `event_processor.handler` directly twice with the same `sns_record` against LocalStack; the first returns `processed == 1`, the second `duplicates == 1`; the events table then holds one item, and after 15 s the transformed table holds exactly one row and `local-notify-sub-it` exactly one notification for that `eventId` (LocalStack emits a `MODIFY` record for the failed conditional put, which the transformer must skip);
  - `GET /events?type=<unique type>` returns only that event.

**Acceptance command:** `make test`

**Commit message:**
```
Add local pollers that drive the processor and transformer

Deliver SNS messages through an SQS subscription and DynamoDB stream
records to the handlers, and test the pipeline end to end over HTTP.
```

---

## T10: Add compose app service and a smoke command for the demo flow

**Goal:** `make up` (`docker compose up -d app`) brings up LocalStack and the runner on port 3000 (host port 20730) in the background. `make smoke` shows an event going through the whole pipeline.

**Files:**
- modify `docker-compose.yml`: add an `app` service. It uses the same build as `tests`, `command: python -m local.runner --stage local --port 3000`, `ports: ['127.0.0.1:20730:3000']` (container port 3000 published on loopback host port 20730, inside the 20700 to 20799 range from the conventions), the same endpoint env as `tests`, depends on healthy `localstack`, and has a healthcheck of `python -c "import urllib.request; urllib.request.urlopen('http://localhost:3000/events?limit=1')"`.
- create `scripts/smoke.sh` (`#!/bin/sh`, `set -eu`). It takes the base URL as `$1` and:
  1. POSTs `{"source":"Billing","type":"Invoice.Created","payload":{"customer":{"id":42},"amount":19.99}}`;
  2. extracts `eventId` with `python -c`;
  3. polls `GET /events/<id>` up to 30 times, 2 s apart, until `"status": "processed"`;
  4. prints the item and exits 0, or exits 1 on timeout.
- modify `Makefile`:
  - `up` runs `$(COMPOSE) up -d app` and waits until it is healthy;
  - `smoke` runs `$(COMPOSE) run --rm --no-deps tests sh scripts/smoke.sh http://app:3000`;
  - `demo` runs `up smoke`.
- modify `docker-compose.yml`: give `tests` and `sls` an explicit no-op `command: "true"`, so a bare `docker compose up` does not start interactive interpreters; the documented demo path remains `make up` then `make smoke`.
- create `tests/integration/test_smoke_script.py`. It runs `scripts/smoke.sh` via `subprocess` against the in-process runner fixture from T9b (in `conftest.py`), checks exit code 0, and checks that the output contains `processed`.

**Tests to write first:** `tests/integration/test_smoke_script.py` as described above.

**Acceptance command:** `make test && make up && make smoke && make down`

**Commit message:**
```
Add compose app service and smoke script for the local demo

make up starts LocalStack and the runner on port 3000 in the background;
make smoke posts an event and polls until the processor has stored it.
```

---

## T11: Regenerate README from template with honest status and layout

**Goal:** the README follows `tools/readme_template.md`. Every Implemented bullet maps to code and at least one test. The mocks are named. The Layout tree is generated from `git ls-files`.

**Files:**
- modify `README.md`:
  - **Title:** "Serverless Event-Driven Architecture".
  - **Opening paragraph:** plain description of the API, SNS, processor, DynamoDB, stream, transformer and notification flow.
  - **Stack line:** "Personal project built on the 2018-era stack (AWS Lambda Python 3.6, Serverless Framework 1.27, API Gateway, SNS, SQS, DynamoDB)."
  - **Status, Implemented:** SPEC §2 items 1 to 9.
  - **Status, Not implemented / known limitations:** every row of §3 and every item of §11. Also state explicitly: "Never deployed to AWS. AWS services are emulated with LocalStack 0.8.6 for integration tests and the demo, and mocked with moto 1.3.3 for unit tests; API Gateway and the Lambda triggers are simulated by `local/runner.py`."
  - **Built with:** Python 3.6.5 with boto3 1.7.28; moto 1.3.3; pytest 3.5.1; Serverless 1.27.3 on Node 8.11.2; LocalStack 0.8.6.
  - **Running it:** `make up` (or `docker compose up -d app`) followed by `make smoke`, and `make down` to stop.
  - **Tests:** `make test`, plus one sentence on coverage: unit tests on moto, integration on LocalStack, and a package template check; no real AWS.
  - **Layout:** the output of `git ls-files`, rendered as a tree by a one-off `python` snippet run in the `tests` container.
  - Delete the template comment.
- create `tests/unit/test_readme.py`, which checks that:
  - every path in the README's Layout block exists in the repo;
  - the README contains none of `ACORIA`, `**Role**`, `Role:`, `**Organization**`, `Organization:`, `Developed during`, `99.9`, `70%`, `10,000`, `Status-Complete` or `img.shields.io` (the bare word "Role" is allowed, e.g. "IAM Role");
  - it contains the words `LocalStack` and `moto`.

**Tests to write first:** `tests/unit/test_readme.py` as described above.

**Acceptance command:** `make test`

**Commit message:**
```
Rewrite README from template with honest status and layout

List what is implemented and tested, what is out of scope, and state
that AWS is emulated with LocalStack and moto and never deployed.
```

---

## T12 — Security and hygiene hardening

**Goal:** a whole-repository review for security, ops-correctness and hygiene problems, with each fix driven by a failing test.

**Found and changed:**
- `functions/api_handler.py` returned `str(exc)` in every `500` body, leaking botocore errors, endpoint URLs and configuration names to clients. It now returns `500 {"error":"Internal server error"}`; the traceback is logged only. A sentinel test sets `AWS_SECRET_ACCESS_KEY`/`AWS_SESSION_TOKEN` to `S3NTINEL-pw`, forces connection-refused and missing-config failures on every route, and asserts the sentinel is in neither the response nor the log.
- `POST /events` accepted any content type, so a page in the developer's browser could post a `text/plain` or form body to the local gateway without a preflight. It now requires `Content-Type: application/json` (case-insensitive, parameters allowed), otherwise `415`.
- Poison events: a body nested deeper than the interpreter's recursion limit made `json.loads` raise `RecursionError` (a `500` in the API, an unhandled error in the processor, and in the transformer an exception that would block the DynamoDB stream shard). Nesting is now limited to 32 levels (`common.json_depth`, iterative) in the API (`400`), the processor (dead-lettered) and the transformer (skipped). The API returns `413` when the SNS message would exceed 256 KiB (counting `\u` escapes), and the transformer skips rows whose flattened fields exceed 350 KiB instead of failing on the 400 KB DynamoDB item limit.
- The processor's dead-letter body could exceed the SQS limit for a maximum-size message, making the dead-letter send itself fail; the original message is now truncated with `"truncated": true`.
- `local/runner.py`: the gateway listened on `0.0.0.0` by default and trusted `Content-Length` (unbounded read, crash on a non-integer). It now defaults to `127.0.0.1` (`--host`, which the compose `app` service sets to `0.0.0.0` inside the container network; the published port stays on `127.0.0.1:20730`), answers `400` for an invalid length and `413` above 1 MiB without reading the body. A comment that claimed SNS never redelivers to a failed Lambda function was corrected.
- Published ports (post-batch item 23): already bound to `127.0.0.1` with a compose-config test; the README states the stack is loopback-only lab software without API authentication.
- Hygiene: the README Layout was missing `tests/unit/test_compose.py`; `tests/unit/test_readme.py` now compares the Layout with `git ls-files`. An era-flavoured phrase in "Running it" was reworded.

**Not changed:** the API remains unauthenticated (documented limitation); the local pollers still drop a message or batch whose handler raised (documented: no retries).

**Acceptance command:** `make test`, then `make up && make smoke`.

**Commit message:**
```
Harden API errors, request limits and poison-record handling

Return a generic 500, require a JSON content type, cap nesting depth
and message size through API, processor and transformer, bound the
local gateway's request body and default it to loopback.
```
