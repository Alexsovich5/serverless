# serverless: Specification

Rebuild of a small event-driven serverless pipeline on the stack developers commonly used in the first half of 2018: AWS Lambda on Python 3.6, Serverless Framework 1.x, API Gateway, SNS, SQS and DynamoDB. The whole pipeline builds, runs and is tested locally in Docker. AWS is never contacted.

Scope source: the technical concept, feature list and stack from the original README (commit `73d5973`). That README's employer, role, dates, status badge and business-impact figures (the "70% cost reduction", "10,000+ requests per second" and "sub-100ms" claims) are not carried over. None of them can be backed by this repo.

---

## 1. Problem

Producers send small JSON "business events" (`source`, `type`, `payload`) over HTTP. The system does four things with them:

1. accepts each one quickly and returns an ID;
2. routes it asynchronously to a processor that validates and enriches it, then stores it;
3. transforms each stored event into a flattened, query-friendly record and sends a notification;
4. lets clients read back single events, or recent events filtered by type.

This has to be expressed as Serverless Framework infrastructure-as-code that `serverless package` compiles to a valid CloudFormation template. The same handler code also has to run end to end on a local AWS emulator, so it can be tested without an AWS account.

## 2. In scope

1. **Event ingestion API** (`apiHandler`, API Gateway proxy integration). `POST /events` accepts a JSON object, assigns a UUID `eventId`, publishes it to the SNS ingest topic and returns `202`. A body that is not JSON, or not a JSON object, returns `400`. Field validation happens later in the processor, so a JSON object with a missing or non-string `type` (for example `{}`) is still accepted with `202` and then dead-lettered.
2. **Event retrieval API.** `GET /events/{id}` returns the stored event (`200`), or `404` if there is none. `GET /events?limit=N&type=T` lists stored events. With `type` it queries the `type-timestamp-index` GSI, newest first. Without it, it scans. `limit` defaults to 50 and must be between 1 and 100, otherwise `400`. Any other method returns `405`.
3. **Event processor** (`eventProcessor`, triggered by the SNS ingest topic). It does four things:
   - validates `source` (non-empty string), `type` (non-empty string) and `payload` (object);
   - enriches the event with `timestamp` and `status`, keeping the `eventId` that the API assigned;
   - writes the event idempotently with `attribute_not_exists(eventId)`, so a redelivered SNS message does not overwrite the stored one;
   - sends events that fail validation to an SQS failed-events queue, with a reason.
4. **Data transformer** (`dataTransformer`, triggered by the DynamoDB stream on the events table). For each `INSERT` record it deserializes the new image and builds the transformed record:
   - lower-cases `source` and `type`;
   - flattens the nested payload into dotted keys;
   - derives `day`.

   It then writes the result to the transformed-events table and publishes a notification to the SNS notifications topic. `MODIFY` and `REMOVE` records are skipped. An `INSERT` image that lacks a required attribute (`eventId`, `source`, `type`, `timestamp`, `payload`) is logged and counted as skipped, not raised.
5. **Shared runtime module** (`functions/common.py`). It provides:
   - configuration from environment variables;
   - boto3 client and resource factories with optional per-service endpoint overrides, which is how the code reaches the local emulator;
   - the API Gateway response helper;
   - a JSON encoder that handles DynamoDB `Decimal` values.
6. **Infrastructure as code** (`serverless.yml`, Serverless 1.27.3). It declares the three functions and their triggers (http, sns, stream), plus:
   - both DynamoDB tables, with provisioned throughput, a stream and the GSI;
   - the SNS notifications topic and the SQS failed-events queue;
   - IAM statements scoped to those resources;
   - environment wiring.

   `make package` runs `serverless package` in a Node 8 container, and a test asserts on the CloudFormation template it generates.
7. **Table definitions** (`config/dynamodb_tables.json`). This is the single source of truth for table key schemas and indexes. The local bootstrap uses it, and a test checks that it matches `serverless.yml`.
8. **Local runtime** (`local/`):
   - `bootstrap.py` creates the tables, topics and queues on LocalStack;
   - `runner.py` stands in for the AWS event sources. An HTTP gateway on port 3000 turns requests into API Gateway proxy events. An ingest poller delivers SNS messages to the processor through an SQS subscription. A stream poller feeds DynamoDB stream records to the transformer. Both pollers catch and log a handler exception per batch and keep running.

   `make up` (which runs `docker compose up -d app`) followed by `make smoke` demonstrates the whole flow.
9. **Tests.** Unit tests for every handler use moto. Integration tests run the real handlers through the local runner against LocalStack. Lint uses flake8. A package test checks the generated CloudFormation template. `make test` runs all of them in the period images. The original README's single `tests/test_handler.py` (listed in its project tree, never committed) is replaced by the `tests/unit/`, `tests/integration/` and `tests/package/` layout.

## 3. Out of scope

| Original README item | Reason |
|---|---|
| Deployment to real AWS (`serverless deploy --stage production`) | Needs an AWS account and costs money, and the rebuild has to be testable offline. `serverless package` produces the same CloudFormation that a deploy would upload. |
| SQS as a Lambda trigger | AWS added native SQS event sources for Lambda in late June 2018, and Serverless added the `sqs` event in 1.28 (July 2018). Both are after the period. Routing uses SNS, which was the standard fan-out trigger in early 2018. SQS is kept as the failed-events queue. |
| "Auto-scaling 0 to 10,000+ requests per second" | This is a platform property of Lambda, not something the repo implements, and it cannot be measured locally. |
| "70% cost reduction" and "Sub-100ms response time / optimized cold starts" | These are fabricated metrics with no code behind them. No cold-start tuning is attempted. |
| "Zero server management" | This describes the managed platform, not a feature. The local emulation does run containers. |
| DynamoDB on-demand billing (`BillingMode: PAY_PER_REQUEST` in the current `serverless.yml`) | On-demand capacity launched in November 2018. Provisioned throughput (5/5) is period-correct. |
| Authentication or authorisation on the API (API keys, IAM auth, Cognito) | The README does not mention it. The API is open and CORS allows `*`, which is stated as a limitation. |
| Pagination tokens on `GET /events` | Not in the README. A bounded `limit` is enough for the core. |
| Multiple stages beyond a `--stage` option | The option is kept in `serverless.yml`, but only `dev` is packaged and tested. |

## 4. Architecture

### 4.1 Target AWS topology (compiled by `serverless package`, never deployed)

```
 client ──HTTP──> API Gateway ──proxy──> [apiHandler λ]
                                            │  GET: GetItem / Query(GSI) / Scan
                                            │  POST: sns:Publish
                                            ▼
                              SNS  event-platform-ingest-<stage>
                                            │ (sns trigger)
                                            ▼
                                   [eventProcessor λ] ──invalid──> SQS event-platform-failed-<stage>
                                            │ PutItem (attribute_not_exists)
                                            ▼
                              DynamoDB event-platform-events-<stage>
                                   (stream NEW_IMAGE)   + GSI type-timestamp-index
                                            │ (stream trigger, INSERT only)
                                            ▼
                                   [dataTransformer λ] ──PutItem──> DynamoDB event-platform-transformed-<stage>
                                            │
                                            └──sns:Publish──> SNS event-platform-notifications-<stage>
```

### 4.2 Local topology (`make up`, i.e. `docker compose up -d app`)

```
 host :3000 ──> app container (python:3.6.5-stretch)  local/runner.py
                 ├─ HTTP gateway thread ──> functions.api_handler.handler(apigw_event)
                 │    (stage `local`; integration tests run their own in-process runner on stage `it`)
                 ├─ ingest poller: SQS local-ingest-sub (subscribed to ingest topic)
                 │        └─ wraps each message as an SNS record ──> functions.event_processor.handler
                 └─ stream poller: dynamodbstreams ListStreams / GetRecords on events table
                          └─ batches records ──> functions.data_transformer.handler
                                  │ boto3 with *_ENDPOINT_URL overrides
                                  ▼
                 localstack container (localstack/localstack:0.8.6)
                   kinesis :4568  dynamodb :4569  dynamodbstreams :4570  sns :4575  sqs :4576
```

In LocalStack 0.8.6 DynamoDB Streams are backed by Kinesis (kinesalite): creating a table with a stream creates a Kinesis stream, and `GetShardIterator`/`GetRecords` are served from it. 0.8.6 does not auto-start dependent services, so `SERVICES` must list `kinesis` explicitly (`SERVICES=dynamodb,dynamodbstreams,kinesis,sns,sqs`). The handlers never call Kinesis directly.

The handler modules are identical in both topologies. Only the environment variables change: the local runner sets the `*_ENDPOINT_URL` overrides and uses LocalStack resource names.

### 4.3 Components

| Component | File | Responsibility |
|---|---|---|
| API handler | `functions/api_handler.py` | Routes by method and path parameters. Publishes to SNS. Reads DynamoDB. |
| Event processor | `functions/event_processor.py` | Validates, enriches, writes idempotently, dead-letters invalid events. |
| Data transformer | `functions/data_transformer.py` | Transforms stream INSERT records, writes the transformed table, notifies. |
| Common | `functions/common.py` | Env config, boto3 factories, response helper, Decimal-aware JSON. |
| IaC | `serverless.yml`, `package.json` | Serverless 1.27.3 service definition. |
| Table config | `config/dynamodb_tables.json` | Key schemas, GSI and stream settings for both tables. |
| Bootstrap | `local/bootstrap.py` | Creates tables, topics and queues on LocalStack, plus the ingest subscription. Idempotent. |
| Local runner | `local/runner.py` | Stand-in event sources: HTTP to API Gateway event, SQS to SNS record, streams to the transformer. |
| Smoke script | `scripts/smoke.sh` | POSTs one event, polls GET until it is processed, then prints it. |

## 5. Data model & interfaces

### 5.1 Environment variables (read by `functions/common.py`)

| Variable | AWS value (serverless.yml) | Local value |
|---|---|---|
| `EVENTS_TABLE` | `${self:service}-events-${self:provider.stage}` | `event-platform-events-local` |
| `TRANSFORMED_TABLE` | `${self:service}-transformed-${self:provider.stage}` | `event-platform-transformed-local` |
| `INGEST_TOPIC_ARN` | `Fn::Join` of region, account and topic name | set by bootstrap |
| `NOTIFY_TOPIC_ARN` | `Ref: NotificationsTopic` | set by bootstrap |
| `FAILED_QUEUE_URL` | `Ref: FailedEventsQueue` | set by bootstrap |
| `DYNAMODB_ENDPOINT_URL`, `DYNAMODBSTREAMS_ENDPOINT_URL`, `SNS_ENDPOINT_URL`, `SQS_ENDPOINT_URL` | unset | `http://localstack:4569` / `:4570` / `:4575` / `:4576` |
| `AWS_DEFAULT_REGION` | Lambda-provided | `us-east-1` |

`common.get_config()` raises `ConfigError` naming any required variable that is missing.

### 5.2 `functions/common.py`

```python
class ConfigError(Exception): ...
def get_config() -> dict           # keys: events_table, transformed_table, ingest_topic_arn, notify_topic_arn, failed_queue_url
def client(service: str)           # boto3.client(service, endpoint_url=os.environ.get(SERVICE_ENDPOINT_URL))
def resource(service: str)         # same for boto3.resource
def response(status: int, body) -> dict   # {"statusCode", "headers": {Content-Type, Access-Control-Allow-Origin: *}, "body": json}
class DecimalEncoder(json.JSONEncoder)    # Decimal -> int if integral else float
def utc_now_iso() -> str          # "2018-05-01T12:00:00.123456Z"
```

### 5.3 HTTP API (API Gateway Lambda-proxy events)

| Method and path | Request | Success | Errors |
|---|---|---|---|
| `POST /events` | JSON object, e.g. `{"source":"billing","type":"invoice.created","payload":{...},"metadata":{...}}` | `202 {"eventId":"<uuid4>","status":"queued"}` (also for an object with missing or invalid fields, which the processor dead-letters) | `400 {"error":"body must be a JSON object"}`, `500` |
| `GET /events/{id}` | none | `200` stored item, with `payload`/`metadata` decoded to objects | `404 {"error":"Event not found"}` |
| `GET /events?limit=N&type=T` | `limit` from 1 to 100 (default 50), `type` optional | `200 {"events":[...],"count":n}` | `400 {"error":"limit must be an integer between 1 and 100"}` |
| anything else | | | `405 {"error":"Method not allowed"}` |

### 5.4 SNS ingest message (API to processor)

```json
{"eventId": "uuid4", "receivedAt": "ISO8601Z", "event": {"source": "...", "type": "...", "payload": {}, "metadata": {}}}
```
SNS `MessageAttributes`: `eventId` (always) and `type`, which is added only when `event.type` is a non-empty string. SNS rejects an attribute with an empty or missing `StringValue`, so an unconditional `type` attribute would turn `POST {}` into a `500`.
The processor receives Lambda SNS events of the form `{"Records":[{"EventSource":"aws:sns","Sns":{"Message":"<json above>", ...}}]}`. The handler returns `{"processed": n, "duplicates": n, "failed": n}`.

### 5.5 DynamoDB `events` table

| Attribute | Type | Notes |
|---|---|---|
| `eventId` | S | HASH key. The UUID assigned by the API. |
| `timestamp` | S | ISO8601 UTC processing time. RANGE key of the GSI. |
| `receivedAt` | S | Time the API accepted the event. |
| `source`, `type` | S | `type` is the HASH key of `type-timestamp-index` (projection ALL). |
| `payload`, `metadata` | S | JSON strings, to avoid float/Decimal conversion. |
| `status` | S | `processed` |

Other settings: provisioned throughput of 5 read and 5 write units on the table and on the GSI. `StreamSpecification: NEW_IMAGE`.

### 5.6 DynamoDB `transformed` table

| Attribute | Type | Notes |
|---|---|---|
| `eventId` | S | HASH key |
| `source`, `type` | S | lower-cased |
| `day` | S | `YYYY-MM-DD` taken from `timestamp` |
| `fields` | S | JSON object of flattened payload, e.g. `{"customer.id":"42","amount":"19.99"}` (values stringified) |
| `fieldCount` | N | |
| `transformedAt` | S | ISO8601 UTC |

`transform(item: dict) -> dict` is a pure function in `data_transformer.py`. The item is the deserialized stream image, so `payload` (and `metadata`) are JSON strings (§5.5): `transform` first does `json.loads(item['payload'])` and flattens the resulting object, never the string. `flatten(obj, prefix="") -> dict` handles nested dicts. Lists are indexed as `key.0`, `key.1`. `transform` raises `ValueError` for an image missing a required attribute; the handler catches it, logs it and counts the record as skipped.

### 5.7 SQS failed-events message

```json
{"eventId": "uuid4 or null", "reason": "Missing required field: payload", "message": "<original SNS Message string>"}
```

### 5.8 SNS notification

Subject `event.transformed`. Message: `{"eventId":"...","source":"...","type":"...","day":"YYYY-MM-DD","fieldCount":n}`.

### 5.9 `config/dynamodb_tables.json`

```json
{"tables": [
  {"logicalId": "EventsTable", "nameSuffix": "events",
   "keySchema": [{"AttributeName": "eventId", "KeyType": "HASH"}],
   "attributes": [{"AttributeName":"eventId","AttributeType":"S"}, {"AttributeName":"type","AttributeType":"S"}, {"AttributeName":"timestamp","AttributeType":"S"}],
   "globalSecondaryIndexes": [{"IndexName":"type-timestamp-index", "KeySchema":[{"AttributeName":"type","KeyType":"HASH"},{"AttributeName":"timestamp","KeyType":"RANGE"}], "Projection":{"ProjectionType":"ALL"}}],
   "stream": "NEW_IMAGE", "read": 5, "write": 5},
  {"logicalId": "TransformedTable", "nameSuffix": "transformed",
   "keySchema": [{"AttributeName": "eventId", "KeyType": "HASH"}],
   "attributes": [{"AttributeName":"eventId","AttributeType":"S"}], "read": 5, "write": 5}
]}
```
Table name = `event-platform-<nameSuffix>-<stage>`.

### 5.10 Local CLI

- `python -m local.bootstrap [--stage local]` prints the created ARNs and URLs as `KEY=value` lines and writes them to `var/local.env`.
- `python -m local.runner [--stage local] [--port 3000] [--poll-interval 1.0]` runs the bootstrap for that stage, then the gateway and both pollers. The compose `app` service uses stage `local`. The integration tests start their own in-process `Runner(stage='it', port=0)` and read every resource name from its bootstrap result, so they never share queues or streams with `app`; `make integration` also stops `app` first.
- `make smoke` runs `scripts/smoke.sh http://localhost:3000`.

## 6. Stack & pinned versions

Every pin below was checked with `tools/check_period.py` (46 packages, 0 problems, exit 0) against a cutoff of 2018-05-31.

### 6.1 Runtime (`requirements.txt`)

boto3 1.7.28 is the newest boto3 release before the 2018-05-31 cutoff. On real Lambda the runtime supplies its own boto3, so this pin governs local runs and tests; it is not a claim about the exact version Lambda shipped.

| Component | Version | Released | Why this was the choice then |
|---|---|---|---|
| Python | 3.6.5 | 2018-03-28 | Newest Lambda Python runtime in early 2018 (python3.6, GA April 2017). |
| boto3 | 1.7.28 | 2018-05-25 | The AWS SDK for Python, and the only realistic choice. Late-May release. |
| botocore | 1.10.28 | 2018-05-25 | Matching boto3 core. |
| s3transfer | 0.1.13 | 2018-02-15 | boto3 dependency. |
| jmespath | 0.9.3 | 2017-05-26 | boto3/botocore dependency. |
| python-dateutil | 2.6.1 | 2017-07-10 | botocore dependency. Capped `<2.7.0` by moto 1.3.3. |
| docutils | 0.14 | 2017-08-03 | botocore dependency. |
| six | 1.11.0 | 2017-09-17 | Shared dependency. |

### 6.2 Development and test (`requirements-dev.txt`)

| Component | Version | Released | Why |
|---|---|---|---|
| pytest | 3.5.1 | 2018-04-24 | Dominant Python test runner. 3.6.0 would need `atomicwrites`, and every atomicwrites release was re-uploaded to PyPI in 2022, so the period gate cannot verify it. 3.5.1 has no such dependency. |
| py / pluggy / attrs / more-itertools | 1.5.3 / 0.6.0 / 18.1.0 / 4.2.0 | 2018-03-22 / 2017-11-24 / 2018-05-03 / 2018-05-24 | pytest dependencies. |
| moto | 1.3.3 | 2018-04-18 | The standard in-process AWS mock for boto3 in 2018: `mock_dynamodb2`, `mock_sns`, `mock_sqs`. |
| moto transitive pins | Jinja2 2.10, MarkupSafe 1.0, boto 2.48.0, cookies 2.2.1, cryptography 2.2.2, asn1crypto 0.24.0, cffi 1.11.5, pycparser 2.18, requests 2.18.4, urllib3 1.22, idna 2.6, chardet 3.0.4, certifi 2018.4.16, xmltodict 0.11.0, Werkzeug 0.14.1, pyaml 17.12.0, PyYAML 3.12, pytz 2018.4, mock 2.0.0, pbr 4.0.3, docker 3.3.0, docker-pycreds 0.2.3, websocket-client 0.48.0, jsondiff 1.1.1, aws-xray-sdk 0.95, jsonpickle 0.9.6, wrapt 1.10.11, responses 0.9.0 | all ≤ 2018-05-27 | Pinned exactly so that pip cannot pull in post-2018 releases, such as Werkzeug 2.x, which break moto 1.3.3. PyYAML is also used to parse `serverless.yml` in tests. |
| flake8 | 3.5.0 | 2017-10-23 | Standard Python linter of the time. |
| pycodestyle / pyflakes / mccabe | 2.3.1 / 1.6.0 / 0.6.1 | 2017-01-31 / 2017-08-03 / 2017-01-26 | Ranges required by flake8 3.5.0. |

### 6.3 Infrastructure tooling

| Component | Version | Released | Why |
|---|---|---|---|
| Serverless Framework (`package.json` devDependency) | 1.27.3 | 2018-05-23 | The most popular Lambda deployment framework of 2018, and the last release before the cutoff. |
| Node.js (image) | 8.11.2 | 2018-05-15 | Active LTS in May 2018. Serverless 1.x needs Node ≥ 4. |
| npm (installed in the sls image) | 6.1.0 | 2018-05-24 | Image builds install the committed `package-lock.json` (resolved as of 2018-05-31, see §11) with `npm ci` (added in npm 5.7.0, February 2018). |
| LocalStack | 0.8.6 | image pushed 2018-04-02 | The usual 2018 local AWS emulator: DynamoDB (DynamoDB Local), DynamoDB Streams (backed by Kinesis/kinesalite, which must also be started), SNS and SQS on separate ports. |
| Docker Compose file format | 2.4 | Compose 1.21.0 (2018-04-10) | Supports both `depends_on: condition: service_healthy` and the service-level `platform:` key (the latter needs format 2.4). |

## 7. Docker images (tags verified on Docker Hub, HTTP 200)

| Image | Use | Notes |
|---|---|---|
| `python:3.6.5-stretch` | `app` and `tests` services: pip install, pytest, flake8, local runner | The full (non-slim) variant, so there is a compiler for sdists (MarkupSafe, PyYAML, wrapt). `python:3.6` also exists, but it now points to 3.6.15 on a newer Debian and is not period-accurate. |
| `node:8.11.2` | `sls` service: `serverless package` | Upgrades npm to 6.1.0 in the Dockerfile. |
| `localstack/localstack:0.8.6` | Local AWS emulator | amd64 only, so it runs under emulation on Apple Silicon (`platform: linux/amd64`). 0.8.7 was pushed in October 2018, after the period, and is not used. |

## 8. Simulated and mocked integrations

The final README must say this plainly: **nothing in this repo talks to real AWS.**

| Real service | Replaced by | Where |
|---|---|---|
| DynamoDB, SNS, SQS (unit tests) | **moto 1.3.3** in-process mocks (`mock_dynamodb2`, `mock_sns`, `mock_sqs`) | `tests/unit/` |
| DynamoDB and DynamoDB Streams, SNS, SQS (integration and demo) | **LocalStack 0.8.6** container; its DynamoDB Streams emulation is itself backed by an internal Kinesis emulator (kinesalite, port 4568) | `docker-compose.yml`, `tests/integration/` |
| AWS Lambda runtime | Handlers called in-process by **`local/runner.py`** with Lambda-shaped event dicts and a stub context | `local/runner.py` |
| API Gateway (Lambda proxy integration) | **`local/runner.py` HTTP gateway**, which turns requests into API Gateway proxy events | `local/runner.py` |
| SNS to Lambda push trigger | **SNS to SQS subscription plus a poller** that wraps each message as an `aws:sns` record | `local/runner.py` |
| DynamoDB Stream to Lambda trigger | **Stream poller** (`ListStreams`/`DescribeStream`/`GetShardIterator`/`GetRecords` on LocalStack) | `local/runner.py` |
| CloudFormation deploy | **`serverless package`** only. The template is generated and asserted on, never deployed. | `make package`, `tests/package/` |
| AWS credentials | Fixed dummy values (`AWS_ACCESS_KEY_ID=testing`) | `docker-compose.yml` |

## 9. Existing code inventory (`git ls-files` at HEAD)

| File | Decision | Reason |
|---|---|---|
| `README.md` | refactor | Regenerated in the final task from `tools/readme_template.md` with an honest Status and a Layout tree built from `git ls-files`. |
| `functions/api_handler.py` | refactor | Routing and the `response()` helper are sound and kept. The hard-coded table and queue URL are replaced by env config. It publishes to SNS instead of SQS. `get_event` stops using the wrong key (the table had a RANGE key that `get_item` never supplied). The `limit` and `type` filter are added. The unused `datetime` import is dropped. |
| `functions/event_processor.py` | refactor | The validate, enrich and store structure is kept. It now consumes SNS records instead of SQS records. It keeps the API's `eventId` instead of minting a second UUID (currently a bug: clients could never GET what they posted). Other changes: idempotent conditional put, type checks on the fields, and dead-lettering to SQS. |
| `requirements.txt` | refactor | Keep boto3 but move the pin from 1.5.0 (December 2017) to 1.7.28, and pin its transitive dependencies exactly. |
| `serverless.yml` | refactor | Keep the service name, the three function definitions, the http/stream triggers and the IAM layout. Remove `BillingMode: PAY_PER_REQUEST` (post-2018) and the `sqs` event (Serverless 1.28, July 2018). Add the SNS trigger, the transformed table, the notifications topic, the failed queue, the GSI, the environment block and scoped IAM ARNs. `dataTransformer` already referenced a file that did not exist, which the rebuild creates. |

No existing file is deleted.

## 10. Test strategy

| Layer | Tooling | What it covers | Target |
|---|---|---|---|
| Lint | flake8 3.5.0 (`setup.cfg`, max-line-length 100) | `functions/`, `local/`, `tests/` | `make lint` |
| Unit | pytest 3.5.1 + moto 1.3.3, in `python:3.6.5-stretch`, no network | see below | `make unit` |
| Package | `serverless package` in `node:8.11.2`, then pytest on `.serverless/cloudformation-template-update-stack.json` | The template contains the 3 Lambda functions, the API Gateway methods (POST /events, GET /events, GET /events/{id}), the SNS subscription, the stream event source mapping, both tables with provisioned throughput and no `BillingMode`, the GSI, the failed queue and the notifications topic | `make package` |
| Integration | pytest against the LocalStack 0.8.6 compose stack, with the runner started in-process as `Runner(stage='it', port=0)` | see below | `make integration` |

The unit layer covers:
- `common`: config errors, endpoint overrides, the Decimal encoder, the response shape.
- `api_handler`: every route and status code, validation of `limit`, the GSI query path, and the content of the SNS message.
- `event_processor`: valid and invalid records, dead-letter content, idempotency (a conditional-check failure counts as a duplicate), and preservation of `eventId`.
- `data_transformer`: `flatten`/`transform` as pure functions (including decoding a JSON-string `payload`), INSERT-only filtering, skipping an incomplete image without raising, the transformed row and the notification.
- `runner`: API Gateway event mapping, and pollers that log a raising handler and keep going.
- The table config matches `serverless.yml`, and every handler path in `serverless.yml` resolves to an importable callable.

The integration layer covers:
- bootstrap idempotency, including exactly one ingest-to-SQS subscription after two bootstraps (one publish, one delivery);
- the stream path: the events-table stream is picked out of `ListStreams` by `TableName` (even with a second stream-enabled table present) and `DescribeStream` returns a shard;
- the full flow of POST over HTTP, then processed item, then transformed row, then notification (read through an SQS subscription to the notifications topic);
- an invalid event (including `POST {}`) landing in the failed queue;
- a duplicate SNS delivery producing one stored item, one transformed row and one notification;
- `GET /events?type=`.

`make test` runs `lint unit package integration` in that order and fails on the first failing step. Every step runs in Docker, so nothing is required on the host except Docker, Compose and make.

## 11. Known limitations that will remain

- It has never been deployed to AWS. IAM policies and the CloudFormation template are checked only structurally, through `serverless package` and template assertions.
- The local runner approximates the Lambda trigger semantics. It does not reproduce concurrency, retries on handler errors, cold starts, timeouts or memory limits. Polling latency is about 1 second.
- The LocalStack 0.8.6 emulation of DynamoDB Streams and SNS is not exact. Tests assert on eventual outcomes with a timeout, not on timing.
- On Apple Silicon hosts the LocalStack image runs under amd64 emulation and starts slowly (60 s or more).
- The API has no authentication, and CORS allows `*`.
- `GET /events` without `type` uses a Scan, which is unordered and does not page beyond `limit`. There are no pagination tokens.
- The payload is stored as a JSON string, so it cannot be queried by payload attributes in DynamoDB.
- The npm transitive tree for Serverless 1.27.3 was resolved once with `npm install --before=2018-05-31` (run with npm 6.14.18, because npm 6.1.0 ignores `--before`) and is committed as `package-lock.json`; the image installs it with `npm ci`. Regenerating the lockfile depends on registry metadata.
- LocalStack 0.8.6 quirks the design works around:
  - DynamoDB Streams need the `kinesis` service running (see §4.2).
  - `DescribeTable` returns DynamoDB Local's own `LatestStreamArn` (region `ddblocal`), which LocalStack's `DescribeStream` does not recognise. The local stream poller and the integration tests therefore take the stream ARN from `dynamodbstreams.list_streams()`. `LatestStreamArn` is only relied on in the moto and CloudFormation paths.
  - Its `ListStreams` ignores the `TableName` filter and returns every stream-enabled table in the container (the `local` and `it` events tables can coexist). Callers therefore filter on the client side, `next(s['StreamArn'] for s in resp['Streams'] if s['TableName'] == table_name)`, and raise an error naming the table when no stream matches; they never take `Streams[0]`. `make integration` also recreates the `localstack` container so each run starts from a clean state.
  - Its SNS `Subscribe` appends a new subscription on every call without deduplicating, so the bootstrap checks `list_subscriptions_by_topic` and subscribes only when no SQS subscription to that queue ARN exists.
  - Its DynamoDB listener emits a stream record for a `PutItem` even when the conditional put fails. A duplicate SNS delivery therefore produces a `MODIFY` record for an existing item. This is harmless only because the transformer processes `INSERT` records alone; the duplicate integration test checks that one transformed row and one notification result.
