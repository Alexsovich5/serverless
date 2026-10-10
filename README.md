# Serverless Event-Driven Architecture

An HTTP API accepts business events and publishes each one, with a generated `eventId`, to an SNS ingest topic. An event processor Lambda subscribed to that topic validates the event, adds a timestamp and status, and stores it in a DynamoDB events table; events that fail validation go to an SQS failed-events queue with a reason. The events table has a stream, and a data transformer Lambda reads its `INSERT` records, lower-cases `source` and `type`, flattens the payload into dotted keys, writes the result to a second table and publishes a notification to an SNS notifications topic. The same API reads stored events back by id or lists them, optionally filtered by type.

Personal project built on the 2018-era stack (AWS Lambda Python 3.6, Serverless Framework 1.27, API Gateway, SNS, SQS, DynamoDB).

## Status

**Implemented**

- Event ingestion API (`functions/api_handler.py`): `POST /events` assigns a UUID `eventId`, publishes to the SNS ingest topic and returns `202`; a body that is not a JSON object returns `400`.
- Event retrieval API: `GET /events/{id}` returns the stored event or `404`; `GET /events?limit=N&type=T` queries the `type-timestamp-index` GSI newest first when `type` is given and scans otherwise; `limit` must be 1 to 100 (default 50); other methods return `405`.
- Event processor (`functions/event_processor.py`, SNS trigger): validates `source`, `type` and `payload`, enriches with `timestamp` and `status` while keeping the API's `eventId`, writes with `attribute_not_exists(eventId)` so redeliveries do not overwrite, and sends invalid events to the SQS failed-events queue with a reason.
- Data transformer (`functions/data_transformer.py`, DynamoDB stream trigger): for `INSERT` records it lower-cases `source` and `type`, flattens the payload, derives `day`, writes to the transformed-events table and publishes to the notifications topic; `MODIFY`/`REMOVE` records and images missing a required attribute are skipped.
- Shared runtime module (`functions/common.py`): configuration from environment variables, boto3 client and resource factories with per-service endpoint overrides, the API Gateway response helper and a `Decimal`-aware JSON encoder.
- Infrastructure as code (`serverless.yml`, Serverless 1.27.3): the three functions with http, sns and stream triggers, both DynamoDB tables with provisioned throughput, stream and GSI, the notifications topic, the failed-events queue, scoped IAM statements and environment wiring. `make package` runs `serverless package` and a test checks the generated CloudFormation template.
- Table definitions (`config/dynamodb_tables.json`): one source for key schemas and indexes, used by the local bootstrap and checked against `serverless.yml` by a test.
- Local runtime (`local/`): `bootstrap.py` creates the tables, topics and queues on LocalStack; `runner.py` runs an HTTP gateway on port 3000 that builds API Gateway proxy events, an ingest poller that delivers SNS messages to the processor through an SQS subscription, and a stream poller that feeds DynamoDB stream records to the transformer.
- Tests: moto unit tests for every handler, integration tests that run the real handlers through the local runner against LocalStack, flake8 lint and a CloudFormation template check, all run by `make test` in the Docker images.

**Not implemented / known limitations**

- Never deployed to AWS. AWS services are emulated with LocalStack 0.8.6 for integration tests and the demo, and mocked with moto 1.3.3 for unit tests; API Gateway and the Lambda triggers are simulated by `local/runner.py`. IAM policies and the CloudFormation template are only checked structurally, through `serverless package` and template assertions.
- No deployment to real AWS (`serverless deploy --stage production`); `serverless package` produces the CloudFormation a deploy would upload.
- SQS is not used as a Lambda trigger; routing uses SNS, and SQS only holds failed events.
- No claim about auto-scaling throughput: scaling is a property of the Lambda platform and is not implemented or measured here.
- No cost or latency figures and no cold-start tuning.
- "Zero server management" is not a feature of this repo; the local emulation runs containers.
- DynamoDB on-demand billing is not used; the tables use provisioned throughput (5/5).
- No authentication or authorisation on the API (no API keys, IAM auth or Cognito), and CORS allows `*`.
- No pagination tokens on `GET /events`. Without `type` it uses a Scan, which is unordered and does not page beyond `limit`.
- Only the `dev` stage is packaged and tested, although `serverless.yml` keeps a `--stage` option.
- The local runner approximates Lambda trigger semantics: no concurrency, retries on handler errors, cold starts, timeouts or memory limits, and polling latency is about 1 second.
- LocalStack 0.8.6 emulates DynamoDB Streams and SNS inexactly, so tests assert on eventual outcomes with a timeout rather than on timing.
- On Apple Silicon hosts the LocalStack image runs under amd64 emulation and can take 60 seconds or more to start.
- The payload is stored as a JSON string, so DynamoDB cannot query by payload attributes.
- The npm dependency tree for Serverless 1.27.3 was resolved once with `npm install --before=2018-05-31` and committed as `package-lock.json`; regenerating it depends on registry metadata.
- Several LocalStack 0.8.6 quirks are worked around in the local poller, bootstrap and tests: Streams need the `kinesis` service, stream ARNs come from `ListStreams` filtered on the client side, SNS `Subscribe` is not deduplicated, `GetRecords` drops `NextShardIterator` so the poller re-reads the shard and skips sequence numbers it has seen, and a failed conditional put still emits a stream record. Details are in `docs/SPEC.md` section 11.

## Built with

- **Python 3.6.5** with boto3 1.7.28; moto 1.3.3 and pytest 3.5.1 for tests
- **Serverless 1.27.3** on Node 8.11.2
- **LocalStack 0.8.6** for the local AWS emulation

## Running it

Everything runs in Docker images from the project's era, so nothing needs installing locally beyond Docker.

```bash
make up                  # or: docker compose up -d app  (LocalStack + runner on http://localhost:20730)
make smoke               # posts an event and polls until it is stored as processed
make down                # stop the stack
```

## Tests

```bash
make test                # runs the suite inside the period image
```

Unit tests run the handlers against moto, integration tests run them through the local runner against LocalStack, and a package test checks the generated CloudFormation template; nothing runs against real AWS.

## Layout

```
.dockerignore
.gitignore
Dockerfile
Dockerfile.sls
Makefile
README.md
config/
  dynamodb_tables.json
docker-compose.yml
docs/
  IMPLEMENTATION_PLAN.md
  SPEC.md
functions/
  __init__.py
  api_handler.py
  common.py
  data_transformer.py
  event_processor.py
local/
  __init__.py
  bootstrap.py
  runner.py
package-lock.json
package.json
requirements-dev.txt
requirements.txt
scripts/
  smoke.sh
serverless.yml
setup.cfg
tests/
  __init__.py
  integration/
    __init__.py
    conftest.py
    test_localstack.py
    test_pipeline.py
    test_smoke_script.py
  package/
    __init__.py
    test_template.py
  unit/
    __init__.py
    conftest.py
    test_api_handler.py
    test_bootstrap.py
    test_common.py
    test_data_transformer.py
    test_environment.py
    test_event_processor.py
    test_readme.py
    test_runner.py
    test_serverless_config.py
```
