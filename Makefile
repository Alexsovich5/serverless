export DOCKER_DEFAULT_PLATFORM := linux/amd64
export COMPOSE_PROJECT_NAME ?= serverless

COMPOSE ?= docker compose
RUN = $(COMPOSE) run --rm --no-deps tests
NO_ENDPOINTS = -e DYNAMODB_ENDPOINT_URL= -e DYNAMODBSTREAMS_ENDPOINT_URL= \
	-e SNS_ENDPOINT_URL= -e SQS_ENDPOINT_URL=
ARGS ?=

.PHONY: build lint unit package integration test down shell

build:
	$(COMPOSE) build tests

lint: build
	$(RUN) flake8 functions local tests

unit: build
	$(COMPOSE) run --rm --no-deps $(NO_ENDPOINTS) tests pytest tests/unit $(ARGS)

package: build
	$(COMPOSE) build sls
	$(COMPOSE) run --rm sls serverless package --stage dev
	$(RUN) pytest tests/package $(ARGS)

integration: build
	$(COMPOSE) rm -sfv localstack
	$(COMPOSE) up -d localstack
	$(COMPOSE) run --rm tests pytest tests/integration $(ARGS)

test: lint unit package integration

down:
	$(COMPOSE) down -v

shell: build
	$(RUN) bash
