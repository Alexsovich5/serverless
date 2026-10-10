export DOCKER_DEFAULT_PLATFORM := linux/amd64
export COMPOSE_PROJECT_NAME ?= serverless

COMPOSE ?= docker compose
RUN = $(COMPOSE) run --rm --no-deps tests
NO_ENDPOINTS = -e DYNAMODB_ENDPOINT_URL= -e DYNAMODBSTREAMS_ENDPOINT_URL= \
	-e SNS_ENDPOINT_URL= -e SQS_ENDPOINT_URL=
ARGS ?=

.PHONY: build lint unit package integration test up smoke demo down shell

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
	-$(COMPOSE) stop app
	$(COMPOSE) rm -sfv localstack
	$(COMPOSE) up -d localstack
	$(COMPOSE) run --rm tests pytest tests/integration $(ARGS)

test: lint unit package integration

up: build
	$(COMPOSE) up -d app
	@i=0; until [ "$$(docker inspect -f '{{.State.Health.Status}}' $$($(COMPOSE) ps -q app))" = healthy ]; do \
		i=$$((i + 1)); \
		if [ $$i -gt 120 ]; then echo 'app did not become healthy' >&2; $(COMPOSE) logs app; exit 1; fi; \
		sleep 2; \
	done; echo 'app is healthy on http://localhost:20730'

smoke:
	$(RUN) sh scripts/smoke.sh http://app:3000

demo: up smoke

down:
	$(COMPOSE) down -v

shell: build
	$(RUN) bash
