export DOCKER_DEFAULT_PLATFORM := linux/amd64
export COMPOSE_PROJECT_NAME ?= serverless

COMPOSE ?= docker compose
RUN = $(COMPOSE) run --rm --no-deps tests
ARGS ?=

.PHONY: build lint unit package test shell

build:
	$(COMPOSE) build tests

lint: build
	$(RUN) flake8 functions local tests

unit: build
	$(RUN) pytest tests/unit $(ARGS)

package: build
	$(COMPOSE) build sls
	$(COMPOSE) run --rm sls serverless package --stage dev
	$(RUN) pytest tests/package $(ARGS)

test: lint unit package

shell: build
	$(RUN) bash
