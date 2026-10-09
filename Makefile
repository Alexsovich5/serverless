export DOCKER_DEFAULT_PLATFORM := linux/amd64
export COMPOSE_PROJECT_NAME ?= serverless

COMPOSE ?= docker compose
RUN = $(COMPOSE) run --rm --no-deps tests
ARGS ?=

.PHONY: build lint unit test shell

build:
	$(COMPOSE) build tests

lint: build
	$(RUN) flake8 functions tests

unit: build
	$(RUN) pytest tests/unit $(ARGS)

test: lint unit

shell: build
	$(RUN) bash
