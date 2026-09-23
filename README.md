# Serverless Event-Driven Architecture

Event-driven serverless architecture on AWS for processing and transforming business events with Lambda functions, API Gateway endpoints, and DynamoDB storage.

Personal project, built to explore an event-driven Lambda pipeline with API Gateway, SQS and DynamoDB. It is not production software — see **Status** below for exactly what is and isn't implemented.

## Status

**Implemented**

- API handler Lambda writing to DynamoDB and enqueueing to SQS
- Event processor Lambda consuming the queue
- Serverless Framework deployment config

**Not implemented / known limitations**

- No data transformer function and no tests (the earlier README claimed both)
- Table and queue names are hardcoded rather than injected
- Never deployed

## Built with

- **Python** — boto3

## Running it

```bash
pip install -r requirements.txt
```

## Layout

```
functions/
  api_handler.py
  event_processor.py
requirements.txt
serverless.yml
```

