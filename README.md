# Serverless Event-Driven Architecture

![Project Status](https://img.shields.io/badge/Status-Complete-brightgreen)
![Timeline](https://img.shields.io/badge/Timeline-February%202018%20--%20May%202018-blue)
![Technology](https://img.shields.io/badge/Tech-AWS%20Lambda%20%7C%20API%20Gateway%20%7C%20DynamoDB-orange)

## Project Overview

Event-driven serverless architecture on AWS for processing and transforming business events with Lambda functions, API Gateway endpoints, and DynamoDB storage.

**Role**: IT Operations Specialist
**Organization**: ACORIA
**Duration**: February 2018 - May 2018
**Project**: #16 of 30 in IT Career Portfolio

## Business Impact

- **Zero Server Management**: Fully serverless infrastructure
- **Auto-scaling**: Handles 0 to 10,000+ requests per second
- **70% Cost Reduction**: Pay-per-invocation vs always-on servers
- **Sub-100ms Response Time**: Optimized Lambda cold starts

## Technology Stack

- **AWS Lambda**: Serverless compute (Python 3.6)
- **API Gateway**: RESTful API management
- **DynamoDB**: NoSQL event storage
- **SQS/SNS**: Event routing and notifications
- **Serverless Framework**: Infrastructure as Code

## Project Structure

```
serverless/
├── README.md
├── serverless.yml
├── requirements.txt
├── functions/
│   ├── event_processor.py
│   ├── api_handler.py
│   └── data_transformer.py
├── config/
│   └── dynamodb_tables.json
└── tests/
    └── test_handler.py
```

## Deployment

```bash
npm install -g serverless
pip install -r requirements.txt
serverless deploy --stage production
```

## Contributing

This is a historical project from February 2018 - May 2018, preserved for portfolio purposes.

## License

Professional portfolio project - ACORIA

---

**Developed during February 2018 - May 2018**
*Part of Alexander Efrem's IT Career Portfolio (2012-2024)*
