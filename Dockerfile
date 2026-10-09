FROM python:3.6.5-stretch

ENV PYTHONPATH=/app PYTHONDONTWRITEBYTECODE=1 AWS_DEFAULT_REGION=us-east-1

WORKDIR /app

COPY requirements.txt requirements-dev.txt ./
RUN pip install --no-cache-dir -r requirements.txt -r requirements-dev.txt

COPY . .
