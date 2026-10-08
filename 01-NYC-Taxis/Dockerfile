FROM python:3.12.8-slim

ARG DEBIAN_FRONTEND=noninteractive

ENV PYTHONUNBUFFERED=1
ENV AIRFLOW_HOME=/app/airflow

RUN apt-get update && apt-get install -y curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR $AIRFLOW_HOME

COPY . .

RUN chmod +x scripts/entrypoint.sh

RUN pip3 install --no-cache-dir poetry==2.4.3

RUN poetry config installer.max-workers 1 \
    && poetry config installer.parallel false \
    && poetry config requests.max-retries 5 \
    && poetry install --without dev \
    && poetry build --format wheel
