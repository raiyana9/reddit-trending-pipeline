# syntax=docker/dockerfile:1
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY config config
COPY shared shared
COPY services services
COPY scripts scripts
COPY data data

RUN useradd --create-home --uid 10001 app
USER app

CMD ["python", "-m", "services.api"]
