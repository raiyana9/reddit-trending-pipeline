# syntax=docker/dockerfile:1
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

# en_core_web_sm ships as a pinned wheel in requirements.txt. Any other model is downloaded at build
# time so containers never fetch models at runtime:  --build-arg SPACY_MODEL=en_core_web_md
ARG SPACY_MODEL=en_core_web_sm
ENV SPACY_MODEL=${SPACY_MODEL}
RUN if [ "$SPACY_MODEL" != "en_core_web_sm" ]; then python -m spacy download "$SPACY_MODEL"; fi

COPY config config
COPY shared shared
COPY services services
COPY scripts scripts
COPY data data

RUN useradd --create-home --uid 10001 app
USER app

CMD ["python", "-m", "services.nlp_worker"]
