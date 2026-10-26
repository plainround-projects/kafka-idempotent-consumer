# The command-line programs as an image. Nothing here serves traffic; the health command is the image's check.
# Run against Kafka and PostgreSQL the container can reach, for example:
#   docker run --rm --network host -e BOOTSTRAP_SERVERS=localhost:9092 kafka-idempotent-consumer:dev idempotent-consumer-produce --payments 10
FROM python:3.12.8-slim-bookworm@sha256:2199a62885a12290dc9c5be3ca0681d367576ab7bf037da120e564723292a2f0

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app
COPY requirements.lock ./
RUN pip install --require-hashes -r requirements.lock

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-deps .

RUN useradd --system --uid 10001 --no-create-home app
USER app

HEALTHCHECK --interval=30s --timeout=20s --retries=3 CMD ["idempotent-consumer-health"]
CMD ["idempotent-consumer-health"]
