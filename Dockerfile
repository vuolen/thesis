FROM python:3.12-slim AS builder

WORKDIR /app

ENV PIPENV_VENV_IN_PROJECT=1
RUN pip install pipenv

COPY Pipfile Pipfile.lock ./
RUN pipenv sync

FROM python:3.12-slim AS runtime

WORKDIR /app

# install dependencies

COPY --from=builder /app/.venv /app/.venv
COPY dagster_project /app/dagster_project
COPY scrapy_project /app/scrapy_project
COPY patterns /app/patterns

RUN mkdir -p /app/data/files /app/data/item_feeds /app/data/.dagster_home
ENV FILES_DIR=/app/data/files
ENV ITEM_FEEDS_DIR=/app/data/item_feeds
ENV DAGSTER_HOME=/app/data/.dagster_home

VOLUME ["/app/data"]

# run dagster dev using the venv
CMD ["/app/.venv/bin/dagster", "dev", "-m", "dagster_project", "--working-directory", ".", "-h", "0.0.0.0"]
