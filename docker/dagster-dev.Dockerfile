FROM python:3.13-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

ENV UV_PROJECT_ENVIRONMENT=/opt/venv
ENV DAGSTER_HOME=/app/dev/dagster_home

WORKDIR /app

COPY pyproject.toml uv.lock README.md ./

RUN uv sync --frozen --no-install-project

COPY dev/ dev/

EXPOSE 3000

CMD ["uv", "run", "dagster", "dev", "-h", "0.0.0.0", "-p", "3000"]
