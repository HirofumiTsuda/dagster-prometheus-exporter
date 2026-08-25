FROM python:3.13-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

ENV UV_PROJECT_ENVIRONMENT=/opt/venv
ENV DAGSTER_HOME=/app/dev/dagster_home

WORKDIR /app

COPY pyproject.toml uv.lock README.md ./

# --group dbt: dagster-dbt/dbt-duckdb live in a separate uv dependency
# group (not the base dependencies), so `uv sync` alone -- as used for
# investigating GraphQL compatibility across Dagster versions (issue #67)
# -- doesn't drag in dagster-dbt's own version pairing with dagster. This
# image runs the actual dev stack, including the jaffle_shop dbt assets in
# dev/dagster_workspace/definitions.py, so it needs the group installed.
RUN uv sync --frozen --no-install-project --group dbt

COPY dev/ dev/

EXPOSE 3000

# --group dbt again here, not just on the sync above: `uv run` does its own
# implicit sync check, and while it doesn't re-prune the group in practice
# (nothing invalidates the venv between build and container start), relying
# on that implicitly would mean whether this container actually has
# dagster-dbt depends on unstated uv sync-skip heuristics rather than
# something declared in this file. Verified empirically: a bare `uv sync`
# does prune the group back out (dagster-dbt is genuinely removed, not just
# left stale), so this isn't a hypothetical concern.
CMD ["uv", "run", "--group", "dbt", "dagster", "dev", "-h", "0.0.0.0", "-p", "3000"]
