import time

from dagster import job, op


@op
def slow_op():
    """A op for monitoring test: it sleeps for 30 seconds to simulate a long-running operation."""
    time.sleep(30)


@op
def failing_op():
    """A op for testing failure counts: it raises an error intentionally."""
    raise RuntimeError("Intentional error for exporter testing")


@job(tags={"dagster/concurrency_key": "heavy_limit"})
def heavy_job():
    slow_op()


@job(tags={"dagster/concurrency_key": "failing_limit"})
def failing_job():
    failing_op()


jobs = [heavy_job, failing_job]
