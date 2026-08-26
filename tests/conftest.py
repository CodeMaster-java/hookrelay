from __future__ import annotations

import os

import pytest
import pytest_asyncio

from hookrelay.backends.memory import MemoryBackend
from hookrelay.retry import RetryPolicy

POSTGRES_URL = os.environ.get("HOOKRELAY_TEST_DATABASE_URL")
REDIS_URL = os.environ.get("HOOKRELAY_TEST_REDIS_URL")

# Deterministic policy for tests: no jitter, tiny delay so retry-driven tests run fast.
TEST_RETRY_POLICY = RetryPolicy(max_attempts=3, base_delay=0.01, max_delay=0.05, jitter=0.0)


@pytest_asyncio.fixture(params=["memory", "postgres", "redis"])
async def backend(request):
    """Yields one backend implementation per param, skipping postgres/redis when
    their test connection env vars aren't set. Tests written against this fixture
    run once per backend, guaranteeing identical behavior across all three."""
    name = request.param

    if name == "memory":
        yield MemoryBackend(retry_policy=TEST_RETRY_POLICY)
        return

    if name == "postgres":
        if not POSTGRES_URL:
            pytest.skip("HOOKRELAY_TEST_DATABASE_URL not set")
        from sqlalchemy.ext.asyncio import create_async_engine

        from hookrelay.backends.postgres import PostgresBackend, metadata

        engine = create_async_engine(POSTGRES_URL)
        pg_backend = PostgresBackend(engine, retry_policy=TEST_RETRY_POLICY)
        await pg_backend.init_schema()
        yield pg_backend
        async with engine.begin() as conn:
            await conn.run_sync(metadata.drop_all)
        await engine.dispose()
        return

    if not REDIS_URL:
        pytest.skip("HOOKRELAY_TEST_REDIS_URL not set")
    from redis.asyncio import from_url

    from hookrelay.backends.redis import RedisBackend

    redis = from_url(REDIS_URL)
    namespace = "hookrelay-test"
    redis_backend = RedisBackend(redis, retry_policy=TEST_RETRY_POLICY, namespace=namespace)
    yield redis_backend
    keys = await redis.keys(f"{namespace}:*")
    if keys:
        await redis.delete(*keys)
    await redis.aclose()
