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


async def _memory_backend():
    yield MemoryBackend(retry_policy=TEST_RETRY_POLICY)


async def _postgres_backend():
    if not POSTGRES_URL:
        pytest.skip("HOOKRELAY_TEST_DATABASE_URL not set")
    from sqlalchemy.ext.asyncio import create_async_engine

    from hookrelay.backends.postgres import PostgresBackend, metadata

    engine = create_async_engine(POSTGRES_URL)
    backend = PostgresBackend(engine, retry_policy=TEST_RETRY_POLICY)
    await backend.init_schema()
    yield backend
    async with engine.begin() as conn:
        await conn.run_sync(metadata.drop_all)
    await engine.dispose()


async def _redis_backend(claim_lease_seconds: float = 300.0):
    if not REDIS_URL:
        pytest.skip("HOOKRELAY_TEST_REDIS_URL not set")
    from redis.asyncio import from_url

    from hookrelay.backends.redis import RedisBackend

    redis = from_url(REDIS_URL)
    namespace = "hookrelay-test"
    backend = RedisBackend(
        redis,
        retry_policy=TEST_RETRY_POLICY,
        namespace=namespace,
        claim_lease_seconds=claim_lease_seconds,
    )
    yield backend
    keys = await redis.keys(f"{namespace}:*")
    if keys:
        await redis.delete(*keys)
    await redis.aclose()


async def _sqlite_backend():
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.pool import StaticPool

    from hookrelay.backends.sqlite import SQLiteBackend, metadata

    # StaticPool keeps a single underlying connection alive for the whole engine,
    # which is required for an in-memory SQLite database to survive across the
    # multiple "connections" SQLAlchemy's async engine otherwise hands out from a
    # pool; without it, each connection would see a fresh, empty database.
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", poolclass=StaticPool)
    backend = SQLiteBackend(engine, retry_policy=TEST_RETRY_POLICY)
    await backend.init_schema()
    yield backend
    async with engine.begin() as conn:
        await conn.run_sync(metadata.drop_all)
    await engine.dispose()


@pytest_asyncio.fixture(params=["memory", "postgres", "redis", "sqlite"])
async def backend(request):
    """Yields one backend implementation per param, skipping postgres/redis when
    their test connection env vars aren't set. Tests written against this fixture
    run once per backend, guaranteeing identical behavior across all of them."""
    factories = {
        "memory": _memory_backend,
        "postgres": _postgres_backend,
        "redis": _redis_backend,
        "sqlite": _sqlite_backend,
    }
    async for instance in factories[request.param]():
        yield instance


@pytest_asyncio.fixture
async def postgres_backend():
    """Raw PostgresBackend, for tests exercising Postgres-only features like
    purge(). Skips when HOOKRELAY_TEST_DATABASE_URL isn't set."""
    async for instance in _postgres_backend():
        yield instance


@pytest_asyncio.fixture
async def sqlite_backend():
    """Raw SQLiteBackend, for tests exercising SQLite-only features like
    purge(). Always available, no external service required."""
    async for instance in _sqlite_backend():
        yield instance


@pytest_asyncio.fixture(params=["postgres", "sqlite"])
async def purge_backend(request):
    """Backend instances that implement purge(): postgres and sqlite. Skips the
    postgres parametrization when HOOKRELAY_TEST_DATABASE_URL isn't set."""
    factories = {"postgres": _postgres_backend, "sqlite": _sqlite_backend}
    async for instance in factories[request.param]():
        yield instance


@pytest_asyncio.fixture
async def redis_backend(request):
    """Raw RedisBackend, for tests exercising Redis-only features like
    reap_stale_claims(). Skips when HOOKRELAY_TEST_REDIS_URL isn't set.

    Accepts indirect parametrization to set claim_lease_seconds, e.g.:
    `@pytest.mark.parametrize("redis_backend", [0.05], indirect=True)`.
    """
    claim_lease_seconds = getattr(request, "param", 300.0)
    async for instance in _redis_backend(claim_lease_seconds=claim_lease_seconds):
        yield instance
