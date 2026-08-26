from __future__ import annotations

import argparse
import asyncio
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from hookrelay.backends.base import Backend
from hookrelay.exceptions import EventNotFoundError


@asynccontextmanager
async def _connect(url: str) -> AsyncIterator[Backend]:
    """Builds a Backend from a connection URL, picking the implementation from its
    scheme, and disposes the underlying connection(s) on exit. Each backend module
    is imported lazily so this CLI does not require every optional dependency to
    be installed, only the one matching the URL you actually pass."""
    if url.startswith("postgresql"):
        from sqlalchemy.ext.asyncio import create_async_engine

        from hookrelay.backends.postgres import PostgresBackend

        engine = create_async_engine(url)
        try:
            yield PostgresBackend(engine)
        finally:
            await engine.dispose()
    elif url.startswith("sqlite"):
        from sqlalchemy.ext.asyncio import create_async_engine

        from hookrelay.backends.sqlite import SQLiteBackend

        engine = create_async_engine(url)
        try:
            yield SQLiteBackend(engine)
        finally:
            await engine.dispose()
    elif url.startswith("redis"):
        from redis.asyncio import from_url

        from hookrelay.backends.redis import RedisBackend

        redis = from_url(url)
        try:
            yield RedisBackend(redis)
        finally:
            await redis.aclose()
    else:
        raise ValueError(
            f"unrecognized backend URL scheme: {url!r}. "
            "Expected a postgresql, sqlite, or redis URL."
        )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="hookrelay", description="Inspect and recover hookrelay dead-lettered events."
    )
    top_level = parser.add_subparsers(dest="command", required=True)

    dead_letters = top_level.add_parser("dead-letters", help="Work with dead-lettered events.")
    dead_letters_commands = dead_letters.add_subparsers(dest="dead_letters_command", required=True)

    list_parser = dead_letters_commands.add_parser("list", help="List dead-lettered events.")
    list_parser.add_argument(
        "--backend",
        required=True,
        help="Backend URL, for example postgresql+asyncpg://..., "
        "sqlite+aiosqlite:///path.db, or redis://...",
    )
    list_parser.add_argument("--limit", type=int, default=100)
    list_parser.add_argument("--offset", type=int, default=0)

    requeue_parser = dead_letters_commands.add_parser(
        "requeue", help="Requeue a dead-lettered event for another full retry cycle."
    )
    requeue_parser.add_argument("event_id")
    requeue_parser.add_argument("--backend", required=True, help="Backend URL, same as `list`.")

    return parser


async def _run_list(args: argparse.Namespace) -> int:
    async with _connect(args.backend) as backend:
        events = await backend.list_dead_letters(limit=args.limit, offset=args.offset)
    if not events:
        print("no dead-lettered events")
        return 0
    for event in events:
        print(
            f"{event.id}  source={event.source}  attempts={event.attempts}"
            f"  updated_at={event.updated_at.isoformat()}"
        )
        print(f"  last_error: {event.last_error}")
    return 0


async def _run_requeue(args: argparse.Namespace) -> int:
    async with _connect(args.backend) as backend:
        requeued = await backend.requeue_dead_letter(args.event_id)
    if not requeued:
        print(str(EventNotFoundError(args.event_id)), file=sys.stderr)
        return 1
    print(f"requeued {args.event_id}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        if args.dead_letters_command == "list":
            return asyncio.run(_run_list(args))
        return asyncio.run(_run_requeue(args))
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
