from __future__ import annotations

from typing import Awaitable, Callable, TypeVar

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase

from app.config import Config

T = TypeVar("T")


class Base(DeclarativeBase):
    pass


def make_engine(cfg: Config):
    return create_async_engine(cfg.database_url, pool_pre_ping=True)


def make_session_factory(engine):
    return async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def run_in_session(
    session_factory: async_sessionmaker[AsyncSession],
    fn: Callable[[AsyncSession], Awaitable[T]],
) -> T:
    """Small helper to execute async DB logic with a managed session.

    Intended use (e.g., reminders/scheduler):
        result = await run_in_session(session_factory, lambda s: repo.do(s, ...))
    """
    async with session_factory() as session:
        return await fn(session)
