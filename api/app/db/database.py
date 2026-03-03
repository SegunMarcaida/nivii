import os
from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

# Engine and session factory are created lazily on first access so that
# importing this module does not require asyncpg at collection time.
_engine = None
_AsyncSessionLocal = None


def get_engine():
    """Return (and lazily create) the async SQLAlchemy engine."""
    global _engine
    if _engine is None:
        database_url = os.environ["DATABASE_URL"]
        _engine = create_async_engine(
            database_url,
            connect_args={"check_same_thread": False},
            echo=False,
        )
    return _engine


def get_session_factory():
    """Return (and lazily create) the async session factory."""
    global _AsyncSessionLocal
    if _AsyncSessionLocal is None:
        _AsyncSessionLocal = async_sessionmaker(
            bind=get_engine(),
            class_=AsyncSession,
            expire_on_commit=False,
        )
    return _AsyncSessionLocal


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency: yields an async database session per request."""
    async with get_session_factory()() as session:
        yield session
