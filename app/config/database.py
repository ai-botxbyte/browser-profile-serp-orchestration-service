from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from app.config.config import get_config


config = get_config()

# Primary Database Engine and Sessions
async_engine = create_async_engine(
    config.database_url, pool_pre_ping=True, echo=False, future=True
)
ASYNC_SESSION_LOCAL = sessionmaker(
    autocommit=False, autoflush=False, bind=async_engine, class_=AsyncSession
)

# Read Replica Database Engines and Sessions
async_read_replica_engine = create_async_engine(
    config.read_replica_database_url, pool_pre_ping=True, echo=False, future=True
)
ASYNC_READ_REPLICA_SESSION_LOCAL = sessionmaker(
    autocommit=False, autoflush=False, bind=async_read_replica_engine, class_=AsyncSession
)


async def get_async_db() -> AsyncGenerator[AsyncSession, None]:
    """Yield an AsyncSession for use in async endpoints/dependencies."""
    async with ASYNC_SESSION_LOCAL() as session:
        yield session


async def get_async_read_replica_db() -> AsyncGenerator[AsyncSession, None]:
    """Yield an AsyncSession for use in async endpoints/dependencies from the read replica."""
    async with ASYNC_READ_REPLICA_SESSION_LOCAL() as session:
        yield session