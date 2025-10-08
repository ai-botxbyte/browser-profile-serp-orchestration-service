from typing import Iterator
from collections.abc import Generator
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from app.config.config import config
from sqlalchemy.orm import sessionmaker as orm_sessionmaker


# Import Base so model metadata is available (used by Alembic / create_all)

engine = create_engine(config.DATABASE_URL, pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine, future=True)

# Async engine + session for use with async code (FastAPI async endpoints)
# Note: settings.ASYNC_DATABASE_URL should be like 'postgresql+asyncpg://user:pass@host/db'
async_engine = create_async_engine(config.ASYNC_DATABASE_URL, pool_pre_ping=True, future=True)
AsyncSessionLocal = orm_sessionmaker(bind=async_engine, class_=AsyncSession, expire_on_commit=False, future=True)

# [x]: create fastapi folder in helper and all function move there.
# [X]: For Read Replica also.
# Read Replica Database Engines and Sessions
read_replica_engine = create_engine(config.READ_DATABASE_URL, pool_pre_ping=True, future=True)
ReadReplicaSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=read_replica_engine, future=True)

# Async read replica engine + session
async_read_replica_engine = create_async_engine(config.ASYNC_READ_DATABASE_URL, pool_pre_ping=True, future=True)
AsyncReadReplicaSessionLocal = orm_sessionmaker(bind=async_read_replica_engine, class_=AsyncSession, expire_on_commit=False, future=True)

def get_db() -> Iterator:
    """Yield a SQLAlchemy Session and ensure it is closed afterwards.

    Returns an iterator compatible with FastAPI Depends or manual use.
    Uses read replica if USE_READ_REPLICA config is True, otherwise uses primary database.
    """
    if config.USE_READ_REPLICA:
        db = ReadReplicaSessionLocal()
    else:
        db = SessionLocal()
    
    try:
        yield db
    finally:
        db.close()

# [] I am confused about this function - is it required? May be - Yes.
async def get_async_db() -> Generator:
    """Yield an AsyncSession for use in async endpoints/dependencies.
    
    Uses read replica if USE_READ_REPLICA config is True, otherwise uses primary database.
    """
    if config.USE_READ_REPLICA:
        async with AsyncReadReplicaSessionLocal() as session:
            yield session
    else:
        async with AsyncSessionLocal() as session:
            yield session


