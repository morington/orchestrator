import os

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from orchestrator.app.configuration.config import Configuration
from orchestrator.app.infrastructure.db.base import Base
from orchestrator.app.infrastructure.store_sql import SqlAlchemyWorkflowStore

PG_URL = os.getenv("TEST_PG_URL", Configuration().postgresql.url())


@pytest.fixture
async def pg_store() -> SqlAlchemyWorkflowStore:
    engine = create_async_engine(PG_URL, pool_size=10, max_overflow=20)
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
            await conn.run_sync(Base.metadata.create_all)
    except Exception as exc:
        pytest.skip(f"PostgreSQL недоступен для интеграционных тестов: {exc}")

    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield SqlAlchemyWorkflowStore(factory, dialect="postgresql")
    await engine.dispose()
