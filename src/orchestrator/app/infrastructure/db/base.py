from typing import Any

from sqlalchemy import JSON, BigInteger, Integer
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase

# JSONB в PostgreSQL, обычный JSON в SQLite/прочих — одна схема, разные диалекты.
JsonType = JSON().with_variant(JSONB(), "postgresql")

# BIGINT в PostgreSQL; SQLite автоинкрементит только INTEGER PRIMARY KEY.
PkType = BigInteger().with_variant(Integer(), "sqlite")


class Base(DeclarativeBase):
    """Базовый класс ORM-моделей оркестратора."""

    type_annotation_map = {dict[str, Any]: JsonType, list: JsonType}
