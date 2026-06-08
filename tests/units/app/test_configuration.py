from orchestrator.app.configuration.config import Configuration, PostgresqlModel, StorageModel


def test_postgresql_url() -> None:
    model = PostgresqlModel(
        host="postgres",
        port=5432,
        username="orchestrator",
        password="secret",
        database="orchestrator",
    )
    assert model.url() == "postgresql+asyncpg://orchestrator:secret@postgres:5432/orchestrator"


def test_postgresql_safe_url_masks_credentials() -> None:
    model = PostgresqlModel(host="localhost", password="secret")
    assert model.safe_url() == "postgresql+asyncpg://***:***@localhost:5432/orchestrator"


def test_configuration_storage_url_postgresql() -> None:
    cfg = Configuration(postgresql=PostgresqlModel(host="db.example", password="pw"))
    assert cfg.storage_url == "postgresql+asyncpg://orchestrator:pw@db.example:5432/orchestrator"


def test_configuration_storage_url_sqlite_memory() -> None:
    cfg = Configuration(storage=StorageModel(backend="sqlite"))
    assert cfg.storage_url == "sqlite+aiosqlite:///:memory:"
