from dishka import AsyncContainer, make_async_container

from orchestrator.app.dependency_injection.providers import (
    ConfigurationProvider,
    ServiceProvider,
    StoreProvider,
)


def build_container() -> AsyncContainer:
    """Собрать APP-контейнер dishka со всеми провайдерами оркестратора."""
    return make_async_container(ConfigurationProvider(), StoreProvider(), ServiceProvider())
