"""Версии контрактов оркестратора (см. docs/CONTRACTS.md)."""

CURRENT_MESSAGE_VERSION = "1.0"
SUPPORTED_MESSAGE_VERSIONS: frozenset[str] = frozenset({"1.0"})

RUNTIME_VERSION = "1.0"
COMPILED_GRAPH_VERSION = "1.0"


def is_supported_message_version(version: str) -> bool:
    """Поддерживается ли версия wire-сообщения."""
    return version in SUPPORTED_MESSAGE_VERSIONS
