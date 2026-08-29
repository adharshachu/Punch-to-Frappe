"""Storage backend factory."""
from config import settings
from storage.event_store import EventStore
from storage.postgres_event_store import PostgresEventStore


def create_event_store():
    """Create the configured event store backend."""
    if settings.STORAGE_BACKEND == "postgres":
        return PostgresEventStore(
            settings.POSTGRES_DSN,
            max_connections=settings.POSTGRES_MAX_CONNECTIONS,
        )
    if settings.STORAGE_BACKEND == "sqlite":
        return EventStore(settings.STORE_PATH)
    raise ValueError(
        f"Unsupported STORAGE_BACKEND={settings.STORAGE_BACKEND!r}. "
        "Use 'postgres' or 'sqlite'."
    )
