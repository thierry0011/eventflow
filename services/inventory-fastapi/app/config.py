"""
Typed settings with pydantic-settings.

Compare with Django's settings.py, which is a module of module-level globals
validated by nothing. Here:

    * every value is TYPE-CHECKED at startup (`pool_size: int` means a stray
      "ten" in the environment crashes the process immediately, not at the
      first database call under load)
    * defaults are declared next to the type
    * a missing required value is a clear ValidationError naming the field
    * your editor autocompletes `settings.kafka_bootstrap_servers`

`@lru_cache` makes `get_settings()` a singleton, so the environment is read
once per process and the same object is injected everywhere.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore",
        case_sensitive=False,
    )

    service_name: str = "inventory"
    environment: str = "local"
    log_level: str = "INFO"

    # --- database ---
    postgres_user: str = "eventflow"
    postgres_password: str = "eventflow"
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "inventory_db"

    db_pool_size: int = 10
    db_max_overflow: int = 5
    db_pool_timeout: int = 10
    db_echo: bool = False          # True prints every SQL statement — superb
                                   # for learning, catastrophic for prod logs

    # --- redis ---
    redis_url: str = "redis://localhost:6379"
    cache_ttl_seconds: int = 60

    # --- kafka ---
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_orders: str = "orders.events"
    kafka_topic_inventory: str = "inventory.events"
    consumer_group: str = "inventory-service"

    # --- domain ---
    reservation_ttl_seconds: int = 900
    outbox_poll_interval_seconds: int = 2
    outbox_batch_size: int = 100

    # --- auth ---
    jwt_secret: str = Field(default="dev-only-shared-jwt-secret-change-me")
    jwt_algorithm: str = "HS256"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def database_url(self) -> str:
        # postgresql+asyncpg:// — the driver is part of the URL. Using the sync
        # `postgresql://` here gives a confusing "greenlet_spawn has not been
        # called" error later, which is SQLAlchemy's way of saying "you asked
        # an async session to use a blocking driver".
        return (f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
                f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}")

    @computed_field  # type: ignore[prop-decorator]
    @property
    def sync_database_url(self) -> str:
        """Alembic runs synchronously; it needs the non-async URL."""
        return (f"postgresql://{self.postgres_user}:{self.postgres_password}"
                f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}")


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
