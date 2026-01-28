from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_dsn: str = Field(alias="DATABASE_DSN")

    statement_timeout_ms: int = Field(default=8000, alias="STATEMENT_TIMEOUT_MS")
    max_returned_rows: int = Field(default=5000, alias="MAX_RETURNED_ROWS")
    default_preview_limit: int = Field(default=200, alias="DEFAULT_PREVIEW_LIMIT")

    allowed_schemas: str = Field(default="public", alias="ALLOWED_SCHEMAS")

    def allowed_schema_set(self) -> set[str]:
        raw = (self.allowed_schemas or "").strip()
        if not raw:
            return set()
        return {s.strip() for s in raw.split(",") if s.strip()}


settings = Settings()
