from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    databricks_host: str = ""
    databricks_token: str = ""
    databricks_http_path: str = ""
    databricks_catalog: str = "main"
    databricks_schema: str = "demand_planning"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )


settings = Settings()
