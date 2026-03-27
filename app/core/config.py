from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Yastubo FastAPI"
    app_env: str = "dev"
    app_debug: bool = True
    api_prefix_v1: str = "/api/v1"
    customer_api_prefix: str = "/api/customer"

    db_host: str = Field(default="localhost", alias="DB_HOST")
    db_port: int = Field(default=3306, alias="DB_PORT")
    db_database: str = Field(default="gfa", alias="DB_DATABASE")
    db_username: str = Field(default="gfa", alias="DB_USERNAME")
    db_password: str = Field(default="gfa", alias="DB_PASSWORD")

    cors_origins: str = "http://127.0.0.1:8000,http://127.0.0.1:5173"
    quote_secret: str = "change-me-fastapi-quote-secret"
    jwt_secret: str = "change-me-fastapi-jwt-secret"
    jwt_algorithm: str = "HS256"
    jwt_access_token_exp_minutes: int = 60
    jwt_refresh_token_exp_minutes: int = 60 * 24 * 7
    public_api_base_url: str = ""
    auth_cookie_secure: bool = False
    auth_cookie_samesite: str = "lax"
    auth_cookie_domain: str = ""
    auth_cookie_path: str = "/"
    frontend_customer_shell_enabled: bool = True
    frontend_shell_entry_url: str = "http://127.0.0.1:5173/resources/js/app.js"
    frontend_customer_legacy_base_url: str = ""
    frontend_customer_legacy_retire_at: str = "2026-04-15"
    frontend_admin_shell_enabled: bool = True
    frontend_seller_shell_enabled: bool = True
    frontend_admin_legacy_base_url: str = ""
    frontend_seller_legacy_base_url: str = ""
    frontend_admin_legacy_retire_at: str = "2026-04-15"
    frontend_seller_legacy_retire_at: str = "2026-04-15"
    frontend_legacy_redirects_enabled: bool = True
    frontend_customer_legacy_redirect_enabled: bool = True
    frontend_admin_legacy_redirect_enabled: bool = False
    frontend_seller_legacy_redirect_enabled: bool = False

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    @property
    def sqlalchemy_url(self) -> str:
        return (
            f"mysql+pymysql://{self.db_username}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_database}?charset=utf8mb4"
        )

    @property
    def parsed_cors_origins(self) -> list[str]:
        return [origin.strip() for origin in self.cors_origins.split(",") if origin.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()
