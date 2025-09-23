from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    database_url: str = "postgresql://username:password@localhost/dbname"
    max_workers: int = 3
    cors_origins: list = ["*"]

    class Config:
        env_file = ".env"


def get_settings():
    return Settings()