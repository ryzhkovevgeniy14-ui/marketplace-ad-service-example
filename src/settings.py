from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    postgres_host: str
    postgres_database_name: str
    postgres_password: str
    postgres_port: int
    postgres_username: str

    jwt_secret: str = "change-me"
    jwt_algorithm: str = "HS256"
    kafka_bootstrap_servers: str = Field(
        default="localhost:9092",
        validation_alias=AliasChoices(
            "KAFKA_BROKERS",
            "kafka_bootstrap_servers",
        ),
    )
    kafka_topic_ads: str = Field(
        default="ads",
        validation_alias=AliasChoices(
            "KAFKA_TOPIC_MARKETPLACE_ADS",
            "kafka_topic_ads",
        ),
    )
    auth_service_url: str = "http://localhost:8000"

    @property
    def database_url(self) -> str:
        return (
            f"postgresql+asyncpg://"
            f"{self.postgres_username}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}"
            f"/{self.postgres_database_name}"
        )
