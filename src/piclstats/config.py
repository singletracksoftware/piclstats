from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql+psycopg://localhost:5432/piclstats"
    scrape_delay_seconds: float = 1.5
    request_timeout_seconds: float = 30.0
    log_level: str = "INFO"

    model_config = {"env_prefix": "PICLSTATS_", "env_file": ".env"}


settings = Settings()
