import zoneinfo
from pydantic import SecretStr
from pydantic import ConfigDict
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    GITHUB_TOKEN: SecretStr

    CLICKHOUSE_HOST: str
    CLICKHOUSE_PORT: int
    CLICKHOUSE_USER: str
    CLICKHOUSE_PASSWORD: SecretStr
    CLICKHOUSE_DATABASE: str

    model_config = ConfigDict(
        env_file=".env",  # type:ignore
    )


timezone_name = "Europe/Moscow"
timezone = zoneinfo.ZoneInfo(timezone_name)
settings = Settings()  # type:ignore
