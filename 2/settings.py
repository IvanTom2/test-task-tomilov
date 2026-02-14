import zoneinfo
from pydantic import SecretStr
from pydantic import ConfigDict
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    GITHUB_TOKEN: SecretStr
    model_config = ConfigDict(
        env_file=".env",  # type:ignore
    )


timezone_name = "Europe/Moscow"
timezone = zoneinfo.ZoneInfo(timezone_name)
settings = Settings()  # type:ignore
