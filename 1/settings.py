from pydantic import SecretStr
from pydantic import ConfigDict
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    PG_USER: str
    PG_PASSWORD: SecretStr
    PG_DATABASE: str
    PG_HOST: str
    PG_PORT: int
    HAS_PGBOUNCER: bool = False

    model_config = ConfigDict(
        env_file=".env",  # type:ignore
    )

    @property
    def PG_DSN(self) -> str:
        return (
            f"postgresql://{self.PG_USER}:{self.PG_PASSWORD.get_secret_value()}"
            f"@{self.PG_HOST}:{self.PG_PORT}/{self.PG_DATABASE}"
        )


settings = Settings()  # type:ignore
