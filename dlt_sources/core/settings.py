from functools import lru_cache
from logging import INFO, _levelToName, getLevelName
from typing import Annotated

from pydantic import Field, constr
from pydantic_settings import BaseSettings, SettingsConfigDict


class LoggingSettings(BaseSettings):
    log_level: Annotated[
        str,
        constr(
            strip_whitespace=True,
            to_upper=True,
            pattern=rf"^({'|'.join(_levelToName.values())})$",
        ),
    ] = getLevelName(INFO)

    model_config = SettingsConfigDict(env_prefix="logging")


class HTTPSettings(BaseSettings):
    timeout: float = Field(
        default=60.0, description="Default timeout (seconds) for HTTP requests"
    )

    model_config = SettingsConfigDict(env_prefix="http")


class ProjectSettings(BaseSettings):
    environment: str = Field(
        default="production", description="A environment which the project is run"
    )

    logging: LoggingSettings = LoggingSettings()
    http: HTTPSettings = HTTPSettings()

    model_config = SettingsConfigDict(
        env_prefix="da_",
        env_nested_delimiter="__",
        case_sensitive=False,
        extra="ignore",  # @TODO: Remove after stabilization
        nested_model_default_partial_update=True,
        frozen=True,
    )

    @property
    def is_production(self) -> bool:
        """A property to check if the environment is production."""
        return self.environment == "production"


@lru_cache(maxsize=1)
def get_settings() -> ProjectSettings:
    """Return the process-wide settings dependency (single cached instance)."""
    return ProjectSettings()
