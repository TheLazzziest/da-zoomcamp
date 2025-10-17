from logging import _levelToName, getLevelName, INFO
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



class ProjectSettings(BaseSettings):
    environment: str = Field(
        default="production", description="A environment which the project is run"
    )

    logging: LoggingSettings = LoggingSettings()

    model_config = SettingsConfigDict(
        env_prefix="da_",
        env_nested_delimiter="__",
        case_sensitive=False,
        extra="ignore",  # @TODO: Remove after stabilization
        nested_model_default_partial_update=True,
        frozen=True
    )

    @property
    def is_production(self) -> bool:
        """A property to check if the environment is production."""
        return self.environment == "production"
