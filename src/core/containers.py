from dependency_injector import containers, providers

from src.sources.calendar import calendar
from src.sources.nyc import nyc
from src.sources.weather import weather


class Container(containers.DeclarativeContainer):
    """A DI container for the application."""

    config: providers.Configuration = providers.Configuration()

    nyc_source = providers.Factory(nyc.factory)
    weather_source = providers.Factory(weather.factory)
    calendar_source = providers.Factory(calendar.factory)
