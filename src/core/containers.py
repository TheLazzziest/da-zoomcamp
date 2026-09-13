from dependency_injector import containers, providers

from src.sources.calendar import calendar
from src.sources.nyc import nyc
from src.sources.nyc311 import nyc311
from src.sources.tlc_lookup import tlc_lookup
from src.sources.weather import weather


class Container(containers.DeclarativeContainer):
    """A DI container for the application."""

    config: providers.Configuration = providers.Configuration()

    nyc_source = providers.Factory(nyc.factory)
    weather_source = providers.Factory(weather.factory)
    calendar_source = providers.Factory(calendar.factory)
    tlc_lookup_source = providers.Factory(tlc_lookup.factory)
    nyc311_source = providers.Factory(nyc311.factory)
