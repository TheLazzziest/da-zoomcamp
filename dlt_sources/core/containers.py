from dependency_injector import containers, providers

from dlt_sources.core.settings import get_settings
from dlt_sources.sources.calendar import calendar
from dlt_sources.sources.nyc import nyc
from dlt_sources.sources.nyc311 import nyc311
from dlt_sources.sources.tlc_lookup import tlc_lookup
from dlt_sources.sources.weather import weather


class Container(containers.DeclarativeContainer):
    """A DI container for the application."""

    settings = providers.Singleton(get_settings)

    config: providers.Configuration = providers.Configuration()

    nyc_source = providers.Factory(nyc.factory)
    weather_source = providers.Factory(weather.factory)
    calendar_source = providers.Factory(calendar.factory)
    tlc_lookup_source = providers.Factory(tlc_lookup.factory)
    nyc311_source = providers.Factory(nyc311.factory)
