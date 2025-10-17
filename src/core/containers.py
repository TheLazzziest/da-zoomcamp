from dependency_injector import containers, providers

from src.sources.nyc import nyc


class Container(containers.DeclarativeContainer):
    """A DI container for the application."""

    config: providers.Configuration = providers.Configuration()

    nyc_source = providers.Factory(nyc.factory)
