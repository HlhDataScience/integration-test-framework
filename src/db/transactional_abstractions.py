from abc import ABC, abstractmethod
from typing import Generic, TypeVar, Any

from pydantic import BaseModel
from pydantic_settings import BaseSettings

# Save typing variables created for mypy and other linters
pydantic_config = TypeVar('pydantic_config', bound= BaseModel)
pydantic_settings_config = TypeVar('pydantic_settings_config', bound= BaseSettings)

class TransactionalFilterInterface(ABC, Generic[pydantic_config, pydantic_settings_config]):
    """
    This ABC it is an interface design to interact with the APIs
    from transaccional services to obtain information useful for testing.
    It must be subclassed, leaving the implementation details to the
    implementations.
    """

    def __init__(
            self,
            config: pydantic_config | pydantic_settings_config,
            env_config: pydantic_settings_config,
    ):
        self.config: pydantic_config | pydantic_settings_config = config
        self.env_config: pydantic_settings_config = env_config

    @abstractmethod
    def setup_http_session(self)-> None:
        """
        This method should implement the http session for the api call.
        :return:
        """

    @abstractmethod
    def filter_from_sqlite_database(self)-> tuple[int, ...] | None:
        """
        This method should implement the fetching from the sqlite database.
        :return:
        """

    @abstractmethod
    def fetch_filtered_records_from_api(self)-> tuple[dict[str, Any], ...] | dict[str, Any] | None:
        """
        This method makes calls to the SQLite database and returns tuples of ids to prepare
        the api calls to the app to test.
        :return:
        """
        raise NotImplementedError()


    @abstractmethod
    def close_connection(self):
        raise NotImplementedError()

    def __del__(self):
        self.close_connection()


