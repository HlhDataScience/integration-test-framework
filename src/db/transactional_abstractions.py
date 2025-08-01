from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from pydantic import BaseModel
from pydantic_settings import BaseSettings
pydantic_config = TypeVar('pydantic_config', bound= BaseModel)
pydantic_settings_config = TypeVar('pydantic_settings_config', bound= BaseSettings)
class TransactionalFilterInterface(Generic[pydantic_config, pydantic_settings_config], ABC):
    """
    This ABC it is an interface desing to interact with the APIs
    from transaccional services to obtain information useful for testing.
    It must be subclassed, leaving the implementation details to the
    implementations.
    """

    def __init__(
            self,
            config: pydantic_config | pydantic_settings_config,
            env_config: pydantic_config,

    ):
        self.config: pydantic_config = config

    ...
