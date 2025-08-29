from typing import TypeVar
from pydantic import BaseModel
from pydantic_settings import BaseSettings

pydantic_config = TypeVar('pydantic_config', bound= BaseModel)
pydantic_settings_config = TypeVar('pydantic_settings_config', bound= BaseSettings)