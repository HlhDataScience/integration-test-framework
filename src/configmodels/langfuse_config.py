from pathlib import Path
from typing import Any
import yaml

from pydantic import BaseModel

class LangfuseConfig(BaseModel):
    """
    Base class for Langfuse configuration.
    """
    client_parameters: dict[str, str]
    public_key: str
    private_key: str
    url: str
    fetch_filters: dict[str, str]

    @classmethod
    def from_yaml(cls, path: Path) -> "LangfuseConfig":
        with path.open("r", encoding="utf-8") as f:
            data: dict[str, Any] = yaml.load(f, Loader=yaml.SafeLoader)

        return cls(**data)