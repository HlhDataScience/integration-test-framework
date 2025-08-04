from pathlib import Path
from typing import Any, ClassVar

import yaml  # type: ignore
from pydantic import Field, BaseModel, field_validator  # type: ignore
from pydantic_settings import SettingsConfigDict, BaseSettings  # type: ignore

ROOT_DIR = Path(__file__).resolve().parents[2]

VIRTUAL_SESSIONS_CONFIG_QUERIES_PATH = ROOT_DIR / "virtual_sessions.yaml"
STUDENTS_GROUPS_CONFIG_QUERIES_PATH = ROOT_DIR / "students_groups.yaml"

class VirtualSessionsEnvConfig(BaseSettings):

    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(env_file=ROOT_DIR / ".env.virtual_sessions_filter")

    API_URL: str
    BEARER_TOKEN: str


class VirtualSessionsFilterConfig(BaseModel):

    sqlite_filter_query: str = Field(..., description="The SQL query to filter the records for the Virtual Sessions API.")
    body_parameters = dict[str, str]

    @classmethod
    def from_yaml(cls, path: Path) -> "VirtualSessionsFilterConfig":

        with path.open("r", encoding="utf-8") as f:
            data: dict[str, Any] = yaml.load(f, Loader=yaml.SafeLoader)

        return cls(**data)


class StudentsGroupsEnvConfig(BaseSettings):

    model_config = SettingsConfigDict(env_file=ROOT_DIR / ".env.student_groups_filter")

    API_URL: str
    BEARER_TOKEN: str

class StudentsGroupsFilterConfig(BaseModel):

    sqlite_filter_query: str = Field(..., description="The SQL query to filter the records for the Students Groups API.")

    @classmethod
    def from_yaml(cls, path: Path) -> "StudentsGroupsFilterConfig":
        with path.open("r", encoding="utf-8") as f:
            data: dict[str, Any] = yaml.load(f, Loader=yaml.SafeLoader)

        return cls(**data)

