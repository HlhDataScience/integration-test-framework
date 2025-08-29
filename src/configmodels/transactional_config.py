from pathlib import Path
from typing import Any, ClassVar

import yaml  # type: ignore
from pydantic import Field, BaseModel, field_validator  # type: ignore
from pydantic_settings import SettingsConfigDict, BaseSettings  # type: ignore

<<<<<<< HEAD
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
=======
#CONSTANTS
ROOT_DIR = Path(__file__).resolve().parents[2]
ENV_PATH = ROOT_DIR / ".env.transactional"
GENERAL_CONFIG_QUERIES_PATH = ROOT_DIR / "virtual_sessions.yaml"



# YAML CONFIGURATIONS
class VirtualSessionsFilterConfig(BaseModel):
    absolute_db_path: str
    sqlite_filter_query: str = Field(..., description="The SQL query to filter the records for the Virtual Sessions API.")
    body_parameters: dict[str, str]
>>>>>>> c117fbf (added the work from the last week)

    @classmethod
    def from_yaml(cls, path: Path) -> "VirtualSessionsFilterConfig":

        with path.open("r", encoding="utf-8") as f:
            data: dict[str, Any] = yaml.load(f, Loader=yaml.SafeLoader)

<<<<<<< HEAD
        return cls(**data)


class StudentsGroupsEnvConfig(BaseSettings):

    model_config = SettingsConfigDict(env_file=ROOT_DIR / ".env.student_groups_filter")

    API_URL: str
    BEARER_TOKEN: str

class StudentsGroupsFilterConfig(BaseModel):

    sqlite_filter_query: str = Field(..., description="The SQL query to filter the records for the Students Groups API.")
=======
        return cls(**data["virtual_sessions"])


class StudentsGroupsFilterConfig(BaseModel):
    absolute_db_path: str
    sqlite_filter_query: str = Field(..., description="The SQL query to filter the records for the Virtual Sessions API.")
    body_parameters: dict[str, str]
>>>>>>> c117fbf (added the work from the last week)

    @classmethod
    def from_yaml(cls, path: Path) -> "StudentsGroupsFilterConfig":
        with path.open("r", encoding="utf-8") as f:
            data: dict[str, Any] = yaml.load(f, Loader=yaml.SafeLoader)

<<<<<<< HEAD
        return cls(**data)

=======
        return cls(**data["student_groups"])
class UnirEmailConfig(BaseModel):
    absolute_db_path: str
    sqlite_filter_query: str

    @classmethod
    def from_yaml(cls, path: Path) -> "UnirEmailConfig":
        with path.open("r", encoding="utf-8") as f:
            data: dict[str, Any] = yaml.load(f, Loader=yaml.SafeLoader)
            return cls(**data["unir_email"])

class EventsConfig(BaseModel):
    absolute_db_path: str
    sqlite_filter_query: str


    @classmethod
    def from_yaml(cls, path: Path) -> "EventsConfig":
        with path.open("r", encoding="utf-8") as f:
            data: dict[str, Any] = yaml.load(f, Loader=yaml.SafeLoader)
            return cls(**data["events"])

class ExamRegistrationsConfig(BaseModel):
    absolute_db_path: str
    sqlite_filter_query: str
    body_parameters: dict[str, Any]

    @classmethod
    def from_yaml(cls, path: Path) -> "ExamRegistrationsConfig":
        with path.open("r", encoding="utf-8") as f:
            data: dict[str, Any] = yaml.load(f, Loader=yaml.SafeLoader)

        return cls(**data["exam_registrations"])

class ExamsConfig(BaseModel):
    absolute_db_path: str
    sqlite_filter_query: str
    body_parameters: str

    @classmethod
    def from_yaml(cls, path: Path) -> "ExamsConfig":
        with path.open("r", encoding="utf-8") as f:
            data: dict[str, Any] = yaml.load(f, Loader=yaml.SafeLoader)

        return cls(**data["exams"])

class TeachingStartConfig(BaseModel):
    absolute_db_path: str
    sqlite_filter_query: str

    @classmethod
    def from_yaml(cls, path: Path) -> "TeachingStartConfig":
        with path.open("r", encoding="utf-8") as f:
            data: dict[str, Any] = yaml.load(f, Loader=yaml.SafeLoader)

        return cls(**data["teaching_start"])

class MentorConfig(BaseModel):
    absolute_db_path: str
    sqlite_filter_query: str

    @classmethod
    def from_yaml(cls, path: Path) -> "MentorConfig":
        with path.open("r", encoding="utf-8") as f:
            data: dict[str, Any] = yaml.load(f, Loader=yaml.SafeLoader)

        return cls(**data["mentors"])

class AcknowledgementsConfig(BaseModel):
    absolute_db_path: str
    sqlite_filter_query: str
    body_parameters: dict[str, Any]

    @classmethod
    def from_yaml(cls, path: Path) -> "AcknowledgementsConfig":
        with path.open("r", encoding="utf-8") as f:
            data: dict[str, Any] = yaml.load(f, Loader=yaml.SafeLoader)

        return cls(**data["acknowledgements"])

class TFEConfig(BaseModel):
    absolute_db_path: str
    sqlite_filter_query: str
    body_parameters: dict[str, Any]

    @classmethod
    def from_yaml(cls, path: Path) -> "TFEConfig":
        with path.open("r", encoding="utf-8") as f:
            data: dict[str, Any] = yaml.load(f, Loader=yaml.SafeLoader)

        return cls(**data["tfe"])

class GradingsConfig(BaseModel):
    absolute_db_path: str
    sqlite_filter_query: str
    body_parameters: dict[str, Any]

    @classmethod
    def from_yaml(cls, path: Path) -> "GradingsConfig":
        with path.open("r", encoding="utf-8") as f:
            data: dict[str, Any] = yaml.load(f, Loader=yaml.SafeLoader)

        return cls(**data["qualifications"])

# ENVIRONMENTAL VARIABLES CONFIGS
class StudentsGroupsEnvConfig(BaseSettings):

    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(env_file=str(ENV_PATH), extra="ignore")

    STUDENTS_GROUPS_API_URL: str
    STUDENTS_GROUPS_API_KEY: str

class VirtualSessionsEnvConfig(BaseSettings):

    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(env_file=str(ENV_PATH), extra="ignore")

    VIRTUAL_SESSIONS_API_URL: str
    VIRTUAL_SESSIONS_BEARER_TOKEN: str

class UnirEmailEnvConfig(BaseSettings):
    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(env_file=str(ENV_PATH), extra="ignore")

    UNIR_EMAIL_API_URL: str
    UNIR_EMAIL_API_KEY: str

class EventsEnvConfig(BaseSettings):
    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(env_file=str(ENV_PATH), extra="ignore")

    EVENTS_API_URL: str
    EVENTS_BEARER_TOKEN: str

class ExamRegistrationsEnvConfig(BaseSettings):
    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(env_file=str(ENV_PATH), extra="ignore")
    EXAM_REGISTRATION_API_URL: str
    EXAM_REGISTRATION_API_KEY: str

class ExamsEnvConfig(BaseSettings):
    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(env_file=str(ENV_PATH), extra="ignore")
    EXAMS_API_URL: str
    EXAMS_API_KEY: str

class TeachingStartEnvConfig(BaseSettings):
    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(env_file=str(ENV_PATH), extra="ignore")
    TEACHING_START_API_URL: str
    TEACHING_START_API_KEY: str

class MentorEnvConfig(BaseSettings):
    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(env_file=str(ENV_PATH), extra="ignore")
    MENTORS_API_URL: str
    MENTORS_API_KEY: str

class AcknowledgementsEnvConfig(BaseSettings):
    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(env_file=str(ENV_PATH), extra="ignore")
    ACKNOWLEDGEMENTS_API_URL: str
    ACKNOWLEDGEMENTS_API_KEY: str
    ACKNOWLEDGEMENTS_PLAN_ID_URL: str

class TFEnvConfig(BaseSettings):
    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(env_file=str(ENV_PATH), extra="ignore")
    TFE_API_URL: str
    TFE_API_KEY: str

class GradingsEnvConfig(BaseSettings):
    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(env_file=str(ENV_PATH), extra="ignore")
    QUALIFICATIONS_API_URL: str
    QUALIFICATIONS_API_KEY: str
    QUALIFICATIONS_PLAN_ID_URL: str
>>>>>>> c117fbf (added the work from the last week)
