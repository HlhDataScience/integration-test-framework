from typing import NamedTuple, Literal
from enum import Enum

from src.transactional_filters.transactional_abstractions import TransactionalFilterInterface
from src.configmodels.transactional_config import (  # type: ignore
    AcknowledgementsEnvConfig,
    AcknowledgementsConfig,
    VirtualSessionsFilterConfig,
    VirtualSessionsEnvConfig,
    StudentsGroupsFilterConfig,
    StudentsGroupsEnvConfig,
    UnirEmailConfig,
    UnirEmailEnvConfig,
    EventsConfig,
    EventsEnvConfig,
    ExamRegistrationsConfig,
    ExamRegistrationsEnvConfig,
    ExamsConfig,
    ExamsEnvConfig,
    TeachingStartConfig,
    TeachingStartEnvConfig,
    MentorConfig,
    MentorEnvConfig,
    TFEnvConfig,
    TFEConfig,
    GradingsConfig,
    GradingsEnvConfig,
)
from src.transactional_filters.transactional_filters import (
    AcknowledgementsFilter,
    EventsFilter,
    ExamRegistrationFilter,
    ExamsFilter,
    GradingsFilter,
    MentorFilter,
    StudentsGroupsFilter,
    TeachingStartFilter,
    TFEFilter,
    UnirEmailFilter,
    VirtualSessionsFilter,
)
from pathlib import Path

ROOT_DIR = Path().resolve()
CONFIG_PATH = ROOT_DIR / "yaml_configurations" / "transactional_filter_configs" / "transactional_filters.yaml"

class RecordOrganizer(Enum):
    ACKNOWLEDGEMENT = "acknowledgement"
    EVENTS = "events"
    EXAMS_REGISTRATION = "exams_registration"
    EXAMS = "exams"
    GRADINGS = "gradings"
    MENTORS = "mentors"
    STUDENTS_GROUPS = "students_groups"
    UNIR_EMAIL = "unir_email"
    VIRTUAL_SESSIONS = "virtual_sessions"
    TFE = "tfe"
    TEACHING_START = "teaching_start"

class FilterToBeProcess(NamedTuple):
    filter_class: TransactionalFilterInterface
    method: Literal["GET", "POST"]
    import_sqlite_records: bool

# Instantiated models constants for the transactional_composer.py pipeline

ACKNOWLEDGEMENTS = FilterToBeProcess(
    filter_class=AcknowledgementsFilter(
        config=AcknowledgementsConfig.from_yaml(CONFIG_PATH),
        env_config=AcknowledgementsEnvConfig(),
        filter_category=RecordOrganizer.ACKNOWLEDGEMENT.value,
    ),
    method="POST",
    import_sqlite_records=True,
)

EVENTS = FilterToBeProcess(
    filter_class=EventsFilter(
        config=EventsConfig.from_yaml(CONFIG_PATH),
        env_config=EventsEnvConfig(),
        filter_category=RecordOrganizer.EVENTS.value,
    ),
    method="GET",
    import_sqlite_records=True,
)

EXAMS_REGISTRATION = FilterToBeProcess(
    filter_class=ExamRegistrationFilter(
        config=ExamRegistrationsConfig.from_yaml(CONFIG_PATH),
        env_config=ExamRegistrationsEnvConfig(),
        filter_category=RecordOrganizer.EXAMS_REGISTRATION.value,
    ),
    method="POST",
    import_sqlite_records=True,
)

EXAMS = FilterToBeProcess(
    filter_class=ExamsFilter(
        config=ExamsConfig.from_yaml(CONFIG_PATH),
        env_config=ExamsEnvConfig(),
        filter_category=RecordOrganizer.EXAMS.value,
    ),
    method="GET",
    import_sqlite_records=True,
)

GRADINGS = FilterToBeProcess(
    filter_class=GradingsFilter(
        config=GradingsConfig.from_yaml(CONFIG_PATH),
        env_config=GradingsEnvConfig(),
        filter_category=RecordOrganizer.GRADINGS.value,
    ),
    method="POST",
    import_sqlite_records=True,
)

MENTORS = FilterToBeProcess(
    filter_class=MentorFilter(
        config=MentorConfig.from_yaml(CONFIG_PATH),
        env_config=MentorEnvConfig(),
        filter_category=RecordOrganizer.MENTORS.value,
    ),
    method="GET",
    import_sqlite_records=False,
)

STUDENTS_GROUPS = FilterToBeProcess(
    filter_class=StudentsGroupsFilter(
        config=StudentsGroupsFilterConfig.from_yaml(CONFIG_PATH),
        env_config=StudentsGroupsEnvConfig(),
        filter_category=RecordOrganizer.STUDENTS_GROUPS.value,
    ),
    method="GET",
    import_sqlite_records=True,
)

UNIR_EMAIL = FilterToBeProcess(
    filter_class=UnirEmailFilter(
        config=UnirEmailConfig.from_yaml(CONFIG_PATH),
        env_config=UnirEmailEnvConfig(),
        filter_category=RecordOrganizer.UNIR_EMAIL.value,
    ),
    method="POST",
    import_sqlite_records=False,
)

VIRTUAL_SESSIONS = FilterToBeProcess(
    filter_class=VirtualSessionsFilter(
        config=VirtualSessionsFilterConfig.from_yaml(CONFIG_PATH),
        env_config=VirtualSessionsEnvConfig(),
        filter_category=RecordOrganizer.VIRTUAL_SESSIONS.value,
    ),
    method="GET",
    import_sqlite_records=True,
)

TFE = FilterToBeProcess(
    filter_class=TFEFilter(
        config=TFEConfig.from_yaml(CONFIG_PATH),
        env_config=TFEnvConfig(),
        filter_category=RecordOrganizer.TFE.value,
    ),
    method="POST",
    import_sqlite_records=True,
)

TEACHING_START = FilterToBeProcess(
    filter_class=TeachingStartFilter(
        config=TeachingStartConfig.from_yaml(CONFIG_PATH),
        env_config=TeachingStartEnvConfig(),
        filter_category=RecordOrganizer.TEACHING_START.value,
    ),
    method="GET",
    import_sqlite_records=False,
)