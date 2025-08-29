
"""
Transactional Filter Implementations Module

This module contains concrete implementations of the TransactionalFilterInterface
for various educational and administrative APIs. Each class is specialized for
a specific service and implements the required hook methods to handle API-specific
request preparation and data processing.

The implementations cover various educational services including:
- Virtual sessions management
- Student groups administration
- Email services
- Event management
- Exam registrations and management
- Teaching coordination
- Mentorship programs
- Academic acknowledgements
- Final degree projects (TFE)
- Academic qualifications

Classes:
    VirtualSessionsFilter: Handles virtual session API interactions
    StudentsGroupsFilter: Manages student group data retrieval
    UnirEmailFilter: Interfaces with email services
    EventsFilter: Processes educational event data
    ExamRegistrationFilter: Handles exam registration workflows
    ExamsFilter: Manages exam-related data
    TeachingStartFilter: Coordinates teaching start processes
    MentorFilter: Manages mentorship program data
    AcknowledgementsFilter: Handles academic acknowledgements
    TFEFilter: Manages final degree project data
    GradingsFilter: Processes academic qualification data

Dependencies:
    - datetime: Calendar and time utilities.
    - sqlite3: Database connectivity
    - typing: Type annotations
    - requests: HTTP client functionality
    - configmodels: Configuration management
    - transactional_filters: Base interface
"""
from datetime import datetime, UTC, timedelta  # type: ignore
import sqlite3
import json
from time import sleep
from typing import Any, Literal, override  # type: ignore
import requests  # type: ignore

from configmodels.transactional_config import (  # type: ignore
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
from configmodels.config_types import pydantic_config, pydantic_settings_config  # type: ignore
from transactional_filters.transactional_abstractions import TransactionalFilterInterface  # type: ignore


class VirtualSessionsFilter(TransactionalFilterInterface[VirtualSessionsFilterConfig, VirtualSessionsEnvConfig]):
    """
    Filter implementation for virtual sessions API.

    This class interacts with virtual session APIs to retrieve session data for students.
    It uses Bearer token authentication and supports POST requests with JSON payloads
    for filtering virtual session information.

    The filter is designed to work with educational virtual session management systems,
    allowing for the retrieval of session data based on student IDs extracted from
    the SQLite database.

    Authentication:
        Uses Bearer token authentication in the Authorization header

    HTTP Method:
        Primarily designed for POST requests with JSON body parameters

    URL Pattern:
        API URLs contain {id} placeholder that gets replaced with user_id
    """

    def __init__(self,
                 config: pydantic_config | pydantic_settings_config,
                 filter_category: str,
                 env_config: pydantic_settings_config):
        """
        Initialize the VirtualSessionsFilter.

        Sets up the filter with virtual sessions-specific configuration,
        including Bearer token authentication and JSON content type headers.

        Args:
            config: Configuration object containing database and API settings
            env_config: Environment configuration with API endpoints and Bearer token

        Attributes:
            __config: Private configuration instance
            __env_config: Private environment configuration instance
            __session: HTTP session for API requests
            __connection: SQLite database connection
            __headers: Headers including Bearer token and JSON content type
        """
        self.__config = config
        self.__env_config = env_config
        self.__session = requests.Session()
        self.__connection = sqlite3.connect(self.__config.absolute_db_path)
        self.__filter_category = filter_category
        self.__headers = {
            "accept": "*/*",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.__env_config.BEARER_TOKEN}"
        }

        super().__init__(config=self.__config, env_config=self.__env_config, session=self.__session,
                         headers=self.__headers, filter_category=self.__filter_category)

    def _prepare_request_kwargs(self, user_id: int | str | Any) -> dict[str, Any]:
        """
        Prepare request parameters for virtual sessions API calls.

        Constructs the complete API URL by replacing the {id} placeholder
        with the provided user_id and includes body parameters from configuration.

        Args:
            user_id: Student ID to retrieve virtual sessions for

        Returns:
            dict[str, Any]: Request parameters including URL and JSON body
                          Format: {"url": complete_url, "json": body_params}
        """
        complete_api_url = self.__env_config.API_URL.replace("{id}", str(user_id))
        return {
            "url": complete_api_url,
            "json": self.__config.body_parameters
        }

class StudentsGroupsFilter(TransactionalFilterInterface[StudentsGroupsFilterConfig, StudentsGroupsEnvConfig]):
    """
    Filter implementation for student groups API.

    This class manages retrieval of student group information through API calls.
    It uses API key authentication and is designed for GET requests to retrieve
    group membership and related data for students.

    Authentication:
        Uses X-Api-Key header for API authentication

    HTTP Method:
        Primarily designed for GET requests

    URL Pattern:
        API URLs contain {id} placeholder for student identification
    """

    def __init__(self,
                 config: pydantic_config | pydantic_settings_config,
                 filter_category: str,
                 env_config: pydantic_settings_config):
        """
        Initialize the StudentsGroupsFilter.

        Sets up the filter for student groups API with API key authentication.

        Args:
            config: Configuration object with database and API settings
            env_config: Environment configuration with API URL and key

        Attributes:
            __config: Private configuration instance
            __env_config: Private environment configuration
            __session: HTTP session for requests
            __connection: SQLite database connection
            __headers: Headers with API key authentication
        """
        self.__config = config
        self.__env_config = env_config
        self.__filter_category = filter_category
        self.__session = requests.Session()
        self.__connection = sqlite3.connect(self.__config.absolute_db_path)
        self.__headers = {'X-Api-Key': self.__env_config.STUDENTS_GROUPS_API_KEY}
        super().__init__(config=self.__config, env_config=self.__env_config, session=self.__session,
                         headers=self.__headers,filter_category=self.__filter_category)

    def _prepare_request_kwargs(self, user_id: int | str | Any) -> dict[str, Any]:
        """
        Prepare request parameters for student groups API calls.

        Constructs the complete URL by replacing the {id} placeholder with
        the user ID. This filter uses GET requests without body parameters.

        Args:
            user_id: Student ID to retrieve group information for

        Returns:
            dict[str, Any]: Request parameters with complete URL
                          Format: {"url": complete_url}
        """
        complete_url = self.__env_config.STUDENTS_GROUPS_API_URL.replace("{id}", str(user_id))
        return {
            "url": complete_url
        }

class UnirEmailFilter(TransactionalFilterInterface[UnirEmailConfig, UnirEmailEnvConfig]):
    """
    Filter implementation for UNIR email services API.

    Handles email-related data retrieval for students through the UNIR email
    system API. Uses API key authentication and GET requests to fetch
    email information and related communication data.

    Authentication:
        Uses X-Api-Key header with UNIR_EMAIL_API_KEY

    HTTP Method:
        GET requests for email data retrieval

    URL Pattern:
        URLs contain {id} placeholder for user identification
    """

    def __init__(self,
                 config: pydantic_config | pydantic_settings_config,
                 filter_category: str,
                 env_config: pydantic_settings_config):
        """
        Initialize the UnirEmailFilter.

        Sets up email services API filter with specific API key authentication
        for UNIR email system access.

        Args:
            config: Configuration with database and API settings
            env_config: Environment config with email API credentials

        Attributes:
            __config: Private configuration instance
            __env_config: Private environment configuration
            __session: HTTP session for API calls
            __connection: Database connection
            __headers: Headers with email API key
        """
        self.__config = config
        self.__env_config = env_config
        self.__session = requests.Session()
        self.__filter_category = filter_category
        self.__connection = sqlite3.connect(self.__config.absolute_db_path)
        self.__headers = {'X-Api-Key': self.__env_config.UNIR_EMAIL_API_KEY}
        super().__init__(config=self.__config, env_config=self.__env_config, session=self.__session,
                         headers=self.__headers,filter_category=self.__filter_category)

    def _prepare_request_kwargs(self, user_id: int | str | Any) -> dict[str, Any]:
        """
        Prepare request parameters for UNIR email API calls.

        Creates the complete API URL by substituting the user ID into
        the URL template for email data retrieval.

        Args:
            user_id: User ID for email data lookup

        Returns:
            dict[str, Any]: Request parameters with complete URL
        """
        complete_url = self.__env_config.UNIR_EMAIL_API_URL.replace("{id}", str(user_id))
        return {
            "url": complete_url
        }

class EventsFilter(TransactionalFilterInterface[EventsConfig, EventsEnvConfig]):
    """
    Filter implementation for educational events API.

    Manages retrieval of educational event data including seminars, workshops,
    conferences, and other academic activities. Uses Bearer token authentication
    for secure access to event management systems.

    Authentication:
        Uses Bearer header with EVENTS_BEARER_TOKEN

    HTTP Method:
        GET requests for event data

    URL Pattern:
        Event API URLs with {id} placeholder for user-specific events
    """

    def __init__(self,
                 config: pydantic_config | pydantic_settings_config,
                 filter_category: str,
                 env_config: pydantic_settings_config):
        """
        Initialize the EventsFilter.

        Configures the filter for educational events API access with
        Bearer token authentication.

        Args:
            config: Configuration for database and API settings
            env_config: Environment configuration with events API credentials

        Attributes:
            __config: Private configuration instance
            __env_config: Private environment configuration
            __session: HTTP session for requests
            __connection: Database connection
            __headers: Headers with Bearer token for events API
        """
        self.__config = config
        self.__env_config = env_config
        self.__session = requests.Session()
        self.__filter_category = filter_category
        self.__connection = sqlite3.connect(self.__config.absolute_db_path)
        self.__headers = {'Bearer': self.__env_config.EVENTS_BEARER_TOKEN}
        super().__init__(config=self.__config, env_config=self.__env_config, session=self.__session,
                         headers=self.__headers,filter_category=self.__filter_category)

    def _prepare_request_kwargs(self, user_id: int | str | Any) -> dict[str, Any]:
        """
        Prepare request parameters for events API calls.

        Constructs the API URL for event data retrieval by replacing
        the ID placeholder with the user identifier.

        Args:
            user_id: User ID to get event information for

        Returns:
            dict[str, Any]: Request parameters with complete events API URL
        """
        end_date = (datetime.now(UTC) + timedelta(weeks=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        complete_url = self.__env_config.EVENTS_API_URL.replace("{id}", str(user_id)).replace("{end_date}", end_date)
        return {
            "url": complete_url
        }

class ExamRegistrationFilter(TransactionalFilterInterface[ExamRegistrationsConfig, ExamRegistrationsEnvConfig]):
    """
    Filter implementation for exam registration API.

    Handles exam registration processes and data retrieval for students.
    Uses specialized authentication token for student crosscutting services
    and supports POST requests with body parameters for registration operations.

    Authentication:
        Uses Student-crosscutting-token header for authentication

    HTTP Method:
        POST requests with JSON body for registration data

    API Features:
        Fixed endpoint URL without user ID parameterization
        Body parameters include user-specific registration data
    """

    def __init__(self,
                 config: pydantic_config | pydantic_settings_config,
                 filter_category: str,
                 env_config: pydantic_settings_config):
        """
        Initialize the ExamRegistrationFilter.

        Sets up exam registration API filter with student crosscutting
        service authentication for registration operations.

        Args:
            config: Configuration with registration settings and parameters
            env_config: Environment config with registration API credentials

        Attributes:
            __config: Private configuration instance
            __env_config: Private environment configuration
            __session: HTTP session for API requests
            __connection: Database connection
            __headers: Headers with student crosscutting token
        """
        self.__config = config
        self.__env_config = env_config
        self.__session = requests.Session()
        self.__filter_category = filter_category
        self.__connection = sqlite3.connect(self.__config.absolute_db_path)
        self.__headers = {'Student-crosscutting-token': self.__env_config.EXAM_REGISTRATION_API_KEY}
        super().__init__(config=self.__config, env_config=self.__env_config, session=self.__session,
                         headers=self.__headers,filter_category=self.__filter_category)

    def _prepare_request_kwargs(self, user_id: int | str | Any) -> dict[str, Any]:
        """
        Prepare request parameters for exam registration API calls.

        Uses a fixed API endpoint with body parameters from configuration.
        The user_id is typically included in the body parameters rather
        than the URL for registration operations.

        Args:
            user_id: Student ID for exam registration (used in body parameters)

        Returns:
            dict[str, Any]: Request parameters with URL and JSON body
                          Format: {"url": registration_url, "json": body_params}
        """
        body_params = self.__config.body_parameters
        body_params["idAlumno"] = user_id
        complete_url = self.__env_config.EXAM_REGISTRATION_API_URL
        return {
            "url": complete_url,
            "json": body_params

        }

class ExamsFilter(TransactionalFilterInterface[ExamsConfig, ExamsEnvConfig]):
    """
    Filter implementation for exams management API.

    Handles examination data retrieval and management operations.
    Provides access to exam schedules, details, and related information
    through API key authenticated requests.

    Authentication:
        Uses X-Api-Key header with EXAMS_API_KEY

    HTTP Method:
        POST requests with JSON body parameters

    API Features:
        Fixed endpoint for exam data operations
        Configurable body parameters for different exam queries
    """

    def __init__(self,
                 config: pydantic_config | pydantic_settings_config,
                 filter_category: str,
                 env_config: pydantic_settings_config):
        """
        Initialize the ExamsFilter.

        Configures the exam management API filter with appropriate
        authentication and connection settings.

        Args:
            config: Configuration with exam query parameters
            env_config: Environment configuration with exam API credentials

        Attributes:
            __config: Private configuration instance
            __env_config: Private environment configuration
            __session: HTTP session for API communication
            __connection: SQLite database connection
            __headers: Headers with exams API key
        """
        self.__config = config
        self.__env_config = env_config
        self.__session = requests.Session()
        self.__filter_category = filter_category
        self.__connection = sqlite3.connect(self.__config.absolute_db_path)
        self.__headers = {'X-Api-Key': self.__env_config.EXAMS_API_KEY}
        super().__init__(config=self.__config, env_config=self.__env_config, session=self.__session,
                         headers=self.__headers,filter_category=self.__filter_category)

    def _prepare_request_kwargs(self, user_id: int | str | Any) -> dict[str, Any]:
        """
        Prepare request parameters for teaching start API calls.

        Constructs the API URL for teaching coordination data by replacing
        the ID placeholder with the user identifier.

        Args:
            user_id: Staff or course ID for teaching start information

        Returns:
            dict[str, Any]: Request parameters with complete URL
        """
        complete_url = self.__env_config.EXAMS_API_URL
        body_params: str = self.__config.body_parameters.replace("{id}", user_id)
        return {
            "url": complete_url,
            "json": body_params
        }

class MentorFilter(TransactionalFilterInterface[MentorConfig, MentorEnvConfig]):
    """
    Filter implementation for mentorship program API.

    Manages mentorship program data retrieval including mentor-student
    relationships, mentoring activities, and program coordination.
    Uses API key authentication for secure access to mentoring data.

    Authentication:
        Uses X-Api-Key header with MENTORS_API_KEY

    HTTP Method:
        GET requests for mentorship data

    URL Pattern:
        Mentor API URLs with {id} placeholder for mentor/student identification
    """

    def __init__(self,
                 config: pydantic_config | pydantic_settings_config,
                 filter_category: str,
                 env_config: pydantic_settings_config):
        """
        Initialize the MentorFilter.

        Configures the mentorship program API filter with appropriate
        authentication for accessing mentor and student data.

        Args:
            config: Configuration for mentorship program settings
            env_config: Environment config with mentors API credentials

        Attributes:
            __config: Private configuration instance
            __env_config: Private environment configuration
            __session: HTTP session for API requests
            __connection: Database connection
            __headers: Headers with mentors API key
        """
        self.__config = config
        self.__env_config = env_config
        self.__session = requests.Session()
        self.__filter_category = filter_category
        self.__connection = sqlite3.connect(self.__config.absolute_db_path)
        self.__headers = {'X-Api-Key': self.__env_config.MENTORS_API_KEY}
        super().__init__(config=self.__config, env_config=self.__env_config, session=self.__session,
                         headers=self.__headers,filter_category=self.__filter_category)

    def _prepare_request_kwargs(self, user_id: int | str | Any) -> dict[str, Any]:
        """
        Prepare request parameters for mentor API calls.

        Creates the complete API URL by replacing the ID placeholder
        with the user identifier for mentorship data retrieval.

        Args:
            user_id: Mentor or student ID for mentorship information

        Returns:
            dict[str, Any]: Request parameters with complete URL
        """
        complete_url = self.__env_config.MENTORS_API_URL.replace("{id}", user_id)
        return {
            "url": complete_url,
        }

class AcknowledgementsFilter(TransactionalFilterInterface[MentorConfig, MentorEnvConfig]):
    """
    Filter implementation for academic acknowledgements API.

    Handles academic acknowledgement data including awards, recognitions,
    and formal acknowledgements for students and staff. Uses API key
    authentication and POST requests with user-specific body parameters.

    Note: This class reuses MentorConfig types but accesses acknowledgements API

    Authentication:
        Uses X-Api-Key header with ACKNOWLEDGEMENTS_API_KEY

    HTTP Method:
        POST requests with JSON body including user_id

    API Features:
        Fixed endpoint with user_id included in request body
        Configurable body parameters for acknowledgement queries
    """

    def __init__(self,
                 config: pydantic_config | pydantic_settings_config,
                 filter_category: str,
                 env_config: pydantic_settings_config):
        """
        Initialize the AcknowledgementsFilter.

        Sets up the academic acknowledgements API filter with specific
        authentication for acknowledgements service access.

        Args:
            config: Configuration with acknowledgement query parameters
            env_config: Environment config with acknowledgements API credentials

        Attributes:
            __config: Private configuration instance
            __env_config: Private environment configuration
            __session: HTTP session for API requests
            __connection: Database connection
            __headers: Headers with acknowledgements API key
        """
        self.__config = config
        self.__env_config = env_config
        self.__session = requests.Session()
        self.__filter_category = filter_category
        self.__connection = sqlite3.connect(self.__config.absolute_db_path)
        self.__headers = {
            "X-Api-Key": self.__env_config.ACKNOWLEDGEMENTS_API_KEY,
            "Content-Type": "application/json; charset=utf-8",
        }
        super().__init__(config=self.__config, env_config=self.__env_config, session=self.__session,
                         headers=self.__headers,filter_category=self.__filter_category)
    def obtain_plan_id(self, user_id:str | int | Any)-> str | None:
        """
        Obtain the academic plan ID for qualification queries.

        This method should be implemented to retrieve the appropriate
        academic plan ID based on the student's program or degree.
        The plan ID is required for complete qualification data retrieval.

        Returns:
            The academic plan identifier (implementation needed)

        Note:
            This method is currently not implemented and should be
            completed based on the specific plan identification logic
            required by the qualifications API.
        """
        try:
            response = self.__session.get(self.__env_config.ACKNOWLEDGEMENTS_PLAN_ID_URL.replace("{id}", user_id))
            response.raise_for_status()
            data = response.json()

            if not self._is_valid_response(data):
                return None

            plan_list = data.get("expedientes", [])
            if not plan_list:
                return None

            if any(p.get("idRefPlan") for p in plan_list):
                return plan_list[0].get("idRefPlan")

        except (requests.exceptions.HTTPError,
                requests.exceptions.ConnectionError,
                json.JSONDecodeError,
                ValueError,
                TypeError) as e:
            self._handle_api_error(e)
            return None
        return None

    @override
    def _fetch_filtered_records_from_api(self, user_id: str, method: Literal["GET", "POST"]) -> list[dict[
        str, Any]] | dict[str, Any] | None:
        """
        overrided method to deal with the 400 status code that sometimes this Api sends due to lack of data
        :param user_id: the id to check
        :param method: the request method to use
        :return:
        """

        try:
            request_kwargs = self._prepare_request_kwargs(user_id=user_id)

            if method == "GET":
                raise ValueError(
                    "This overrided method only deals with POST requests, as the api intended use of this particular endpoint.")
            else:
                response = self._session.post(**request_kwargs)

                if response.status_code != 200:
                    return None
                else:
                    data = response.json()
                    return self._transform_response_data(data)
        except (requests.exceptions.HTTPError,
                requests.exceptions.ConnectionError,
                json.JSONDecodeError,
                ValueError,
                TypeError) as e:
            self._handle_api_error(e)
            return None

    def _prepare_request_kwargs(self, user_id: int | str | Any) -> dict[str, Any]:
        """
        Prepare request parameters for acknowledgements API calls.

        Modifies the configured body parameters to include the user_id
        and prepares the request for acknowledgement data retrieval.

        Args:
            user_id: User ID to retrieve acknowledgement data for

        Returns:
            dict[str, Any]: Request parameters with URL and JSON body including user_id
        """
        body_params = self.__config.body_parameters
        body_params["IdIntegracionAlumno"] = user_id
        body_params["IdPlan"] = self.obtain_plan_id(user_id= user_id)
        complete_url = self.__env_config.ACKNOWLEDGEMENTS_API_URL
        return {
            "url": complete_url,
            "json": body_params
        }

    @override
    def filter_fetched_records_from_api_pipeline(self,
                                                 user_ids: list[str],
                                                 method: Literal["POST", "GET"],
                                                 sleep_time: float
                                                 ) -> list[tuple[str,dict[str, Any]]] | None:
        try:
            raw_results = []
            self.setup_http_session()

            for user_id in user_ids:
                fetched_record = self.filter_fetched_records_from_api(user_id=user_id, method=method)
                sleep(sleep_time)
                raw_results.append((user_id, fetched_record))

            basic_filtered_results = []
            fetched_filtered_ids: list = []
            for id_, fet_record in raw_results:
                if fet_record is not None:
                    basic_filtered_results.append((id_, fet_record))

            for user_id, fetched_record in basic_filtered_results:
                if fetched_record["asignaturas"]  is not None or fetched_record["asignaturas"] != []:
                    for asignatura in fetched_record["asignaturas"]:
                        if "reconocimientos" in asignatura.keys():
                            fetched_filtered_ids.append((user_id, fetched_record))

            return fetched_filtered_ids

        except (ValueError, TypeError, Exception) as e:
            print(f"Pipeline error: {e}")
        return None

class TFEFilter(TransactionalFilterInterface[TFEConfig, TFEnvConfig]):
    """
    Filter implementation for Final Degree Projects (TFE) API.

    Manages final degree project (Trabajo Fin de Estudios) data including
    project assignments, submissions, evaluations, and related academic
    information. Uses student crosscutting service authentication.

    Authentication:
        Uses Student-crosscutting-token header with TFE_API_KEY

    HTTP Method:
        GET requests for TFE project data

    API Features:
        Fixed endpoint URL
        User ID included in body parameters for project queries
    """

    def __init__(self,
                 config: pydantic_config | pydantic_settings_config,
                 filter_category: str,
                 env_config: pydantic_settings_config):
        """
        Initialize the TFEFilter.

        Configures the final degree projects API filter with student
        crosscutting service authentication for TFE data access.

        Args:
            config: Configuration with TFE query parameters
            env_config: Environment config with TFE API credentials

        Attributes:
            __config: Private configuration instance
            __env_config: Private environment configuration
            __session: HTTP session for API requests
            __connection: Database connection
            __headers: Headers with TFE API crosscutting token
        """
        self.__config = config
        self.__env_config = env_config
        self.__session = requests.Session()
        self.__filter_category = filter_category
        self.__connection = sqlite3.connect(self.__config.absolute_db_path)
        self.__headers = {'Student-crosscutting-token': self.__env_config.TFE_API_KEY}
        super().__init__(config=self.__config, env_config=self.__env_config, session=self.__session,
                         headers=self.__headers,filter_category=self.__filter_category)

    def _prepare_request_kwargs(self, user_id: int | str | Any) -> dict[str, Any]:
        """
        Prepare request parameters for TFE API calls.

        Modifies body parameters to include user_id for TFE project
        data retrieval. Note: This implementation appears to have
        an inconsistency - it modifies body_params but doesn't include
        them in the return value.

        Args:
            user_id: Student ID for TFE project information

        Returns:
            dict[str, Any]: Request parameters with URL only

        Note:
            The current implementation modifies body_params but doesn't
            include them in the returned parameters. This may need review.
        """
        body_params = self.__config.body_parameters
        body_params["user_id"] = user_id
        complete_url = self.__env_config.TFE_API_URL
        return {
            "url": complete_url,
        }

class GradingsFilter(TransactionalFilterInterface[GradingsConfig, GradingsEnvConfig]):
    """
    Filter implementation for academic qualifications API.

    Manages academic qualification data including grades, transcripts,
    degree classifications, and academic records. Requires both user
    identification and plan identification for comprehensive data retrieval.

    Authentication:
        Uses X-Api-Key header with QUALIFICATIONS_API_KEY

    HTTP Method:
        POST requests with JSON body including user_id and plan_id

    API Features:
        Fixed endpoint for qualifications queries
        Requires plan_id in addition to user_id for complete data retrieval
        Configurable body parameters for different qualification queries
    """

    def __init__(self,
                 config: pydantic_config | pydantic_settings_config,
                 filter_category: str,
                 env_config: pydantic_settings_config):
        """
        Initialize the GradingsFilter.

        Sets up the academic qualifications API filter with appropriate
        authentication for accessing student qualification records.

        Args:
            config: Configuration with qualification query parameters
            env_config: Environment config with qualifications API credentials

        Attributes:
            __config: Private configuration instance
            __env_config: Private environment configuration
            __session: HTTP session for API requests
            __connection: Database connection
            __headers: Headers with qualifications API key
        """
        self.__config = config
        self.__env_config = env_config
        self.__session = requests.Session()
        self.__filter_category = filter_category
        self.__connection = sqlite3.connect(self.__config.absolute_db_path)
        self.__headers = {'X-Api-Key': self.__env_config.QUALIFICATIONS_API_KEY}
        super().__init__(config=self.__config, env_config=self.__env_config, session=self.__session,
                         headers=self.__headers,filter_category=self.__filter_category)

    def obtain_plan_id(self, user_id:str | int | Any)-> str | None:
        """
        Obtain the academic plan ID for qualification queries.

        This method should be implemented to retrieve the appropriate
        academic plan ID based on the student's program or degree.
        The plan ID is required for complete qualification data retrieval.

        Returns:
            The academic plan identifier (implementation needed)

        Note:
            This method is currently not implemented and should be
            completed based on the specific plan identification logic
            required by the qualifications API.
        """
        try:
            response = self.__session.get(self.__env_config.QUALIFICATIONS_PLAN_ID_URL.replace("{id}", user_id))
            response.raise_for_status()
            data = response.json()
            if not self._is_valid_response(data):
                return None
            plan_id = data["expedientes"]
            if any(p["idRefPlan"] for p in plan_id):  # type: ignore
                plan_id = plan_id[0]["idRefPlan"]
                return plan_id
        except (requests.exceptions.HTTPError,
                requests.exceptions.ConnectionError,
                json.JSONDecodeError,
                ValueError,
                TypeError) as e:
            self._handle_api_error(e)
            return None
        return None

    @override
    def _fetch_filtered_records_from_api(self, user_id: str, method: Literal["GET", "POST"]) -> list[dict[
        str, Any]] | dict[str, Any] | None:
        """
        overrided method to deal with the 400 status code that sometimes this Api sends due to lack of data
        :param user_id: the id to check
        :param method: the request method to use
        :return:
        """

        try:
            request_kwargs = self._prepare_request_kwargs(user_id=user_id)

            if method == "GET":
                raise ValueError(
                    "This overrided method only deals with POST requests, as the api intended use of this particular endpoint.")
            else:
                response = self._session.post(**request_kwargs)

                if response.status_code != 200:
                    return None
                else:
                    data = response.json()
                    return self._transform_response_data(data)
        except (requests.exceptions.HTTPError,
                requests.exceptions.ConnectionError,
                json.JSONDecodeError,
                ValueError,
                TypeError) as e:
            self._handle_api_error(e)
            return None

    def _prepare_request_kwargs(self, user_id: int | str | Any) -> dict[str, Any]:
        """
        Prepare request parameters for qualifications API calls.

        Modifies body parameters to include both user_id and plan_id
        for comprehensive qualification data retrieval.

        Args:
            user_id: Student ID for qualification records

        Returns:
            dict[str, Any]: Request parameters with URL and JSON body
                          including user_id and plan_id

        Note:
            The URL references TFE_API_URL instead of a qualifications-specific
            URL, which may need to be corrected in the environment configuration.
        """
        body_params = self.__config.body_parameters
        body_params["idIntegracionAlumno"] = user_id
        body_params["idPlan"] = self.obtain_plan_id(user_id= user_id)
        complete_url = self.__env_config.QUALIFICATIONS_API_URL  # Note: This should likely be QUALIFICATIONS_API_URL
        return {
            "url": complete_url,
            "json": body_params
        }

    @override
    def filter_fetched_records_from_api_pipeline(self,
                                                 user_ids: list[str],
                                                 method: Literal["POST", "GET"],
                                                 sleep_time: float
                                                 ) -> list[tuple[str,dict[str, Any]]] | None:
        try:
            raw_results = []
            self.setup_http_session()

            for user_id in user_ids:
                fetched_record = self.filter_fetched_records_from_api(user_id=user_id, method=method)
                sleep(sleep_time)
                raw_results.append((user_id, fetched_record))

            basic_filtered_results = []
            fetched_filtered_ids: list = []
            for id_, fet_record in raw_results:
                if fet_record is not None:
                    basic_filtered_results.append((id_, fet_record))
            current_years = ("2024-2025", "2025-2026", "2026-2027", "2027-2028", "2028-2029", "2029-2030")
            for user_id, fetched_record in basic_filtered_results:
                if fetched_record["asignaturas"]  is not None or fetched_record["asignaturas"] != []:
                    for asignatura in fetched_record["asignaturas"]:
                        if "convocatorias" in asignatura.keys() and asignatura["numeroMatriculas"] > 0:
                            if any( a["anyoAcademico"] in current_years for a in asignatura["convocatorias"] ):
                                fetched_filtered_ids.append((user_id, fetched_record))

            return fetched_filtered_ids

        except (ValueError, TypeError, Exception) as e:
            print(f"Pipeline error: {e}")
        return None
class TeachingStartFilter(TransactionalFilterInterface[TeachingStartConfig, TeachingStartEnvConfig]):
    """
    A transactional filter for integrating with the Teaching Start API and
    retrieving data based on user identifiers.

    This class extends the `TransactionalFilterInterface` and encapsulates
    the setup of database connections, API session handling, and request
    preparation for the Teaching Start integration.

    Attributes
    ----------
    __config : TeachingStartConfig | TeachingStartEnvConfig
        The configuration object containing local filter settings, including
        the absolute path to the SQLite database.
    __env_config : TeachingStartEnvConfig
        The environment-specific configuration object containing API-related
        settings such as base URL and API key.
    __session : requests.Session
        A persistent HTTP session used for making requests to the Teaching
        Start API.
    __connection : sqlite3.Connection
        A SQLite connection object pointing to the database defined in config.
    __headers : dict[str, str]
        Default HTTP headers to be included with each request, including
        the API key.

    Methods
    -------
    _prepare_request_kwargs(user_id: int | str | Any) -> dict[str, Any]:
        Builds the request parameters for a given user ID by formatting the
        Teaching Start API URL with the provided identifier.
    """

    def __init__(
            self,
            config: pydantic_config | pydantic_settings_config,
            filter_category: str,
            env_config: pydantic_settings_config
    ):
        """
        Initialize a TeachingStartFilter instance.

        Parameters
        ----------
        config : TeachingStartConfig | pydantic_settings_config
            The configuration object containing local settings such as
            database path.
        env_config : TeachingStartEnvConfig | pydantic_settings_config
            The environment-specific configuration containing API endpoint
            and authentication details.

        Notes
        -----
        - Establishes a SQLite connection using the configured database path.
        - Initializes a requests session for communication with the Teaching
          Start API.
        - Sets up request headers with the API key from the environment
          configuration.
        """
        self.__config = config
        self.__env_config = env_config
        self.__session = requests.Session()
        self.__filter_category = filter_category
        self.__connection = sqlite3.connect(self.__config.absolute_db_path)
        self.__headers = {'X-Api-Key': self.__env_config.TEACHING_START_API_KEY}
        super().__init__(config=self.__config, env_config=self.__env_config, session=self.__session,
                         headers=self.__headers,filter_category=self.__filter_category)

    def _prepare_request_kwargs(self, user_id: int | str | Any) -> dict[str, Any]:
        """
        Construct the keyword arguments for an HTTP request to the Teaching Start API.

        Parameters
        ----------
        user_id : int | str | Any
            The identifier of the user to query. This value replaces the
            placeholder `{id}` in the API URL.

        Returns
        -------
        dict[str, Any]
            A dictionary containing the prepared request keyword arguments,
            including:
            - `"url"`: The full Teaching Start API URL with the user ID inserted.
        """
        complete_url = self.__env_config.TEACHING_START_API_URL.replace("{id}", user_id)
        return {
            "url": complete_url,
        }

    @override
    def filter_fetched_records_from_api_pipeline(self, user_ids: list[str], method: Literal["POST", "GET"],
                                                 sleep_time: float) -> list[str] | None:
        """
          Process multiple user IDs through the API filtering pipeline.
          This method has been overridden to filter records based on course end date.

          Iterates through a list of user IDs, making API calls for each one
          with a configurable delay between requests to avoid rate limiting.
          Returns user IDs that have at least one course with an end date in the future.

          Args:
              user_ids: List of user identifiers to process
              method: HTTP method to use for all requests ("GET" or "POST")
              sleep_time: Delay in seconds between API requests

          Returns:
              list[str] | None: List of user IDs that have courses with future end dates,
                              or None if pipeline fails

          Side Effects:
              - Calls setup_http_session() to configure the session
              - Introduces delays between requests via sleep()
              - Filters out records with null/min date values ("0001-01-01T00:00:00")
              - Filters out records with course end dates in the past
          """
        try:
            raw_results = []
            self.setup_http_session()

            for user_id in user_ids:
                fetched_record = self.filter_fetched_records_from_api(user_id=user_id, method=method)
                sleep(sleep_time)
                raw_results.append((user_id, fetched_record))

            raw_results = [i for i in raw_results if i[1] is not None]
            filtered_results = []

            for tup in raw_results:
                filtered_data = []
                for json_data in tup[1]:
                    # Parse the date string to datetime object if it is contain in a single dictionary
                    if isinstance(json_data, dict):
                        try:
                            # Handle the special case of "0001-01-01T00:00:00" which represents a null/min date
                            if json_data["C_dtFinCurso"] == "0001-01-01T00:00:00":
                                # This date is in the past, so skip or handle as needed
                                continue

                            # Parse the date string
                            course_end_date = datetime.fromisoformat(json_data["C_dtFinCurso"].replace('Z', '+00:00'))

                            # Make it timezone-aware if it's not already
                            if course_end_date.tzinfo is None:
                                course_end_date = course_end_date.replace(tzinfo=UTC)

                            # Compare with current time
                            if course_end_date >= datetime.now(UTC):
                                filtered_data.append(json_data)

                        except (ValueError, KeyError):
                            # Handle invalid date format or missing key
                            continue

                    # handles the case when it is a list.
                    if isinstance(json_data, list):
                        filtered_list = []
                        for j in json_data:
                            try:
                                # Handle the special case of "0001-01-01T00:00:00" which represents a null/min date
                                if j["C_dtFinCurso"] == "0001-01-01T00:00:00":
                                    continue

                                course_end_date = datetime.fromisoformat(j["C_dtFinCurso"].replace('Z', '+00:00'))
                                if course_end_date.tzinfo is None:
                                    course_end_date = course_end_date.replace(tzinfo=UTC)
                                if course_end_date >= datetime.now(UTC):
                                    filtered_list.append(j)
                            except (ValueError, KeyError):
                                continue

                        # Add valid courses from the list to filtered_data
                        filtered_data.extend(filtered_list)

                # Only append user if they have at least one valid course
                if filtered_data:
                    filtered_results.append((tup[0], filtered_data))

            # Extract user IDs from filtered results
            filtered_results_filtered_ids = [i[0] for i in filtered_results]
            return filtered_results_filtered_ids


        except(ValueError, TypeError, Exception) as e:
            print(e)
            return None

