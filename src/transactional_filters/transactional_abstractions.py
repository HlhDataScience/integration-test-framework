"""
Transactional Filter Interface Module

This module provides an abstract base class for creating transactional filters that interact
with APIs and SQLite databases to obtain information useful for testing purposes.

The module implements the Template Method pattern to define a standard workflow for API calls
while allowing subclasses to customize specific steps through hook methods.

Classes:
    TransactionalFilterInterface: Abstract base class for transactional API filters

Dependencies:
    - abc: For abstract base class functionality
    - json: For JSON data handling
    - logging: For error logging
    - time: For sleep functionality in pipelines
    - typing: For type hints and generics
    - sqlite3: For database operations
    - requests: For HTTP API calls
    - urllib3: For retry strategies
"""

from abc import ABC, abstractmethod
import json
import logging
from time import sleep
from typing import Generic, Any, Literal
import sqlite3

import requests  # type: ignore
from requests.adapters import HTTPAdapter  # type: ignore
from urllib3.util import Retry  # type: ignore

from src.configmodels.config_types import pydantic_config, pydantic_settings_config


class TransactionalFilterInterface(ABC, Generic[pydantic_config, pydantic_settings_config]):
    """
    Abstract base class for transactional API filters.

    This ABC provides an interface design to interact with APIs from transactional services
    to obtain information useful for testing. It implements the Template Method pattern,
    defining a standard workflow for API calls while allowing subclasses to customize
    specific steps through hook methods.

    The class handles:
    - HTTP session configuration with retry strategies
    - SQLite database queries for initial filtering
    - API calls with error handling
    - Data transformation and validation
    - Connection management

    Type Parameters:
        pydantic_config: Configuration model type for the specific implementation
        pydantic_settings_config: Environment settings configuration type

    Attributes:
        _config: Configuration object containing API and database settings
        _env_config: Environment configuration with API keys and URLs
        _session: HTTP session for API calls
        _headers: HTTP headers for API requests
        _connection: SQLite database connection
    """

    def __init__(
            self,
            config: pydantic_config | pydantic_settings_config,
            env_config: pydantic_settings_config,
            filter_category: str,
            session: requests.Session,
            headers: dict[str, str] | None = None,

    ):
        """
        Initialize the TransactionalFilterInterface.

        Args:
            config: Configuration object containing database paths and query settings
            env_config: Environment configuration with API endpoints and authentication
            session: HTTP session object for making API requests
            headers: Optional HTTP headers dictionary for API requests

        Raises:
            sqlite3.Error: If database connection cannot be established
        """
        self._config: pydantic_config | pydantic_settings_config = config
        self._env_config: pydantic_settings_config = env_config
        self._session: requests.Session = session
        self._headers: dict[str, str] | None = headers or {}
        self._connection = sqlite3.connect(self._config.absolute_db_path)
        self._filter_category = filter_category
    def setup_http_session(self) -> None:
        """
        Configure the HTTP session with retry strategy and headers.

        Sets up:
        - Retry strategy with exponential backoff for failed requests
        - HTTP adapters for both HTTP and HTTPS protocols
        - Default headers for all requests in the session

        The retry strategy handles:
        - Maximum 2 retry attempts
        - Backoff factor of 1 second
        - Retries on 429, 500, 502, 503, 504 status codes
        - Allowed methods: HEAD, GET, OPTIONS, POST
        """
        # Retry strategy
        retry_strategy = Retry(
            total=2,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self._session.mount("http://", adapter)
        self._session.mount("https://", adapter)

        # Set headers
        self._session.headers.update(self._headers)

    def filter_from_sqlite_database(self) -> tuple[int, ...] | None:
        """
        Fetch user IDs from the SQLite database using the configured query.

        Executes the SQL query specified in the configuration and returns
        a tuple of user IDs that can be used for further API filtering.

        Returns:
            tuple[int, ...] | None: Tuple of user IDs from the first column of query results,
                                  or None if an error occurs or no results found

        Raises:
            sqlite3.OperationalError: Logged but not propagated if SQL query fails
        """
        try:
            cursor = self._connection.cursor()
            cursor.execute(self._config.sqlite_filter_query)

            user_ids = tuple(row[0] for row in cursor.fetchall())
            cursor.close()

            return user_ids
        except sqlite3.OperationalError as e:
            logging.error(f"unexpected operational error fetching rows:\n{e}")
        return None

    def _fetch_filtered_records_from_api(self, user_id: str, method: Literal["GET", "POST"]) -> list[dict[
        str, Any]] | dict[str, Any] | None:
        """
        Template method that defines the standard workflow for API calls.

        This method implements the Template Method pattern, providing a standardized
        workflow for making API calls while allowing subclasses to customize specific
        steps through hook methods.

        Workflow:
        1. Prepare request parameters (customizable via _prepare_request_kwargs)
        2. Make HTTP request (GET or POST)
        3. Validate response (customizable via _is_valid_response)  
        4. Transform response data (customizable via _transform_response_data)

        Args:
            user_id: User identifier to use in the API request
            method: HTTP method to use ("GET" or "POST")

        Returns:
            tuple[dict[str, Any], ...] | dict[str, Any] | None: 
                Transformed response data or None if request fails

        Raises:
            Various exceptions are caught and logged via _handle_api_error hook
        """
        try:
            # Step 1: Prepare the request (customizable)
            request_kwargs = self._prepare_request_kwargs(user_id=user_id)
            if method == "GET":
                response = self._session.get(**request_kwargs)

                response.raise_for_status()
                data = response.json()

                if not self._is_valid_response(data):
                    return None

                # Step 4: Transform response (customizable)
                return self._transform_response_data(data)

            else:
                # Step 2: Make the request (standard)
                response = self._session.post(**request_kwargs)
                response.raise_for_status()

                # Step 3: Process response (standard with customizable validation)
                data = response.json()

                if not self._is_valid_response(data):
                    return None

                # Step 4: Transform response (customizable)
                return self._transform_response_data(data)

        except (requests.exceptions.HTTPError,
                requests.exceptions.ConnectionError,
                json.JSONDecodeError,
                ValueError,
                TypeError) as e:
            self._handle_api_error(e)
            return None

    def filter_fetched_records_from_api(self, user_id: str, method: Literal["POST", "GET"]) -> list[dict[
        str, Any]] | dict[str, Any] | None:
        """
        Wrapper method for filtering records from API. This is thought to make just one request and test.

        Provides a safe wrapper around the template method with additional
        exception handling for any unexpected errors.

        Args:
            user_id: User identifier for the API request
            method: HTTP method to use ("GET" or "POST")

        Returns:
            tuple[dict[str, Any], ...] | dict[str, Any] | None:
                Filtered records or None if operation fails
        """
        try:
            result = self._fetch_filtered_records_from_api(user_id=user_id, method=method)
            if result:
                return result
        except Exception as e:
            print(e)
            return None
        return None

    def filter_fetched_records_from_api_pipeline(self, user_ids: list[str], method: Literal["POST", "GET"],
                                                 sleep_time: float) -> list[str] | None:
        """
        Process multiple user IDs through the API filtering pipeline.

        Iterates through a list of user IDs, making API calls for each one
        with a configurable delay between requests to avoid rate limiting.
        Only returns user IDs that successfully returned data from the API.

        Args:
            user_ids: List of user identifiers to process
            method: HTTP method to use for all requests ("GET" or "POST")
            sleep_time: Delay in seconds between API requests

        Returns:
            list[str] | None: List of user IDs that returned valid data,
                            or None if pipeline fails

        Side Effects:
            - Calls setup_http_session() to configure the session
            - Introduces delays between requests via sleep()
        """
        try:
            raw_results = []
            self.setup_http_session()

            for user_id in user_ids:
                fetched_record = self.filter_fetched_records_from_api(user_id=user_id, method=method)
                sleep(sleep_time)
                raw_results.append((user_id, fetched_record))
            filtered_results = []

            for user_id, fetched_record in raw_results:
                if self._is_valid_non_empty_data(fetched_record):
                    filtered_results.append((self._filter_category, user_id, fetched_record))

            return filtered_results

        except (ValueError, TypeError, Exception) as e:
            print(f"Pipeline error: {e}")
        return None
    @staticmethod
    def _is_valid_json_object(self, obj) -> bool:
        """
        Check if a JSON object (dict) contains meaningful data.

        Args:
            obj: Object to validate

        Returns:
            bool: True if object contains meaningful data
        """
        if not isinstance(obj, dict):
            return False

        if not obj:  # Empty dict
            return False

        # Check if all values are None, empty strings, empty lists, or empty dicts
        for value in obj.values():
            if value is None:
                continue
            if isinstance(value, str) and not value.strip():
                continue
            if isinstance(value, (list, dict)) and not value:
                continue
            # If we find at least one meaningful value, the object is valid
            return True

        # All values were empty/None
        return False

    def _is_valid_non_empty_data(self, data) -> bool:
        """
        Check if data is valid and non-empty.

        Args:
            data: Data to validate

        Returns:
            bool: True if data is valid and non-empty, False otherwise
        """
        # Handle None
        if data is None:
            return False

        # Handle empty lists
        if isinstance(data, list):
            if not data:  # Empty list
                return False
            # Check if all items in list are empty/invalid
            return any(self._is_valid_json_object(item) for item in data)

        # Handle empty dictionaries
        if isinstance(data, dict):
            if not data:  # Empty dict
                return False
            return self._is_valid_json_object(data)

        # Handle strings (JSON strings, empty strings, etc.)
        if isinstance(data, str):
            if not data.strip():  # Empty or whitespace-only string
                return False
            # Try to parse as JSON if it looks like JSON
            if data.strip().startswith(('{', '[')):
                try:
                    import json
                    parsed = json.loads(data)
                    return self._is_valid_non_empty_data(parsed)
                except json.JSONDecodeError:
                    return False
            # Non-JSON string with content is considered valid
            return True

        # Handle other types (int, float, bool, etc.)
        # Consider them valid if they're not falsy in a meaningful way
        if isinstance(data, (int, float)):
            return True  # Even 0 is valid data
        if isinstance(data, bool):
            return True  # Both True and False are valid

        # For any other type, check if it's truthy
        return bool(data)

    # HOOK METHODS - subclasses override these to customize behavior
    @abstractmethod
    def _prepare_request_kwargs(self, user_id: int | str | Any) -> dict[str, Any]:
        """
        Hook method: Prepare request parameters for the API call.

        This abstract method must be implemented by subclasses to define
        how request parameters (URL, headers, body, etc.) are prepared
        for the specific API being called.

        Args:
            user_id: User identifier to include in the request

        Returns:
            dict[str, Any]: Dictionary of keyword arguments for requests.get/post
                          (e.g., {'url': '...', 'json': {...}, 'headers': {...}})

        Raises:
            NotImplementedError: If not implemented by subclass
        """
        raise NotImplementedError()

    @staticmethod
    def _is_valid_response(data: Any) -> bool:
        """
        Hook method: Validate response data.

        Default implementation checks that data is not None and not 0.
        Subclasses can override this method to implement more specific
        validation logic for their particular API responses.

        Args:
            data: Response data to validate

        Returns:
            bool: True if response data is valid, False otherwise
        """
        return data is not None and data != 0

    @staticmethod
    def _transform_response_data(data: Any) -> list[dict[str, Any]] | dict[str, Any]:
        """
        Hook method: Transform response data into the desired format.

        Default implementation converts lists to tuples and returns
        dictionaries as-is. Subclasses can override this method to
        implement custom data transformation logic.

        Args:
            data: Raw response data from the API

        Returns:
            tuple[dict[str, Any], ...] | dict[str, Any]: Transformed data
        """
        if isinstance(data, dict):
            return data
        if isinstance(data, list):
            return list(d for d in data)
        return data

    @staticmethod
    def _handle_api_error(error: Exception) -> None:
        """
        Hook method: Handle API errors.

        Default implementation logs the error. Subclasses can override
        this method to implement custom error handling logic, such as
        sending notifications, retrying with different parameters, etc.

        Args:
            error: Exception that occurred during API call
        """
        logging.error(f"unexpected error fetching data from the API:\n{error}")

    def close_connection(self):
        """
        Close the SQLite database connection.

        Should be called when the filter instance is no longer needed
        to properly clean up database resources.
        """
        if self._connection:
            self._connection.close()

    def __del__(self):
        """
        Destructor to ensure database connection is closed.

        Automatically called when the object is garbage collected
        to prevent resource leaks.
        """
        self.close_connection()