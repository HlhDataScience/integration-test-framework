import json
import sqlite3
from typing import Any
import logging

import requests  # type: ignore
from requests.adapters import HTTPAdapter  # type: ignore
from urllib3.util import Retry  # type: ignore

from configmodels.transactional_config import (
    VirtualSessionsFilterConfig,
    VirtualSessionsEnvConfig,
    StudentsGroupsFilterConfig,
    StudentsGroupsEnvConfig
)  # type: ignore
from transactional_abstractions import TransactionalFilterInterface, pydantic_settings_config, pydantic_config  # type: ignore


class VirtualSessionsFilter(TransactionalFilterInterface[VirtualSessionsFilterConfig, VirtualSessionsEnvConfig]):
    """
    This class interacts with the students Ids from SQLite database.
    It uses them to further filter and obtain information useful for testing.
    It inherits from TransactionalFilterInterface, implementing all its methods.
    It is type save as it implements TypeVars.
    """
    def __init__(self,
                 config: pydantic_config | pydantic_settings_config,
                 env_config: pydantic_settings_config):

        self.__config = config
        self.__env_config = env_config
        self.__session = requests.Session()
        self.__connection = sqlite3.connect(self.__config.absolute_db_path)
        self.__headers = {
            "accept":"*/*",
            "Content-Type":"application/json",
            "Authorization":f"Bearer {self.__env_config.BEARER_TOKEN}"
        }

        super().__init__(config= self.__config , env_config= self.__env_config)

    def setup_http_session(self)-> None:

        retry_strategy = Retry(
            total=2,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.__session.mount("http://", adapter)
        self.__session.mount("https://", adapter)

        # Set __headers
        self.__session.headers.update(self.__headers)

    def filter_from_sqlite_database(self) -> tuple[int, ...] | None:

        try:
            cursor = self.__connection.cursor()
            cursor.execute(self.__config.sqlite_filter_query)

            user_ids = tuple(row[0] for row in cursor.fetchall())
            cursor.close()

            return user_ids
        except sqlite3.OperationalError as e:
            logging.error(f"unexpected operational error fetching rows:\n{e}")
        return None

    def fetch_filtered_records_from_api(self) -> tuple[dict[str, Any], ...] | dict[str, Any] | None:

        try:

            response = self.__session.post(
                url= self.__env_config.API_URL,
                json=self.__config.body_parameters
            )
            response.raise_for_status()

            data = response.json()

            if not data or data == 0:
                return None

            if isinstance(data, dict):
                return data
            if isinstance(data, list):
                data = tuple(d for d in data)
                return data
        except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError, json.JSONDecodeError, ValueError, TypeError) as e:
            logging.error(f"unexpected error fetching data from the API:\n{e}")
        return None

    def close_connection(self):

        if self.__connection:
            self.__connection.close()



class StudentsGroupsFilter(TransactionalFilterInterface[StudentsGroupsFilterConfig, StudentsGroupsEnvConfig]):

    def __init__(self,
                 config: pydantic_config | pydantic_settings_config,
                 env_config: pydantic_settings_config):
        self.__config = config
        self.__env_config = env_config
        self.__session = requests.Session()
        self.__connection = sqlite3.connect(self.__config.absolute_db_path)
        self.__headers = {'X-Api-Key': self.__env_config.BEARER_TOKEN}
        super().__init__(self.__config, self.__env_config)

    def setup_http_session(self)-> None:
        # Retry strategy
        retry_strategy = Retry(
            total=2,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.__session.mount("http://", adapter)
        self.__session.mount("https://", adapter)

        # Set __headers
        self.__session.headers.update(self.__headers)

    def filter_from_sqlite_database(self) -> tuple[int, ...] | None:

        try:
            cursor = self.__connection.cursor()
            cursor.execute(self.__config.sqlite_filter_query)

            user_ids = tuple(row[0] for row in cursor.fetchall())
            cursor.close()

            return user_ids
        except sqlite3.OperationalError as e:
            logging.error(f"unexpected operational error fetching rows:\n{e}")
        return None

    def fetch_filtered_records_from_api(self) -> tuple[dict[str, Any], ...] | dict[str, Any] | None:

        try:

            response = self.__session.post(
                url= self.__env_config.API_URL,
            )
            response.raise_for_status()

            data = response.json()

            if not data or data == 0:
                return None

            if isinstance(data, dict):
                return data
            if isinstance(data, list):
                data = tuple(d for d in data)
                return data
        except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError, json.JSONDecodeError, ValueError, TypeError) as e:
            logging.error(f"unexpected error fetching data from the API:\n{e}")
        return None

    def close_connection(self):

        if self.__connection:
            self.__connection.close()


