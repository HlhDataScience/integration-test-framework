from collections.abc import Iterator, Generator
import json
from json import JSONDecodeError
from pathlib import Path
import sqlite3
import time
import requests  # type: ignore
from typing import Any, Optional
import sys

from requests.adapters import HTTPAdapter  # type: ignore
from urllib3.util import Retry  # type: ignore

from src.configmodels.dbconfig import (
    DBConfigQueries,
    FetchingConfig,
    APIEnvVariables,
    StudentRecord,
    DB_CONFIG_QUERIES_PATH,
    DB_FETCHING_CONFIG_PATH)

from src.utils.log_services import setup_custom_logging

# Configure logging
PROJECT_ROOT = Path(__file__).resolve().parents[3]  # the third level of parenthood. aka root directory.
LOGGER_FOLDER = PROJECT_ROOT / "database_logs"
LOGGER_FOLDER.mkdir(parents=True, exist_ok=True)
logger = setup_custom_logging(log_file_path=LOGGER_FOLDER, logger_name="database.log")


class APIToSQLiteClient:


    def __init__(self,
                 queries_configuration: DBConfigQueries,
                 fetching_configuration: FetchingConfig,
                 env_variables: APIEnvVariables,
                 record_formater: type[StudentRecord]
                 ):

        # Mangled internal variables to difficult the outside use.
        self.__queries_configuration = queries_configuration
        self.__fetching_configuration = fetching_configuration
        self.__env_variables = env_variables
        self.__record_formater = record_formater
        self.__connection = sqlite3.connect(self.__fetching_configuration.absolute_db_path)
        self.__checkpoint_file = self.__fetching_configuration.absolute_checkpoint_path
        self.__session = requests.Session()
        self.__headers = {
            "accept":"*/*",
            "Content-Type":"application/json",
            "Authorization":f"Bearer {self.__env_variables.BEARER_TOKEN}"}
        self.__checkpoint_dictionary = (self._load_checkpoint() or None)
    def setup_http_session(self):

        # Retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.__session.mount("http://", adapter)
        self.__session.mount("https://", adapter)

        # Set __headers
        self.__session.headers.update(self.__headers)
        logger.info("HTTP session initialized with retry strategy")

    def initialize_database(self):

        try:
            write_ahead, faster_writer, larger_cache, exclusive_access, page_size = self.__queries_configuration.sql_optimizations

            # SQLite performance optimizations
            self.__connection.execute(write_ahead)  # Write-Ahead Logging
            self.__connection.execute(faster_writer)  # Faster writes
            self.__connection.execute(larger_cache)  # Larger cache
            self.__connection.execute(exclusive_access)  # Exclusive access
            self.__connection.execute(page_size)  # Optimal page size

            # Create table
            self.__connection.execute(self.__queries_configuration.create_table_sql)

            # Create indexes
            for index_sql in self.__queries_configuration.create_index_sql:
                self.__connection.execute(index_sql)

            self.__connection.commit()
            logger.info(f"Database initialized: {self.__fetching_configuration.db_path}")
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise
    def _save_checkpoint(self, offset: int, api_call:int, total_fetched: int)-> None:
        """
        Loads the information related to the last checkpoint to reinitialize the fetching process from there.
        :return: None
        """
        try:
            with open(self.__checkpoint_file, "w", encoding="utf-8") as f:
                json.dump({
                    "api_call": api_call,
                    "total_fetched": total_fetched,
                    "offset": offset,
                }, f)
        except JSONDecodeError as json_error:
            logger.error(f"Failed to save checkpoint: {json_error}")
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")

    def _load_checkpoint(self) -> dict[str, Any] | None:
        """
        Loads the information related to the last checkpoint to reinitialize the fetching process from there.
        :return: the int form the last commited offset.
        """

        try:
            if self.__checkpoint_file.exists():
                with open(self.__checkpoint_file, "r", encoding="utf-8") as f:
                    return json.load(f)
        except JSONDecodeError as json_error:
            logger.error(f"Failed to load checkpoint: {json_error}")
        except Exception as e:
            logger.error(f"Failed to load checkpoint: {e}")

        return None

    def _fetch_api_data(self, offset: int, limit: int) -> Optional[dict[str, Any]]:

        payload = {
            "offset": offset,
            "limit": limit
        }

        try:
            logger.info(f"Fetching API data: offset={offset}, limit={limit}")
            response = self.__session.post(self.__env_variables.API_URL, json=payload, timeout=30)
            response.raise_for_status()

            self.__fetching_configuration.total_api_calls += 1
            data = response.json()

            # Log API response metadata
            meta = data.get('meta', {})
            records_count = len(data.get('data', []))

            logger.info(f"API call {self.__fetching_configuration.total_api_calls}: fetched {records_count} records, "
                        f"total count: {meta.get('totalCount', 'unknown')}")
            self.__fetching_configuration.total_records_fetched += records_count
            return data

        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for offset {offset}: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON response for offset {offset}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching data for offset {offset}: {e}")
            return None

    def api_data_generator(self) -> Generator[dict[str, Any], None, None]:


        checkpoint = self.__checkpoint_dictionary
        if checkpoint is None:
            self._save_checkpoint(
                offset=0,
                api_call= self.__fetching_configuration.total_api_calls,
                total_fetched=self.__fetching_configuration.total_records_fetched
                )
            offset = 0
        else:
            offset = checkpoint["offset"]

        has_more_data = True
        limit = self.__fetching_configuration.api_limit
        total_records = self.__fetching_configuration.total_records

        while has_more_data and offset < total_records:
            time.sleep(0.5)
            api_data = self._fetch_api_data(offset, limit)

            if api_data is None:
                logger.warning(f"Failed to fetch data for offset {offset}, stopping generator")
                break

            data_array = api_data.get('data', [])
            if not data_array:
                logger.info("No more data available, stopping generator")
                break

            yield api_data

            if len(data_array) < limit:
                logger.info(f"Received {len(data_array)} records (less than limit), no more data")
                has_more_data = False

            offset += limit
            if self.__checkpoint_dictionary is None:
                self._save_checkpoint(
                    offset=offset,
                    api_call=self.__fetching_configuration.total_api_calls,
                    total_fetched=self.__fetching_configuration.total_records_fetched)

            else:
                self._save_checkpoint(
                offset=offset,
                api_call=self.__fetching_configuration.total_api_calls,
                total_fetched= self.__fetching_configuration.total_records_fetched + self.__checkpoint_dictionary["total_fetched"]
                )
    def record_generator(self)-> Generator[tuple[Any, ...], None, None]:

        for api_response in  self.api_data_generator():
            data_array = api_response.get('data', [])

            for raw_record in data_array:
                try:

                    student_record = self.__record_formater(**raw_record)

                    yield student_record.to_db_tuple()

                except Exception as e:
                    logger.error(f"Failed to convert record {raw_record}: {e}")
                    continue

    def _batch_generator(self)-> Generator[list[tuple[Any, ...]], None, None]:

        batch_size = self.__fetching_configuration.batch_size
        batch = []

        for record_tuple in self.record_generator():
            batch.append(record_tuple)
            if len(batch) == batch_size:
                yield batch
                batch = [] # Really important, as this line resets the generator to yield the rest of the batches

        if batch:
            yield batch

    def _batch_insert_generator(self, record_batches: Iterator[list[tuple[Any, ...]]])-> None:

        total_processed_records =  0

        try:
            for batch in record_batches:
                if not batch:
                    continue
                cursor = self.__connection.cursor()

                cursor.executemany(self.__queries_configuration.insert_sql, batch)

                batch_size = len(batch)
                total_processed_records += batch_size
                self.__fetching_configuration.total_records_inserted += batch_size

                fetched_offset = (self.__checkpoint_dictionary if self.__checkpoint_dictionary is not None else 0)
                if  not isinstance(fetched_offset, int):
                    fetched_offset = fetched_offset["offset"]
                if fetched_offset > 0:
                    logger.info(f"Inserted batch of {batch_size} records. "
                                    f"Session total: {total_processed_records}, "
                                    f"Total records inserted: {self.__fetching_configuration.total_records_inserted + fetched_offset} "
                                    )
                else:
                    logger.info(f"Inserted batch of {batch_size} records. "
                                    f"Session total: {total_processed_records}, "
                                    f"Total records inserted: {self.__fetching_configuration.total_records_inserted} "
                                    )


        # Commit periodically for better performance and recovery
                if total_processed_records % (self.__fetching_configuration.batch_size * 10) == 0:
                    self.__connection.commit()
                    logger.info(f"Committed transaction at {total_processed_records} records")

        except Exception as e:
            logger.error(f"Error in batch insert generator: {e}")
            self.__connection.rollback()
            raise

    def fetch_and_import_all_with_generator(self):

        logger.info(
            f"Starting generator based API fetch and import process for up to {self.__fetching_configuration.total_records} records")
        start_time = time.time()

        try:
            # Begin transaction for better performance
            self.__connection.execute("BEGIN")

            batch_generator = self._batch_generator()
            self._batch_insert_generator(batch_generator)


        except Exception as e:
            logger.error(f"Failed to fetch data from API endpoint: {e}")
            self.__connection.rollback()
            raise

        finally:
            elapsed = time.time() - start_time

            logger.info(f"Generator processing completed in {elapsed:.2f} seconds")
            logger.info(f"Total API calls: {self.__fetching_configuration.total_api_calls}")
            logger.info(f"Total records fetched: {self.__fetching_configuration.total_records_fetched}")
            logger.info(f"Total records inserted in Session: {self.__fetching_configuration.total_records_inserted}")


            if elapsed > 0:
                rate = self.__fetching_configuration.total_records_inserted / elapsed
                logger.info(f"Average processing rate: {rate:.2f} records/second")



    def get_stats(self) -> dict[str, Any]:

        try:
            total_records_value, unique_students_value, status_counts_value, recent_imports_value = self.__queries_configuration.db_stats_queries
            cursor = self.__connection.cursor()

            cursor.execute(total_records_value)
            total_records = cursor.fetchone()[0]

            cursor.execute(unique_students_value)
            unique_students = cursor.fetchone()[0]

            cursor.execute(status_counts_value)
            status_counts = cursor.fetchall()

            cursor.execute(recent_imports_value)
            recent_imports = cursor.fetchall()

            return {
                'total_records': total_records,
                'unique_students': unique_students,
                'status_distribution': dict(status_counts),
                'recent_imports': dict(recent_imports)
            }
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            return {}

    def close(self):
        if self.__connection:
            # Optimize database after processing
            self.__connection.execute(self.__queries_configuration.postprocess_optimizations)
            self.__connection.close()
            logger.info("Database connection closed")

        if self.__session:
            self.__session.close()
            logger.info("HTTP session closed")


def main():
    """Main function to run the API importer."""
    # Initialize processor
    processor = APIToSQLiteClient(
        queries_configuration=DBConfigQueries.from_yaml(path=DB_CONFIG_QUERIES_PATH),
        fetching_configuration=FetchingConfig.from_yaml(path=DB_FETCHING_CONFIG_PATH),
        record_formater= StudentRecord ,
        env_variables=APIEnvVariables()
    )

    try:
        # Setup HTTP session
        processor.setup_http_session()

        # Initialize database
        processor.initialize_database()

        # Fetch and import all data
        processor.fetch_and_import_all_with_generator()

        # Show statistics if requested
        stats = processor.get_stats()
        logger.info("Database Statistics:")
        logger.info(f"  Total records: {stats.get('total_records', 0)}")
        logger.info(f"  Unique students: {stats.get('unique_students', 0)}")
        logger.info(f"  Status distribution: {stats.get('status_distribution', {})}")
        logger.info(f"  Recent imports: {stats.get('recent_imports', {})}")

    except Exception as e:
        logger.error(f"Processing failed: {e}")
        sys.exit(1)
    finally:
        processor.close()


if __name__ == '__main__':
    main()