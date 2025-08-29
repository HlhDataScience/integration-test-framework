"""
Database Configuration and API Settings Module

This module provides configuration classes for managing database operations and API interactions
in a data fetching and processing system. It handles SQLite database configurations, API
environment variables, and fetching parameters through Pydantic settings classes that can
load configuration from YAML files and environment variables.


Classes:
    DBConfigQueries: Manages SQL queries and database configuration settings
    FetchingConfig: Handles API fetching parameters and database path management
    APIEnvVariables: Manages API credentials and __connection settings from environment variables

Constants:
    ROOT_DIR: Project root directory path
    DB_CONFIG_QUERIES_PATH: Path to database configuration YAML file
    DB_FETCHING_CONFIG_PATH: Path to database fetching configuration YAML file
"""

from datetime import datetime
from pathlib import Path
from typing import ClassVar, Optional

from pydantic import BaseModel, Field, field_validator  # type: ignore
from pydantic_settings import BaseSettings, SettingsConfigDict  # type: ignore
import requests  # type: ignore 
import yaml  # type: ignore

ROOT_DIR = Path(__file__).resolve().parents[2]
<<<<<<< HEAD
DB_CONFIG_QUERIES_PATH = ROOT_DIR / "dbconfigqueries.yaml"
DB_FETCHING_CONFIG_PATH = ROOT_DIR / "dbfetchingconfig.yaml"
=======
CONFIG_DIR = ROOT_DIR / "yaml_configurations" /"database_configs"
DB_CONFIG_QUERIES_PATH = CONFIG_DIR  / "dbconfigqueries.yaml"
DB_FETCHING_CONFIG_PATH = CONFIG_DIR  / "dbfetchingconfig.yaml"
>>>>>>> c117fbf (added the work from the last week)


class DBConfigQueries(BaseSettings):
    """
    Database SQL Configuration Manager

    This class handles the settings needed to fetch and parse data from API
    to introduce it in the SQLite local Data Base. It manages SQL queries for
    database creation, optimization, statistics, and post-processing operations.

    Attributes:
        create_table_sql (str): SQL statement for creating the main data table
        create_index_sql (list[str]): List of SQL statements for creating database indexes
        insert_sql (str): SQL statement template for inserting data records
        sql_optimizations (tuple[str, ...]): Tuple of SQL statements for database optimization
        db_stats_queries (tuple[str, ...]): Tuple of SQL queries for database statistics
        postprocess_optimizations (str): SQL statements for post-processing optimizations

    Methods:
        from_yaml: Class method to create instance from YAML configuration file
    """

    # Creation of the database
    create_table_sql: str
    create_index_sql: list[str]
    insert_sql: str

    # SQLite optimizations
    sql_optimizations: tuple[str, ...]

    # Database stats
    db_stats_queries: tuple[str, ...]

    # Postprocessing optimizations after fetching data and before closing the db __connection
    postprocess_optimizations: str

    @classmethod
    def from_yaml(cls, path: Path) -> "DBConfigQueries":
        """
        Create DBConfigQueries instance from YAML configuration file

        Loads database configuration settings from a YAML file and creates a new
        DBConfigQueries instance with the loaded data.

        Args:
            path (Path): Path to the YAML configuration file containing database queries

        Returns:
            DBConfigQueries: New instance populated with configuration from YAML file

        Raises:
            FileNotFoundError: If the specified YAML file doesn't exist
            yaml.YAMLError: If the YAML file is malformed or can't be parsed
            ValidationError: If the YAML data doesn't match the expected schema
        """
        with path.open("r", encoding="utf-8") as f:
            data = yaml.load(f, Loader=yaml.SafeLoader)
        return cls(**data)


class FetchingConfig(BaseSettings):
    """
    API Fetching and Database Configuration Manager

    Manages configuration settings for API data fetching operations and database
    storage parameters. Handles batch processing settings, record tracking, and
    database path management with automatic directory creation.

    Attributes:
        db_path (str): Relative path to the database directory from project root
        db_name (str): Name of the SQLite database file
        batch_size (int): Number of records to process in each batch
        api_limit (int): Maximum number of records to fetch per API call
        total_records (int): Total number of records expected to be processed
        total_api_calls (int): Total number of API calls made during fetching
        total_records_fetched (int): Actual number of records fetched from API
        total_record_inserted (int): Number of records successfully inserted into database

    Properties:
        absolute_db_path: Computed absolute path to the database file

    Methods:
        from_yaml: Class method to create instance from YAML configuration file
    """

    # API fetching settings
    db_path: str
    db_name: str
    checkpoint_name: str
    batch_size: int
    api_limit: int
    total_records: int
    total_api_calls: int
    total_records_fetched: int
    total_records_inserted: int

    @property
    def absolute_db_path(self) -> Path:
        """
        Get absolute path to the database file

        Constructs the absolute path to the SQLite database file by combining
        the project root directory with the configured database path and name.
        Creates the database directory if it doesn't exist.

        Returns:
            Path: Absolute path to the SQLite database file

        Side Effects:
            Creates the database directory structure if it doesn't exist
        """
        project_root = Path(__file__).resolve().parents[2]
        absolute_path = project_root / self.db_path
        absolute_path.mkdir(parents=True, exist_ok=True)
        absolute_db_path = absolute_path / self.db_name
        return absolute_db_path

    @property
    def absolute_checkpoint_path(self)-> Path:
        """
        Get absolute path to the checkpoint file

        Constructs the absolute path to the SQLite database file by combining
        the project root directory with the configured database path and name.
        Creates the database directory if it doesn't exist.

        Returns:
            Path: Absolute path to the SQLite database file

        Side Effects:
            Creates the database directory structure if it doesn't exist
        """
        project_root = Path(__file__).resolve().parents[2]
        absolute_path = project_root / self.db_path
        absolute_path.mkdir(parents=True, exist_ok=True)
        absolute_checkpoint_path = absolute_path / self.checkpoint_name
        return absolute_checkpoint_path

    @classmethod
    def from_yaml(cls, path: Path) -> "FetchingConfig":
        """
        Create FetchingConfig instance from YAML configuration file

        Loads API fetching and database configuration settings from a YAML file
        and creates a new FetchingConfig instance with the loaded data.

        Args:
            path (Path): Path to the YAML configuration file containing fetching settings

        Returns:
            FetchingConfig: New instance populated with configuration from YAML file

        Raises:
            FileNotFoundError: If the specified YAML file doesn't exist
            yaml.YAMLError: If the YAML file is malformed or can't be parsed
            ValidationError: If the YAML data doesn't match the expected schema
        """
        with path.open("r", encoding="utf-8") as f:
            data = yaml.load(f, Loader=yaml.SafeLoader)
        return cls(**data)


class APIEnvVariables(BaseSettings):
    """
    API Environment Variables Configuration Manager

    Manages API __connection settings loaded from environment variables. Handles
    API URL, authentication key, and HTTP __headers configuration with automatic
    JSON parsing for header strings.

    Attributes:
        model_config (ClassVar[SettingsConfigDict]): Pydantic settings configuration
<<<<<<< HEAD
            specifying the environment file location (.env.api)
=======
            specifying the environment file location (.env.db)
>>>>>>> c117fbf (added the work from the last week)
        API_URL (str): Base URL for the API endpoint
        API_KEY (str): Authentication key for API access
        BEARER_TOKEN (dict[str, str]): HTTP __headers to include in API requests

    Methods:
        parse_headers: Field validator for parsing HEADERS from JSON string format
    """

<<<<<<< HEAD
    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(env_file=ROOT_DIR / ".env.api")
=======
    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(env_file=ROOT_DIR / ".env.db")
>>>>>>> c117fbf (added the work from the last week)
    API_URL: str
    BEARER_TOKEN: str


class StudentRecord(BaseModel):
    # Identity fields
    id: Optional[int] = None
    idRefIntegracionAlumno: Optional[int] = None
    idRefPlan: Optional[int] = None
    idRefVersionPlan: Optional[int] = None
    idRefNodo: Optional[int] = None
    idRefUniversidad: Optional[int] = None
    idRefTipoEstudio: Optional[int] = None

    # Date fields
    fechaApertura: Optional[str] = None

    # Display names
    universidadDisplayName: Optional[str] = None
    centroEstudioDisplayName: Optional[str] = None
    tipoEstudioDisplayName: Optional[str] = None
    tituloDisplayName: Optional[str] = None
    estadoDisplayName: Optional[str] = None
    tipoSituacionDisplayName: Optional[str] = None
    displayNameDocumentoIdentificacionAlumno: Optional[str] = None
    displayNameNombreAlumno: Optional[str] = None

    # Student info
    idRefTipoDocumentoIdentificacionPais: Optional[str] = None
    alumnoNroDocIdentificacion: Optional[str] = None
    alumnoNombre: Optional[str] = None
    alumnoApellido1: Optional[str] = None
    alumnoApellido2: Optional[str] = None

    # Academic info
    nombreEstudio: Optional[str] = None
    nombrePlan: Optional[str] = None
    acronimoUniversidad: Optional[str] = None
    idUniversidad: Optional[int] = None

    # Counters with defaults
    countSeguimientos: int = Field(default=0)
    countAnotaciones: int = Field(default=0)
    countRelacionados: int = Field(default=0)

    # Metadata
    import_timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())

    @classmethod
    @field_validator('countSeguimientos', 'countAnotaciones', 'countRelacionados', mode="before")
    def validate_counts(cls, v):
        """Ensure counts are non-negative integers"""
        if v is None:
            return 0
        return max(0, int(v))

    @classmethod
    @field_validator('fechaApertura', mode="before")
    def validate_fecha_apertura(cls, v):
        """Validate date format if provided"""
        if v and not isinstance(v, str):
            try:
                return str(v)
            except ValueError:
                return None
        return v

    def to_db_tuple(self)->tuple:
        """
        Converts the validated model into a database tuple format
        :return:
        tuple with all the attributes for the record.
        """
        return(
            self.id,
            self.idRefIntegracionAlumno,
            self.idRefPlan,
            self.idRefVersionPlan,
            self.idRefNodo,
            self.idRefUniversidad,
            self.idRefTipoEstudio,
            self.fechaApertura,
            self.universidadDisplayName,
            self.centroEstudioDisplayName,
            self.tipoEstudioDisplayName,
            self.tituloDisplayName,
            self.idRefTipoDocumentoIdentificacionPais,
            self.alumnoNroDocIdentificacion,
            self.alumnoNombre,
            self.alumnoApellido1,
            self.alumnoApellido2,
            self.nombreEstudio,
            self.nombrePlan,
            self.acronimoUniversidad,
            self.idUniversidad,
            self.estadoDisplayName,
            self.tipoSituacionDisplayName,
            self.displayNameDocumentoIdentificacionAlumno,
            self.displayNameNombreAlumno,
            self.countSeguimientos,
            self.countAnotaciones,
            self.countRelacionados,
            self.import_timestamp
        )