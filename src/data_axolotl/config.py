"""Configuration parser for DataAxolotl.

Reads YAML configuration files and produces DataAxolotlConfig objects.
Supports environment variable substitution using $ENVVAR syntax.
"""

import os
import re
import yaml
from pathlib import Path
from typing import Any, Dict, Optional, List, Iterator
import inspect
from contextlib import contextmanager

from pydantic import BaseModel, model_validator
from rich.console import Console

from .connectors.identifiers import IncludeDirective, FqTable
from .connectors.snowflake_connection import SnowflakeConn
from .connectors.sqlite_connection import SqliteConn
from .connectors.base_connection import BaseConnection
from .connectors.state_dao import StateDAO


class BaseConnectionConfig(BaseModel):
    data_axolotl_config: DataAxolotlConfig

    name: str
    type: str
    params: Dict[str, Any]
    include: Optional[List[IncludeDirective]] = None
    exclude: List[IncludeDirective] = []

    max_threads: Optional[int] = None
    run_timeout_seconds: Optional[int] = None
    connection_timeout_seconds: Optional[int] = None
    query_timeout_seconds: Optional[int] = None
    exclude_expensive_queries: Optional[bool] = None
    exclude_complex_queries: Optional[bool] = None

    def get_conn(self, run_id: int, console: Console) -> BaseConnection[BaseConnectionConfig, Any]:
        raise NotImplementedError('Need to implement get_conn in subclass of BaseConnectionConfig')

    def database_is_included_at_all(self, database: str) -> bool:
        """ Return True if this database is included in _any_ metrics. """
        for exc in self.exclude:
            if exc.schema is None and exc.database_lower == database.lower():
                return False
        if self.include is None:
            return True
        for inc in self.include:
            if inc.database_lower == database.lower():
                return True
        return False

    def include_table_at_all(self, table: FqTable) -> bool:
        """ Whether this table or any of its columns are included """
        if table.schema.lower() == 'information_schema':
            return False
        # check for excluding specifically this table, schema, or db
        for exc in self.exclude:
            if (
                exc.database_lower == table.database.lower()
                and exc.schema_lower == table.schema.lower()
                and exc.table_lower == table.table.lower()
                and exc.column is None
                and exc.metric is None
            ):
                return False
            if (
                exc.database_lower == table.database.lower()
                and exc.schema_lower == table.schema.lower()
                and exc.table is None
                and exc.column is None
                and exc.metric is None
            ):
                return False
            if (
                exc.database.lower() == table.database.lower()
                and exc.schema is None
                and exc.table is None
                and exc.column is None
                and exc.metric is None
            ):
                return False
        if self.include is None:
            return True
        for inc in self.include:
            # match db.schema.table, db.schema.table:metric, db.schema.table.column, db.schema.table.column:metric
            if (
                inc.database_lower == table.database.lower()
                and inc.schema_lower == table.schema.lower()
                and inc.table_lower == table.table.lower()
            ):
                return True
            # match db.schema
            if (
                inc.database_lower == table.database.lower()
                and inc.schema_lower == table.schema.lower()
                and inc.table is None
            ):
                return True
            # match db
            if (
                inc.database.lower() == table.database.lower()
                and inc.schema is None
            ):
                return True
        return False

    def include_table_metrics(self, table: FqTable) -> bool:
        """ Whether this table itself is included """
        if table.schema.lower() == 'information_schema':
            return False
        # check for excluding specifically this table
        for exc in self.exclude:
            if (
                exc.database_lower == table.database.lower()
                and exc.schema_lower == table.schema.lower()
                and exc.table_lower == table.table.lower()
                and exc.column is None
                and exc.metric is None
            ):
                return False
            if (
                exc.database_lower == table.database.lower()
                and exc.schema_lower == table.schema.lower()
                and exc.table is None
                and exc.column is None
                and exc.metric is None
            ):
                return False
            if (
                exc.database_lower == table.database.lower()
                and exc.schema is None
                and exc.table is None
                and exc.column is None
                and exc.metric is None
            ):
                return False
        if self.include is None:
            return True
        for inc in self.include:
            # match db.schema.table but NOT db.schema.table.column
            if (
                inc.database_lower == table.database.lower()
                and inc.schema_lower == table.schema.lower()
                and inc.table_lower == table.table.lower()
                and inc.column is None
            ):
                return True
            # match db.schema
            if (
                inc.database_lower == table.database.lower()
                and inc.schema_lower == table.schema.lower()
                and inc.table is None
            ):
                return True
            # match db
            if (
                inc.database_lower == table.database.lower()
                and inc.schema is None
            ):
                return True
        return False

    def include_column(self, table: FqTable, column: str) -> bool:
        """ Whether this column is included in metrics """
        if table.schema.lower() == 'information_schema':
            return False
        # check for excluding specifically this column
        for exc in self.exclude:
            # exclude whole column
            if (
                exc.database_lower == table.database.lower()
                and exc.schema_lower == table.schema.lower()
                and exc.table_lower == table.table.lower()
                and exc.column_lower == column.lower()
                and exc.metric is None
            ):
                return False
            # exclude whole table
            if (
                exc.database_lower == table.database.lower()
                and exc.schema_lower == table.schema.lower()
                and exc.table_lower == table.table.lower()
                and exc.column is None
                and exc.metric is None
            ):
                return False
            # exclude whole schema
            if (
                exc.database_lower == table.database.lower()
                and exc.schema_lower == table.schema.lower()
                and exc.table is None
                and exc.column is None
                and exc.metric is None
            ):
                return False
            # exclude whole db
            if (
                exc.database_lower == table.database.lower()
                and exc.schema is None
                and exc.table is None
                and exc.column is None
                and exc.metric is None
            ):
                return False
        if self.include is None:
            return True
        for inc in self.include:
            # match db.schema.table.column, db.schema.table.column:metric
            if (
                inc.database_lower == table.database.lower()
                and inc.schema_lower == table.schema.lower()
                and inc.table_lower == table.table.lower()
                and inc.column_lower == column.lower()
            ):
                return True
            # match db.schema.table
            if (
                inc.database_lower == table.database.lower()
                and inc.schema_lower == table.schema.lower()
                and inc.table_lower == table.table.lower()
                and inc.column is None
            ):
                return True
            # match db.schema
            if (
                inc.database_lower == table.database.lower()
                and inc.schema_lower == table.schema.lower()
                and inc.table is None
            ):
                return True
            # match db
            if (
                inc.database_lower == table.database.lower()
                and inc.schema is None
            ):
                return True
        return False

    def include_column_metric(self, table: FqTable, column: str, metric: str) -> bool:
        if table.schema.lower() == 'information_schema':
            return False
        if not self.include_column(table, column):
            return False

        for exc in self.exclude:
            # exclude specifically this metric
            if (
                exc.database_lower == table.database.lower()
                and exc.schema_lower == table.schema.lower()
                and exc.table_lower == table.table.lower()
                and exc.column_lower == column.lower()
                and exc.metric_lower == metric.lower()
            ):
                return False
        if self.include is None:
            return True
        for inc in self.include:
            # match db.schema.table.column:metric
            if (
                inc.database_lower == table.database.lower()
                and inc.schema_lower == table.schema.lower()
                and inc.table_lower == table.table.lower()
                and inc.column_lower == column.lower()
                and inc.metric_lower == metric.lower()
            ):
                return True
            # match db.schema.table.column
            if (
                inc.database_lower == table.database.lower()
                and inc.schema_lower == table.schema.lower()
                and inc.table_lower == table.table.lower()
                and inc.column_lower == column.lower()
                and inc.metric_lower is None
            ):
                return True
            # match db.schema.table
            if (
                inc.database_lower == table.database.lower()
                and inc.schema_lower == table.schema.lower()
                and inc.table_lower == table.table.lower()
                and inc.column is None
            ):
                return True
            # match db.schema
            if (
                inc.database_lower == table.database.lower()
                and inc.schema_lower == table.schema.lower()
                and inc.table is None
            ):
                return True
            # match db
            if (
                inc.database_lower == table.database.lower()
                and inc.schema is None
            ):
                return True
        return False


class SnowflakeConnectionConfig(BaseConnectionConfig):
    """Configuration options for Snowflake connections."""
    type: str = "snowflake"

    @model_validator(mode="after")
    def validate_authentication(self):
        params = self.params

        # error for missing required params
        required_options = {'account', 'user'}
        missing_options = required_options - set(params.keys())
        if missing_options:
            raise ValueError(f'Missing required connection params on snowflake connection {self.name}: [{", ".join(missing_options)}] are required.')

        # error for missing or multiple password methods
        if (
            'password' not in params
            and 'private_key' not in params
            and 'private_key_file' not in params
        ):
            raise ValueError(f'Missing password on snowflake connection {self.name}. Please provide either a password, private_key, or private_key_file connection param.')

        if 'private_key' in params and 'private_key_file' in params:
            raise ValueError(
                "Cannot provide both private_key and private_key_file."
            )
        if 'password' in params and ('private_key' in params or 'private_key_file' in params):
            raise ValueError(
                "Cannot provide both password and private key authentication."
            )
        return self

    def get_conn(self, run_id: int, console: Console) -> BaseConnection[BaseConnectionConfig, Any]:
        return SnowflakeConn(self.data_axolotl_config, self, run_id, console)


class SqliteConnectionConfig(BaseConnectionConfig):
    type: str = "sqlite"
    path: str = "./local.db"

    def get_conn(self, run_id: int, console: Console) -> BaseConnection[BaseConnectionConfig, Any]:
        return SqliteConn(self.data_axolotl_config, self, run_id, console)


class StateConfig(BaseModel):
    connection: str
    prefix: str


class DataAxolotlConfig(BaseModel):
    """Top-level configuration for DataAxolotl."""
    max_threads: int = 10
    run_timeout_seconds: int = 600
    connection_timeout_seconds: int = 600
    query_timeout_seconds: int = 60
    exclude_expensive_queries: bool = False
    exclude_complex_queries: bool = False

    state: str | StateConfig = 'local.db'

    connections: dict[str, BaseConnectionConfig]

    @contextmanager
    def get_state_dao(self) -> Iterator[StateDAO]:
        run_id = -1
        console = Console()
        if isinstance(self.state, str):
            sqlite_config = SqliteConnectionConfig(
                name="local_state",
                data_axolotl_config=self,
                params={'path': self.state},
            )
            with sqlite_config.get_conn(run_id, console) as conn:
                yield StateDAO(conn)
        else:
            if not self.state.connection in self.connections:
                raise ValueError(f'Connection referenced in state ({self.state.connection}) is not defined.')
            connection_config = self.connections[self.state.connection]
            with connection_config.get_conn(run_id, console) as conn:
                yield StateDAO(conn, self.state.prefix)


def _substitute_env_vars(value: Any) -> Any:
    """
    substitute $ENVVARS in the config config

    Args:
        value: The configuration value to process (can be str, dict, list, etc.)

    Returns:
        The value with all environment variables substituted

    Raises:
        KeyError: If an environment variable is referenced but not set
    """
    if isinstance(value, str):
        match = re.match(r"^\$(\w+)$", value)
        if match:
            env_var_name = match.group(1)
            if env_var_name not in os.environ:
                raise KeyError(
                    f"Required environment variable '{env_var_name}' is not set"
                )
            return os.environ[env_var_name]
        return value
    elif isinstance(value, dict):
        return {k: _substitute_env_vars(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [_substitute_env_vars(item) for item in value]
    else:
        return value


def parse_config(config_path: str | Path) -> DataAxolotlConfig:
    """
    Parse a YAML configuration file to an DataAxolotlConfig.

    Args:
        config_path: Path to the config

    Returns:
        DataAxolotlConfig

    Raises:
        FileNotFoundError: If the configuration file doesn't exist
        ValueError: If the configuration is invalid
        KeyError: If a required environment variable is not set
    """
    config_path = Path(config_path)

    if not config_path.exists():
        raise FileNotFoundError(f"Configfile not found: {config_path}")

    # Read and parse YAML file
    with open(config_path, "r") as f:
        raw_config = yaml.safe_load(f)

    # Substitute environment variables throughout the config
    config = _substitute_env_vars(raw_config)
    return config_from_dict(config)


def config_from_dict(config: Dict) -> DataAxolotlConfig:
    connections: Dict[str, BaseConnectionConfig] = {}

    data_axolotl_config = DataAxolotlConfig(
        **{
            k: v
            for k, v in config.items()
            if k != 'connections'
        },
        connections={},
    )

    for conn_name, conn_config in config.pop("connections", {}).items():
        conn_type = conn_config.get("type", "snowflake")
        if conn_type == "snowflake":
            # Check if there's a connection-specific metrics override in metrics.<conn_name>
            ##connection_specific_metrics = metrics_section.get(conn_name)
            opts = {
                **conn_config,
                'name': conn_name,
                'type': 'snowflake',
                'include': parse_include_list(conn_config.get("include", None)),
                'exclude': parse_include_list(conn_config.get("exclude", [])),
            }
            data_axolotl_config.connections[conn_name] = SnowflakeConnectionConfig(
                **opts,
                data_axolotl_config=data_axolotl_config,
            )
        else:
            raise ValueError(f"Invalid connection type {conn_type}")

    return data_axolotl_config

def parse_include_list(items: List[str] | None) -> List[IncludeDirective] | None:
    if items is None:
        return None
    for item in items:
        if not isinstance(item, str):
            raise ValueError(f"Include and Exclude rules must be strings. Found {item!r}")
    return [
        IncludeDirective.from_string(item)
        for item in items
    ]

def load_config(config_path: str | Path = "config.yaml") -> DataAxolotlConfig:
    """
    Load configuration from a file with a convenient default path.

    This is a convenience wrapper around parse_config that uses a default
    configuration file path of "conf/config.yaml" in the current directory.

    Args:
        config_path: Path to the YAML configuration file (default: "conf/config.yaml")

    Returns:
        DataAxolotlConfig object containing all parsed configuration
    """
    return parse_config(config_path)
