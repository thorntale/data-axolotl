"""Configuration parser for Axolotl.

Reads YAML configuration files and produces AxolotlConfig objects.
Supports environment variable substitution using $ENVVAR syntax.
"""

import os
import re
import yaml
from pathlib import Path
from typing import Any, Dict, Optional

from pydantic import BaseModel, model_validator


class MetricsConfig(BaseModel):
    """Configuration for metrics collection."""

    max_threads: int = 10
    per_query_timeout_seconds: int = 60
    per_column_timeout_seconds: int = 60
    per_run_timeout_seconds: int = 300
    per_database_timeout_seconds: int = 300
    exclude_expensive_queries: bool = False
    exclude_complex_queries: bool = False

class Database(BaseModel):
    """Configuration for a single db."""
    database: str

    include_schemas: Optional[list[str]] = []
    exclude_schemas: Optional[list[str]] = ["INFORMATION_SCHEMA"]
    metrics_config: Optional[MetricsConfig] = None

class SnowflakeConnection(BaseModel):
    """Configuration options for Snowflake connections."""

    # Connection type
    type: str = "snowflake"

    # Required fields
    user: str
    account: str
    warehouse: str

    # Password authentication (mutually exclusive with private key auth)
    password: Optional[str] = None

    # Private key authentication (mutually exclusive with password)
    private_key: Optional[bytes] = None
    private_key_path: Optional[str] = None
    private_key_passphrase: Optional[str] = None
    private_key_file: Optional[str] = None
    private_key_file_pwd: Optional[str] = None

    databases: dict[str, Database]

    # Optional additional fields that can be passed to snowflake.connector.connect()
    role: Optional[str] = None
    authenticator: Optional[str] = None
    session_parameters: Optional[dict] = None
    timezone: Optional[str] = None
    autocommit: Optional[bool] = None
    client_session_keep_alive: Optional[bool] = None
    validate_default_parameters: Optional[bool] = None
    paramstyle: Optional[str] = None
    application: Optional[str] = None

    @model_validator(mode="after")
    def validate_authentication(self):
        """Validate that either password or private key authentication is configured."""
        has_password = self.password is not None
        has_private_key = (
            self.private_key is not None
            or self.private_key_file is not None
            or self.private_key_path is not None
        ) and self.authenticator == "SNOWFLAKE_JWT"

        if not has_password and not has_private_key:
            raise ValueError(
                "Must have either password or private key authentication configured"
            )

        return self


class AxolotlConfig(BaseModel):
    """Top-level configuration for Axolotl."""

    connections: dict[str, SnowflakeConnection]
    metrics_config: MetricsConfig


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


def _parse_snowflake_connections(
    conn_config: dict[str, Any],
    default_metrics_config: MetricsConfig,
) -> SnowflakeConnection:
    """
    Parse a Snowflake connection configuration into a SnowflakeOptions object.

    Args:
        conn_config: Dictionary containing connection configuration
        default_metrics_config: Default metrics configuration to use if not specified

    Returns:
        SnowflakeOptions

    Raises:
        ValueError: if a field is missing
    """
    # Parse the SnowflakeConnection first
    connection = SnowflakeConnection(**conn_config)

    # Override metrics in each database entry if it doesn't exist
    for db_config in connection.databases.values():
        if db_config.metrics_config is None:
            db_config.metrics_config = default_metrics_config
    return connection


def parse_config(config_path: str | Path) -> AxolotlConfig:
    """
    Parse a YAML configuration file to an AxolotlConfig.

    Args:
        config_path: Path to the config

    Returns:
        AxolotlConfig

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

    # Parse default metrics configuration first
    metrics_section = config.get("metrics", {})

    # Extract default metrics, but exclude dicts (those are connection-specific overrides)
    default_metrics_dict = {
        k: v for k, v in metrics_section.items() if not isinstance(v, dict)
    }
    default_metrics_config = MetricsConfig(**default_metrics_dict)

    # TODO: later, support conn types that aren't snowflake
    connections: dict[str, SnowflakeConnection] = {}
    connections_section = config.get("connections", {})

    for conn_name, conn_config in connections_section.items():
        conn_type = conn_config.get("type", "snowflake")
        if conn_type == "snowflake":
            # Check if there's a connection-specific metrics override in metrics.<conn_name>
            ##connection_specific_metrics = metrics_section.get(conn_name)
            connections[conn_name] = _parse_snowflake_connections(
                #conn_name,
                conn_config,
                default_metrics_config,
            )
        else:
            raise ValueError(f"Invalid connection type {conn_type}")

    return AxolotlConfig(
        connections=connections,
        metrics_config=default_metrics_config,
    )


def load_config(config_path: str | Path = "conf/config.yaml") -> AxolotlConfig:
    """
    Load configuration from a file with a convenient default path.

    This is a convenience wrapper around parse_config that uses a default
    configuration file path of "conf/config.yaml" in the current directory.

    Args:
        config_path: Path to the YAML configuration file (default: "conf/config.yaml")

    Returns:
        AxolotlConfig object containing all parsed configuration
    """
    return parse_config(config_path)
