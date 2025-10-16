"""Configuration parser for Axolotl.

Reads TOML configuration files and produces AxolotlConfig objects.
Supports environment variable substitution using $ENVVAR syntax.
"""

import os
import re
import tomllib
from pathlib import Path
from typing import Any, NamedTuple

from typing import NamedTuple, List, Tuple, TypedDict, NotRequired


class MetricsConfig(NamedTuple):
    """Configuration for metrics collection."""

    max_threads: int
    per_query_timeout_seconds: int
    per_column_timeout_seconds: int
    per_run_timeout_seconds: int
    exclude_expensive_queries: bool


class SnowflakeOptions(TypedDict):
    # Required fields
    user: str
    account: str
    database: str
    warehouse: str

    #TODO make these mutually exclusive!
    include_schemas: NotRequired[List[str]]
    exclude_schemas: NotRequired[List[str]]

    # Password authentication (mutually exclusive with private key auth)
    password: NotRequired[str]

    # Private key authentication (mutually exclusive with password)
    private_key: NotRequired[bytes]
    private_key_path: NotRequired[str]
    private_key_passphrase: NotRequired[str]

    # Optional additional fields that can be passed to snowflake.connector.connect()
    role: NotRequired[str]
    authenticator: NotRequired[str]
    session_parameters: NotRequired[dict]
    timezone: NotRequired[str]
    autocommit: NotRequired[bool]
    client_session_keep_alive: NotRequired[bool]
    validate_default_parameters: NotRequired[bool]
    paramstyle: NotRequired[str]
    application: NotRequired[str]

    ## Metrics config for this database. If it's missing, use the default
    metricsConfig: MetricsConfig


class AxolotlConfig(NamedTuple):
    """Top-level configuration for Axolotl."""

    connections: dict[str, SnowflakeOptions]
    default_metrics_config: MetricsConfig


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


def _parse_snowflake_connection(
    conn_name: str,
    conn_config: dict[str, Any],
    default_metrics_config: MetricsConfig,
    connection_specific_metrics: dict[str, Any] | None = None,
) -> SnowflakeOptions:
    """
    Parse a Snowflake connection configuration into a SnowflakeOptions object.

    Args:
        conn_name: Name of the connection (for error messages)
        conn_config: Dictionary containing connection configuration
        default_metrics_config: Default metrics configuration to use if not specified
        connection_specific_metrics: Optional connection-specific metrics from metrics.<conn_name>

    Returns:
        SnowflakeOptions

    Raises:
        ValueError: if a field is missing
    """
    conn_type = conn_config.get("type")
    if conn_type != "snowflake":
        raise ValueError(
            f"Expected type 'snowflake' but '{conn_name}' has unsupported type '{conn_type}'. "
        )

    # Required fields for SnowflakeOptions
    required_fields = ["account", "user", "database", "warehouse"]
    missing_fields = [field for field in required_fields if field not in conn_config]

    if missing_fields:
        raise ValueError(
            f"Connection '{conn_name}' is missing required fields: {', '.join(missing_fields)}"
        )

    # Validate authentication method
    has_password = "password" in conn_config
    has_private_key = "private_key_file" in conn_config or "private_key" in conn_config

    if not has_password and not has_private_key:
        raise ValueError(
            f"'{conn_name}' must have either password or private key authentication configured"
        )

    # Use connection-specific metrics if available, otherwise use default
    metrics_config = default_metrics_config
    if connection_specific_metrics:
        metrics_config = _parse_metrics_config(connection_specific_metrics)

    # Create SnowflakeOptions with metricsConfig field
    return SnowflakeOptions(**conn_config, metricsConfig=metrics_config)


def _parse_metrics_config(metrics_config: dict[str, Any]) -> MetricsConfig:
    """
    Parse a metrics config section, with defaults, from a dictionary
    """
    return MetricsConfig(
        max_threads=metrics_config.get("max_threads", 10),
        per_query_timeout_seconds=metrics_config.get("per_query_timeout_seconds", 10),
        per_column_timeout_seconds=metrics_config.get("per_column_timeout_seconds", 60),
        per_run_timeout_seconds=metrics_config.get("per_run_timeout_seconds", 60 * 5),
        exclude_expensive_queries=metrics_config.get(
            "exclude_expensive_queries", False
        ),
    )


def parse_config(config_path: str | Path) -> AxolotlConfig:
    """
    parse a TOML configuration file to an AxolotlConfig.

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

    # Read and parse TOML file
    with open(config_path, "rb") as f:
        raw_config = tomllib.load(f)

    # Substitute environment variables throughout the config
    config = _substitute_env_vars(raw_config)

    # Parse default metrics configuration first
    metrics_section = config.get("metrics", {})

    # Extract default metrics, but exclude dicts
    default_metrics_dict = {
        k: v for k, v in metrics_section.items() if not isinstance(v, dict)
    }
    default_metrics_config = _parse_metrics_config(default_metrics_dict)

    ## TODO: later, support conn types that aren't snowflake
    connections: dict[str, SnowflakeOptions] = {}
    connections_section = config.get("connections", {})
    for conn_name, conn_config in connections_section.items():
        if conn_config["type"] == "snowflake":
            # Check if there's a connection-specific metrics override in metrics.<conn_name>
            connection_specific_metrics = metrics_section.get(conn_name)
            connections[conn_name] = _parse_snowflake_connection(
                conn_name,
                conn_config,
                default_metrics_config,
                connection_specific_metrics,
            )
        else:
            raise ValueError(f"Invalid connection type {conn_config["type"]}")

    return AxolotlConfig(
        connections=connections,
        default_metrics_config=default_metrics_config,
    )


def load_config(config_path: str | Path = "conf/config.toml") -> AxolotlConfig:
    """
    Load configuration from a file with a convenient default path.

    This is a convenience wrapper around parse_config that uses a default
    configuration file path of "config.toml" in the current directory.

    Args:
        config_path: Path to the TOML configuration file (default: "config.toml")

    Returns:
        AxolotlConfig object containing all parsed configuration
    """
    return parse_config(config_path)
