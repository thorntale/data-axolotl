"""Configuration parser for Axolotl.

Reads TOML configuration files and produces AxolotlConfig objects.
Supports environment variable substitution using $ENVVAR syntax.
"""

import os
import re
import tomllib
from pathlib import Path
from typing import Any, NamedTuple, Dict

from .snowflake_connection import SnowflakeOptions


class MetricsConfig(NamedTuple):
    """Configuration for metrics collection."""

    max_threads: int
    query_timeout_seconds: int


class AxolotlConfig(NamedTuple):
    """Top-level configuration for Axolotl."""

    connections: dict[str, SnowflakeOptions]
    metrics: dict[str, MetricsConfig]


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
    conn_name: str, conn_config: Dict[str, Any]
) -> SnowflakeOptions:
    """
    Parse a Snowflake connection configuration into a SnowflakeOptions object.

    Args:
        conn_name: Name of the connection (for error messages)
        conn_config: Dictionary containing connection configuration

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
    required_fields = ["account", "user", "database", "warehouse", "schema"]
    missing_fields = [field for field in required_fields if field not in conn_config]

    if missing_fields:
        raise ValueError(
            f"Connection '{conn_name}' is missing required fields: {', '.join(missing_fields)}"
        )

    # Validate authentication method
    has_password = "password" in conn_config
    has_private_key = "private_key_path" in conn_config or "private_key" in conn_config

    if not has_password and not has_private_key:
        raise ValueError(
            f"'{conn_name}' must have either password or private key authentication configured"
        )

    return SnowflakeOptions(**conn_config)  # type: ignore


def _parse_metrics_config(metrics_config: Dict[str, Any]) -> MetricsConfig:
    """
    Parse a metrics configuration section.

    Args:
        metrics_config: Dictionary containing metrics configuration

    Returns:
        MetricsConfig object
    """
    return MetricsConfig(
        max_threads=metrics_config.get("max_threads", 10),
        query_timeout_seconds=metrics_config.get("query_timeout_seconds", 60),
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

    ## TODO: later, support conn types that aren't snowflake
    connections: dict[str, SnowflakeOptions] = {}
    connections_section = config.get("connections", {})
    for conn_name, conn_config in connections_section.items():
        if conn_config["type"] == "snowflake":
            connections[conn_name] = _parse_snowflake_connection(conn_name, conn_config)
        else:
            raise ValueError(f"Invalid connection type {conn_config["type"]}")

    # Parse metrics configurations
    metrics: dict[str, MetricsConfig] = {}
    metrics_section = config.get("metrics", {})

    for metrics_name, metrics_config in metrics_section.items():
        metrics[metrics_name] = _parse_metrics_config(metrics_config)

    return AxolotlConfig(
        connections=connections,
        metrics=metrics,
    )


def load_config(config_path: str | Path = "config.toml") -> AxolotlConfig:
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
