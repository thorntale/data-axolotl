"""Configuration parser for Axolotl.

Reads YAML configuration files and produces AxolotlConfig objects.
Supports environment variable substitution using $ENVVAR syntax.
"""

import os
import re
import yaml
from pathlib import Path
from typing import Any, Dict, Optional, List
import inspect

import snowflake.connector
from pydantic import BaseModel, model_validator
from .state_dao import IncludeDerictive


class BaseConnectionConfig(BaseModel):
    name: str
    type: str
    params: Dict[str, Any]
    include: Optional[List[IncludeDerictive]] = None
    exclude: List[IncludeDerictive] = []

    max_threads: Optional[int] = None
    run_timeout_seconds: Optional[int] = None
    connection_timeout_seconds: Optional[int] = None
    query_timeout_seconds: Optional[int] = None
    exclude_expensive_queries: Optional[bool] = None
    exclude_complex_queries: Optional[bool] = None

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

class AxolotlConfig(BaseModel):
    """Top-level configuration for Axolotl."""
    max_threads: int = 10
    run_timeout_seconds: int = 60
    connection_timeout_seconds: int = 300
    query_timeout_seconds: int = 60
    exclude_expensive_queries: bool = False
    exclude_complex_queries: bool = False

    connections: dict[str, BaseConnectionConfig]


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

    # TODO: later, support conn types that aren't snowflake
    connections: Dict[str, BaseConnectionConfig] = {}

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
            connections[conn_name] = SnowflakeConnectionConfig(**opts)
        else:
            raise ValueError(f"Invalid connection type {conn_type}")

    return AxolotlConfig(
        connections=connections,
        **config,
    )

def parse_include_list(items: List[str] | None) -> List[IncludeDerictive] | None:
    if items is None:
        return None
    for item in items:
        if not isinstance(item, str):
            raise ValueError(f"Include and Exclude rules must be strings. Found {item!r}")
    return [
        IncludeDerictive.from_string(item)
        for item in items
    ]

def load_config(config_path: str | Path = "config.yaml") -> AxolotlConfig:
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
