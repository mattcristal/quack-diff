"""Configuration management using Pydantic Settings.

Supports configuration via:
- Environment variables (QUACK_DIFF_ prefix)
- YAML configuration file (quack-diff.yaml)
- Snowflake connections.toml (~/.snowflake/connections.toml)
- CLI arguments (highest priority)
"""

import logging
import tomllib
from pathlib import Path
from typing import Any

import yaml
from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)

# Default path for Snowflake connections.toml
SNOWFLAKE_CONNECTIONS_PATH = Path.home() / ".snowflake" / "connections.toml"


def load_snowflake_connection(
    connection_name: str,
    connections_file: Path | None = None,
) -> dict[str, Any]:
    """Load connection details from Snowflake connections.toml file.

    The connections.toml file follows Snowflake's standard format:
        [connections.my_connection]
        account = "my_account"
        user = "my_user"
        password = "my_password"
        ...

    Or the alternative format:
        [my_connection]
        account = "my_account"
        ...

    Args:
        connection_name: Name of the connection profile to load
        connections_file: Path to connections.toml (defaults to ~/.snowflake/connections.toml)

    Returns:
        Dictionary of connection parameters

    Raises:
        FileNotFoundError: If connections.toml doesn't exist
        KeyError: If connection_name is not found in the file
    """
    file_path = connections_file or SNOWFLAKE_CONNECTIONS_PATH

    if not file_path.exists():
        raise FileNotFoundError(
            f"Snowflake connections file not found: {file_path}\n"
            "Create it at ~/.snowflake/connections.toml or specify a custom path."
        )

    with open(file_path, "rb") as f:
        config = tomllib.load(f)

    # Try both formats: [connections.name] and [name]
    connection_data = None

    # Format 1: [connections.connection_name]
    if "connections" in config and connection_name in config["connections"]:
        connection_data = config["connections"][connection_name]
        logger.debug(f"Found connection '{connection_name}' under [connections] section")

    # Format 2: [connection_name] at root level
    elif connection_name in config:
        connection_data = config[connection_name]
        logger.debug(f"Found connection '{connection_name}' at root level")

    # Format 3: Check for "default" connection name
    elif connection_name == "default":
        if "default" in config:
            connection_data = config["default"]
        elif "connections" in config and "default" in config["connections"]:
            connection_data = config["connections"]["default"]

    if connection_data is None:
        available = []
        if "connections" in config:
            available.extend(config["connections"].keys())
        available.extend(k for k in config if k != "connections" and isinstance(config[k], dict))
        raise KeyError(
            f"Connection '{connection_name}' not found in {file_path}.\n"
            f"Available connections: {', '.join(available) or 'none'}"
        )

    return dict(connection_data)


class SnowflakeConfig(BaseSettings):
    """Snowflake connection configuration.

    Supports multiple credential sources (in priority order):
    1. Explicit parameters (account, user, password, etc.)
    2. Environment variables (QUACK_DIFF_SNOWFLAKE_*)
    3. connection_name -> reads from ~/.snowflake/connections.toml

    Authentication methods:
    - password: Standard username/password (default)
    - externalbrowser: SSO via web browser (SAML 2.0)
    - key_pair: RSA key-based authentication
    """

    model_config = SettingsConfigDict(env_prefix="QUACK_DIFF_SNOWFLAKE_")

    # Connection name from ~/.snowflake/connections.toml
    connection_name: str | None = Field(
        default=None,
        description="Connection profile name from ~/.snowflake/connections.toml",
    )
    connections_file: Path | None = Field(
        default=None,
        description="Custom path to connections.toml file",
    )

    # Authentication method
    authenticator: str | None = Field(
        default=None,
        description="Auth method: 'password' (default), 'externalbrowser' for SSO, 'key_pair'",
    )

    # Explicit credentials (override connection_name if provided)
    account: str | None = Field(default=None, description="Snowflake account identifier")
    user: str | None = Field(default=None, description="Snowflake username")
    password: str | None = Field(default=None, description="Snowflake password")
    database: str | None = Field(default=None, description="Default database")
    schema_name: str | None = Field(default=None, alias="schema", description="Default schema")
    warehouse: str | None = Field(default=None, description="Compute warehouse")
    role: str | None = Field(default=None, description="User role")

    # Key pair authentication
    private_key_path: Path | None = Field(
        default=None,
        description="Path to RSA private key file for key_pair authentication",
    )
    private_key_passphrase: str | None = Field(
        default=None,
        description="Passphrase for encrypted private key",
    )

    # Private: stores loaded connection data
    _connection_loaded: bool = False

    @model_validator(mode="after")
    def load_from_connection_name(self) -> "SnowflakeConfig":
        """Load credentials from connections.toml if connection_name is provided."""
        if self.connection_name and not self._connection_loaded:
            try:
                conn_data = load_snowflake_connection(
                    self.connection_name,
                    self.connections_file,
                )

                # Map connection.toml keys to our config fields
                # Only set values that aren't already explicitly provided
                field_mapping = {
                    "account": "account",
                    "accountname": "account",  # Alternative key
                    "user": "user",
                    "username": "user",  # Alternative key
                    "password": "password",
                    "database": "database",
                    "schema": "schema_name",
                    "warehouse": "warehouse",
                    "warehousename": "warehouse",  # Alternative key
                    "role": "role",
                    "rolename": "role",  # Alternative key
                    "authenticator": "authenticator",
                    "private_key_path": "private_key_path",
                    "privatekey": "private_key_path",  # Alternative key
                    "private_key": "private_key_path",  # Alternative key
                    "private_key_passphrase": "private_key_passphrase",
                    "privatekeypassphrase": "private_key_passphrase",  # Alternative key
                }

                for toml_key, config_field in field_mapping.items():
                    if toml_key in conn_data:
                        current_value = getattr(self, config_field)
                        if current_value is None:
                            object.__setattr__(self, config_field, conn_data[toml_key])
                            logger.debug(
                                f"Loaded {config_field} from connection '{self.connection_name}'"
                            )

                object.__setattr__(self, "_connection_loaded", True)
                logger.info(f"Loaded Snowflake connection profile: {self.connection_name}")

            except (FileNotFoundError, KeyError) as e:
                logger.warning(f"Could not load connection '{self.connection_name}': {e}")

        return self

    def is_configured(self) -> bool:
        """Check if minimum required settings are provided.

        Requirements vary by authentication method:
        - password (default): account, user, password
        - externalbrowser: account only (user optional, browser handles auth)
        - key_pair: account, user, private_key_path
        """
        if not self.account:
            return False

        auth_method = (self.authenticator or "").lower()

        # External browser SSO - only account required
        if auth_method in ("externalbrowser", "ext_browser"):
            return True

        # Key pair authentication
        if auth_method == "key_pair":
            return all([self.user, self.private_key_path])

        # Password authentication (default)
        return all([self.user, self.password])


class DatabaseConfig(BaseSettings):
    """Configuration for an attached database.

    Supports multiple database types:
    - snowflake: Uses global snowflake config or explicit connection params
    - duckdb: Attaches a local DuckDB file

    Example YAML:
        databases:
          sf:
            type: snowflake
            # Uses global snowflake config
          local:
            type: duckdb
            path: ./data/local.duckdb
    """

    model_config = SettingsConfigDict(extra="allow")

    type: str = Field(
        default="snowflake",
        description="Database type: 'snowflake' or 'duckdb'",
    )
    path: Path | None = Field(
        default=None,
        description="Path to DuckDB database file (for type='duckdb')",
    )
    connection_name: str | None = Field(
        default=None,
        description="Snowflake connection profile from ~/.snowflake/connections.toml",
    )
    # Additional Snowflake fields (override global config)
    account: str | None = Field(default=None, description="Snowflake account identifier")
    user: str | None = Field(default=None, description="Snowflake username")
    password: str | None = Field(default=None, description="Snowflake password")
    database: str | None = Field(default=None, description="Snowflake database")
    schema_name: str | None = Field(default=None, alias="schema", description="Snowflake schema")
    warehouse: str | None = Field(default=None, description="Snowflake warehouse")
    role: str | None = Field(default=None, description="Snowflake role")
    authenticator: str | None = Field(default=None, description="Authentication method")


class DiffDefaults(BaseSettings):
    """Default settings for diff operations."""

    model_config = SettingsConfigDict(env_prefix="QUACK_DIFF_")

    threshold: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="Default mismatch threshold (0.0 = exact match, 0.01 = 1% tolerance)",
    )
    sample_size: int | None = Field(
        default=None,
        gt=0,
        description="Maximum rows to compare (None = all rows)",
    )
    hash_algorithm: str = Field(
        default="md5",
        description="Hashing algorithm for row comparison",
    )
    null_sentinel: str = Field(
        default="<NULL>",
        description="Sentinel value for NULL representation in hashes",
    )
    column_delimiter: str = Field(
        default="|#|",
        description="Delimiter between column values in hash concatenation",
    )


class Settings(BaseSettings):
    """Main application settings."""

    model_config = SettingsConfigDict(
        env_prefix="QUACK_DIFF_",
        env_nested_delimiter="__",
        extra="ignore",
    )

    # Database configurations
    snowflake: SnowflakeConfig = Field(default_factory=SnowflakeConfig)

    # Databases to auto-attach (keys are aliases, values are database configs)
    databases: dict[str, dict[str, Any]] = Field(
        default_factory=dict,
        description="Databases to auto-attach. Keys are aliases, values are database configs.",
    )

    # Diff defaults
    defaults: DiffDefaults = Field(default_factory=DiffDefaults)

    # Config file path
    config_file: Path | None = Field(
        default=None,
        description="Path to YAML configuration file",
    )

    # Verbosity
    verbose: bool = Field(default=False, description="Enable verbose output")
    debug: bool = Field(default=False, description="Enable debug mode")

    @model_validator(mode="before")
    @classmethod
    def load_yaml_config(cls, data: dict[str, Any]) -> dict[str, Any]:
        """Load configuration from YAML file if specified or exists in default locations."""
        config_file = data.get("config_file")

        # Check default locations if not explicitly specified
        if config_file is None:
            default_locations = [
                Path("quack-diff.yaml"),
                Path("quack-diff.yml"),
                Path.home() / ".quack-diff.yaml",
                Path.home() / ".config" / "quack-diff" / "config.yaml",
            ]
            for loc in default_locations:
                if loc.exists():
                    config_file = loc
                    break

        if config_file is not None:
            config_path = Path(config_file)
            if config_path.exists():
                with open(config_path) as f:
                    yaml_config = yaml.safe_load(f) or {}

                # Merge YAML config with environment/CLI config
                # Environment variables take precedence
                for key, value in yaml_config.items():
                    if key not in data or data[key] is None:
                        data[key] = value

        return data


# Global settings instance (lazy loaded)
_settings: Settings | None = None


def get_settings(config_file: Path | None = None, **overrides: Any) -> Settings:
    """Get or create the settings instance.

    Args:
        config_file: Optional path to YAML configuration file
        **overrides: Additional settings to override

    Returns:
        Settings instance
    """
    global _settings

    if _settings is None or config_file is not None or overrides:
        settings_data: dict[str, Any] = {}
        if config_file:
            settings_data["config_file"] = config_file
        settings_data.update(overrides)
        _settings = Settings(**settings_data)

    return _settings


def reset_settings() -> None:
    """Reset the global settings instance (useful for testing)."""
    global _settings
    _settings = None
