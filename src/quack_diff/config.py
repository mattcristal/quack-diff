"""Configuration management using Pydantic Settings.

Supports configuration via:
- Environment variables (QUACK_DIFF_ prefix)
- YAML configuration file (quack-diff.yaml)
- CLI arguments (highest priority)
"""

from pathlib import Path
from typing import Any

import yaml
from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class SnowflakeConfig(BaseSettings):
    """Snowflake connection configuration."""

    model_config = SettingsConfigDict(env_prefix="QUACK_DIFF_SNOWFLAKE_")

    account: str | None = Field(default=None, description="Snowflake account identifier")
    user: str | None = Field(default=None, description="Snowflake username")
    password: str | None = Field(default=None, description="Snowflake password")
    database: str | None = Field(default=None, description="Default database")
    schema_name: str | None = Field(
        default=None, alias="schema", description="Default schema"
    )
    warehouse: str | None = Field(default=None, description="Compute warehouse")
    role: str | None = Field(default=None, description="User role")

    def is_configured(self) -> bool:
        """Check if minimum required settings are provided."""
        return all([self.account, self.user, self.password])


class PostgresConfig(BaseSettings):
    """PostgreSQL connection configuration."""

    model_config = SettingsConfigDict(env_prefix="QUACK_DIFF_POSTGRES_")

    connection_string: str | None = Field(
        default=None,
        description="PostgreSQL connection string (postgresql://user:pass@host:port/db)",
    )
    host: str | None = Field(default=None, description="PostgreSQL host")
    port: int = Field(default=5432, description="PostgreSQL port")
    user: str | None = Field(default=None, description="PostgreSQL username")
    password: str | None = Field(default=None, description="PostgreSQL password")
    database: str | None = Field(default=None, description="PostgreSQL database name")

    def get_connection_string(self) -> str | None:
        """Build or return the connection string."""
        if self.connection_string:
            return self.connection_string
        if all([self.host, self.user, self.database]):
            pwd = f":{self.password}" if self.password else ""
            return f"postgresql://{self.user}{pwd}@{self.host}:{self.port}/{self.database}"
        return None

    def is_configured(self) -> bool:
        """Check if minimum required settings are provided."""
        return self.get_connection_string() is not None


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
    postgres: PostgresConfig = Field(default_factory=PostgresConfig)

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
