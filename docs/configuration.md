# Configuration

quack-diff supports configuration via environment variables or a config file.

## Environment variables

```bash
# Snowflake
export QUACK_DIFF_SNOWFLAKE_ACCOUNT="your-account"
export QUACK_DIFF_SNOWFLAKE_USER="your-user"
export QUACK_DIFF_SNOWFLAKE_PASSWORD="your-password"
```

## Config file

Use a `quack-diff.yaml` in the current directory or a path you specify:

```yaml
snowflake:
  account: your-account
  user: your-user
  database: your-database

defaults:
  threshold: 0.0
```

## Defaults

- **threshold**: `0.0` — Maximum allowed fractional difference (e.g. `0.01` = 1%) before the command exits with a non-zero status.
