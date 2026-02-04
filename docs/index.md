# quack-diff

Regression testing tool for modern data warehouses, powered by duck-db

<!-- markdownlint-disable-next-line MD057 -->
![quack-diff logo](docs/images/quack_diff.jpg)

## Features

- **DuckDB-First Architecture**: Uses DuckDB as a universal adapter for database connectivity
- **Dialect-Safe Hashing**: Handles NULL values and type mismatches correctly across databases
- **Time-Travel Support**: Compare data against historical snapshots (Snowflake, Delta Lake)
- **CI/CD Ready**: Exit codes for pipeline integration, environment variable configuration
- **User-Friendly**: Rich terminal output with beautiful diff tables

## Quick Start

### Basic compare

```bash
# Compare two tables
quack-diff compare --source db1.users --target db2.users --key id

# Compare with threshold (allow up to 1% difference)
quack-diff compare --source prod.orders --target dev.orders --key order_id --threshold 0.01
```

### Time-Travel compare (Snowflake)

```bash
# Compare current data with 5 minutes ago
quack-diff compare \
  --source snowflake.orders \
  --target snowflake.orders \
  --source-at "5 minutes ago" \
  --key order_id
```

### Schema Comparison

```bash
quack-diff schema --source db1.users --target db2.users
```

## How It Works

quack-diff leverages DuckDB's extension system to connect to external databases:

1. **Attach**: Mount remote databases using DuckDB extensions (snowflake)
2. **Hash**: Generate row-level hashes using dialect-safe SQL (handles NULLs, type coercion)
3. **Compare**: Identify mismatches by comparing hash values
4. **Report**: Display results in beautiful terminal tables

## Next steps

- [Installation](installation.md) — Install quack-diff with uv or pip
- [Configuration](configuration.md) — Environment variables and config file
