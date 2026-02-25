# quack-diff

[![CI](https://github.com/mattcristal/quack-diff/actions/workflows/ci.yml/badge.svg?event=push)](https://github.com/mattcristal/quack-diff/actions/workflows/ci.yml?query=branch%3Amain)
[![codecov](https://codecov.io/gh/mattcristal/quack-diff/graph/badge.svg)](https://codecov.io/gh/mattcristal/quack-diff)

![quack-diff logo](images/quack_diff.jpg)

The zero-dependency regression testing tool for modern data warehouses.

> [!WARNING]
> ⚠️ This project is high work-in-progress.

---

**Documentation**: [github.io/quack-diff](https://mattcristal.github.io/quack-diff/)

---

## Features

- **DuckDB-First Architecture**: Uses DuckDB as a universal adapter for database connectivity
- **Dialect-Safe Hashing**: Handles NULL values and type mismatches correctly across databases
- **Time-Travel Support**: Compare data against historical snapshots (Snowflake, Delta Lake)
- **CI/CD Ready**: Exit codes for pipeline integration, environment variable configuration
- **User-Friendly**: Rich terminal output with beautiful diff tables

## Installation

```bash
uv add quack-diff
```

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

### Count check (bronze/silver/gold)

Validate that pipeline layers have the same number of rows or distinct keys (no row loss):

```bash
# Same row count across layers
quack-diff count -t bronze.orders -t silver.orders -t gold.orders

# Same distinct ID count
quack-diff count -t bronze.orders -t silver.orders -t gold.orders --key order_id

# Per-table GROUP BY (count distinct groups in second table)
quack-diff count \
  -t sf.GOLD.FCT_INVOICE \
  -t "sf.RAW.INVOICE_LINES[salesid,linenum,tariffcode,linestartdate]"

# Add SUM validation alongside row counts
quack-diff count \
  -t sf.GOLD.FCT_INVOICE \
  -t "sf.RAW.INVOICE_LINES[salesid,linenum]" \
  --sum-column QUANTITY --sum-column qty

# JSON for CI/CD
quack-diff count -t bronze.orders -t silver.orders -t gold.orders --key order_id --json
```

The output shows a **per-metric status** (Count, Sum) plus a **global status**:

```text
╭──────────────────────── Count Summary ────────────────────────╮
│   Table              Count                   Sum              │
│   gold.fct_invoice   277,583    252,068,690.39               │
│   raw.invoice_lines  277,583    252,068,690.39               │
│                                                               │
│   Count               MATCH                  MATCH            │
│   Status              MATCH                                   │
╰───────────────────────────────────────────────────────────────╯
```

#### Thresholds

Use `--count-threshold` and `--sum-threshold` to allow small differences. Each accepts a **percentage** (e.g. `5%`) or an **absolute** value (e.g. `100`):

```bash
# Allow up to 1% difference on counts and 500 absolute on sums
quack-diff count \
  -t bronze.orders -t silver.orders \
  --sum-column amount \
  --count-threshold 1% \
  --sum-threshold 500
```

When values differ but fall within the threshold the status shows **PASS (within …)** instead of MATCH.

## Configuration

quack-diff supports configuration via environment variables:

```bash
# Snowflake
export QUACK_DIFF_SNOWFLAKE_ACCOUNT="your-account"
export QUACK_DIFF_SNOWFLAKE_USER="your-user"
export QUACK_DIFF_SNOWFLAKE_PASSWORD="your-password"
```

Or via a `quack-diff.yaml` configuration file:

```yaml
snowflake:
  account: your-account
  user: your-user
  database: your-database

defaults:
  threshold: 0.0
```

## How It Works

quack-diff leverages DuckDB's extension system to connect to external databases:

1. **Attach**: Mount remote databases using DuckDB extensions (snowflake)
2. **Hash**: Generate row-level hashes using dialect-safe SQL (handles NULLs, type coercion)
3. **Compare**: Identify mismatches by comparing hash values
4. **Report**: Display results in beautiful terminal tables

## Development

```bash
# Clone the repository
git clone https://github.com/matteorenoldi/quack-diff.git
cd quack-diff

# Install with dev dependencies
uv sync

# Install pre-commit hooks
uv run prek install

# Run tests
uv run pytest

# Lint
uv run ruff check .
```

## License

MIT License - see [LICENSE](LICENSE) for details.
