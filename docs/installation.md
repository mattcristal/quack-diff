# Installation

## With uv (recommended)

```bash
uv add quack-diff
```

## With pip

```bash
pip install quack-diff
```

## Snowflake support

For Snowflake connectivity, install the optional extras:

```bash
uv add quack-diff[snowflake]
# or
pip install quack-diff[snowflake]
```

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
