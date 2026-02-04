# Security

## SQL Injection Prevention

quack-diff implements comprehensive SQL injection prevention mechanisms to ensure safe handling of user-provided input in SQL queries.

### Overview

SQL injection is a serious security vulnerability where malicious users can manipulate SQL queries by injecting their own SQL code through user input. quack-diff addresses this through:

1. **Input Sanitization** - All SQL identifiers (table names, column names, database names) are validated
2. **Path Validation** - File paths used in `ATTACH` statements are validated
3. **Parameterized Queries** - Where possible, parameterized queries are used
4. **Value Escaping** - String values in IN clauses are properly escaped

### Sanitization Functions

All sanitization functions are available in `quack_diff.core.sql_utils`:

#### `sanitize_identifier(identifier: str) -> str`

Validates and sanitizes SQL identifiers (table names, column names, database aliases).

**Allowed characters:**

- Alphanumeric: `a-z`, `A-Z`, `0-9`
- Underscore: `_`
- Hyphen: `-`
- Dot: `.` (for qualified names like `schema.table`)

**Blocked patterns:**

- SQL injection keywords: `DROP`, `DELETE`, `UNION`, `SELECT ... FROM`, etc.
- SQL comments: `--`, `/* */`
- Statement separators: `;`
- Boolean injection patterns: `OR 1=1`, `AND 1=1`

**Examples:**

```python
from quack_diff.core.sql_utils import sanitize_identifier, SQLInjectionError

# Valid identifiers
sanitize_identifier("users")  # ✓ Returns: "users"
sanitize_identifier("my_schema.my_table")  # ✓ Returns: "my_schema.my_table"
sanitize_identifier("db.schema.table")  # ✓ Returns: "db.schema.table"

# Invalid identifiers (raise SQLInjectionError)
sanitize_identifier("users; DROP TABLE students")  # ✗ SQL injection
sanitize_identifier("users-- comment")  # ✗ SQL comment
sanitize_identifier("users OR 1=1")  # ✗ Boolean injection
sanitize_identifier("table!@#")  # ✗ Invalid characters
```

#### `sanitize_path(path: str) -> str`

Validates file paths used in `ATTACH` statements.

**Allowed characters:**

- Alphanumeric: `a-z`, `A-Z`, `0-9`
- Path separators: `/`, `\`
- Special path chars: `.`, `-`, `_`, `:`, `~`, `()`, `[]`, spaces

**Blocked patterns:**

- SQL injection: `;`, `--`, `/* */`
- Command injection attempts

**Examples:**

```python
from quack_diff.core.sql_utils import sanitize_path

# Valid paths
sanitize_path("/path/to/database.duckdb")  # ✓
sanitize_path("C:/Users/data.db")  # ✓
sanitize_path("./relative/path.db")  # ✓

# Invalid paths (raise SQLInjectionError)
sanitize_path("/path/to/db.duckdb; DROP TABLE")  # ✗ SQL injection
sanitize_path("/path/to/db.duckdb-- comment")  # ✗ SQL comment
```

#### `quote_identifier(identifier: str) -> str`

Sanitizes and quotes an identifier for safe use in SQL queries.

```python
from quack_diff.core.sql_utils import quote_identifier

quote_identifier("table")  # Returns: '"table"'
quote_identifier("my_schema.my_table")  # Returns: '"my_schema"."my_table"'
```

#### `validate_limit(limit: int | None) -> int | None`

Validates `LIMIT` clause values.

```python
from quack_diff.core.sql_utils import validate_limit

validate_limit(10)  # ✓ Returns: 10
validate_limit(None)  # ✓ Returns: None
validate_limit(-1)  # ✗ Raises ValueError
validate_limit(0)  # ✗ Raises ValueError
```

### Implementation Details

#### Connector Layer

The `DuckDBConnector` class sanitizes all user input:

```python
from quack_diff.core.connector import DuckDBConnector

connector = DuckDBConnector()

# Sanitized automatically
connector.attach_duckdb(
    name="other_db",  # Sanitized
    path="/path/to/db.duckdb"  # Sanitized
)

# These would raise SQLInjectionError:
# connector.attach_duckdb("db; DROP TABLE users", "path.db")
# connector.attach_duckdb("db", "/path; DELETE FROM users")
```

#### Query Builder Layer

The `QueryBuilder` class sanitizes all identifiers used in query construction:

```python
from quack_diff.core.query_builder import QueryBuilder

builder = QueryBuilder()

# All identifiers are sanitized
query = builder.build_hash_query(
    table="my_schema.my_table",  # Sanitized
    columns=["id", "name", "email"],  # Each sanitized
    key_column="id"  # Sanitized
)
```

### Remaining Considerations

#### Value Escaping in IN Clauses

Currently, the `build_sample_query` method uses string interpolation for key values in `IN` clauses. To prevent SQL injection, single quotes in values are escaped (`'` → `''`):

```python
# Current implementation
keys = ["value1", "value2"]
escaped_keys = [str(k).replace("'", "''") for k in keys]
formatted_keys = ", ".join(f"'{k}'" for k in escaped_keys)
```

**Future improvement:** Consider using parameterized queries with placeholders for better security.

#### Time-Travel Timestamp Values

Timestamp values in Snowflake time-travel queries are interpolated as strings. These values come from trusted sources (user configuration) but should be validated if exposed directly to end-users.

### Best Practices

1. **Never concatenate user input directly into SQL**

   ```python
   # BAD - Don't do this
   query = f"SELECT * FROM {user_input}"

   # GOOD - Use sanitization
   from quack_diff.core.sql_utils import sanitize_identifier
   safe_table = sanitize_identifier(user_input)
   query = f"SELECT * FROM {safe_table}"
   ```

2. **Use parameterized queries where possible**

   ```python
   # GOOD - DuckDB supports parameterized queries
   connector.execute("SELECT * FROM users WHERE id = ?", [user_id])
   ```

3. **Validate all inputs at the entry point**

   ```python
   # Validate early in your application
   try:
       safe_name = sanitize_identifier(user_table_name)
   except SQLInjectionError as e:
       return error_response(f"Invalid table name: {e}")
   ```

4. **Be especially careful with dynamic identifiers**
   - Table names from user input
   - Column names from user input
   - Database names from user input
   - File paths from user input

### Error Handling

SQL injection attempts raise `SQLInjectionError`, a subclass of `ValueError`:

```python
from quack_diff.core.sql_utils import sanitize_identifier, SQLInjectionError

try:
    safe_name = sanitize_identifier(user_input)
except SQLInjectionError as e:
    print(f"Security error: {e}")
except ValueError as e:
    print(f"Validation error: {e}")
```

### Testing

Comprehensive tests for all sanitization functions are available in `tests/core/test_sql_utils.py`. The tests cover:

- Valid identifier formats
- SQL injection patterns
- Invalid characters
- Edge cases (empty, too long, malformed)
- Path validation
- Value escaping

Run the security tests:

```bash
uv run pytest tests/core/test_sql_utils.py -v
```

### Reporting Security Issues

If you discover a security vulnerability in quack-diff, please report it privately to the maintainers. Do not open a public issue.

## Additional Security Considerations

### File System Access

When attaching DuckDB databases, ensure:

- File paths are validated
- Appropriate file system permissions are set
- Database files are from trusted sources

### Snowflake Credentials

When using Snowflake integration:

- Store credentials securely (use environment variables or `~/.snowflake/connections.toml`)
- Never hardcode credentials in source code
- Use read-only roles when possible
- Limit warehouse access appropriately

### Network Security

For Snowflake connections:

- Connections use TLS by default
- Consider using network policies in Snowflake
- Use IP allowlists where appropriate
