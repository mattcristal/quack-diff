# SQL Utilities API Reference

## Module: `quack_diff.core.sql_utils`

Provides SQL sanitization and security utilities to prevent SQL injection attacks.

## Functions

### `sanitize_identifier(identifier: str, max_length: int = 255) -> str`

Sanitize a SQL identifier (table name, column name, database name, etc.).

**Parameters:**

- `identifier` (str): The identifier to sanitize
- `max_length` (int, optional): Maximum allowed length. Default: 255

**Returns:**

- str: The validated identifier

**Raises:**

- `SQLInjectionError`: If the identifier contains unsafe characters or patterns
- `ValueError`: If the identifier is empty or too long

**Example:**

```python
from quack_diff.core.sql_utils import sanitize_identifier

# Valid identifiers
sanitize_identifier("users")  # Returns: "users"
sanitize_identifier("my_schema.my_table")  # Returns: "my_schema.my_table"

# Invalid identifiers (raises SQLInjectionError)
sanitize_identifier("users; DROP TABLE")  # Raises: SQLInjectionError
```

---

### `quote_identifier(identifier: str) -> str`

Quote a SQL identifier for safe use in queries.

**Parameters:**

- `identifier` (str): The identifier to quote

**Returns:**

- str: The quoted identifier

**Raises:**

- `SQLInjectionError`: If the identifier is unsafe

**Example:**

```python
from quack_diff.core.sql_utils import quote_identifier

quote_identifier("table")  # Returns: '"table"'
quote_identifier("schema.table")  # Returns: '"schema"."table"'
```

---

### `sanitize_path(path: str) -> str`

Sanitize a file path for use in SQL ATTACH statements.

**Parameters:**

- `path` (str): The file path to sanitize

**Returns:**

- str: The validated path

**Raises:**

- `SQLInjectionError`: If the path contains unsafe patterns
- `ValueError`: If the path is empty

**Example:**

```python
from quack_diff.core.sql_utils import sanitize_path

sanitize_path("/path/to/database.duckdb")  # Returns: "/path/to/database.duckdb"
sanitize_path("./data/mydb.duckdb")  # Returns: "./data/mydb.duckdb"

# Invalid paths (raises SQLInjectionError)
sanitize_path("/path; DROP TABLE")  # Raises: SQLInjectionError
```

---

### `validate_limit(limit: int | None) -> int | None`

Validate a LIMIT value for SQL queries.

**Parameters:**

- `limit` (int | None): The limit value to validate

**Returns:**

- int | None: The validated limit value

**Raises:**

- `ValueError`: If the limit is invalid (negative or zero)

**Example:**

```python
from quack_diff.core.sql_utils import validate_limit

validate_limit(10)  # Returns: 10
validate_limit(None)  # Returns: None
validate_limit(-1)  # Raises: ValueError
```

---

### `build_parameterized_in_clause(values: list[Any]) -> tuple[str, list[Any]]`

Build a parameterized IN clause for safe SQL queries.

**Parameters:**

- `values` (list): List of values for the IN clause

**Returns:**

- tuple[str, list]: Tuple of (placeholders_string, values_list)

**Raises:**

- `ValueError`: If values list is empty

**Example:**

```python
from quack_diff.core.sql_utils import build_parameterized_in_clause

placeholders, params = build_parameterized_in_clause([1, 2, 3])
# placeholders: "?, ?, ?"
# params: [1, 2, 3]

query = f"SELECT * FROM users WHERE id IN ({placeholders})"
result = connector.execute(query, params)
```

---

### `escape_like_pattern(pattern: str) -> str`

Escape special characters in a LIKE pattern.

**Parameters:**

- `pattern` (str): The pattern to escape

**Returns:**

- str: The escaped pattern

**Example:**

```python
from quack_diff.core.sql_utils import escape_like_pattern

escape_like_pattern("test_pattern%")  # Returns: "test\\_pattern\\%"
```

---

## Exception

### `class SQLInjectionError(ValueError)`

Raised when a potential SQL injection attempt is detected.

**Inheritance:** `ValueError` → `SQLInjectionError`

**Example:**

```python
from quack_diff import SQLInjectionError
from quack_diff.core.sql_utils import sanitize_identifier

try:
    sanitize_identifier("users; DROP TABLE students")
except SQLInjectionError as e:
    print(f"Security error: {e}")
    # Output: Security error: Invalid identifier '...': contains SQL injection pattern
```

---

## Security Patterns Detected

The sanitization functions detect and block common SQL injection patterns:

### Statement Separators

- `;` - Multiple statement execution

### SQL Comments

- `--` - Single-line comment
- `/* */` - Multi-line comment

### SQL Keywords

- `DROP`, `DELETE`, `INSERT`, `UPDATE` - Data modification
- `UNION` - Union injection
- `SELECT ... FROM` - Nested queries
- `EXEC`, `CREATE` - Administrative commands

### Boolean Injection

- `OR ... = ...` - OR-based injection
- `AND ... = ...` - AND-based injection

### Invalid Characters

- Special characters: `!@#$%^&*()`
- Quotes in unsafe contexts

---

## Usage Examples

### Basic Sanitization

```python
from quack_diff import DuckDBConnector, SQLInjectionError

connector = DuckDBConnector()

# Safe: Sanitization happens automatically
try:
    connector.attach_duckdb(
        name="my_db",
        path="/path/to/database.duckdb"
    )
except SQLInjectionError as e:
    print(f"Invalid input: {e}")
```

### Query Building

```python
from quack_diff.core.query_builder import QueryBuilder
from quack_diff import SQLInjectionError

builder = QueryBuilder()

try:
    query = builder.build_hash_query(
        table="my_schema.my_table",
        columns=["id", "name", "email"],
        key_column="id"
    )
except SQLInjectionError as e:
    print(f"Invalid identifier: {e}")
```

### Manual Sanitization

```python
from quack_diff.core.sql_utils import sanitize_identifier, sanitize_path

# Validate user input before use
user_table_name = input("Enter table name: ")

try:
    safe_table = sanitize_identifier(user_table_name)
    print(f"Using table: {safe_table}")
except SQLInjectionError as e:
    print(f"Invalid table name: {e}")
```

---

## Best Practices

1. **Always sanitize user input** - Never trust user-provided identifiers or paths
2. **Use parameterized queries** - When dealing with values, use parameterized queries
3. **Validate early** - Sanitize at the entry point of your application
4. **Handle errors gracefully** - Catch `SQLInjectionError` and provide clear feedback
5. **Use automatic sanitization** - The core modules handle sanitization automatically

---

## See Also

- [Security Guide](../security.md) - Comprehensive security documentation
- [API Reference](../index.md) - Main documentation index
