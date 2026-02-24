import re
from collections import defaultdict, deque
from typing import Any

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from app.db import get_engine

_IDENTIFIER_RE = re.compile(r"^[a-z_][a-z0-9_]*$")
_ON_DELETE_SQL = {
    "restrict": "RESTRICT",
    "cascade": "CASCADE",
    "set null": "SET NULL",
}


class SchemaExecutionError(RuntimeError):
    pass


def execute_schema_proposal(
    proposed_schema: dict[str, Any],
    *,
    mode: str = "dry_run",
) -> dict[str, Any]:
    statements = build_schema_statements(proposed_schema)
    return execute_statements(statements, mode=mode)


def build_schema_statements(proposed_schema: dict[str, Any]) -> list[str]:
    tables = proposed_schema.get("tables", [])
    if not isinstance(tables, list):
        raise SchemaExecutionError("proposed_schema.tables must be an array.")

    ordered_tables = _topological_order_tables(tables)
    statements: list[str] = []

    for table in ordered_tables:
        statements.append(_build_create_table_statement(table))
    for table in ordered_tables:
        statements.extend(_build_index_statements(table))

    return statements


def execute_statements(statements: list[str], *, mode: str = "dry_run") -> dict[str, Any]:
    normalized_mode = mode.strip().lower()
    if normalized_mode not in {"dry_run", "apply"}:
        raise SchemaExecutionError("mode must be one of: dry_run, apply.")

    try:
        engine = get_engine()
    except Exception as exc:
        return {
            "executed": False,
            "mode": normalized_mode,
            "success": False,
            "statement_count": len(statements),
            "statements": statements,
            "error": str(exc),
        }

    should_commit = normalized_mode == "apply"

    try:
        with engine.connect() as connection:
            transaction = connection.begin()
            try:
                for statement in statements:
                    connection.execute(text(statement))
                if should_commit:
                    transaction.commit()
                else:
                    transaction.rollback()
            except Exception:
                transaction.rollback()
                raise
    except SQLAlchemyError as exc:
        return {
            "executed": True,
            "mode": normalized_mode,
            "success": False,
            "statement_count": len(statements),
            "statements": statements,
            "error": str(exc),
        }

    return {
        "executed": True,
        "mode": normalized_mode,
        "success": True,
        "statement_count": len(statements),
        "statements": statements,
        "error": None,
    }


def _build_create_table_statement(table: dict[str, Any]) -> str:
    table_name = _safe_ident(table.get("name"), "table.name")
    columns = table.get("columns", [])
    foreign_keys = table.get("foreign_keys", [])

    if not isinstance(columns, list):
        raise SchemaExecutionError(f"Table '{table_name}' columns must be an array.")
    if not isinstance(foreign_keys, list):
        raise SchemaExecutionError(f"Table '{table_name}' foreign_keys must be an array.")

    column_defs: list[str] = []
    primary_keys: list[str] = []

    for column in columns:
        column_name = _safe_ident(column.get("name"), f"{table_name}.column.name")
        column_type = str(column.get("type", "")).strip()
        if not column_type:
            raise SchemaExecutionError(f"Column '{table_name}.{column_name}' is missing type.")

        nullable = bool(column.get("nullable"))
        is_primary_key = bool(column.get("primary_key"))
        is_unique = bool(column.get("unique"))
        default = column.get("default")

        parts = [_quote_ident(column_name), column_type]
        if default is not None:
            parts.append(f"DEFAULT {default}")
        if not nullable:
            parts.append("NOT NULL")
        if is_unique and not is_primary_key:
            parts.append("UNIQUE")

        column_defs.append(" ".join(parts))
        if is_primary_key:
            primary_keys.append(column_name)

    if primary_keys:
        quoted_pk_columns = ", ".join(_quote_ident(name) for name in primary_keys)
        column_defs.append(f"PRIMARY KEY ({quoted_pk_columns})")

    for foreign_key in foreign_keys:
        fk_column = _safe_ident(foreign_key.get("column"), f"{table_name}.foreign_keys.column")
        ref_table = _safe_ident(
            foreign_key.get("ref_table"),
            f"{table_name}.foreign_keys.ref_table",
        )
        ref_column = _safe_ident(
            foreign_key.get("ref_column"),
            f"{table_name}.foreign_keys.ref_column",
        )
        on_delete = str(foreign_key.get("on_delete", "")).strip().lower()
        on_delete_sql = _ON_DELETE_SQL.get(on_delete)
        if on_delete_sql is None:
            raise SchemaExecutionError(
                f"Foreign key on '{table_name}.{fk_column}' has invalid on_delete '{on_delete}'."
            )

        column_defs.append(
            "FOREIGN KEY ({fk_col}) REFERENCES {ref_table} ({ref_col}) ON DELETE {on_delete}".format(
                fk_col=_quote_ident(fk_column),
                ref_table=_quote_ident(ref_table),
                ref_col=_quote_ident(ref_column),
                on_delete=on_delete_sql,
            )
        )

    inner = ", ".join(column_defs)
    return f"CREATE TABLE IF NOT EXISTS {_quote_ident(table_name)} ({inner})"


def _build_index_statements(table: dict[str, Any]) -> list[str]:
    table_name = _safe_ident(table.get("name"), "table.name")
    indexes = table.get("indexes", [])
    if not isinstance(indexes, list):
        raise SchemaExecutionError(f"Table '{table_name}' indexes must be an array.")

    statements: list[str] = []
    for index in indexes:
        index_name = _safe_ident(index.get("name"), f"{table_name}.indexes.name")
        columns = index.get("columns", [])
        unique = bool(index.get("unique"))

        if not isinstance(columns, list) or not columns:
            raise SchemaExecutionError(
                f"Index '{index_name}' on table '{table_name}' must have at least one column."
            )
        quoted_columns = ", ".join(
            _quote_ident(_safe_ident(column_name, f"{table_name}.{index_name}.columns"))
            for column_name in columns
        )
        unique_sql = "UNIQUE " if unique else ""
        statements.append(
            f"CREATE {unique_sql}INDEX IF NOT EXISTS {_quote_ident(index_name)} "
            f"ON {_quote_ident(table_name)} ({quoted_columns})"
        )
    return statements


def _topological_order_tables(tables: list[dict[str, Any]]) -> list[dict[str, Any]]:
    table_by_name: dict[str, dict[str, Any]] = {}
    dependencies: dict[str, set[str]] = defaultdict(set)
    dependents: dict[str, set[str]] = defaultdict(set)

    for table in tables:
        table_name = _safe_ident(table.get("name"), "table.name")
        if table_name in table_by_name:
            raise SchemaExecutionError(f"Duplicate table name '{table_name}'.")
        table_by_name[table_name] = table

    for table_name, table in table_by_name.items():
        for foreign_key in table.get("foreign_keys", []):
            ref_table = _safe_ident(
                foreign_key.get("ref_table"),
                f"{table_name}.foreign_keys.ref_table",
            )
            if ref_table in table_by_name and ref_table != table_name:
                dependencies[table_name].add(ref_table)
                dependents[ref_table].add(table_name)

    in_degree = {table_name: len(dependencies[table_name]) for table_name in table_by_name}
    queue = deque(sorted(name for name, degree in in_degree.items() if degree == 0))
    ordered: list[dict[str, Any]] = []

    while queue:
        current = queue.popleft()
        ordered.append(table_by_name[current])
        for dependent in sorted(dependents[current]):
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)

    if len(ordered) != len(tables):
        return [table_by_name[name] for name in sorted(table_by_name.keys())]
    return ordered


def _safe_ident(value: Any, label: str) -> str:
    if not isinstance(value, str):
        raise SchemaExecutionError(f"{label} must be a string.")
    name = value.strip().lower()
    if not _IDENTIFIER_RE.match(name):
        raise SchemaExecutionError(
            f"{label} has invalid identifier '{value}'. Use lowercase snake_case identifiers."
        )
    return name


def _quote_ident(identifier: str) -> str:
    return f'"{identifier}"'
