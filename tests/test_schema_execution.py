import copy
import unittest
from unittest.mock import patch

from app.schema_options import getSchemaOptions
from app.schema_execution import build_schema_statements, execute_statements


def _option_map(schema_options):
    return {option["id"]: option["table"] for option in schema_options["table_options"]}


def _payload(option_ids):
    options = _option_map(getSchemaOptions())
    return {
        "schema_name": "interview_schema",
        "dialect": "postgres",
        "tables": [copy.deepcopy(options[option_id]) for option_id in option_ids],
        "selected_options": list(option_ids),
        "rationale": "selected",
    }


class _FakeTransaction:
    def __init__(self):
        self.committed = False
        self.rolled_back = False

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


class _FakeConnection:
    def __init__(self):
        self.transaction = _FakeTransaction()
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def begin(self):
        return self.transaction

    def execute(self, statement):
        self.executed.append(str(statement))


class _FakeEngine:
    def __init__(self, connection):
        self.connection = connection

    def connect(self):
        return self.connection


class SchemaExecutionTests(unittest.TestCase):
    def test_build_schema_statements_includes_create_and_indexes(self):
        payload = _payload(["users_core", "datasets_core"])
        statements = build_schema_statements(payload)
        sql_blob = "\n".join(statements).lower()

        self.assertIn('create table if not exists "users"', sql_blob)
        self.assertIn('create table if not exists "datasets"', sql_blob)
        self.assertIn('create unique index if not exists "users_email_idx"', sql_blob)
        self.assertIn('create index if not exists "datasets_owner_idx"', sql_blob)

    def test_execute_statements_dry_run_rolls_back(self):
        fake_connection = _FakeConnection()
        fake_engine = _FakeEngine(fake_connection)
        statements = ['CREATE TABLE IF NOT EXISTS "users" ("id" uuid NOT NULL)']

        with patch("app.schema_execution.get_engine", return_value=fake_engine):
            result = execute_statements(statements, mode="dry_run")

        self.assertTrue(result["success"])
        self.assertTrue(fake_connection.transaction.rolled_back)
        self.assertFalse(fake_connection.transaction.committed)
        self.assertEqual(result["statement_count"], 1)

    def test_execute_statements_apply_commits(self):
        fake_connection = _FakeConnection()
        fake_engine = _FakeEngine(fake_connection)
        statements = ['CREATE TABLE IF NOT EXISTS "users" ("id" uuid NOT NULL)']

        with patch("app.schema_execution.get_engine", return_value=fake_engine):
            result = execute_statements(statements, mode="apply")

        self.assertTrue(result["success"])
        self.assertTrue(fake_connection.transaction.committed)
        self.assertFalse(fake_connection.transaction.rolled_back)


if __name__ == "__main__":
    unittest.main()
