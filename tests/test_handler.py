import unittest
from unittest.mock import patch

from app.main import process_user_message


class HandlerRoutingTests(unittest.TestCase):
    def test_handler_skips_gemini_for_non_db_messages(self):
        with patch("app.main.proposeSchemaFromOptions", side_effect=AssertionError("must not be called")):
            with patch(
                "app.main.execute_schema_proposal",
                side_effect=AssertionError("schema execution must not be called"),
            ):
                response = process_user_message("Tell me a joke about coding interviews.")

        self.assertFalse(response.is_database_question)
        self.assertIsNone(response.proposed_schema)
        self.assertIsNone(response.schema_execution)
        self.assertIn("database schemas", response.assistant_message.lower())

    def test_handler_executes_schema_for_db_messages(self):
        mocked_proposal = {
            "schema_name": "x",
            "dialect": "postgres",
            "tables": [],
            "selected_options": ["users_core"],
            "rationale": "ok",
        }
        mocked_execution = {
            "executed": True,
            "mode": "dry_run",
            "success": True,
            "statement_count": 1,
            "statements": ["CREATE TABLE IF NOT EXISTS \"users\" (...)"],
            "error": None,
        }

        with patch("app.main.proposeSchemaFromOptions") as propose_mock:
            with patch("app.main.proposedSchemaToDict", return_value=mocked_proposal):
                with patch("app.main.execute_schema_proposal", return_value=mocked_execution):
                    propose_mock.return_value = object()
                    response = process_user_message("Design a postgres schema with users table")

        self.assertTrue(response.is_database_question)
        self.assertEqual(response.proposed_schema, mocked_proposal)
        self.assertEqual(response.schema_execution, mocked_execution)
        self.assertIsNone(response.error)
        self.assertIn("succeeded", response.assistant_message.lower())

    def test_handler_reports_execution_failure(self):
        mocked_proposal = {
            "schema_name": "x",
            "dialect": "postgres",
            "tables": [],
            "selected_options": ["users_core"],
            "rationale": "ok",
        }
        mocked_execution = {
            "executed": True,
            "mode": "dry_run",
            "success": False,
            "statement_count": 1,
            "statements": ["CREATE TABLE IF NOT EXISTS \"users\" (...)"],
            "error": "syntax error",
        }

        with patch("app.main.proposeSchemaFromOptions") as propose_mock:
            with patch("app.main.proposedSchemaToDict", return_value=mocked_proposal):
                with patch("app.main.execute_schema_proposal", return_value=mocked_execution):
                    propose_mock.return_value = object()
                    response = process_user_message("Design database tables and schema for users")

        self.assertTrue(response.is_database_question)
        self.assertEqual(response.error, "syntax error")
        self.assertIn("failed", response.assistant_message.lower())


if __name__ == "__main__":
    unittest.main()
