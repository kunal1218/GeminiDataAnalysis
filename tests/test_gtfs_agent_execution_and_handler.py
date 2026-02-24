import unittest
from unittest.mock import patch

from app.gtfs_agent import executeParameterizedQuery
from app.main import process_user_message, warm_agent_schema


class _FakeRow:
    def __init__(self, payload):
        self._mapping = payload


class _FakeResult:
    def __init__(self, rows):
        self._rows = [_FakeRow(item) for item in rows]

    def __iter__(self):
        return iter(self._rows)


class _FakeConnection:
    def __init__(self, rows):
        self._rows = rows
        self.last_sql = None
        self.last_params = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, statement, params):
        self.last_sql = str(statement)
        self.last_params = params
        return _FakeResult(self._rows)


class _FakeEngine:
    def __init__(self, connection):
        self._connection = connection

    def connect(self):
        return self._connection


class ExecutionAndHandlerTests(unittest.TestCase):
    def test_execute_parameterized_query_uses_bound_params(self):
        fake_connection = _FakeConnection([{"route_id": "1"}, {"route_id": "2"}])
        fake_engine = _FakeEngine(fake_connection)
        query_plan = {
            "sql": "SELECT routes.route_id FROM routes LIMIT LEAST($1, 50)",
            "params": [2],
            "safety": {"row_limit": 2},
        }

        with patch("app.gtfs_agent.get_engine", return_value=fake_engine):
            result = executeParameterizedQuery(query_plan)

        self.assertTrue(result["success"])
        self.assertEqual(result["row_count"], 2)
        self.assertIn(":p1", fake_connection.last_sql)
        self.assertEqual(fake_connection.last_params["p1"], 2)

    def test_process_user_message_non_db_skips_agent_schema(self):
        with patch("app.main.getAgentSchema", side_effect=AssertionError("must not call")):
            response = process_user_message("Tell me a joke about transit.")

        self.assertFalse(response.is_database_question)
        self.assertFalse(response.query_executed)
        self.assertIsNone(response.agent_schema)

    def test_process_user_message_db_returns_display_payload(self):
        agent_schema = {
            "display_templates": [
                {
                    "key": "routes_table",
                    "title_template": "Routes ({row_count})",
                    "columns": [{"name": "route_id", "label": "Route ID"}],
                    "row_id_field": "route_id",
                    "formatting": {"time_fields": [], "latlon_fields": [], "color_fields": []},
                }
            ]
        }
        query_plan = {
            "sql": "SELECT routes.route_id FROM routes LIMIT LEAST($1, 50)",
            "params": [1],
            "display_key": "routes_table",
            "param_map": {"limit": 1},
            "clarifying_question": None,
        }
        execution_result = {
            "executed": True,
            "success": True,
            "rows": [{"route_id": "10"}],
            "row_count": 1,
            "columns": ["route_id"],
            "error": None,
        }

        with patch("app.main.getAgentSchema", return_value=agent_schema):
            with patch("app.main.proposeQueryPlan", return_value=query_plan):
                with patch("app.main.executeParameterizedQuery", return_value=execution_result):
                    response = process_user_message("list routes")

        self.assertTrue(response.is_database_question)
        self.assertTrue(response.query_executed)
        self.assertEqual(response.row_count, 1)
        self.assertEqual(response.display["title"], "Routes (1)")
        self.assertEqual(response.display["rows"][0]["route_id"], "10")

    def test_process_user_message_db_failure_returns_useful_hint(self):
        agent_schema = {
            "display_templates": [
                {
                    "key": "routes_table",
                    "title_template": "Routes ({row_count})",
                    "columns": [{"name": "route_id", "label": "Route ID"}],
                    "row_id_field": "route_id",
                    "formatting": {"time_fields": [], "latlon_fields": [], "color_fields": []},
                }
            ]
        }
        query_plan = {
            "sql": "SELECT routes.route_id FROM routes LIMIT LEAST($1, 50)",
            "params": [1],
            "display_key": "routes_table",
            "param_map": {"limit": 1},
            "clarifying_question": None,
        }
        execution_result = {
            "executed": True,
            "success": False,
            "rows": [],
            "row_count": 0,
            "columns": [],
            "error": (
                "(psycopg2.OperationalError) could not translate host name "
                "\"postgres.railway.internal\" to address"
            ),
        }

        with patch("app.main.getAgentSchema", return_value=agent_schema):
            with patch(
                "app.main.getAgentSchemaStatus",
                return_value={"source": "fallback", "last_error": None, "cache_age_seconds": 3},
            ):
                with patch("app.main.proposeQueryPlan", return_value=query_plan):
                    with patch("app.main.executeParameterizedQuery", return_value=execution_result):
                        response = process_user_message("arrivals for stop_id 1234")

        self.assertTrue(response.is_database_question)
        self.assertTrue(response.query_executed)
        self.assertIn("public connection url", response.assistant_message.lower())
        self.assertIsNotNone(response.display)

    def test_execute_parameterized_query_handles_runtime_config_error(self):
        query_plan = {
            "sql": "SELECT routes.route_id FROM routes LIMIT LEAST($1, 50)",
            "params": [1],
            "safety": {"row_limit": 1},
        }

        with patch(
            "app.gtfs_agent.get_engine",
            side_effect=RuntimeError(
                "Detected Railway internal DB host outside Railway runtime."
            ),
        ):
            result = executeParameterizedQuery(query_plan)

        self.assertFalse(result["success"])
        self.assertIn("outside Railway runtime", result["error"])

    def test_warm_agent_schema_does_not_crash_on_db_config_error(self):
        with patch(
            "app.main.validate_database_config",
            side_effect=RuntimeError("missing DATABASE_PUBLIC_URL"),
        ):
            with patch("app.main.verify_database_connection", side_effect=AssertionError("must not call")):
                warm_agent_schema()


if __name__ == "__main__":
    unittest.main()
