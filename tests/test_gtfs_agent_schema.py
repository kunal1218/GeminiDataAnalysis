import copy
import json
import os
import unittest
from unittest.mock import patch

from app.gtfs_agent import (
    AgentSchemaError,
    _call_gemini_json,
    _normalize_agent_schema,
    SOURCE_OF_TRUTH_SCHEMA,
    _validate_agent_schema,
    clearAgentSchemaCache,
    getAgentSchema,
    getAgentSchemaStatus,
    isDatabaseQuestion,
)


class _FakeHTTPResponse:
    def __init__(self, payload: dict):
        self._payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")


def _sample_agent_schema(max_limit: int = 50) -> dict:
    display_key = "generic_table"
    return {
        "dialect": "postgres",
        "tables": copy.deepcopy(SOURCE_OF_TRUTH_SCHEMA["tables"]),
        "joins": copy.deepcopy(SOURCE_OF_TRUTH_SCHEMA["joins"]),
        "query_templates": [
            {
                "key": "list_routes",
                "description": "List routes.",
                "required_inputs": [],
                "sql_template": "SELECT routes.route_id FROM routes LIMIT LEAST($1, 50)",
                "params": ["limit"],
                "default_limit": min(10, max_limit),
                "display_key": display_key,
                "safety_notes": "Bounded query.",
            },
            {
                "key": "route_details",
                "description": "Route details.",
                "required_inputs": [{"name": "route_id", "type": "string", "notes": "Route ID"}],
                "sql_template": (
                    "SELECT routes.route_id FROM routes "
                    "WHERE routes.route_id = $1 LIMIT LEAST($2, 50)"
                ),
                "params": ["route_id", "limit"],
                "default_limit": 1,
                "display_key": display_key,
                "safety_notes": "Bounded query.",
            },
            {
                "key": "list_stops",
                "description": "List stops.",
                "required_inputs": [],
                "sql_template": "SELECT stops.stop_id FROM stops LIMIT LEAST($1, 50)",
                "params": ["limit"],
                "default_limit": min(10, max_limit),
                "display_key": display_key,
                "safety_notes": "Bounded query.",
            },
            {
                "key": "stop_details",
                "description": "Stop details.",
                "required_inputs": [{"name": "stop_id", "type": "string", "notes": "Stop ID"}],
                "sql_template": (
                    "SELECT stops.stop_id FROM stops "
                    "WHERE stops.stop_id = $1 LIMIT LEAST($2, 50)"
                ),
                "params": ["stop_id", "limit"],
                "default_limit": 1,
                "display_key": display_key,
                "safety_notes": "Bounded query.",
            },
            {
                "key": "stops_on_route",
                "description": "Stops on route.",
                "required_inputs": [{"name": "route_id", "type": "string", "notes": "Route ID"}],
                "sql_template": (
                    "SELECT stops.stop_id FROM stops "
                    "INNER JOIN stop_times ON stop_times.stop_id = stops.stop_id "
                    "INNER JOIN trips ON trips.trip_id = stop_times.trip_id "
                    "WHERE trips.route_id = $1 LIMIT LEAST($2, 50)"
                ),
                "params": ["route_id", "limit"],
                "default_limit": min(10, max_limit),
                "display_key": display_key,
                "safety_notes": "Bounded query.",
            },
            {
                "key": "routes_serving_stop",
                "description": "Routes serving stop.",
                "required_inputs": [{"name": "stop_id", "type": "string", "notes": "Stop ID"}],
                "sql_template": (
                    "SELECT routes.route_id FROM routes "
                    "INNER JOIN trips ON trips.route_id = routes.route_id "
                    "INNER JOIN stop_times ON stop_times.trip_id = trips.trip_id "
                    "WHERE stop_times.stop_id = $1 LIMIT LEAST($2, 50)"
                ),
                "params": ["stop_id", "limit"],
                "default_limit": min(10, max_limit),
                "display_key": display_key,
                "safety_notes": "Bounded query.",
            },
            {
                "key": "arrivals_for_stop",
                "description": "Arrivals for stop.",
                "required_inputs": [{"name": "stop_id", "type": "string", "notes": "Stop ID"}],
                "sql_template": (
                    "SELECT stop_times.arrival_time FROM stop_times "
                    "WHERE stop_times.stop_id = $1 LIMIT LEAST($2, 50)"
                ),
                "params": ["stop_id", "limit"],
                "default_limit": min(10, max_limit),
                "display_key": display_key,
                "safety_notes": "Bounded query.",
            },
            {
                "key": "busiest_stops",
                "description": "Busiest stops.",
                "required_inputs": [],
                "sql_template": (
                    "SELECT stops.stop_id, COUNT(*)::bigint AS c "
                    "FROM stops INNER JOIN stop_times ON stop_times.stop_id = stops.stop_id "
                    "GROUP BY stops.stop_id ORDER BY c DESC LIMIT LEAST($1, 50)"
                ),
                "params": ["limit"],
                "default_limit": min(10, max_limit),
                "display_key": display_key,
                "safety_notes": "Bounded query.",
            },
            {
                "key": "busiest_routes",
                "description": "Busiest routes.",
                "required_inputs": [],
                "sql_template": (
                    "SELECT routes.route_id, COUNT(*)::bigint AS c "
                    "FROM routes INNER JOIN trips ON trips.route_id = routes.route_id "
                    "GROUP BY routes.route_id ORDER BY c DESC LIMIT LEAST($1, 50)"
                ),
                "params": ["limit"],
                "default_limit": min(10, max_limit),
                "display_key": display_key,
                "safety_notes": "Bounded query.",
            },
            {
                "key": "accessible_stops",
                "description": "Accessible stops.",
                "required_inputs": [],
                "sql_template": (
                    "SELECT stops.stop_id FROM stops "
                    "WHERE stops.wheelchair_boarding = 1 LIMIT LEAST($1, 50)"
                ),
                "params": ["limit"],
                "default_limit": min(10, max_limit),
                "display_key": display_key,
                "safety_notes": "Bounded query.",
            },
            {
                "key": "accessible_trips",
                "description": "Accessible trips.",
                "required_inputs": [],
                "sql_template": (
                    "SELECT trips.trip_id FROM trips "
                    "WHERE trips.wheelchair_accessible = 1 LIMIT LEAST($1, 50)"
                ),
                "params": ["limit"],
                "default_limit": min(10, max_limit),
                "display_key": display_key,
                "safety_notes": "Bounded query.",
            },
        ],
        "display_templates": [
            {
                "key": display_key,
                "title_template": "Results ({row_count})",
                "columns": [{"name": "route_id", "label": "Route ID"}],
                "row_id_field": None,
                "formatting": {"time_fields": [], "latlon_fields": [], "color_fields": []},
            }
        ],
        "constraints": {"max_limit": max_limit, "require_limit": True, "no_select_star": True},
    }


class AgentSchemaTests(unittest.TestCase):
    def setUp(self):
        clearAgentSchemaCache()

    def tearDown(self):
        clearAgentSchemaCache()

    def test_is_database_question_true(self):
        self.assertTrue(isDatabaseQuestion("Show arrivals for stop_id 1234"))
        self.assertTrue(isDatabaseQuestion("Please show me what routes there are"))
        self.assertTrue(isDatabaseQuestion("What trips have occurred?"))
        self.assertTrue(isDatabaseQuestion("How many people went to Smith & 5th?"))
        self.assertTrue(isDatabaseQuestion("How many stops do we have?"))

    def test_is_database_question_false(self):
        self.assertFalse(isDatabaseQuestion("Write me a haiku about rain"))

    def test_validation_rejects_hallucinated_column(self):
        schema = _sample_agent_schema()
        schema["tables"]["routes"]["columns"].append("invented_column")
        errors = _validate_agent_schema(
            schema,
            SOURCE_OF_TRUTH_SCHEMA,
            strict_templates=False,
        )
        self.assertTrue(any("tables.routes.columns" in error for error in errors))

    def test_normalize_agent_schema_clamps_max_limit_and_adds_limit(self):
        schema = _sample_agent_schema()
        schema["constraints"]["max_limit"] = 500
        schema["query_templates"][0]["sql_template"] = (
            "SELECT routes.route_id, routes.route_short_name FROM routes"
        )
        schema["query_templates"][1]["sql_template"] = (
            "SELECT stops.stop_id, stops.stop_name FROM stops WHERE ($1::text IS NULL OR stops.stop_name ILIKE '%' || $1 || '%')"
        )
        normalized = _normalize_agent_schema(schema, SOURCE_OF_TRUTH_SCHEMA)
        errors = _validate_agent_schema(
            normalized,
            SOURCE_OF_TRUTH_SCHEMA,
            strict_templates=False,
        )
        self.assertFalse(
            any("must include a bounded LIMIT" in error for error in errors),
            msg=f"unexpected validation errors: {errors}",
        )
        self.assertEqual(normalized["constraints"]["max_limit"], 50)
        self.assertIn("limit 50", normalized["query_templates"][0]["sql_template"].lower())
        self.assertIn("limit 50", normalized["query_templates"][1]["sql_template"].lower())

    def test_get_agent_schema_uses_cache(self):
        mocked_schema = _sample_agent_schema()
        with patch("app.gtfs_agent._build_agent_schema_uncached", return_value=mocked_schema) as mocked:
            first = getAgentSchema()
            second = getAgentSchema()

        self.assertEqual(mocked.call_count, 1)
        self.assertEqual(first["dialect"], "postgres")
        self.assertEqual(second["dialect"], "postgres")

    def test_get_agent_schema_status_falls_back_to_last_known_good_cache(self):
        with patch.dict(os.environ, {"SCHEMA_CACHE_SECONDS": "0"}, clear=False):
            with patch(
                "app.gtfs_agent.proposeAgentSchemaFromTruth",
                return_value=_sample_agent_schema(),
            ):
                first_schema = getAgentSchema()
            with patch(
                "app.gtfs_agent.proposeAgentSchemaFromTruth",
                side_effect=AgentSchemaError("Gemini request failed with HTTP 401."),
            ):
                second_schema = getAgentSchema()
        status = getAgentSchemaStatus()
        self.assertEqual(status["source"], "cached_last_good")
        self.assertIn("HTTP 401", status.get("last_error") or "")
        self.assertEqual(first_schema["dialect"], "postgres")
        self.assertEqual(second_schema["dialect"], "postgres")
        self.assertEqual(first_schema, second_schema)

    def test_call_gemini_json_retries_after_timeout(self):
        call_count = {"value": 0}

        def fake_urlopen(_request, timeout):
            call_count["value"] += 1
            self.assertEqual(timeout, 1.0)
            if call_count["value"] == 1:
                raise TimeoutError("first timeout")
            return _FakeHTTPResponse(
                {
                    "candidates": [
                        {"content": {"parts": [{"text": "{\"ok\":true}"}]}}
                    ]
                }
            )

        with patch.dict(
            os.environ,
            {
                "GEMINI_API_KEY": "test_key",
                "GEMINI_MODEL": "gemini-2.0-flash",
                "GEMINI_TIMEOUT_SECONDS": "1",
                "GEMINI_RETRY_COUNT": "1",
            },
            clear=False,
        ):
            with patch("app.gtfs_agent.urlrequest.urlopen", side_effect=fake_urlopen):
                with patch("app.gtfs_agent.time.sleep", return_value=None):
                    output = _call_gemini_json("test prompt")

        self.assertEqual(call_count["value"], 2)
        self.assertEqual(output, "{\"ok\":true}")


if __name__ == "__main__":
    unittest.main()
