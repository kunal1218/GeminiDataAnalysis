import unittest
from unittest.mock import patch

from app.gtfs_agent import (
    AgentSchemaError,
    SOURCE_OF_TRUTH_SCHEMA,
    _minimal_fallback_agent_schema,
    _validate_agent_schema,
    clearAgentSchemaCache,
    getAgentSchema,
    getAgentSchemaStatus,
    isDatabaseQuestion,
)


class AgentSchemaTests(unittest.TestCase):
    def setUp(self):
        clearAgentSchemaCache()

    def tearDown(self):
        clearAgentSchemaCache()

    def test_is_database_question_true(self):
        self.assertTrue(isDatabaseQuestion("Show arrivals for stop_id 1234"))
        self.assertTrue(isDatabaseQuestion("Please show me what routes there are"))
        self.assertTrue(isDatabaseQuestion("What trips have occurred?"))

    def test_is_database_question_false(self):
        self.assertFalse(isDatabaseQuestion("Write me a haiku about rain"))

    def test_validation_rejects_hallucinated_column(self):
        schema = _minimal_fallback_agent_schema()
        schema["tables"]["routes"]["columns"].append("invented_column")
        errors = _validate_agent_schema(
            schema,
            SOURCE_OF_TRUTH_SCHEMA,
            strict_templates=False,
        )
        self.assertTrue(any("tables.routes.columns" in error for error in errors))

    def test_get_agent_schema_uses_cache(self):
        mocked_schema = _minimal_fallback_agent_schema()
        with patch("app.gtfs_agent._build_agent_schema_uncached", return_value=mocked_schema) as mocked:
            first = getAgentSchema()
            second = getAgentSchema()

        self.assertEqual(mocked.call_count, 1)
        self.assertEqual(first["dialect"], "postgres")
        self.assertEqual(second["dialect"], "postgres")

    def test_get_agent_schema_status_surfaces_gemini_error_detail(self):
        with patch(
            "app.gtfs_agent.proposeAgentSchemaFromTruth",
            side_effect=AgentSchemaError("Gemini request failed with HTTP 401."),
        ):
            schema = getAgentSchema()

        status = getAgentSchemaStatus()
        self.assertEqual(status["source"], "fallback")
        self.assertIn("HTTP 401", status.get("last_error") or "")
        self.assertEqual(schema["dialect"], "postgres")


if __name__ == "__main__":
    unittest.main()
