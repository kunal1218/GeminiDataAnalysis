import copy
import json
import unittest
from unittest.mock import patch

from app.schema_synthesis import (
    clearSchemaProposalCache,
    getSchemaOptions,
    isDatabaseQuestion,
    proposeSchemaFromOptions,
    validateProposedSchema,
    SchemaValidationError,
)


def _option_map(schema_options):
    return {option["id"]: option["table"] for option in schema_options["table_options"]}


def _valid_payload_from_options(option_ids):
    schema_options = getSchemaOptions()
    options = _option_map(schema_options)
    return {
        "schema_name": "interview_schema",
        "dialect": "postgres",
        "tables": [copy.deepcopy(options[option_id]) for option_id in option_ids],
        "selected_options": list(option_ids),
        "rationale": "Selected from allowed options only.",
    }


class DatabaseIntentTests(unittest.TestCase):
    def test_is_database_question_true_cases(self):
        true_cases = [
            "Create tables for users and datasets in Postgres",
            "Design a schema with foreign keys and indexes",
            "Write SQL query for joining users and datasets",
            "Show me a Prisma model for this database",
            "Can you help with a migration plan for these tables?",
        ]
        for text in true_cases:
            with self.subTest(text=text):
                self.assertTrue(isDatabaseQuestion(text))

    def test_is_database_question_false_cases(self):
        false_cases = [
            "Tell me a joke about cats",
            "What is the weather in San Francisco?",
            "Can you set the dinner table for tonight?",
            "Write me a cover letter for a product role",
            "Explain Newton's second law",
        ]
        for text in false_cases:
            with self.subTest(text=text):
                self.assertFalse(isDatabaseQuestion(text))


class ValidationTests(unittest.TestCase):
    def test_validation_rejects_invented_fields(self):
        schema_options = getSchemaOptions()
        payload = _valid_payload_from_options(["users_core"])
        payload["tables"][0]["columns"].append(
            {
                "name": "invented_column",
                "type": "text",
                "nullable": True,
                "primary_key": False,
                "unique": False,
                "default": None,
            }
        )

        with self.assertRaises(SchemaValidationError):
            validateProposedSchema(payload, schema_options)


class CachingTests(unittest.TestCase):
    def setUp(self):
        clearSchemaProposalCache()

    def tearDown(self):
        clearSchemaProposalCache()

    def test_caching_prevents_repeated_calls(self):
        schema_options = getSchemaOptions()
        payload = _valid_payload_from_options(["users_core", "datasets_core"])
        raw_response = json.dumps(payload)

        with patch("app.schema_synthesis._call_gemini_schema", return_value=raw_response) as mocked:
            first = proposeSchemaFromOptions("design a users and datasets schema", schema_options)
            second = proposeSchemaFromOptions("design a users and datasets schema", schema_options)

        self.assertEqual(mocked.call_count, 1)
        self.assertEqual(first.schema_name, second.schema_name)
        self.assertEqual(first.selected_options, second.selected_options)


if __name__ == "__main__":
    unittest.main()
