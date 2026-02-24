import unittest
from unittest.mock import patch

from app.main import process_user_message


class HandlerRoutingTests(unittest.TestCase):
    def test_handler_skips_gemini_for_non_db_messages(self):
        with patch("app.main.proposeSchemaFromOptions", side_effect=AssertionError("must not be called")):
            response = process_user_message("Tell me a joke about coding interviews.")

        self.assertFalse(response.is_database_question)
        self.assertIsNone(response.proposed_schema)
        self.assertIn("database schemas", response.assistant_message.lower())


if __name__ == "__main__":
    unittest.main()
