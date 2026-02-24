import os
import unittest
from unittest.mock import patch

from app.gtfs_agent import _minimal_fallback_agent_schema, proposeQueryPlan


class QueryPlanningTests(unittest.TestCase):
    def setUp(self):
        self.schema = _minimal_fallback_agent_schema()

    def test_arrivals_requires_stop_id(self):
        plan = proposeQueryPlan("Show arrivals for this stop", self.schema)
        self.assertIsNotNone(plan["clarifying_question"])
        self.assertIn("stop_id", plan["clarifying_question"])
        self.assertIsNone(plan["sql"])

    def test_row_limit_is_capped_to_max_result_rows(self):
        with patch.dict(os.environ, {"MAX_RESULT_ROWS": "5"}, clear=False):
            schema = _minimal_fallback_agent_schema()
            plan = proposeQueryPlan("list routes top 999", schema)

        self.assertIsNone(plan["clarifying_question"])
        self.assertEqual(plan["template_key"], "list_routes")
        self.assertEqual(plan["safety"]["row_limit"], 5)
        self.assertEqual(plan["params"][0], 5)

    def test_nearby_stops_parses_lat_lon_and_radius(self):
        plan = proposeQueryPlan("nearby stops around 37.7749, -122.4194 within 2 km", self.schema)
        self.assertIsNone(plan["clarifying_question"])
        self.assertEqual(plan["template_key"], "list_stops")
        self.assertAlmostEqual(float(plan["params"][1]), 37.7749, places=4)
        self.assertAlmostEqual(float(plan["params"][2]), -122.4194, places=4)
        self.assertAlmostEqual(float(plan["params"][3]), 2.0, places=2)

    def test_how_many_people_maps_to_stop_service_volume(self):
        plan = proposeQueryPlan("How many people went to Smith & 5th?", self.schema)
        self.assertIsNone(plan["clarifying_question"])
        self.assertEqual(plan["template_key"], "stop_service_volume")
        self.assertEqual(plan["params"][0], None)
        self.assertEqual(plan["params"][1], "Smith & 5th")

    def test_how_many_people_requires_location(self):
        plan = proposeQueryPlan("How many people were at this stop?", self.schema)
        self.assertIsNotNone(plan["clarifying_question"])
        self.assertIn("stop_id or stop_name", plan["clarifying_question"])
        self.assertIsNone(plan["sql"])


if __name__ == "__main__":
    unittest.main()
