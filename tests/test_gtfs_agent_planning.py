import os
import unittest
from unittest.mock import patch

from app.gtfs_agent import proposeQueryPlan


def _planning_schema(max_limit: int = 50) -> dict:
    return {
        "dialect": "postgres",
        "tables": {
            "routes": {
                "columns": [
                    "route_id",
                    "agency_id",
                    "route_short_name",
                    "route_long_name",
                    "route_desc",
                    "route_type",
                    "route_url",
                    "route_color",
                    "route_text_color",
                    "route_sort_order",
                ]
            },
            "trips": {
                "columns": [
                    "route_id",
                    "service_id",
                    "trip_id",
                    "trip_headsign",
                    "direction_id",
                    "direction",
                    "block_id",
                    "shape_id",
                    "wheelchair_accessible",
                    "branch_letter",
                    "boarding_type",
                ]
            },
            "stop_times": {
                "columns": [
                    "trip_id",
                    "arrival_time",
                    "departure_time",
                    "stop_id",
                    "stop_sequence",
                    "pickup_type",
                    "drop_off_type",
                    "timepoint",
                    "shape_dist_traveled",
                ]
            },
            "stops": {
                "columns": [
                    "stop_id",
                    "stop_code",
                    "stop_name",
                    "stop_desc",
                    "stop_lat",
                    "stop_lon",
                    "stop_url",
                    "location_type",
                    "wheelchair_boarding",
                    "platform_code",
                    "parent_station",
                    "level_id",
                    "zone_id",
                ]
            },
        },
        "joins": [
            {
                "left_table": "routes",
                "left_column": "route_id",
                "right_table": "trips",
                "right_column": "route_id",
                "type": "inner",
            },
            {
                "left_table": "trips",
                "left_column": "trip_id",
                "right_table": "stop_times",
                "right_column": "trip_id",
                "type": "inner",
            },
            {
                "left_table": "stops",
                "left_column": "stop_id",
                "right_table": "stop_times",
                "right_column": "stop_id",
                "type": "inner",
            },
            {
                "left_table": "stops",
                "left_column": "parent_station",
                "right_table": "stops",
                "right_column": "stop_id",
                "type": "left",
                "alias_right_table": "parent_stop",
            },
        ],
        "query_templates": [
            {
                "key": "list_routes",
                "description": "List GTFS routes.",
                "required_inputs": [],
                "sql_template": "SELECT routes.route_id FROM routes LIMIT LEAST($1, 50)",
                "params": ["limit"],
                "default_limit": min(25, max_limit),
                "display_key": "routes_table",
                "safety_notes": "Bounded query.",
            },
            {
                "key": "list_stops",
                "description": "List stops with optional nearby bounding box filter.",
                "required_inputs": [],
                "sql_template": (
                    "SELECT stops.stop_id, stops.stop_name, stops.stop_lat, stops.stop_lon "
                    "FROM stops "
                    "WHERE ($2::double precision IS NULL OR $3::double precision IS NULL "
                    "OR ($4::double precision IS NOT NULL AND "
                    "stops.stop_lat BETWEEN ($2 - ($4 / 111.0)) AND ($2 + ($4 / 111.0)) "
                    "AND stops.stop_lon BETWEEN ($3 - ($4 / NULLIF(111.0 * COS(RADIANS($2)), 0))) "
                    "AND ($3 + ($4 / NULLIF(111.0 * COS(RADIANS($2)), 0))))) "
                    "ORDER BY stops.stop_name LIMIT LEAST($1, 50)"
                ),
                "params": ["limit", "lat", "lon", "radius_km"],
                "default_limit": min(50, max_limit),
                "display_key": "stops_table",
                "safety_notes": "Bounded query.",
            },
            {
                "key": "busiest_stops",
                "description": "Top stops by scheduled stop events.",
                "required_inputs": [],
                "sql_template": (
                    "SELECT stops.stop_id, stops.stop_name, COUNT(*)::bigint AS scheduled_stop_events "
                    "FROM stop_times INNER JOIN stops ON stops.stop_id = stop_times.stop_id "
                    "GROUP BY stops.stop_id, stops.stop_name "
                    "ORDER BY scheduled_stop_events DESC LIMIT LEAST($1, 50)"
                ),
                "params": ["limit"],
                "default_limit": min(10, max_limit),
                "display_key": "busiest_stops_table",
                "safety_notes": "Bounded query.",
            },
            {
                "key": "stop_service_volume",
                "description": "Estimate service volume for stop.",
                "required_inputs": [
                    {
                        "name": "stop_id|stop_name",
                        "type": "string",
                        "notes": "Provide stop id or stop name.",
                    }
                ],
                "sql_template": (
                    "SELECT COALESCE(MAX(stops.stop_name), $2, $1) AS stop_label, COUNT(*)::bigint AS scheduled_stop_events "
                    "FROM stop_times INNER JOIN stops ON stops.stop_id = stop_times.stop_id "
                    "WHERE (($1::text IS NOT NULL AND stop_times.stop_id = $1) "
                    "OR ($2::text IS NOT NULL AND stops.stop_name ILIKE '%' || $2 || '%')) LIMIT 1"
                ),
                "params": ["stop_id", "stop_name"],
                "default_limit": 1,
                "display_key": "stop_volume_summary",
                "safety_notes": "Bounded query.",
            },
            {
                "key": "arrivals_for_stop",
                "description": "Arrivals for stop.",
                "required_inputs": [{"name": "stop_id", "type": "string", "notes": "Stop ID"}],
                "sql_template": (
                    "SELECT stop_times.arrival_time, stop_times.departure_time "
                    "FROM stop_times "
                    "WHERE stop_times.stop_id = $1 ORDER BY stop_times.arrival_time LIMIT LEAST($2, 50)"
                ),
                "params": ["stop_id", "limit"],
                "default_limit": min(30, max_limit),
                "display_key": "arrivals_table",
                "safety_notes": "Bounded query.",
            },
        ],
        "display_templates": [
            {
                "key": "routes_table",
                "title_template": "Routes ({row_count})",
                "columns": [{"name": "route_id", "label": "Route ID"}],
                "row_id_field": "route_id",
                "formatting": {"time_fields": [], "latlon_fields": [], "color_fields": []},
            },
            {
                "key": "stops_table",
                "title_template": "Stops ({row_count})",
                "columns": [{"name": "stop_id", "label": "Stop ID"}],
                "row_id_field": "stop_id",
                "formatting": {"time_fields": [], "latlon_fields": [], "color_fields": []},
            },
            {
                "key": "busiest_stops_table",
                "title_template": "Busiest ({row_count})",
                "columns": [{"name": "stop_id", "label": "Stop ID"}],
                "row_id_field": "stop_id",
                "formatting": {"time_fields": [], "latlon_fields": [], "color_fields": []},
            },
            {
                "key": "stop_volume_summary",
                "title_template": "Volume ({row_count})",
                "columns": [{"name": "scheduled_stop_events", "label": "Events"}],
                "row_id_field": None,
                "formatting": {"time_fields": [], "latlon_fields": [], "color_fields": []},
            },
            {
                "key": "arrivals_table",
                "title_template": "Arrivals ({row_count})",
                "columns": [{"name": "arrival_time", "label": "Arrival"}],
                "row_id_field": None,
                "formatting": {"time_fields": [], "latlon_fields": [], "color_fields": []},
            },
        ],
        "constraints": {"max_limit": max_limit, "require_limit": True, "no_select_star": True},
    }


class QueryPlanningTests(unittest.TestCase):
    def setUp(self):
        self.schema = _planning_schema()

    def test_arrivals_requires_stop_id(self):
        plan = proposeQueryPlan("Show arrivals for this stop", self.schema)
        self.assertIsNotNone(plan["clarifying_question"])
        self.assertIn("stop_id", plan["clarifying_question"])
        self.assertIsNone(plan["sql"])

    def test_row_limit_is_capped_to_max_result_rows(self):
        with patch.dict(os.environ, {"MAX_RESULT_ROWS": "5"}, clear=False):
            schema = _planning_schema(max_limit=5)
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

    def test_gemini_planner_possible_query(self):
        feasibility_payload = '{"possible": true, "reason": "can answer from stops headers"}'
        sql_payload = (
            '{"sql": "SELECT stops.stop_id, stops.stop_name '
            'FROM stops WHERE stops.stop_name ILIKE \'%\' || $1 || \'%\' LIMIT LEAST($2, 50)", '
            '"params": ["Smith & 5th", 25], "row_limit": 25, "reason": "matched stop_name"}'
        )
        with patch.dict(os.environ, {"GEMINI_API_KEY": "test"}, clear=False):
            with patch(
                "app.gtfs_agent._call_gemini_json",
                side_effect=[feasibility_payload, sql_payload],
            ) as mocked_call:
                plan = proposeQueryPlan("find Smith & 5th stop", self.schema)

        self.assertIsNone(plan["clarifying_question"])
        self.assertEqual(plan["template_key"], "gemini_generated")
        self.assertIn("from stops", plan["sql"].lower())
        self.assertEqual(plan["params"][0], "Smith & 5th")
        self.assertEqual(mocked_call.call_count, 2)
        first_call_kwargs = mocked_call.call_args_list[0].kwargs
        self.assertEqual(first_call_kwargs.get("retry_count"), 0)

    def test_gemini_planner_not_possible(self):
        feasibility_payload = '{"possible": false, "reason": "not in data"}'
        with patch.dict(os.environ, {"GEMINI_API_KEY": "test"}, clear=False):
            with patch(
                "app.gtfs_agent._call_gemini_json",
                side_effect=[feasibility_payload],
            ) as mocked_call:
                plan = proposeQueryPlan("what was fare revenue by day", self.schema)

        self.assertEqual(plan["clarifying_question"], "not possible")
        self.assertIsNone(plan["sql"])
        self.assertEqual(plan["not_possible_reason"], "not in data")
        self.assertEqual(mocked_call.call_count, 1)

    def test_gemini_not_possible_still_uses_known_template(self):
        feasibility_payload = '{"possible": false, "reason": "could not map intent"}'
        with patch.dict(os.environ, {"GEMINI_API_KEY": "test"}, clear=False):
            with patch(
                "app.gtfs_agent._call_gemini_json",
                side_effect=[feasibility_payload],
            ) as mocked_call:
                plan = proposeQueryPlan("top 10 busiest stops", self.schema)

        self.assertEqual(plan["template_key"], "busiest_stops")
        self.assertIsNone(plan["clarifying_question"])
        self.assertEqual(plan["params"][0], 10)
        self.assertEqual(mocked_call.call_count, 1)

    def test_busiest_stop_singular_still_maps_to_busiest_stops(self):
        feasibility_payload = '{"possible": false, "reason": "could not map intent"}'
        with patch.dict(os.environ, {"GEMINI_API_KEY": "test"}, clear=False):
            with patch(
                "app.gtfs_agent._call_gemini_json",
                side_effect=[feasibility_payload],
            ) as mocked_call:
                plan = proposeQueryPlan("what is the busiest stop", self.schema)

        self.assertEqual(plan["template_key"], "busiest_stops")
        self.assertIsNone(plan["clarifying_question"])
        self.assertEqual(plan["params"][0], 10)
        self.assertEqual(mocked_call.call_count, 1)

    def test_gemini_contradictory_feasibility_continues_to_sql_generation(self):
        feasibility_payload = (
            '{"possible": false, "reason": "The request can be determined by counting '
            'how often each stop appears in stop_times."}'
        )
        sql_payload = (
            '{"sql": "SELECT stops.stop_id, stops.stop_name, COUNT(*)::bigint AS scheduled_stop_events '
            'FROM stop_times INNER JOIN stops ON stops.stop_id = stop_times.stop_id '
            'GROUP BY stops.stop_id, stops.stop_name '
            'ORDER BY scheduled_stop_events DESC '
            'LIMIT LEAST($1, 50)", '
            '"params": [10], "row_limit": 10, "reason": "counted stop_times by stop"}'
        )
        with patch.dict(os.environ, {"GEMINI_API_KEY": "test"}, clear=False):
            with patch(
                "app.gtfs_agent._call_gemini_json",
                side_effect=[feasibility_payload, sql_payload],
            ) as mocked_call:
                plan = proposeQueryPlan("top 10 busiest stops", self.schema)

        self.assertEqual(plan["template_key"], "gemini_generated")
        self.assertIsNone(plan["clarifying_question"])
        self.assertIn("from stop_times", plan["sql"].lower())
        self.assertEqual(plan["params"][0], 10)
        self.assertEqual(mocked_call.call_count, 2)

    def test_gemini_planner_invalid_sql_returns_not_possible(self):
        feasibility_payload = '{"possible": true, "reason": "can answer"}'
        sql_payload = (
            '{"sql": "SELECT fake_table.fake_col FROM fake_table LIMIT 10", '
            '"params": [], "row_limit": 10, "reason": "bad"}'
        )
        with patch.dict(os.environ, {"GEMINI_API_KEY": "test"}, clear=False):
            with patch(
                "app.gtfs_agent._call_gemini_json",
                side_effect=[feasibility_payload, sql_payload],
            ) as mocked_call:
                plan = proposeQueryPlan("give me fake data", self.schema)

        self.assertEqual(plan["clarifying_question"], "not possible")
        self.assertIsNone(plan["sql"])
        self.assertEqual(mocked_call.call_count, 2)


if __name__ == "__main__":
    unittest.main()
