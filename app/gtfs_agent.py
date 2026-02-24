import copy
import json
import os
import re
import time
from threading import Lock
from typing import Any
from urllib import error as urlerror
from urllib import request as urlrequest

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from app.db import get_engine

DEFAULT_GEMINI_MODEL = "gemini-2.0-flash"

SOURCE_OF_TRUTH_SCHEMA: dict[str, Any] = {
    "version": "gtfs-4-table-v1",
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
}

_REQUIRED_TEMPLATE_KEYS = {
    "list_routes",
    "route_details",
    "list_stops",
    "stop_details",
    "stops_on_route",
    "routes_serving_stop",
    "arrivals_for_stop",
    "busiest_stops",
    "busiest_routes",
    "accessible_stops",
    "accessible_trips",
}

_AGENT_SCHEMA_CACHE: dict[str, Any] | None = None
_AGENT_SCHEMA_CACHE_CREATED_AT = 0.0
_AGENT_SCHEMA_LOCK = Lock()
_AGENT_SCHEMA_SOURCE = "unknown"
_AGENT_SCHEMA_LAST_ERROR: str | None = None
_AGENT_SCHEMA_LAST_GEMINI_ATTEMPT_AT = 0.0
_AGENT_SCHEMA_LAST_GEMINI_SUCCESS_AT = 0.0


class AgentSchemaError(RuntimeError):
    pass


class QueryPlanError(RuntimeError):
    pass


def isDatabaseQuestion(userText: str) -> bool:
    if not userText:
        return False
    text = " ".join(userText.strip().lower().split())
    if len(text) < 3:
        return False

    non_db_signals = (
        "dinner table",
        "table tennis",
        "furniture",
        "restaurant",
        "movie route",
        "bus route map image",
    )
    if any(signal in text for signal in non_db_signals):
        return False

    db_signals = (
        "postgres",
        "sql",
        "query",
        "schema",
        "table",
        "column",
        "join",
        "route_id",
        "trip_id",
        "stop_id",
        "gtfs",
        "arrival",
        "departure",
        "stop times",
        "busiest stops",
        "busiest routes",
        "accessible stops",
        "accessible trips",
        "wheelchair",
        "nearby stops",
        "route details",
        "stop details",
        "how many people went to",
    )
    if any(signal in text for signal in db_signals):
        return True

    entity_tokens = {
        "route",
        "routes",
        "stop",
        "stops",
        "trip",
        "trips",
        "arrival",
        "arrivals",
        "departure",
        "departures",
        "schedule",
        "schedules",
        "stop_times",
    }
    intent_tokens = {
        "show",
        "list",
        "what",
        "which",
        "find",
        "get",
        "top",
        "busiest",
        "nearby",
        "accessible",
        "accessibility",
        "serving",
        "details",
    }
    token_set = set(re.findall(r"[a-z0-9_]+", text))
    if token_set.intersection(entity_tokens) and token_set.intersection(intent_tokens):
        return True

    patterns = (
        r"\blist\b.*\broutes?\b",
        r"\blist\b.*\bstops?\b",
        r"\bshow\b.*\broutes?\b",
        r"\bshow\b.*\btrips?\b",
        r"\bwhat\b.*\broutes?\b",
        r"\bwhat\b.*\btrips?\b",
        r"\bwhat\b.*\bstops?\b",
        r"\bwhich\b.*\broutes?\b.*\bstop\b",
        r"\bstops?\b.*\bon\b.*\broute\b",
        r"\broutes?\b.*\bthere\b.*\bare\b",
        r"\btrips?\b.*\boccur",
        r"\barrivals?\b.*\bstop\b",
        r"\bdepartures?\b.*\bstop\b",
        r"\bhow many\b.*\bpeople\b.*\bwent to\b",
    )
    return any(re.search(pattern, text) for pattern in patterns)


def getAgentSchema() -> dict[str, Any]:
    global _AGENT_SCHEMA_CACHE, _AGENT_SCHEMA_CACHE_CREATED_AT

    ttl_seconds = _read_int_env("SCHEMA_CACHE_SECONDS", 300)
    now = time.time()

    with _AGENT_SCHEMA_LOCK:
        if (
            _AGENT_SCHEMA_CACHE is not None
            and ttl_seconds > 0
            and now - _AGENT_SCHEMA_CACHE_CREATED_AT <= ttl_seconds
        ):
            return copy.deepcopy(_AGENT_SCHEMA_CACHE)

    schema = _build_agent_schema_uncached()
    with _AGENT_SCHEMA_LOCK:
        _AGENT_SCHEMA_CACHE = copy.deepcopy(schema)
        _AGENT_SCHEMA_CACHE_CREATED_AT = time.time()
    return copy.deepcopy(schema)


def getAgentSchemaStatus() -> dict[str, Any]:
    with _AGENT_SCHEMA_LOCK:
        return {
            "source": _AGENT_SCHEMA_SOURCE,
            "last_error": _AGENT_SCHEMA_LAST_ERROR,
            "last_gemini_attempt_at": _AGENT_SCHEMA_LAST_GEMINI_ATTEMPT_AT,
            "last_gemini_success_at": _AGENT_SCHEMA_LAST_GEMINI_SUCCESS_AT,
            "cache_age_seconds": max(0.0, time.time() - _AGENT_SCHEMA_CACHE_CREATED_AT)
            if _AGENT_SCHEMA_CACHE_CREATED_AT
            else None,
        }


def clearAgentSchemaCache() -> None:
    global _AGENT_SCHEMA_CACHE, _AGENT_SCHEMA_CACHE_CREATED_AT
    with _AGENT_SCHEMA_LOCK:
        _AGENT_SCHEMA_CACHE = None
        _AGENT_SCHEMA_CACHE_CREATED_AT = 0.0


def _build_agent_schema_uncached() -> dict[str, Any]:
    global _AGENT_SCHEMA_SOURCE, _AGENT_SCHEMA_LAST_ERROR
    global _AGENT_SCHEMA_LAST_GEMINI_ATTEMPT_AT, _AGENT_SCHEMA_LAST_GEMINI_SUCCESS_AT

    _AGENT_SCHEMA_LAST_GEMINI_ATTEMPT_AT = time.time()
    try:
        raw_schema = proposeAgentSchemaFromTruth(SOURCE_OF_TRUTH_SCHEMA)
    except AgentSchemaError as exc:
        _AGENT_SCHEMA_SOURCE = "fallback"
        _AGENT_SCHEMA_LAST_ERROR = (
            f"Gemini schema generation failed: {exc}. Using fallback schema."
        )
        fallback = _minimal_fallback_agent_schema()
        fallback_errors = _validate_agent_schema(
            fallback,
            SOURCE_OF_TRUTH_SCHEMA,
            strict_templates=False,
        )
        if fallback_errors:
            error_text = "; ".join(fallback_errors)
            raise AgentSchemaError(f"Failed to build fallback agent schema: {error_text}")
        return fallback

    raw_schema = _normalize_agent_schema(raw_schema, SOURCE_OF_TRUTH_SCHEMA)
    errors = _validate_agent_schema(raw_schema, SOURCE_OF_TRUTH_SCHEMA, strict_templates=True)
    if not errors:
        _AGENT_SCHEMA_SOURCE = "gemini"
        _AGENT_SCHEMA_LAST_ERROR = None
        _AGENT_SCHEMA_LAST_GEMINI_SUCCESS_AT = time.time()
        return raw_schema

    repair_error_text: str | None = None
    try:
        repair_schema = _propose_repaired_agent_schema(
            SOURCE_OF_TRUTH_SCHEMA,
            previous_schema=raw_schema,
            validation_errors=errors,
        )
    except AgentSchemaError as exc:
        repair_error_text = str(exc)
        repair_schema = {}
    repair_schema = _normalize_agent_schema(repair_schema, SOURCE_OF_TRUTH_SCHEMA)
    repair_errors = _validate_agent_schema(repair_schema, SOURCE_OF_TRUTH_SCHEMA, strict_templates=True)
    if not repair_errors:
        _AGENT_SCHEMA_SOURCE = "gemini_repair"
        _AGENT_SCHEMA_LAST_ERROR = None
        _AGENT_SCHEMA_LAST_GEMINI_SUCCESS_AT = time.time()
        return repair_schema

    _AGENT_SCHEMA_SOURCE = "fallback"
    primary_errors = "; ".join(errors[:3]) if errors else "unknown validation error"
    if repair_error_text:
        _AGENT_SCHEMA_LAST_ERROR = (
            "Gemini schema failed validation; "
            f"repair failed: {repair_error_text}; "
            f"validation details: {primary_errors}. Using fallback schema."
        )
    else:
        _AGENT_SCHEMA_LAST_ERROR = (
            f"Gemini schema failed validation: {primary_errors}. Using fallback schema."
        )
    fallback = _minimal_fallback_agent_schema()
    fallback_errors = _validate_agent_schema(
        fallback,
        SOURCE_OF_TRUTH_SCHEMA,
        strict_templates=False,
    )
    if fallback_errors:
        error_text = "; ".join(fallback_errors)
        raise AgentSchemaError(f"Failed to build fallback agent schema: {error_text}")
    return fallback


def proposeAgentSchemaFromTruth(truthSchema: dict[str, Any]) -> dict[str, Any]:
    prompt = _build_schema_prompt(truthSchema)
    payload = _call_gemini_json(prompt)
    return _extract_json_object(payload)


def _propose_repaired_agent_schema(
    truth_schema: dict[str, Any],
    previous_schema: dict[str, Any],
    validation_errors: list[str],
) -> dict[str, Any]:
    prompt = _build_repair_prompt(truth_schema, previous_schema, validation_errors)
    payload = _call_gemini_json(prompt)
    return _extract_json_object(payload)


def proposeQueryPlan(userText: str, agentSchema: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(agentSchema, dict):
        raise QueryPlanError("agentSchema must be a JSON object.")

    templates = agentSchema.get("query_templates", [])
    if not isinstance(templates, list) or not templates:
        raise QueryPlanError("agentSchema.query_templates must contain at least one template.")

    max_limit = _read_int_env("MAX_RESULT_ROWS", 50)
    gemini_plan = _propose_query_plan_from_headers(userText, max_limit)
    gemini_not_possible_plan: dict[str, Any] | None = None
    if gemini_plan is not None and gemini_plan.get("template_key") != "gemini_not_possible":
        return gemini_plan
    if gemini_plan is not None and gemini_plan.get("template_key") == "gemini_not_possible":
        gemini_not_possible_plan = gemini_plan

    template_map = {
        template.get("key"): template
        for template in templates
        if isinstance(template, dict) and isinstance(template.get("key"), str)
    }
    template_key = _choose_template_key(userText, template_map)
    if not template_key:
        if gemini_not_possible_plan is not None:
            return gemini_not_possible_plan
        return {
            "clarifying_question": (
                "What GTFS query do you want? For example: list routes, stop details, "
                "arrivals for stop_id, busiest stops, or nearby stops."
            ),
            "sql": None,
            "params": [],
            "display_key": None,
            "safety": {"row_limit": 0},
        }

    template = template_map[template_key]
    extracted = _extract_user_values(userText)
    row_limit = _compute_row_limit(extracted, template, max_limit)

    required_inputs = template.get("required_inputs", [])
    required_groups: list[set[str]] = []
    for item in required_inputs:
        if not isinstance(item, dict):
            continue
        name_value = item.get("name")
        if not isinstance(name_value, str):
            continue
        group = {part.strip() for part in name_value.split("|") if part.strip()}
        if group:
            required_groups.append(group)

    params_list: list[Any] = []
    param_map: dict[str, Any] = {}
    for param_name in template.get("params", []):
        value = _resolve_param_value(param_name, extracted, row_limit)
        params_list.append(value)
        param_map[param_name] = value

    missing_groups: list[str] = []
    for group in required_groups:
        has_any = any(
            _has_required_value(param_map.get(name)) or _has_required_value(extracted.get(name))
            for name in group
        )
        if not has_any:
            missing_groups.append(" or ".join(sorted(group)))

    if missing_groups:
        question = "I need more detail before querying: " + ", ".join(missing_groups)
        return {
            "clarifying_question": question,
            "sql": None,
            "params": [],
            "display_key": template.get("display_key"),
            "safety": {"row_limit": row_limit},
        }

    sql = str(template.get("sql_template", "")).strip()
    if not sql:
        raise QueryPlanError(f"Template '{template_key}' has empty sql_template.")

    sql, params_list = _apply_sql_safety(sql, params_list, row_limit, max_limit)
    return {
        "template_key": template_key,
        "sql": sql,
        "params": params_list,
        "param_map": param_map,
        "display_key": template.get("display_key"),
        "clarifying_question": None,
        "safety": {
            "row_limit": row_limit,
            "max_limit": max_limit,
            "no_select_star": True,
        },
    }


def _propose_query_plan_from_headers(user_text: str, max_limit: int) -> dict[str, Any] | None:
    if not os.getenv("GEMINI_API_KEY", "").strip():
        return None

    prompt = _build_query_plan_prompt(user_text, SOURCE_OF_TRUTH_SCHEMA, max_limit)
    try:
        payload_text = _call_gemini_json(prompt)
        payload = _extract_json_object(payload_text)
    except AgentSchemaError:
        return None

    possible = payload.get("possible")
    if possible is not True:
        reason = payload.get("reason") if isinstance(payload.get("reason"), str) else None
        return _not_possible_query_plan(max_limit, reason=reason)

    sql = payload.get("sql")
    params = payload.get("params", [])
    if not isinstance(sql, str) or not sql.strip():
        return _not_possible_query_plan(max_limit, reason="Gemini returned empty SQL.")
    if not isinstance(params, list):
        return _not_possible_query_plan(max_limit, reason="Gemini returned invalid params.")

    normalized_params: list[Any] = []
    for param in params:
        if isinstance(param, (str, int, float, bool)) or param is None:
            normalized_params.append(param)
        else:
            normalized_params.append(str(param))

    requested_limit = payload.get("row_limit")
    row_limit = requested_limit if isinstance(requested_limit, int) else max_limit
    row_limit = max(1, min(row_limit, max_limit))
    sql, normalized_params = _apply_sql_safety(sql, normalized_params, row_limit, max_limit)

    validation_errors = _validate_sql_template(
        sql,
        max_limit,
        SOURCE_OF_TRUTH_SCHEMA,
        "gemini_generated",
    )
    placeholders = [int(value) for value in re.findall(r"\$(\d+)", sql)]
    if placeholders and max(placeholders) > len(normalized_params):
        return _not_possible_query_plan(max_limit, reason="SQL placeholders exceed params.")
    if validation_errors:
        return _not_possible_query_plan(
            max_limit,
            reason="; ".join(validation_errors[:2]),
        )

    return {
        "template_key": "gemini_generated",
        "sql": sql,
        "params": normalized_params,
        "param_map": {},
        "display_key": None,
        "clarifying_question": None,
        "safety": {
            "row_limit": row_limit,
            "max_limit": max_limit,
            "no_select_star": True,
        },
    }


def _not_possible_query_plan(max_limit: int, reason: str | None = None) -> dict[str, Any]:
    return {
        "template_key": "gemini_not_possible",
        "sql": None,
        "params": [],
        "param_map": {},
        "display_key": None,
        "clarifying_question": "not possible",
        "not_possible_reason": reason,
        "safety": {
            "row_limit": 0,
            "max_limit": max_limit,
            "no_select_star": True,
        },
    }


def executeParameterizedQuery(query_plan: dict[str, Any]) -> dict[str, Any]:
    clarifying = query_plan.get("clarifying_question")
    if clarifying:
        return {
            "executed": False,
            "success": False,
            "rows": [],
            "row_count": 0,
            "columns": [],
            "error": "Query plan is incomplete and requires clarification.",
        }

    sql = query_plan.get("sql")
    params = query_plan.get("params", [])
    if not isinstance(sql, str) or not sql.strip():
        raise QueryPlanError("Query plan is missing SQL.")
    if not isinstance(params, list):
        raise QueryPlanError("Query plan params must be a list.")

    converted_sql, bind_params = _convert_postgres_params(sql, params)
    row_limit = int(query_plan.get("safety", {}).get("row_limit", _read_int_env("MAX_RESULT_ROWS", 50)))
    row_limit = max(1, min(row_limit, _read_int_env("MAX_RESULT_ROWS", 50)))

    try:
        engine = get_engine()
        with engine.connect() as connection:
            result = connection.execute(text(converted_sql), bind_params)
            rows = [dict(row._mapping) for row in result]
    except SQLAlchemyError as exc:
        return {
            "executed": True,
            "success": False,
            "rows": [],
            "row_count": 0,
            "columns": [],
            "error": str(exc),
        }

    if len(rows) > row_limit:
        rows = rows[:row_limit]

    columns = list(rows[0].keys()) if rows else []
    return {
        "executed": True,
        "success": True,
        "rows": rows,
        "row_count": len(rows),
        "columns": columns,
        "error": None,
    }


def renderDisplayPayload(
    rows: list[dict[str, Any]],
    query_plan: dict[str, Any],
    agent_schema: dict[str, Any],
) -> dict[str, Any]:
    display_key = query_plan.get("display_key")
    templates = agent_schema.get("display_templates", [])
    template = None
    for item in templates:
        if isinstance(item, dict) and item.get("key") == display_key:
            template = item
            break

    if template is None:
        return {
            "key": display_key,
            "title": "Query Results",
            "columns": [{"name": key, "label": key} for key in (rows[0].keys() if rows else [])],
            "rows": rows,
            "row_id_field": None,
            "formatting": {"time_fields": [], "latlon_fields": [], "color_fields": []},
        }

    columns = template.get("columns", [])
    display_rows = []
    for row in rows:
        display_row = {}
        for column in columns:
            name = column.get("name")
            if isinstance(name, str):
                display_row[name] = row.get(name)
        display_rows.append(display_row)

    title_template = str(template.get("title_template", "Query Results"))
    title_context = {"row_count": len(rows)}
    param_map = query_plan.get("param_map")
    if isinstance(param_map, dict):
        title_context.update(param_map)
    if rows:
        title_context.update(rows[0])
    try:
        title = title_template.format(**title_context)
    except Exception:
        title = title_template

    return {
        "key": display_key,
        "title": title,
        "columns": columns,
        "rows": display_rows,
        "row_id_field": template.get("row_id_field"),
        "formatting": template.get("formatting", {}),
    }


def _build_schema_prompt(truth_schema: dict[str, Any]) -> str:
    truth_json = json.dumps(truth_schema, ensure_ascii=True)
    return (
        "You are generating a STRICT machine-readable agent schema for GTFS query planning.\n"
        "Use only tables/columns/joins from truthSchema. Do not invent fields.\n"
        "Return JSON only, no markdown, no comments.\n"
        "Contract:\n"
        "{"
        '"dialect":"postgres",'
        '"tables":{"routes":{"columns":[string]},"trips":{"columns":[string]},"stop_times":{"columns":[string]},"stops":{"columns":[string]}},'
        '"joins":[{"left_table":string,"left_column":string,"right_table":string,"right_column":string,"type":"inner"|"left","alias_right_table":string?}],'
        '"query_templates":[{"key":string,"description":string,"required_inputs":[{"name":string,"type":"string"|"number"|"enum"|"latlon","notes":string}],"sql_template":string,"params":[string],"default_limit":number,"display_key":string,"safety_notes":string}],'
        '"display_templates":[{"key":string,"title_template":string,"columns":[{"name":string,"label":string}],"row_id_field":string|null,"formatting":{"time_fields":[string],"latlon_fields":[string],"color_fields":[string]}}],'
        '"constraints":{"max_limit":number,"require_limit":true,"no_select_star":true}'
        "}\n"
        "Rules:\n"
        "1) Include query_templates for exactly these keys at minimum:\n"
        "list_routes, route_details, list_stops, stop_details, stops_on_route, routes_serving_stop, "
        "arrivals_for_stop, busiest_stops, busiest_routes, accessible_stops, accessible_trips.\n"
        "2) SQL must be parameterized with $1,$2... and include LIMIT bounded by constraints.max_limit.\n"
        "3) No SELECT *.\n"
        "4) Use only declared joins and columns.\n"
        "5) For stop name search use ILIKE with concatenated wildcards.\n"
        "6) For nearby stops use bounding box with 111km/deg and cos(lat) for longitude.\n"
        "7) Do not reference calendar/service-date tables.\n"
        f"truthSchema={truth_json}"
    )


def _build_repair_prompt(
    truth_schema: dict[str, Any],
    previous_schema: dict[str, Any],
    validation_errors: list[str],
) -> str:
    truth_json = json.dumps(truth_schema, ensure_ascii=True)
    previous_json = json.dumps(previous_schema, ensure_ascii=True)
    error_json = json.dumps(validation_errors, ensure_ascii=True)
    return (
        "Repair the previous agent schema JSON.\n"
        "Return JSON only.\n"
        "Do not invent new tables/columns.\n"
        f"truthSchema={truth_json}\n"
        f"validationErrors={error_json}\n"
        f"previousSchema={previous_json}"
    )


def _build_query_plan_prompt(user_text: str, truth_schema: dict[str, Any], max_limit: int) -> str:
    truth_json = json.dumps(
        {
            "dialect": truth_schema.get("dialect"),
            "tables": truth_schema.get("tables"),
            "joins": truth_schema.get("joins"),
        },
        ensure_ascii=True,
    )
    request_json = json.dumps(user_text, ensure_ascii=True)
    return (
        "You are a Postgres query planner over GTFS data.\n"
        "Return JSON only with this exact shape:\n"
        "{"
        '"possible": boolean,'
        '"sql": string|null,'
        '"params": [any],'
        '"row_limit": number|null,'
        '"reason": string'
        "}\n"
        "Rules:\n"
        "1) If the request cannot be answered from available headers/joins, set possible=false and sql=null.\n"
        "2) If possible=true, sql must be SELECT-only, parameterized with $1,$2..., and must include LIMIT.\n"
        "3) Use only tables/columns and join keys from truthSchema.\n"
        "4) Never use SELECT *.\n"
        "5) LIMIT must be bounded and <= max_limit.\n"
        f"max_limit={max_limit}\n"
        f"userRequest={request_json}\n"
        f"truthSchema={truth_json}"
    )


def _call_gemini_json(prompt: str) -> str:
    api_key = os.getenv("GEMINI_API_KEY", "").strip()
    if not api_key:
        raise AgentSchemaError("GEMINI_API_KEY is required for agent schema generation.")

    model = os.getenv("GEMINI_MODEL", DEFAULT_GEMINI_MODEL).strip() or DEFAULT_GEMINI_MODEL
    timeout_seconds = _read_float_env("GEMINI_TIMEOUT_SECONDS", 30.0)
    retry_count = max(0, _read_int_env("GEMINI_RETRY_COUNT", 1))
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"

    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0, "responseMimeType": "application/json"},
    }
    request = urlrequest.Request(
        url=url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    attempts = retry_count + 1
    raw: str | None = None
    last_error_message = ""
    for attempt in range(1, attempts + 1):
        try:
            with urlrequest.urlopen(request, timeout=timeout_seconds) as response:
                raw = response.read().decode("utf-8")
            break
        except urlerror.HTTPError as exc:
            detail = _extract_http_error_detail(exc)
            suffix = f": {detail}" if detail else ""
            last_error_message = (
                f"Gemini request failed for model '{model}' with HTTP {exc.code}{suffix}."
            )
            if attempt < attempts and exc.code in {408, 429, 500, 502, 503, 504}:
                time.sleep(0.5 * attempt)
                continue
            raise AgentSchemaError(last_error_message) from exc
        except urlerror.URLError as exc:
            reason = str(getattr(exc, "reason", "")).strip()
            suffix = f": {reason}" if reason else ""
            last_error_message = (
                f"Gemini request failed for model '{model}' due to network error{suffix}."
            )
            if attempt < attempts:
                time.sleep(0.5 * attempt)
                continue
            raise AgentSchemaError(last_error_message) from exc
        except TimeoutError as exc:
            last_error_message = f"Gemini request timed out for model '{model}'."
            if attempt < attempts:
                time.sleep(0.5 * attempt)
                continue
            raise AgentSchemaError(last_error_message) from exc

    if raw is None:
        raise AgentSchemaError(last_error_message or "Gemini request failed unexpectedly.")

    try:
        payload_json = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise AgentSchemaError("Gemini API response was not valid JSON.") from exc

    candidates = payload_json.get("candidates", [])
    if not candidates:
        raise AgentSchemaError("Gemini returned no candidates.")
    parts = candidates[0].get("content", {}).get("parts", [])
    text_output = "".join(part.get("text", "") for part in parts if isinstance(part, dict)).strip()
    if not text_output:
        raise AgentSchemaError("Gemini returned empty schema content.")
    return text_output


def _extract_http_error_detail(exc: urlerror.HTTPError) -> str:
    try:
        raw = exc.read().decode("utf-8", errors="ignore").strip()
    except Exception:
        return ""
    if not raw:
        return ""
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return raw[:200]
    if isinstance(payload, dict):
        err = payload.get("error")
        if isinstance(err, dict):
            message = err.get("message")
            if isinstance(message, str) and message.strip():
                return message.strip()[:200]
    return raw[:200]


def _extract_json_object(raw_text: str) -> dict[str, Any]:
    text_payload = raw_text.strip()
    if text_payload.startswith("```"):
        text_payload = re.sub(r"^```(?:json)?\s*", "", text_payload, flags=re.IGNORECASE)
        text_payload = re.sub(r"\s*```$", "", text_payload)

    start = text_payload.find("{")
    end = text_payload.rfind("}")
    if start < 0 or end < 0 or end <= start:
        raise AgentSchemaError("Gemini output did not contain a JSON object.")
    try:
        parsed = json.loads(text_payload[start : end + 1])
    except json.JSONDecodeError as exc:
        raise AgentSchemaError("Gemini output JSON parsing failed.") from exc
    if not isinstance(parsed, dict):
        raise AgentSchemaError("Gemini output must be a JSON object.")
    return parsed


def _validate_agent_schema(
    agent_schema: dict[str, Any],
    truth_schema: dict[str, Any],
    *,
    strict_templates: bool,
) -> list[str]:
    errors: list[str] = []
    required_top_keys = {
        "dialect",
        "tables",
        "joins",
        "query_templates",
        "display_templates",
        "constraints",
    }
    if set(agent_schema.keys()) != required_top_keys:
        errors.append("Top-level keys must match contract exactly.")

    if agent_schema.get("dialect") != "postgres":
        errors.append("dialect must be 'postgres'.")

    truth_tables = truth_schema["tables"]
    tables = agent_schema.get("tables")
    if not isinstance(tables, dict):
        errors.append("tables must be an object.")
    else:
        if set(tables.keys()) != set(truth_tables.keys()):
            errors.append("tables keys must match SOURCE_OF_TRUTH_SCHEMA tables.")
        for table_name, table_info in truth_tables.items():
            candidate = tables.get(table_name, {})
            columns = candidate.get("columns") if isinstance(candidate, dict) else None
            if not isinstance(columns, list):
                errors.append(f"tables.{table_name}.columns must be an array.")
                continue
            if len(columns) != len(set(columns)):
                errors.append(f"tables.{table_name}.columns contains duplicates.")
            if set(columns) != set(table_info["columns"]):
                errors.append(f"tables.{table_name}.columns must match SOURCE_OF_TRUTH_SCHEMA.")

    joins = agent_schema.get("joins")
    if not isinstance(joins, list):
        errors.append("joins must be an array.")
    else:
        allowed_join_set = {_canonical_join(item) for item in truth_schema["joins"]}
        candidate_join_set = set()
        for join in joins:
            if not isinstance(join, dict):
                errors.append("Each join must be an object.")
                continue
            candidate_join_set.add(_canonical_join(join))
        if candidate_join_set != allowed_join_set:
            errors.append("joins must exactly match allowed GTFS joins.")

    constraints = agent_schema.get("constraints")
    if not isinstance(constraints, dict):
        errors.append("constraints must be an object.")
        max_limit = _read_int_env("MAX_RESULT_ROWS", 50)
    else:
        if set(constraints.keys()) != {"max_limit", "require_limit", "no_select_star"}:
            errors.append("constraints keys must match contract.")
        max_limit = constraints.get("max_limit")
        if not isinstance(max_limit, int) or max_limit <= 0:
            errors.append("constraints.max_limit must be a positive integer.")
            max_limit = _read_int_env("MAX_RESULT_ROWS", 50)
        env_max_limit = _read_int_env("MAX_RESULT_ROWS", 50)
        if isinstance(max_limit, int) and max_limit > env_max_limit:
            errors.append("constraints.max_limit must not exceed MAX_RESULT_ROWS.")
        if constraints.get("require_limit") is not True:
            errors.append("constraints.require_limit must be true.")
        if constraints.get("no_select_star") is not True:
            errors.append("constraints.no_select_star must be true.")

    display_templates = agent_schema.get("display_templates")
    display_key_set: set[str] = set()
    if not isinstance(display_templates, list):
        errors.append("display_templates must be an array.")
    else:
        for template in display_templates:
            if not isinstance(template, dict):
                errors.append("Each display_template must be an object.")
                continue
            expected_keys = {"key", "title_template", "columns", "row_id_field", "formatting"}
            if set(template.keys()) != expected_keys:
                errors.append("display_template keys must match contract.")
            key = template.get("key")
            if not isinstance(key, str) or not key:
                errors.append("display_template.key must be a non-empty string.")
            else:
                if key in display_key_set:
                    errors.append(f"Duplicate display_template key: {key}")
                display_key_set.add(key)
            columns = template.get("columns")
            if not isinstance(columns, list):
                errors.append("display_template.columns must be an array.")
            else:
                for column in columns:
                    if not isinstance(column, dict) or set(column.keys()) != {"name", "label"}:
                        errors.append("display_template.columns items must have name and label.")
            formatting = template.get("formatting")
            if not isinstance(formatting, dict):
                errors.append("display_template.formatting must be an object.")
            elif set(formatting.keys()) != {"time_fields", "latlon_fields", "color_fields"}:
                errors.append("display_template.formatting keys must match contract.")

    query_templates = agent_schema.get("query_templates")
    template_key_set: set[str] = set()
    if not isinstance(query_templates, list):
        errors.append("query_templates must be an array.")
    else:
        for template in query_templates:
            if not isinstance(template, dict):
                errors.append("Each query_template must be an object.")
                continue
            expected_keys = {
                "key",
                "description",
                "required_inputs",
                "sql_template",
                "params",
                "default_limit",
                "display_key",
                "safety_notes",
            }
            if set(template.keys()) != expected_keys:
                errors.append("query_template keys must match contract.")
            key = template.get("key")
            if not isinstance(key, str) or not key:
                errors.append("query_template.key must be a non-empty string.")
                continue
            if key in template_key_set:
                errors.append(f"Duplicate query_template key: {key}")
            template_key_set.add(key)

            required_inputs = template.get("required_inputs")
            if not isinstance(required_inputs, list):
                errors.append(f"query_template '{key}' required_inputs must be an array.")
            else:
                for item in required_inputs:
                    if not isinstance(item, dict):
                        errors.append(f"query_template '{key}' required_inputs item must be an object.")
                        continue
                    if set(item.keys()) != {"name", "type", "notes"}:
                        errors.append(f"query_template '{key}' required_inputs item keys invalid.")
                        continue
                    if item.get("type") not in {"string", "number", "enum", "latlon"}:
                        errors.append(f"query_template '{key}' has invalid input type.")

            params = template.get("params")
            if not isinstance(params, list) or not all(isinstance(param, str) for param in params):
                errors.append(f"query_template '{key}' params must be array of strings.")

            sql_template = template.get("sql_template")
            if not isinstance(sql_template, str) or not sql_template.strip():
                errors.append(f"query_template '{key}' sql_template must be a non-empty string.")
            else:
                errors.extend(_validate_sql_template(sql_template, max_limit, truth_schema, key))
                placeholder_ids = [int(item) for item in re.findall(r"\$(\d+)", sql_template)]
                if placeholder_ids and max(placeholder_ids) > len(params):
                    errors.append(f"query_template '{key}' uses placeholder index outside params.")

            default_limit = template.get("default_limit")
            if not isinstance(default_limit, int) or default_limit <= 0:
                errors.append(f"query_template '{key}' default_limit must be positive integer.")
            elif isinstance(max_limit, int) and default_limit > max_limit:
                errors.append(f"query_template '{key}' default_limit exceeds constraints.max_limit.")

            display_key = template.get("display_key")
            if not isinstance(display_key, str) or not display_key:
                errors.append(f"query_template '{key}' display_key must be a non-empty string.")
            elif display_key_set and display_key not in display_key_set:
                errors.append(f"query_template '{key}' references unknown display_key '{display_key}'.")

    if strict_templates and not _REQUIRED_TEMPLATE_KEYS.issubset(template_key_set):
        missing = sorted(_REQUIRED_TEMPLATE_KEYS - template_key_set)
        errors.append(f"query_templates missing required keys: {', '.join(missing)}")

    return errors


def _normalize_agent_schema(
    agent_schema: dict[str, Any],
    truth_schema: dict[str, Any],
) -> dict[str, Any]:
    if not isinstance(agent_schema, dict):
        return agent_schema

    normalized = copy.deepcopy(agent_schema)
    env_max_limit = _read_int_env("MAX_RESULT_ROWS", 50)

    constraints = normalized.get("constraints")
    if isinstance(constraints, dict):
        max_limit = constraints.get("max_limit")
        if not isinstance(max_limit, int) or max_limit <= 0:
            max_limit = env_max_limit
        max_limit = min(max_limit, env_max_limit)
        constraints["max_limit"] = max_limit
        constraints["require_limit"] = True
        constraints["no_select_star"] = True
    else:
        max_limit = env_max_limit

    truth_table_names = set(truth_schema.get("tables", {}).keys())
    query_templates = normalized.get("query_templates")
    if isinstance(query_templates, list):
        for template in query_templates:
            if not isinstance(template, dict):
                continue

            sql_template = template.get("sql_template")
            if isinstance(sql_template, str) and sql_template.strip():
                table_refs = {
                    table_name
                    for table_name in truth_table_names
                    if re.search(rf"\b{re.escape(table_name)}\b", sql_template.lower())
                }
                if table_refs:
                    template["sql_template"] = _normalize_sql_limit_clause(sql_template, max_limit)

            default_limit = template.get("default_limit")
            if not isinstance(default_limit, int) or default_limit <= 0:
                template["default_limit"] = min(25, max_limit)
            elif default_limit > max_limit:
                template["default_limit"] = max_limit

    return normalized


def _normalize_sql_limit_clause(sql_template: str, max_limit: int) -> str:
    sql = sql_template.strip()
    lower_sql = sql.lower()

    least_match = re.search(r"\blimit\s+least\s*\(\s*\$(\d+)\s*,\s*(\d+)\s*\)", lower_sql)
    if least_match:
        current_bound = int(least_match.group(2))
        if current_bound > max_limit:
            return re.sub(
                r"\blimit\s+least\s*\(\s*\$(\d+)\s*,\s*\d+\s*\)",
                lambda match: f"LIMIT LEAST(${match.group(1)}, {max_limit})",
                sql,
                count=1,
                flags=re.IGNORECASE,
            )
        return sql

    param_limit_match = re.search(r"\blimit\s+\$(\d+)\b", lower_sql)
    if param_limit_match:
        placeholder_idx = param_limit_match.group(1)
        return re.sub(
            r"\blimit\s+\$\d+\b",
            f"LIMIT LEAST(${placeholder_idx}, {max_limit})",
            sql,
            count=1,
            flags=re.IGNORECASE,
        )

    const_limit_match = re.search(r"\blimit\s+(\d+)\b", lower_sql)
    if const_limit_match:
        current_bound = int(const_limit_match.group(1))
        if current_bound <= max_limit:
            return sql
        return re.sub(
            r"\blimit\s+\d+\b",
            f"LIMIT {max_limit}",
            sql,
            count=1,
            flags=re.IGNORECASE,
        )

    sql_no_semicolon = sql.rstrip().rstrip(";")
    return f"{sql_no_semicolon} LIMIT {max_limit}"


def _validate_sql_template(
    sql_template: str,
    max_limit: int,
    truth_schema: dict[str, Any],
    template_key: str,
) -> list[str]:
    errors: list[str] = []
    lower_sql = sql_template.lower()
    if re.search(r"\bselect\s+\*", lower_sql):
        errors.append(f"query_template '{template_key}' uses SELECT * which is not allowed.")

    limit_bound = _extract_limit_bound(lower_sql)
    if limit_bound is None:
        errors.append(f"query_template '{template_key}' must include a bounded LIMIT.")
    elif limit_bound > max_limit:
        errors.append(f"query_template '{template_key}' LIMIT exceeds constraints.max_limit.")

    alias_map = _extract_alias_map(lower_sql)
    for identifier, column in re.findall(r"\b([a-z_][a-z0-9_]*)\.([a-z_][a-z0-9_]*)\b", lower_sql):
        table_name = alias_map.get(identifier, identifier if identifier in truth_schema["tables"] else None)
        if not table_name:
            continue
        if table_name not in truth_schema["tables"]:
            errors.append(f"query_template '{template_key}' references unknown table '{table_name}'.")
            continue
        allowed_columns = set(truth_schema["tables"][table_name]["columns"])
        if column not in allowed_columns:
            errors.append(
                f"query_template '{template_key}' references unknown column '{identifier}.{column}'."
            )
    return errors


def _extract_alias_map(lower_sql: str) -> dict[str, str]:
    alias_map: dict[str, str] = {}
    reserved = {"on", "where", "group", "order", "limit", "left", "inner", "right", "join"}
    pattern = re.compile(
        r"\b(from|join)\s+([a-z_][a-z0-9_]*)(?:\s+(?:as\s+)?([a-z_][a-z0-9_]*))?",
        re.IGNORECASE,
    )
    for match in pattern.finditer(lower_sql):
        table_name = match.group(2).lower()
        alias = (match.group(3) or table_name).lower()
        if alias in reserved:
            alias = table_name
        alias_map[alias] = table_name
        alias_map.setdefault(table_name, table_name)
    return alias_map


def _extract_limit_bound(lower_sql: str) -> int | None:
    least_match = re.search(r"\blimit\s+least\s*\(\s*\$\d+\s*,\s*(\d+)\s*\)", lower_sql)
    if least_match:
        return int(least_match.group(1))
    const_match = re.search(r"\blimit\s+(\d+)\b", lower_sql)
    if const_match:
        return int(const_match.group(1))
    return None


def _canonical_join(join: dict[str, Any]) -> tuple[str, str, str, str, str, str | None]:
    return (
        str(join.get("left_table", "")),
        str(join.get("left_column", "")),
        str(join.get("right_table", "")),
        str(join.get("right_column", "")),
        str(join.get("type", "")),
        join.get("alias_right_table"),
    )


def _minimal_fallback_agent_schema() -> dict[str, Any]:
    max_limit = _read_int_env("MAX_RESULT_ROWS", 50)
    return {
        "dialect": "postgres",
        "tables": copy.deepcopy(SOURCE_OF_TRUTH_SCHEMA["tables"]),
        "joins": copy.deepcopy(SOURCE_OF_TRUTH_SCHEMA["joins"]),
        "query_templates": [
            {
                "key": "list_routes",
                "description": "List GTFS routes with optional route_type filter.",
                "required_inputs": [],
                "sql_template": (
                    "SELECT routes.route_id, routes.route_short_name, routes.route_long_name, "
                    "routes.route_type, routes.route_color, routes.route_text_color "
                    "FROM routes "
                    "WHERE ($2::int IS NULL OR routes.route_type = $2) "
                    f"ORDER BY routes.route_sort_order NULLS LAST, routes.route_short_name LIMIT LEAST($1, {max_limit})"
                ),
                "params": ["limit", "route_type"],
                "default_limit": min(25, max_limit),
                "display_key": "routes_table",
                "safety_notes": "Bounded by MAX_RESULT_ROWS.",
            },
            {
                "key": "list_stops",
                "description": "List stops with optional nearby bounding box filter.",
                "required_inputs": [],
                "sql_template": (
                    "SELECT stops.stop_id, stops.stop_name, stops.stop_lat, stops.stop_lon, "
                    "stops.wheelchair_boarding, stops.parent_station "
                    "FROM stops "
                    "WHERE ($2::double precision IS NULL OR $3::double precision IS NULL "
                    "OR ($4::double precision IS NOT NULL AND "
                    "stops.stop_lat BETWEEN ($2 - ($4 / 111.0)) AND ($2 + ($4 / 111.0)) "
                    "AND stops.stop_lon BETWEEN ($3 - ($4 / NULLIF(111.0 * COS(RADIANS($2)), 0))) "
                    "AND ($3 + ($4 / NULLIF(111.0 * COS(RADIANS($2)), 0))))) "
                    "ORDER BY stops.stop_name "
                    f"LIMIT LEAST($1, {max_limit})"
                ),
                "params": ["limit", "lat", "lon", "radius_km"],
                "default_limit": min(50, max_limit),
                "display_key": "stops_table",
                "safety_notes": "Bounding box for nearby search; bounded rows.",
            },
            {
                "key": "busiest_stops",
                "description": "Top stops by scheduled stop events.",
                "required_inputs": [],
                "sql_template": (
                    "SELECT stops.stop_id, stops.stop_name, COUNT(*)::bigint AS scheduled_stop_events "
                    "FROM stop_times "
                    "INNER JOIN stops ON stops.stop_id = stop_times.stop_id "
                    "GROUP BY stops.stop_id, stops.stop_name "
                    "ORDER BY scheduled_stop_events DESC, stops.stop_name "
                    f"LIMIT LEAST($1, {max_limit})"
                ),
                "params": ["limit"],
                "default_limit": min(10, max_limit),
                "display_key": "busiest_stops_table",
                "safety_notes": "Aggregated and row-limited.",
            },
            {
                "key": "busiest_routes",
                "description": "Top routes by scheduled stop events.",
                "required_inputs": [],
                "sql_template": (
                    "SELECT routes.route_id, routes.route_short_name, routes.route_long_name, "
                    "COUNT(*)::bigint AS scheduled_stop_events "
                    "FROM stop_times "
                    "INNER JOIN trips ON trips.trip_id = stop_times.trip_id "
                    "INNER JOIN routes ON routes.route_id = trips.route_id "
                    "GROUP BY routes.route_id, routes.route_short_name, routes.route_long_name "
                    "ORDER BY scheduled_stop_events DESC, routes.route_short_name "
                    f"LIMIT LEAST($1, {max_limit})"
                ),
                "params": ["limit"],
                "default_limit": min(10, max_limit),
                "display_key": "busiest_routes_table",
                "safety_notes": "Aggregated and row-limited.",
            },
            {
                "key": "stop_service_volume",
                "description": (
                    "Estimate service volume for a stop by counting scheduled stop events, "
                    "distinct trips, and distinct routes."
                ),
                "required_inputs": [
                    {
                        "name": "stop_id|stop_name",
                        "type": "string",
                        "notes": "Provide either stop_id or stop name substring.",
                    }
                ],
                "sql_template": (
                    "SELECT "
                    "COALESCE(MAX(stops.stop_name), $2, $1) AS stop_label, "
                    "COUNT(*)::bigint AS scheduled_stop_events, "
                    "COUNT(DISTINCT stop_times.trip_id)::bigint AS distinct_trips, "
                    "COUNT(DISTINCT trips.route_id)::bigint AS distinct_routes "
                    "FROM stop_times "
                    "INNER JOIN trips ON trips.trip_id = stop_times.trip_id "
                    "INNER JOIN stops ON stops.stop_id = stop_times.stop_id "
                    "WHERE (($1::text IS NOT NULL AND stop_times.stop_id = $1) "
                    "OR ($2::text IS NOT NULL AND stops.stop_name ILIKE '%' || $2 || '%')) "
                    "LIMIT 1"
                ),
                "params": ["stop_id", "stop_name"],
                "default_limit": 1,
                "display_key": "stop_volume_summary",
                "safety_notes": "Bounded aggregate query for stop-level service volume.",
            },
            {
                "key": "arrivals_for_stop",
                "description": "Upcoming arrivals/departures at a stop.",
                "required_inputs": [
                    {"name": "stop_id", "type": "string", "notes": "GTFS stop_id value."}
                ],
                "sql_template": (
                    "SELECT stop_times.arrival_time, stop_times.departure_time, routes.route_short_name, "
                    "trips.trip_headsign, stop_times.stop_sequence "
                    "FROM stop_times "
                    "INNER JOIN trips ON trips.trip_id = stop_times.trip_id "
                    "INNER JOIN routes ON routes.route_id = trips.route_id "
                    "WHERE stop_times.stop_id = $1 "
                    "ORDER BY stop_times.arrival_time "
                    f"LIMIT LEAST($2, {max_limit})"
                ),
                "params": ["stop_id", "limit"],
                "default_limit": min(30, max_limit),
                "display_key": "arrivals_table",
                "safety_notes": "Requires stop_id and bounded limit.",
            },
        ],
        "display_templates": [
            {
                "key": "routes_table",
                "title_template": "Routes ({row_count})",
                "columns": [
                    {"name": "route_id", "label": "Route ID"},
                    {"name": "route_short_name", "label": "Short Name"},
                    {"name": "route_long_name", "label": "Long Name"},
                    {"name": "route_type", "label": "Type"},
                ],
                "row_id_field": "route_id",
                "formatting": {"time_fields": [], "latlon_fields": [], "color_fields": ["route_color"]},
            },
            {
                "key": "stops_table",
                "title_template": "Stops ({row_count})",
                "columns": [
                    {"name": "stop_id", "label": "Stop ID"},
                    {"name": "stop_name", "label": "Stop Name"},
                    {"name": "stop_lat", "label": "Latitude"},
                    {"name": "stop_lon", "label": "Longitude"},
                    {"name": "wheelchair_boarding", "label": "Wheelchair"},
                ],
                "row_id_field": "stop_id",
                "formatting": {
                    "time_fields": [],
                    "latlon_fields": ["stop_lat", "stop_lon"],
                    "color_fields": [],
                },
            },
            {
                "key": "busiest_stops_table",
                "title_template": "Top busiest stops ({row_count})",
                "columns": [
                    {"name": "stop_id", "label": "Stop ID"},
                    {"name": "stop_name", "label": "Stop Name"},
                    {"name": "scheduled_stop_events", "label": "Scheduled Stop Events"},
                ],
                "row_id_field": "stop_id",
                "formatting": {"time_fields": [], "latlon_fields": [], "color_fields": []},
            },
            {
                "key": "busiest_routes_table",
                "title_template": "Top busiest routes ({row_count})",
                "columns": [
                    {"name": "route_id", "label": "Route ID"},
                    {"name": "route_short_name", "label": "Short Name"},
                    {"name": "route_long_name", "label": "Long Name"},
                    {"name": "scheduled_stop_events", "label": "Scheduled Stop Events"},
                ],
                "row_id_field": "route_id",
                "formatting": {"time_fields": [], "latlon_fields": [], "color_fields": []},
            },
            {
                "key": "stop_volume_summary",
                "title_template": "Estimated service volume for {stop_name} {stop_id}",
                "columns": [
                    {"name": "stop_label", "label": "Stop"},
                    {"name": "scheduled_stop_events", "label": "Scheduled Stop Events"},
                    {"name": "distinct_trips", "label": "Distinct Trips"},
                    {"name": "distinct_routes", "label": "Distinct Routes"},
                ],
                "row_id_field": None,
                "formatting": {
                    "time_fields": [],
                    "latlon_fields": [],
                    "color_fields": [],
                },
            },
            {
                "key": "arrivals_table",
                "title_template": "Arrivals for stop {stop_id} ({row_count})",
                "columns": [
                    {"name": "arrival_time", "label": "Arrival"},
                    {"name": "departure_time", "label": "Departure"},
                    {"name": "route_short_name", "label": "Route"},
                    {"name": "trip_headsign", "label": "Headsign"},
                    {"name": "stop_sequence", "label": "Sequence"},
                ],
                "row_id_field": None,
                "formatting": {
                    "time_fields": ["arrival_time", "departure_time"],
                    "latlon_fields": [],
                    "color_fields": [],
                },
            },
        ],
        "constraints": {"max_limit": max_limit, "require_limit": True, "no_select_star": True},
    }


def _choose_template_key(user_text: str, template_map: dict[str, dict[str, Any]]) -> str | None:
    text_lower = user_text.lower()
    if "stop_service_volume" in template_map:
        if re.search(r"\bhow many\b.*\bpeople\b", text_lower) and re.search(
            r"\b(?:at|to|for)\b",
            text_lower,
        ):
            return "stop_service_volume"

    ordered_rules = [
        (
            "stop_service_volume",
            (
                "how many people went to",
                "how many people at",
                "how many riders at",
                "how many riders went to",
                "how many went to",
                "traffic at",
            ),
        ),
        ("arrivals_for_stop", ("arrival", "arrivals", "departure", "departures", "schedule")),
        ("routes_serving_stop", ("routes serving", "serve stop", "which routes stop")),
        ("stops_on_route", ("stops on route", "stops for route", "route stops")),
        ("busiest_stops", ("busiest stops", "top stops", "most used stops")),
        ("busiest_routes", ("busiest routes", "top routes", "most used routes")),
        ("accessible_stops", ("accessible stops", "wheelchair stops")),
        ("accessible_trips", ("accessible trips", "wheelchair trips")),
        ("route_details", ("route details", "details for route", "route info")),
        ("stop_details", ("stop details", "details for stop", "stop info")),
        ("list_stops", ("nearby stops", "list stops", "show stops")),
        ("list_routes", ("list routes", "show routes", "all routes")),
    ]

    for key, signals in ordered_rules:
        if key not in template_map:
            continue
        if any(signal in text_lower for signal in signals):
            return key

    # Avoid routing specific intents to generic list templates when specialized
    # templates are unavailable (e.g., fallback schema); ask clarifying instead.
    specific_intent_markers = (
        "busiest",
        "arrival",
        "arrivals",
        "departure",
        "departures",
        "accessible",
        "accessibility",
        "serving",
        "details",
        "nearby",
    )
    if any(marker in text_lower for marker in specific_intent_markers):
        return None

    if "route" in text_lower and "list_routes" in template_map:
        return "list_routes"
    if "trip" in text_lower and "accessible_trips" in template_map:
        return "accessible_trips"
    if "stop" in text_lower and "list_stops" in template_map:
        return "list_stops"
    return None


def _extract_user_values(user_text: str) -> dict[str, Any]:
    values: dict[str, Any] = {}
    text_lower = user_text.lower()

    quoted_match = re.search(r'"([^"]+)"', user_text)
    if quoted_match:
        values["quoted"] = quoted_match.group(1)

    route_type_match = re.search(r"\broute[_ ]type\s*[:=]?\s*(\d+)\b", text_lower)
    if route_type_match:
        values["route_type"] = int(route_type_match.group(1))

    route_id_match = re.search(r"\broute[_ ]id\s*[:=]?\s*([a-z0-9_-]+)\b", text_lower)
    if route_id_match:
        values["route_id"] = route_id_match.group(1)

    route_short_match = re.search(r"\broute(?:\s+short\s+name)?\s*[:=]?\s*([a-z0-9_-]+)\b", text_lower)
    if route_short_match and route_short_match.group(1) not in {"id", "details", "stops"}:
        values.setdefault("route_short_name", route_short_match.group(1))

    stop_id_match = re.search(r"\bstop[_ ]id\s*[:=]?\s*([a-z0-9_-]+)\b", text_lower)
    if stop_id_match:
        values["stop_id"] = stop_id_match.group(1)

    stop_name_match = re.search(r"\bstop(?:\s+name)?\s*(?:contains|like|named)?\s*\"([^\"]+)\"", user_text, re.I)
    if stop_name_match:
        values["stop_name"] = stop_name_match.group(1)
    elif "quoted" in values:
        values.setdefault("stop_name", values["quoted"])
    elif "stop_id" not in values:
        location_match = re.search(
            r"\b(?:to|at|for)\s+([A-Za-z0-9&'./\-\s]{2,}?)(?:[?.!,;:]\s*)?$",
            user_text,
            re.I,
        )
        if location_match:
            candidate = location_match.group(1).strip().strip(".,!?;:")
            candidate_lower = candidate.lower()
            disallowed = {
                "this",
                "that",
                "there",
                "here",
                "stop",
                "route",
                "trip",
                "this stop",
                "that stop",
                "the stop",
            }
            if candidate and candidate_lower not in disallowed and not candidate_lower.startswith("stop_id"):
                values["stop_name"] = candidate

    lat_match = re.search(r"\blat(?:itude)?\s*[:=]?\s*(-?\d+(?:\.\d+)?)", text_lower)
    lon_match = re.search(r"\b(?:lon|lng|longitude)\s*[:=]?\s*(-?\d+(?:\.\d+)?)", text_lower)
    if lat_match and lon_match:
        values["lat"] = float(lat_match.group(1))
        values["lon"] = float(lon_match.group(1))
    else:
        pair_match = re.search(r"(-?\d+\.\d+)\s*,\s*(-?\d+\.\d+)", text_lower)
        if pair_match:
            values["lat"] = float(pair_match.group(1))
            values["lon"] = float(pair_match.group(2))

    radius_match = re.search(r"(\d+(?:\.\d+)?)\s*(?:km|kilometer|kilometers)\b", text_lower)
    if radius_match:
        values["radius_km"] = float(radius_match.group(1))

    top_match = re.search(r"\btop\s+(\d+)\b", text_lower)
    if top_match:
        values["limit"] = int(top_match.group(1))
        values["top_n"] = int(top_match.group(1))
    else:
        limit_match = re.search(r"\blimit\s+(\d+)\b", text_lower)
        if limit_match:
            values["limit"] = int(limit_match.group(1))

    return values


def _compute_row_limit(extracted: dict[str, Any], template: dict[str, Any], max_limit: int) -> int:
    default_limit = template.get("default_limit", max_limit)
    if not isinstance(default_limit, int) or default_limit <= 0:
        default_limit = max_limit
    requested = extracted.get("limit", default_limit)
    if not isinstance(requested, int):
        requested = default_limit
    return max(1, min(requested, max_limit))


def _resolve_param_value(param_name: str, extracted: dict[str, Any], row_limit: int) -> Any:
    normalized = param_name.lower()
    if normalized in {"limit", "top_n", "n"}:
        return row_limit
    if normalized in {"route_type"}:
        return extracted.get("route_type")
    if normalized in {"route_id"}:
        return extracted.get("route_id")
    if normalized in {"route_short_name"}:
        return extracted.get("route_short_name")
    if normalized in {"stop_id"}:
        return extracted.get("stop_id")
    if normalized in {"stop_name", "stop_name_substring", "name_substring"}:
        return extracted.get("stop_name") or extracted.get("quoted")
    if "lat" == normalized or normalized.endswith("_lat"):
        return extracted.get("lat")
    if normalized in {"lon", "lng"} or normalized.endswith("_lon"):
        return extracted.get("lon")
    if "radius" in normalized:
        radius = extracted.get("radius_km")
        if radius is None and extracted.get("lat") is not None and extracted.get("lon") is not None:
            return 1.0
        return radius
    return extracted.get(param_name)


def _has_required_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _apply_sql_safety(sql: str, params: list[Any], row_limit: int, max_limit: int) -> tuple[str, list[Any]]:
    lower_sql = sql.lower()
    if re.search(r"\bselect\s+\*", lower_sql):
        raise QueryPlanError("Unsafe query plan: SELECT * is not allowed.")

    placeholders = [int(value) for value in re.findall(r"\$(\d+)", sql)]
    if placeholders and max(placeholders) > len(params):
        raise QueryPlanError("Unsafe query plan: placeholder index is out of bounds.")

    limit_least_match = re.search(r"\blimit\s+least\s*\(\s*\$(\d+)\s*,\s*(\d+)\s*\)", lower_sql)
    if limit_least_match:
        param_idx = int(limit_least_match.group(1)) - 1
        template_cap = int(limit_least_match.group(2))
        safe_limit = min(row_limit, max_limit, template_cap)
        if 0 <= param_idx < len(params):
            params[param_idx] = safe_limit
        return sql, params

    limit_param_match = re.search(r"\blimit\s+\$(\d+)\b", lower_sql)
    if limit_param_match:
        param_idx = int(limit_param_match.group(1)) - 1
        safe_limit = min(row_limit, max_limit)
        if 0 <= param_idx < len(params):
            params[param_idx] = safe_limit
        return sql, params

    limit_const_match = re.search(r"\blimit\s+(\d+)\b", lower_sql)
    if limit_const_match:
        current_limit = int(limit_const_match.group(1))
        safe_limit = min(current_limit, max_limit)
        if safe_limit != current_limit:
            sql = re.sub(r"\blimit\s+\d+\b", f"LIMIT {safe_limit}", sql, flags=re.IGNORECASE)
        return sql, params

    sql = sql.rstrip().rstrip(";")
    sql = f"{sql} LIMIT {min(row_limit, max_limit)}"
    return sql, params


def _convert_postgres_params(sql: str, params: list[Any]) -> tuple[str, dict[str, Any]]:
    bind_params: dict[str, Any] = {}

    def repl(match: re.Match[str]) -> str:
        idx = int(match.group(1))
        if idx <= 0 or idx > len(params):
            raise QueryPlanError(f"Invalid SQL placeholder index ${idx}.")
        key = f"p{idx}"
        bind_params[key] = params[idx - 1]
        return f":{key}"

    converted = re.sub(r"\$(\d+)", repl, sql)
    return converted, bind_params


def _read_int_env(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _read_float_env(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


__all__ = [
    "AgentSchemaError",
    "DEFAULT_GEMINI_MODEL",
    "QueryPlanError",
    "SOURCE_OF_TRUTH_SCHEMA",
    "clearAgentSchemaCache",
    "executeParameterizedQuery",
    "getAgentSchema",
    "getAgentSchemaStatus",
    "isDatabaseQuestion",
    "proposeAgentSchemaFromTruth",
    "proposeQueryPlan",
    "renderDisplayPayload",
]
