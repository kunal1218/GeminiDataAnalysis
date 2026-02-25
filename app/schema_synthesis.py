import copy
import hashlib
import json
import os
import re
import time
from threading import Lock
from typing import Any, Literal
from urllib import error as urlerror
from urllib import request as urlrequest

from pydantic import BaseModel, Field, ValidationError

from app.schema_options import getSchemaOptions as _get_schema_options

DEFAULT_GEMINI_MODEL = "gemini-2.0-flash"
_SCHEMA_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_SCHEMA_CACHE_LOCK = Lock()

_VALID_ON_DELETE = {"restrict", "cascade", "set null"}
_VALID_POSTGRES_TYPES = {
    "uuid",
    "text",
    "int",
    "integer",
    "bigint",
    "boolean",
    "timestamptz",
    "timestamp",
    "timestamp with time zone",
    "date",
    "jsonb",
    "numeric",
    "varchar",
}
_NUMERIC_PATTERN = re.compile(r"^numeric\(\d+(,\d+)?\)$", re.IGNORECASE)
_VARCHAR_PATTERN = re.compile(r"^varchar\(\d+\)$", re.IGNORECASE)

_TOP_LEVEL_KEYS = {"schema_name", "dialect", "tables", "selected_options", "rationale"}
_TABLE_KEYS = {"name", "columns", "indexes", "foreign_keys"}
_COLUMN_KEYS = {"name", "type", "nullable", "primary_key", "unique", "default"}
_INDEX_KEYS = {"name", "columns", "unique"}
_FK_KEYS = {"column", "ref_table", "ref_column", "on_delete"}


class SchemaSynthesisError(RuntimeError):
    pass


class SchemaValidationError(SchemaSynthesisError):
    pass


class SchemaColumn(BaseModel):
    name: str = Field(min_length=1)
    type: str = Field(min_length=1)
    nullable: bool
    primary_key: bool
    unique: bool
    default: str | None = None


class SchemaIndex(BaseModel):
    name: str = Field(min_length=1)
    columns: list[str] = Field(default_factory=list)
    unique: bool


class SchemaForeignKey(BaseModel):
    column: str = Field(min_length=1)
    ref_table: str = Field(min_length=1)
    ref_column: str = Field(min_length=1)
    on_delete: Literal["restrict", "cascade", "set null"]


class SchemaTable(BaseModel):
    name: str = Field(min_length=1)
    columns: list[SchemaColumn] = Field(default_factory=list)
    indexes: list[SchemaIndex] = Field(default_factory=list)
    foreign_keys: list[SchemaForeignKey] = Field(default_factory=list)


class ProposedSchema(BaseModel):
    schema_name: str = Field(min_length=1)
    dialect: Literal["postgres"]
    tables: list[SchemaTable] = Field(default_factory=list)
    selected_options: list[str] = Field(default_factory=list)
    rationale: str = Field(min_length=1)


def isDatabaseQuestion(userText: str) -> bool:
    if not userText:
        return False

    text = " ".join(userText.lower().split())
    if len(text) < 3:
        return False

    non_db_hints = (
        "dinner table",
        "furniture",
        "table tennis",
        "multiplication table",
        "restaurant",
        "chair",
    )
    if any(hint in text for hint in non_db_hints):
        return False

    high_confidence_patterns = (
        r"\bpostgres(?:ql)?\b",
        r"\bsql\b",
        r"\bdatabase\b",
        r"\bschema\b",
        r"\bforeign key(?:s)?\b",
        r"\bprimary key(?:s)?\b",
        r"\bmigrat(?:e|ion|ions)\b",
        r"\bnormaliz(?:e|ation)\b",
        r"\berd\b",
        r"\bentity[- ]relationship\b",
        r"\bprisma (?:model|schema)\b",
    )
    if any(re.search(pattern, text) for pattern in high_confidence_patterns):
        return True

    action_words = r"(create|design|define|build|generate|write|draft|model|optimize|list|show)"
    db_objects = r"(table|tables|column|columns|query|queries|index|indexes|join|joins|constraint|constraints)"
    if re.search(rf"\b{action_words}\b.*\b{db_objects}\b", text):
        return True
    if re.search(rf"\b{db_objects}\b.*\b{action_words}\b", text):
        return True

    query_like = (
        r"\bwhat tables\b",
        r"\bwhich tables\b",
        r"\bshow .* columns\b",
        r"\blist .* columns\b",
    )
    return any(re.search(pattern, text) for pattern in query_like)


def getSchemaOptions() -> dict[str, Any]:
    return _get_schema_options()


def proposeSchemaFromOptions(userRequest: str, schemaOptions: dict[str, Any]) -> ProposedSchema:
    if not isinstance(schemaOptions, dict):
        raise SchemaSynthesisError("schemaOptions must be an object.")

    cache_key = _build_cache_key(userRequest, schemaOptions)
    cached_schema = _get_cached_schema(cache_key)
    if cached_schema is not None:
        return cached_schema

    prompt = _build_schema_prompt(userRequest, schemaOptions)
    raw_response = _call_gemini_schema(prompt)

    try:
        proposed_schema = _parse_and_validate(raw_response, schemaOptions)
    except SchemaValidationError as first_error:
        repair_prompt = _build_repair_prompt(
            user_request=userRequest,
            schema_options=schemaOptions,
            invalid_output=raw_response,
            validation_error=str(first_error),
        )
        repair_response = _call_gemini_schema(repair_prompt)
        try:
            proposed_schema = _parse_and_validate(repair_response, schemaOptions)
        except SchemaValidationError as second_error:
            raise SchemaSynthesisError(
                "Schema synthesis failed: Gemini returned invalid JSON after one repair attempt."
            ) from second_error

    _set_cached_schema(cache_key, proposed_schema)
    return proposed_schema


def proposedSchemaToDict(schema: ProposedSchema) -> dict[str, Any]:
    if hasattr(schema, "model_dump"):
        return schema.model_dump()
    return schema.dict()


def clearSchemaProposalCache() -> None:
    with _SCHEMA_CACHE_LOCK:
        _SCHEMA_CACHE.clear()


def _build_cache_key(user_request: str, schema_options: dict[str, Any]) -> str:
    version = str(schema_options.get("version", "v0"))
    normalized_request = " ".join(user_request.split()).strip().lower()
    raw_key = f"{normalized_request}|{version}"
    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()


def _get_cache_ttl_seconds() -> int:
    raw_ttl = os.getenv("SCHEMA_CACHE_SECONDS", "300")
    try:
        return max(0, int(raw_ttl))
    except ValueError:
        return 300


def _get_cached_schema(cache_key: str) -> ProposedSchema | None:
    ttl_seconds = _get_cache_ttl_seconds()
    if ttl_seconds <= 0:
        return None

    now = time.time()
    with _SCHEMA_CACHE_LOCK:
        cached = _SCHEMA_CACHE.get(cache_key)
        if cached is None:
            return None
        created_at, payload = cached
        if now - created_at > ttl_seconds:
            _SCHEMA_CACHE.pop(cache_key, None)
            return None
        return _coerce_proposed_schema(copy.deepcopy(payload))


def _set_cached_schema(cache_key: str, schema: ProposedSchema) -> None:
    ttl_seconds = _get_cache_ttl_seconds()
    if ttl_seconds <= 0:
        return

    payload = proposedSchemaToDict(schema)
    with _SCHEMA_CACHE_LOCK:
        _SCHEMA_CACHE[cache_key] = (time.time(), copy.deepcopy(payload))


def _get_gemini_model() -> str:
    configured = os.getenv("GEMINI_MODEL", "").strip()
    if configured:
        return configured
    return DEFAULT_GEMINI_MODEL


def _get_gemini_timeout_seconds() -> float:
    raw_timeout = os.getenv("GEMINI_TIMEOUT_SECONDS", "20")
    try:
        return max(1.0, float(raw_timeout))
    except ValueError:
        return 20.0


def _call_gemini_schema(prompt: str) -> str:
    api_key = os.getenv("GEMINI_API_KEY", "").strip()
    if not api_key:
        raise SchemaSynthesisError("Schema synthesis is unavailable because GEMINI_API_KEY is missing.")

    model = _get_gemini_model()
    timeout_seconds = _get_gemini_timeout_seconds()
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"

    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0,
            "responseMimeType": "application/json",
        },
    }
    body = json.dumps(payload).encode("utf-8")
    request = urlrequest.Request(
        url=url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urlrequest.urlopen(request, timeout=timeout_seconds) as response:
            raw_response = response.read().decode("utf-8")
    except urlerror.HTTPError as exc:
        raise SchemaSynthesisError(f"Gemini request failed with HTTP {exc.code}.") from exc
    except urlerror.URLError as exc:
        raise SchemaSynthesisError("Gemini request failed due to a network error.") from exc
    except TimeoutError as exc:
        raise SchemaSynthesisError("Gemini request timed out.") from exc

    try:
        response_payload = json.loads(raw_response)
    except json.JSONDecodeError as exc:
        raise SchemaSynthesisError("Gemini returned a non-JSON API response.") from exc

    candidates = response_payload.get("candidates", [])
    if not candidates:
        raise SchemaSynthesisError("Gemini returned no candidates.")

    parts = candidates[0].get("content", {}).get("parts", [])
    text_output = "".join(part.get("text", "") for part in parts if isinstance(part, dict)).strip()
    if not text_output:
        raise SchemaSynthesisError("Gemini returned an empty candidate payload.")
    return text_output


def _build_schema_prompt(user_request: str, schema_options: dict[str, Any]) -> str:
    options_json = json.dumps(schema_options, separators=(",", ":"), ensure_ascii=True)
    setup_mandate = _build_setup_mandate_text()
    return (
        f"{setup_mandate}\n"
        "You are a PostgreSQL schema selector.\n"
        "Select ONLY from schemaOptions.table_options. Never invent new tables, columns, indexes, or foreign keys.\n"
        "If user asks for unsupported entities, choose the closest allowed options and explain tradeoffs in rationale.\n"
        "Return ONLY valid JSON with EXACT keys and structure:\n"
        '{'
        '"schema_name": string,'
        '"dialect":"postgres",'
        '"tables":[{"name":string,"columns":[{"name":string,"type":string,"nullable":boolean,'
        '"primary_key":boolean,"unique":boolean,"default":string|null}],"indexes":[{"name":string,'
        '"columns":[string],"unique":boolean}],"foreign_keys":[{"column":string,"ref_table":string,'
        '"ref_column":string,"on_delete":"restrict"|"cascade"|"set null"}]}],'
        '"selected_options":[string],'
        '"rationale":string'
        '}.\n'
        "All arrays must be present even if empty.\n"
        "Do not add any extra keys.\n"
        f"schemaOptions={options_json}\n"
        f"userRequest={user_request.strip()}"
    )


def _build_repair_prompt(
    user_request: str,
    schema_options: dict[str, Any],
    invalid_output: str,
    validation_error: str,
) -> str:
    options_json = json.dumps(schema_options, separators=(",", ":"), ensure_ascii=True)
    setup_mandate = _build_setup_mandate_text()
    return (
        f"{setup_mandate}\n"
        "Your previous response was invalid.\n"
        f"Validation error: {validation_error}\n"
        "Fix it now.\n"
        "Return JSON only, no markdown.\n"
        "Use ONLY items from schemaOptions.\n"
        "No extra keys allowed.\n"
        f"schemaOptions={options_json}\n"
        f"userRequest={user_request.strip()}\n"
        f"invalidOutput={invalid_output}"
    )


def _build_setup_mandate_text() -> str:
    return (
        "MANDATORY: YOU MUST ADHERE EXACTLY TO OUR SETUP, CONTRACT SHAPE, AND VALIDATION RULES.\n"
        "MANDATORY: DO NOT INVENT TABLES, COLUMNS, INDEXES, FOREIGN KEYS, OR EXTRA KEYS.\n"
        "MANDATORY: IF YOU CANNOT COMPLY EXACTLY, RETURN THE SPECIFIED JSON WITH A CLEAR SAFE RATIONALE."
    )


def _parse_and_validate(raw_response_text: str, schema_options: dict[str, Any]) -> ProposedSchema:
    try:
        payload = _extract_json_payload(raw_response_text)
    except (ValueError, json.JSONDecodeError) as exc:
        raise SchemaValidationError(f"Invalid JSON payload from Gemini: {exc}") from exc

    return validateProposedSchema(payload, schema_options)


def validateProposedSchema(payload: dict[str, Any], schema_options: dict[str, Any]) -> ProposedSchema:
    _validate_contract_shape(payload)
    proposed_schema = _coerce_proposed_schema(payload)
    _validate_types(proposed_schema)
    _validate_against_schema_options(proposed_schema, schema_options)
    return proposed_schema


def _coerce_proposed_schema(payload: dict[str, Any]) -> ProposedSchema:
    try:
        if hasattr(ProposedSchema, "model_validate"):
            return ProposedSchema.model_validate(payload)
        return ProposedSchema.parse_obj(payload)
    except ValidationError as exc:
        raise SchemaValidationError(f"Schema contract parse failed: {exc}") from exc


def _extract_json_payload(raw_text: str) -> dict[str, Any]:
    text = raw_text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*```$", "", text)

    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end < 0 or end <= start:
        raise ValueError("Response does not contain a JSON object.")
    return json.loads(text[start : end + 1])


def _validate_contract_shape(payload: Any) -> None:
    if not isinstance(payload, dict):
        raise SchemaValidationError("Top-level payload must be an object.")
    _validate_exact_keys(payload, _TOP_LEVEL_KEYS, "root")

    tables = payload.get("tables")
    if not isinstance(tables, list):
        raise SchemaValidationError("tables must be an array.")

    selected_options = payload.get("selected_options")
    if not isinstance(selected_options, list):
        raise SchemaValidationError("selected_options must be an array.")

    for table_index, table in enumerate(tables):
        if not isinstance(table, dict):
            raise SchemaValidationError(f"tables[{table_index}] must be an object.")
        _validate_exact_keys(table, _TABLE_KEYS, f"tables[{table_index}]")

        columns = table.get("columns")
        indexes = table.get("indexes")
        foreign_keys = table.get("foreign_keys")

        if not isinstance(columns, list):
            raise SchemaValidationError(f"tables[{table_index}].columns must be an array.")
        if not isinstance(indexes, list):
            raise SchemaValidationError(f"tables[{table_index}].indexes must be an array.")
        if not isinstance(foreign_keys, list):
            raise SchemaValidationError(f"tables[{table_index}].foreign_keys must be an array.")

        for col_index, column in enumerate(columns):
            if not isinstance(column, dict):
                raise SchemaValidationError(
                    f"tables[{table_index}].columns[{col_index}] must be an object."
                )
            _validate_exact_keys(
                column,
                _COLUMN_KEYS,
                f"tables[{table_index}].columns[{col_index}]",
            )

        for idx_index, index in enumerate(indexes):
            if not isinstance(index, dict):
                raise SchemaValidationError(
                    f"tables[{table_index}].indexes[{idx_index}] must be an object."
                )
            _validate_exact_keys(index, _INDEX_KEYS, f"tables[{table_index}].indexes[{idx_index}]")

        for fk_index, foreign_key in enumerate(foreign_keys):
            if not isinstance(foreign_key, dict):
                raise SchemaValidationError(
                    f"tables[{table_index}].foreign_keys[{fk_index}] must be an object."
                )
            _validate_exact_keys(
                foreign_key,
                _FK_KEYS,
                f"tables[{table_index}].foreign_keys[{fk_index}]",
            )


def _validate_exact_keys(payload: dict[str, Any], expected_keys: set[str], path: str) -> None:
    payload_keys = set(payload.keys())
    missing_keys = expected_keys - payload_keys
    extra_keys = payload_keys - expected_keys
    if missing_keys or extra_keys:
        raise SchemaValidationError(
            f"{path} has invalid keys. Missing={sorted(missing_keys)} Extra={sorted(extra_keys)}"
        )


def _validate_types(proposed_schema: ProposedSchema) -> None:
    for table in proposed_schema.tables:
        for column in table.columns:
            normalized_type = column.type.strip().lower()
            if (
                normalized_type not in _VALID_POSTGRES_TYPES
                and not _NUMERIC_PATTERN.match(normalized_type)
                and not _VARCHAR_PATTERN.match(normalized_type)
            ):
                raise SchemaValidationError(
                    f"Unsupported column type '{column.type}' for table '{table.name}'."
                )

        for foreign_key in table.foreign_keys:
            if foreign_key.on_delete.lower() not in _VALID_ON_DELETE:
                raise SchemaValidationError(
                    f"Invalid on_delete='{foreign_key.on_delete}' for table '{table.name}'."
                )


def _validate_against_schema_options(
    proposed_schema: ProposedSchema,
    schema_options: dict[str, Any],
) -> None:
    table_options = schema_options.get("table_options")
    if not isinstance(table_options, list):
        raise SchemaValidationError("schemaOptions.table_options must be an array.")

    option_map: dict[str, dict[str, Any]] = {}
    for option in table_options:
        if not isinstance(option, dict):
            continue
        option_id = option.get("id")
        table = option.get("table")
        if isinstance(option_id, str) and isinstance(table, dict):
            option_map[option_id] = table

    if not proposed_schema.selected_options:
        raise SchemaValidationError("selected_options must include at least one option id.")
    if len(set(proposed_schema.selected_options)) != len(proposed_schema.selected_options):
        raise SchemaValidationError("selected_options must not contain duplicates.")

    for selected in proposed_schema.selected_options:
        if selected not in option_map:
            raise SchemaValidationError(f"selected_options contains unknown option '{selected}'.")

    expected_tables: dict[str, dict[str, Any]] = {}
    for selected in proposed_schema.selected_options:
        table = option_map[selected]
        table_name = table.get("name")
        if isinstance(table_name, str):
            expected_tables[table_name] = table

    proposed_table_map = {table.name: table for table in proposed_schema.tables}
    if len(proposed_table_map) != len(proposed_schema.tables):
        raise SchemaValidationError("tables must not contain duplicate names.")

    expected_names = set(expected_tables.keys())
    proposed_names = set(proposed_table_map.keys())

    missing_tables = expected_names - proposed_names
    extra_tables = proposed_names - expected_names
    if missing_tables or extra_tables:
        raise SchemaValidationError(
            f"tables do not match selected_options. Missing={sorted(missing_tables)} Extra={sorted(extra_tables)}"
        )

    for table_name, proposed_table in proposed_table_map.items():
        expected_table = expected_tables[table_name]
        _validate_table_exact_match(table_name, proposed_table, expected_table)


def _validate_table_exact_match(
    table_name: str,
    proposed_table: SchemaTable,
    expected_table: dict[str, Any],
) -> None:
    expected_columns = {column["name"]: column for column in expected_table.get("columns", [])}
    expected_indexes = {index["name"]: index for index in expected_table.get("indexes", [])}
    expected_foreign_keys = {
        foreign_key["column"]: foreign_key for foreign_key in expected_table.get("foreign_keys", [])
    }

    if len(proposed_table.columns) != len(expected_columns):
        raise SchemaValidationError(f"Table '{table_name}' has unexpected columns.")
    for column in proposed_table.columns:
        expected_column = expected_columns.get(column.name)
        if expected_column is None:
            raise SchemaValidationError(f"Table '{table_name}' has unknown column '{column.name}'.")

        if column.type != expected_column.get("type"):
            raise SchemaValidationError(
                f"Table '{table_name}' column '{column.name}' has invalid type '{column.type}'."
            )
        if column.nullable != expected_column.get("nullable"):
            raise SchemaValidationError(
                f"Table '{table_name}' column '{column.name}' has invalid nullable."
            )
        if column.primary_key != expected_column.get("primary_key"):
            raise SchemaValidationError(
                f"Table '{table_name}' column '{column.name}' has invalid primary_key."
            )
        if column.unique != expected_column.get("unique"):
            raise SchemaValidationError(
                f"Table '{table_name}' column '{column.name}' has invalid unique."
            )
        if _normalize_default(column.default) != _normalize_default(expected_column.get("default")):
            raise SchemaValidationError(
                f"Table '{table_name}' column '{column.name}' has invalid default."
            )

    if len(proposed_table.indexes) != len(expected_indexes):
        raise SchemaValidationError(f"Table '{table_name}' has unexpected indexes.")
    for index in proposed_table.indexes:
        expected_index = expected_indexes.get(index.name)
        if expected_index is None:
            raise SchemaValidationError(f"Table '{table_name}' has unknown index '{index.name}'.")
        if index.columns != expected_index.get("columns"):
            raise SchemaValidationError(f"Table '{table_name}' index '{index.name}' has invalid columns.")
        if index.unique != expected_index.get("unique"):
            raise SchemaValidationError(f"Table '{table_name}' index '{index.name}' has invalid unique.")

    if len(proposed_table.foreign_keys) != len(expected_foreign_keys):
        raise SchemaValidationError(f"Table '{table_name}' has unexpected foreign keys.")
    for foreign_key in proposed_table.foreign_keys:
        expected_fk = expected_foreign_keys.get(foreign_key.column)
        if expected_fk is None:
            raise SchemaValidationError(
                f"Table '{table_name}' has unknown foreign key on '{foreign_key.column}'."
            )
        if foreign_key.ref_table != expected_fk.get("ref_table"):
            raise SchemaValidationError(
                f"Table '{table_name}' foreign key '{foreign_key.column}' has invalid ref_table."
            )
        if foreign_key.ref_column != expected_fk.get("ref_column"):
            raise SchemaValidationError(
                f"Table '{table_name}' foreign key '{foreign_key.column}' has invalid ref_column."
            )
        if foreign_key.on_delete != expected_fk.get("on_delete"):
            raise SchemaValidationError(
                f"Table '{table_name}' foreign key '{foreign_key.column}' has invalid on_delete."
            )


def _normalize_default(value: Any) -> str | None:
    if value is None:
        return None
    return str(value).strip()


__all__ = [
    "DEFAULT_GEMINI_MODEL",
    "ProposedSchema",
    "SchemaSynthesisError",
    "SchemaValidationError",
    "clearSchemaProposalCache",
    "getSchemaOptions",
    "isDatabaseQuestion",
    "proposeSchemaFromOptions",
    "proposedSchemaToDict",
    "validateProposedSchema",
]
