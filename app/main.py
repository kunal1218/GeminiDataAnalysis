import json
import os
import re
import ssl
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Literal

import asyncpg
import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, ValidationError

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
STATIC_DIR = BASE_DIR / "static"

DATABASE_URL = os.getenv("DATABASE_URL", "")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")
MAX_RESULT_ROWS = int(os.getenv("MAX_RESULT_ROWS", "50"))
SCHEMA_CACHE_SECONDS = int(os.getenv("SCHEMA_CACHE_SECONDS", "300"))
GEMINI_TIMEOUT_SECONDS = float(os.getenv("GEMINI_TIMEOUT_SECONDS", "20"))

FORBIDDEN_SQL_RE = re.compile(
    r"\b(insert|update|delete|drop|alter|create|truncate|grant|revoke|"
    r"copy|merge|call|do|vacuum|analyze|comment|refresh)\b",
    re.IGNORECASE,
)

_db_pool: asyncpg.Pool | None = None
_schema_cache_text = ""
_schema_cache_loaded_at = 0.0


class ChatMessage(BaseModel):
    role: Literal["user", "assistant"]
    content: str = Field(min_length=1, max_length=4_000)


class ChatRequest(BaseModel):
    message: str = Field(min_length=1, max_length=4_000)
    history: list[ChatMessage] = Field(default_factory=list)


class QueryPlan(BaseModel):
    sql: str = Field(min_length=1, max_length=8_000)
    params: list[Any] = Field(default_factory=list)
    reason: str = ""


class GeminiPlan(BaseModel):
    assistant_message: str = ""
    run_query: bool = False
    query: QueryPlan | None = None


class ChatResponse(BaseModel):
    assistant_message: str
    query_executed: bool = False
    sql: str | None = None
    rows: list[dict[str, Any]] = Field(default_factory=list)
    row_count: int = 0
    error: str | None = None


app = FastAPI(title="Gemini DB Chat")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


def _build_ssl_context() -> ssl.SSLContext | None:
    ssl_value = os.getenv("DATABASE_SSL")
    if ssl_value is not None:
        if ssl_value.lower() in {"true", "1", "yes", "require"}:
            return ssl.create_default_context()
        return None
    if "railway" in DATABASE_URL:
        return ssl.create_default_context()
    return None


def _require_db_pool() -> asyncpg.Pool:
    if _db_pool is None:
        raise HTTPException(status_code=500, detail="Database is not initialized.")
    return _db_pool


def _extract_json_payload(text: str) -> dict[str, Any]:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*```$", "", cleaned)

    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start == -1 or end == -1:
        raise ValueError("Gemini response did not contain JSON.")
    return json.loads(cleaned[start : end + 1])


def _validate_read_only_sql(raw_sql: str) -> str:
    sql = raw_sql.strip().rstrip(";").strip()
    if not sql:
        raise HTTPException(status_code=400, detail="Empty SQL query.")
    if ";" in sql:
        raise HTTPException(status_code=400, detail="Multiple SQL statements are not allowed.")
    if not re.match(r"^(select|with)\b", sql, flags=re.IGNORECASE):
        raise HTTPException(status_code=400, detail="Only SELECT queries are allowed.")
    if FORBIDDEN_SQL_RE.search(sql):
        raise HTTPException(status_code=400, detail="Non read-only SQL is not allowed.")
    return sql


def _enforce_limit(sql: str) -> str:
    if re.search(r"\blimit\s+\d+\b", sql, flags=re.IGNORECASE):
        return sql
    if re.search(r"\bcount\s*\(", sql, flags=re.IGNORECASE):
        return sql
    return f"{sql} LIMIT {MAX_RESULT_ROWS}"


def _history_to_text(history: list[ChatMessage]) -> str:
    history_tail = history[-8:]
    lines = []
    for msg in history_tail:
        speaker = "User" if msg.role == "user" else "Assistant"
        lines.append(f"{speaker}: {msg.content.strip()}")
    return "\n".join(lines) if lines else "(no previous messages)"


def _build_planner_prompt(message: str, history: list[ChatMessage], schema: str) -> str:
    return f"""
You are a PostgreSQL read-only query planner.
You must return strict JSON only.

Output schema:
{{
  "assistant_message": "string",
  "run_query": true or false,
  "query": {{
    "sql": "PostgreSQL SELECT statement using asyncpg placeholders like $1",
    "params": ["value1", 123, null],
    "reason": "short reason for this query"
  }} or null
}}

Rules:
1) Never produce write queries (INSERT/UPDATE/DELETE/ALTER/DROP/CREATE/etc).
2) Use only SELECT or WITH...SELECT.
3) Keep assistant_message concise.
4) If question is unrelated to the available schema, set run_query=false and query=null.
5) If a query is needed, include LIMIT {MAX_RESULT_ROWS} unless user asks for aggregate counts only.
6) Return raw JSON only. No code fences.

Database schema:
{schema}

Conversation so far:
{_history_to_text(history)}

Latest user message:
{message}
""".strip()


async def _call_gemini(prompt: str) -> GeminiPlan:
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.1,
            "responseMimeType": "application/json",
        },
    }
    params = {"key": GEMINI_API_KEY}

    async with httpx.AsyncClient(timeout=GEMINI_TIMEOUT_SECONDS) as client:
        response = await client.post(url, params=params, json=payload)
        response.raise_for_status()
        data = response.json()

    candidates = data.get("candidates", [])
    if not candidates:
        raise HTTPException(status_code=502, detail="Gemini returned no candidates.")

    parts = candidates[0].get("content", {}).get("parts", [])
    text = "".join(part.get("text", "") for part in parts if part.get("text"))
    if not text:
        raise HTTPException(status_code=502, detail="Gemini returned empty content.")

    try:
        parsed = _extract_json_payload(text)
        return GeminiPlan.model_validate(parsed)
    except (ValueError, json.JSONDecodeError, ValidationError) as exc:
        raise HTTPException(status_code=502, detail=f"Invalid Gemini JSON payload: {exc}") from exc


async def _refresh_schema_cache() -> str:
    global _schema_cache_text, _schema_cache_loaded_at

    now = time.time()
    if _schema_cache_text and (now - _schema_cache_loaded_at) < SCHEMA_CACHE_SECONDS:
        return _schema_cache_text

    pool = _require_db_pool()
    query = """
        SELECT
            table_schema,
            table_name,
            column_name,
            data_type
        FROM information_schema.columns
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY table_schema, table_name, ordinal_position
        LIMIT 1000;
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query)

    table_map: dict[str, list[str]] = defaultdict(list)
    for row in rows:
        table = f"{row['table_schema']}.{row['table_name']}"
        table_map[table].append(f"{row['column_name']} ({row['data_type']})")

    if not table_map:
        _schema_cache_text = "No tables discovered."
    else:
        lines = []
        for table_name, columns in table_map.items():
            lines.append(f"- {table_name}: {', '.join(columns)}")
        _schema_cache_text = "\n".join(lines)
    _schema_cache_loaded_at = now
    return _schema_cache_text


async def _run_read_only_query(sql: str, params: list[Any]) -> list[dict[str, Any]]:
    pool = _require_db_pool()
    async with pool.acquire() as conn:
        async with conn.transaction(readonly=True):
            rows = await conn.fetch(sql, *params)
    return [dict(row) for row in rows]


@app.on_event("startup")
async def on_startup() -> None:
    global _db_pool
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is required.")
    if not GEMINI_API_KEY:
        raise RuntimeError("GEMINI_API_KEY is required.")

    _db_pool = await asyncpg.create_pool(
        dsn=DATABASE_URL,
        min_size=1,
        max_size=5,
        command_timeout=15,
        ssl=_build_ssl_context(),
    )


@app.on_event("shutdown")
async def on_shutdown() -> None:
    global _db_pool
    if _db_pool is not None:
        await _db_pool.close()
        _db_pool = None


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/health")
async def health() -> dict[str, Any]:
    return {
        "status": "ok",
        "db_configured": bool(DATABASE_URL),
        "gemini_configured": bool(GEMINI_API_KEY),
        "model": GEMINI_MODEL,
    }


@app.post("/api/chat", response_model=ChatResponse)
async def chat(request: ChatRequest) -> ChatResponse:
    schema_text = await _refresh_schema_cache()
    prompt = _build_planner_prompt(request.message, request.history, schema_text)
    plan = await _call_gemini(prompt)

    assistant_text = plan.assistant_message.strip() or "I can help with that."
    response = ChatResponse(assistant_message=assistant_text)

    if not plan.run_query:
        return response
    if plan.query is None:
        return response

    if not isinstance(plan.query.params, list):
        raise HTTPException(status_code=400, detail="Query params must be a list.")

    sql = _enforce_limit(_validate_read_only_sql(plan.query.sql))
    try:
        rows = await _run_read_only_query(sql, plan.query.params)
    except Exception as exc:
        response.error = f"Query execution failed: {exc}"
        return response

    response.query_executed = True
    response.sql = sql
    response.rows = rows
    response.row_count = len(rows)
    return response
