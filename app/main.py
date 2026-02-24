from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from app.gtfs_agent import (
    AgentSchemaError,
    QueryPlanError,
    executeParameterizedQuery,
    getAgentSchema,
    getAgentSchemaStatus,
    isDatabaseQuestion,
    proposeQueryPlan,
    renderDisplayPayload,
)

BASE_DIR = Path(__file__).resolve().parent.parent
STATIC_DIR = BASE_DIR / "static"

app = FastAPI()
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


class ChatMessage(BaseModel):
    role: Literal["user", "assistant"]
    content: str = Field(min_length=1, max_length=4000)


class ChatRequest(BaseModel):
    message: str = Field(min_length=1, max_length=4000)
    history: list[ChatMessage] = Field(default_factory=list)


class ChatResponse(BaseModel):
    assistant_message: str
    query_executed: bool = False
    sql: str | None = None
    params: list[Any] = Field(default_factory=list)
    rows: list[dict[str, Any]] = Field(default_factory=list)
    row_count: int = 0
    error: str | None = None
    is_database_question: bool = False
    agent_schema: dict[str, Any] | None = None
    query_plan: dict[str, Any] | None = None
    display: dict[str, Any] | None = None
    agent_status: dict[str, Any] | None = None


@app.on_event("startup")
def warm_agent_schema() -> None:
    try:
        getAgentSchema()
    except AgentSchemaError:
        # Non-fatal: first DB question can still retry schema generation.
        return


def process_user_message(user_text: str) -> ChatResponse:
    if not isDatabaseQuestion(user_text):
        return ChatResponse(
            assistant_message=(
                "I can help with GTFS queries. Try: "
                "\"show me what routes there are\", "
                "\"arrivals for stop_id 1234\", "
                "\"top 10 busiest stops\", "
                "or \"accessible trips\"."
            ),
            is_database_question=False,
        )

    try:
        agent_schema = getAgentSchema()
        agent_status = getAgentSchemaStatus()
        query_plan = proposeQueryPlan(user_text, agent_schema)
    except (AgentSchemaError, QueryPlanError) as exc:
        return ChatResponse(
            assistant_message="I could not build a safe GTFS query plan right now.",
            error=str(exc),
            is_database_question=True,
            agent_status=getAgentSchemaStatus(),
        )

    clarifying_question = query_plan.get("clarifying_question")
    if clarifying_question:
        return ChatResponse(
            assistant_message=clarifying_question,
            is_database_question=True,
            agent_schema=agent_schema,
            query_plan=query_plan,
            agent_status=agent_status,
        )

    execution = executeParameterizedQuery(query_plan)
    if not execution.get("success"):
        raw_error = str(execution.get("error") or "")
        friendly_error = _friendly_db_error(raw_error)
        preview_display = renderDisplayPayload([], query_plan, agent_schema)
        return ChatResponse(
            assistant_message=(
                "I generated a valid query plan, but could not execute it right now. "
                f"{friendly_error}"
            ),
            query_executed=bool(execution.get("executed")),
            sql=query_plan.get("sql"),
            params=query_plan.get("params", []),
            error=raw_error,
            is_database_question=True,
            agent_schema=agent_schema,
            query_plan=query_plan,
            display=preview_display,
            agent_status=agent_status,
        )

    rows = execution.get("rows", [])
    display_payload = renderDisplayPayload(rows, query_plan, agent_schema)
    return ChatResponse(
        assistant_message=display_payload.get("title", "Query completed."),
        query_executed=True,
        sql=query_plan.get("sql"),
        params=query_plan.get("params", []),
        rows=rows,
        row_count=execution.get("row_count", len(rows)),
        is_database_question=True,
        agent_schema=agent_schema,
        query_plan=query_plan,
        display=display_payload,
        agent_status=agent_status,
    )


def _friendly_db_error(raw_error: str) -> str:
    lower = raw_error.lower()
    if "could not translate host name" in lower or "name or service not known" in lower:
        return (
            "Database host lookup failed. Use Railway's public connection URL for this deployment "
            "(not an internal-only host)."
        )
    if "password authentication failed" in lower:
        return "Database authentication failed. Verify DATABASE_URL username/password."
    if "timeout" in lower:
        return "Database connection timed out. Check network access and DB availability."
    return "Check DATABASE_URL / DATABASE_SSL settings and retry."


@app.get("/")
def root() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.post("/api/chat", response_model=ChatResponse)
def chat(request: ChatRequest) -> ChatResponse:
    return process_user_message(request.message)


@app.get("/health")
def health() -> dict[str, Any]:
    return {"ok": True}
