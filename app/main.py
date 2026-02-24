import os
from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from app.schema_synthesis import (
    SchemaSynthesisError,
    getSchemaOptions,
    isDatabaseQuestion,
    proposeSchemaFromOptions,
    proposedSchemaToDict,
)
from app.schema_execution import SchemaExecutionError, execute_schema_proposal

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
    rows: list[dict[str, Any]] = Field(default_factory=list)
    row_count: int = 0
    error: str | None = None
    proposed_schema: dict[str, Any] | None = None
    schema_execution: dict[str, Any] | None = None
    is_database_question: bool = False


def _schema_execution_mode() -> str:
    mode = os.getenv("SCHEMA_EXECUTION_MODE", "dry_run").strip().lower()
    if mode not in {"dry_run", "apply"}:
        return "dry_run"
    return mode


def process_user_message(user_text: str) -> ChatResponse:
    if not isDatabaseQuestion(user_text):
        return ChatResponse(
            assistant_message=(
                "I can synthesize database schemas when your request is about SQL, tables, "
                "columns, indexes, migrations, or Postgres design."
            ),
            is_database_question=False,
        )

    try:
        schema_options = getSchemaOptions()
        proposed_schema = proposeSchemaFromOptions(user_text, schema_options)
    except SchemaSynthesisError:
        return ChatResponse(
            assistant_message=(
                "I could not generate a valid schema proposal from the allowed options right now."
            ),
            error="Schema synthesis unavailable. Please retry with a clearer DB request.",
            is_database_question=True,
        )

    proposal_dict = proposedSchemaToDict(proposed_schema)
    selected = proposal_dict.get("selected_options", [])
    selected_text = ", ".join(selected) if selected else "no options"

    try:
        execution_result = execute_schema_proposal(
            proposal_dict,
            mode=_schema_execution_mode(),
        )
    except SchemaExecutionError:
        execution_result = {
            "executed": False,
            "mode": _schema_execution_mode(),
            "success": False,
            "statement_count": 0,
            "statements": [],
            "error": "Schema execution failed before reaching database execution.",
        }

    if execution_result.get("success"):
        assistant_message = (
            f"Schema proposal ready from allowed options: {selected_text}. "
            f"Execution mode={execution_result.get('mode')} succeeded."
        )
    else:
        assistant_message = (
            f"Schema proposal ready from allowed options: {selected_text}. "
            f"Execution mode={execution_result.get('mode')} failed."
        )

    return ChatResponse(
        assistant_message=assistant_message,
        proposed_schema=proposal_dict,
        schema_execution=execution_result,
        error=execution_result.get("error"),
        is_database_question=True,
    )


@app.get("/")
def root():
    return FileResponse(STATIC_DIR / "index.html")


@app.post("/api/chat", response_model=ChatResponse)
def chat(request: ChatRequest):
    return process_user_message(request.message)


@app.get("/health")
def health():
    return {"ok": True}
