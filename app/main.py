from pathlib import Path
from typing import Literal

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

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


@app.get("/")
def root():
    return FileResponse(STATIC_DIR / "index.html")


@app.post("/api/chat")
def chat(request: ChatRequest):
    return {
        "assistant_message": (
            "UI is connected. Next step is wiring this endpoint to Gemini for real answers."
        ),
        "query_executed": False,
        "sql": None,
        "rows": [],
        "row_count": 0,
        "error": None,
    }


@app.get("/health")
def health():
    return {"ok": True}
