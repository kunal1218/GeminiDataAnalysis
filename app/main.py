from fastapi import FastAPI

app = FastAPI()


@app.get("/")
def root():
    return {"ok": True, "service": "gemini-data-analysis", "health": "/health"}


@app.get("/health")
def health():
    return {"ok": True}
