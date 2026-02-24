# Gemini + Railway Postgres Chat (FastAPI)

Minimal web app for interview demos:
- FastAPI backend
- Simple chat frontend (no auth, direct to chat)
- Gemini API call to produce structured query plans
- Read-only PostgreSQL execution against Railway

## 1) Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Fill `.env` with your real values:
- `GEMINI_API_KEY`
- `DATABASE_URL` (Railway Postgres URL)

## 2) Run

```bash
uvicorn app.main:app --reload
```

Open:
- http://127.0.0.1:8000

## 3) How it works

1. Frontend sends `message + history` to `/api/chat`.
2. Backend fetches DB schema from `information_schema`.
3. Backend prompts Gemini to return strict JSON:
   - `assistant_message`
   - `run_query`
   - optional query object (`sql`, `params`, `reason`)
4. Backend validates SQL as read-only, enforces `LIMIT`, runs query in a read-only transaction.
5. Frontend displays assistant text, executed SQL, and result rows.

## 4) Safety notes

- SQL is blocked unless it starts with `SELECT` or `WITH`.
- Common write/mutation keywords are rejected.
- Query runs with `readonly=True` transaction.
- Still treat this as demo-only. For production, add stronger SQL parsing/allowlists.
