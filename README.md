# GTFS Agent Chat (FastAPI + Postgres + Gemini)

Minimal interview app:
- FastAPI backend
- Simple chat frontend
- GTFS-only query agent over 4 source tables (`routes`, `trips`, `stop_times`, `stops`)
- Gemini generates and caches an agent schema (templates + display spec)
- Backend executes parameterized SQL safely with row limits

## 1) Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Required env vars:
- `DATABASE_URL`
- `GEMINI_API_KEY`

Optional env vars:
- `DATABASE_SSL` (default handled by code)
- `GEMINI_MODEL` (defaults to `gemini-2.0-flash`)
- `GEMINI_TIMEOUT_SECONDS` (default `20`)
- `MAX_RESULT_ROWS` (default `50`)
- `SCHEMA_CACHE_SECONDS` (default `300`)

## 2) Run

```bash
uvicorn app.main:app --reload
```

Open:
- http://127.0.0.1:8000

## 3) Behavior

1. `isDatabaseQuestion()` gates requests.
2. Non-DB requests return normal chat fallback and never call Gemini schema generation.
3. DB requests use cached `getAgentSchema()` (Gemini once per cache window).
4. `proposeQueryPlan()` selects a template and builds parameterized SQL.
5. SQL executes against Postgres with enforced row limits.
6. `renderDisplayPayload()` maps results to UI-friendly display templates.
