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
- `DATABASE_URL` (Railway internal URL; used when running on Railway)
- `DATABASE_PUBLIC_URL` (Railway public proxy URL; used outside Railway)
  (backward-compatible alias: `DATABASE_URL_PUBLIC`)
- `GEMINI_API_KEY`

Database URL selection:
1. If `RAILWAY_ENVIRONMENT` or `RAILWAY_PROJECT_ID` exists, app requires and uses `DATABASE_URL`.
2. Otherwise (local/Vercel/etc.), app prefers `DATABASE_PUBLIC_URL`.
3. If `DATABASE_PUBLIC_URL` is missing, app falls back to `DATABASE_URL`.

Vercel note:
- `VERCEL=1` is treated as external runtime even if Railway marker env vars are present, so `DATABASE_PUBLIC_URL` is selected.

Safety checks:
- Outside Railway runtime, internal hosts (`*.railway.internal`) are rejected with a clear startup error.
- Startup runs `SELECT 1` and fails fast when DB config is invalid.
- Public Railway hosts enforce SSL (`sslmode=require`).

Optional env vars:
- `DATABASE_SSL` (default handled by code)
- `GEMINI_MODEL` (defaults to `gemini-2.0-flash`)
- `GEMINI_TIMEOUT_SECONDS` (default `30`)
- `GEMINI_RETRY_COUNT` (default `1`, total attempts = retry + 1)
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
