# AGENTS.md

## Scope
This repo hosts the ImmunoStream / AIRR preprocessing web app. The backend is a FastAPI service that also serves the static UI.

## Entry points
- Backend: app/main.py (Dockerfile runs `uvicorn main:app`).
- UI: ui/ (static HTML/CSS/JS served at /ui).

## Data storage
- **PostgreSQL**: users, auth_tokens, sessions (with state_json JSONB). Set `DATABASE_URL` (e.g. in Docker Compose).
- **Session files** (uploads, logs, pipeline outputs): directory per session under `SESSION_FILES_DIR` (default `/data/session_files/<session_id>`). Bind-mounted in Docker as volume `session_files`.
- Tables: `users` (id, username, password_salt, password_hash, created_at), `auth_tokens` (token, user_id, created_at), `sessions` (id, user_id, display_name, created_at, updated_at, state_json).

## External tools
- pRESTO tools must be available on PATH: FilterSeq.py, MaskPrimers.py, CollapseSeq.py, BuildConsensus.py.

## API shape (high level)
- Auth: /auth/register, /auth/login, /auth/me.
- Sessions: /sessions, /session/start, /session/{id}/upload, /session/{id}/run, /session/{id}/state, /session/{id}/download, /session/{id}/log.

## Style and conventions
- See STYLE_GUIDE.md for formatting and naming rules.

## Notes
- There is a refactored module layout under app/api/, app/models.py, and app/utils/, but the running app currently uses app/main.py directly. Keep changes consistent with the entrypoint unless you also update the Dockerfile and wiring.
- No test files matched "*test*" in the repo root; verify if you add tests.