# AGENTS.md

## Scope
This repo hosts the ImmunoStream / AIRR preprocessing web app. The backend is a FastAPI service that also serves the static UI.

## Entry points
- Backend: app/main.py (Dockerfile runs `uvicorn main:app`).
- UI: ui/ (static HTML/CSS/JS served at /ui).

## Data storage
- File-based JSON under /data (bind-mounted in Docker).
- Auth: /data/_auth/users.json and /data/_auth/tokens.json.
- Sessions: /data/users/<user_id>/sessions/<session_id>/state.json plus artifacts and logs.

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