"""
API routes for the application.

This module contains all FastAPI route handlers.
"""
import uuid
import pathlib
from typing import Optional, Dict, Any

from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Body, Depends
from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from app.config import BASE_DIR
from app.models import RunBody, SessionState, AuthBody
from app.units import UNITS
from app.utils import (
    save_upload_canonical,
    guess_aux_role,
    load_state,
    save_state,
)
from app import auth_utils

router = APIRouter()

auth_scheme = HTTPBearer(auto_error=False)


def _user_sessions_root(user_id: str) -> pathlib.Path:
    return BASE_DIR / "users" / user_id / "sessions"


def _session_dir_for_user(user_id: str, session_id: str) -> pathlib.Path:
    return _user_sessions_root(user_id) / session_id


def _require_user(
    creds: Optional[HTTPAuthorizationCredentials] = Depends(auth_scheme),
) -> Dict[str, str]:
    if not creds or not creds.credentials:
        raise HTTPException(status_code=401, detail="Missing auth token.")
    record = auth_utils.get_user_by_token(BASE_DIR, creds.credentials)
    if not record:
        raise HTTPException(status_code=401, detail="Invalid or expired token.")
    return record


def _load_session_for_user(
    user: Dict[str, str],
    session_id: str,
) -> tuple[pathlib.Path, SessionState]:
    session_dir = _session_dir_for_user(user["user_id"], session_id)
    if not session_dir.exists():
        raise HTTPException(status_code=404, detail="Session not found.")
    state = load_state(session_dir)
    if state.owner_user_id and state.owner_user_id != user["user_id"]:
        raise HTTPException(status_code=403, detail="Access denied for this session.")
    return session_dir, state


@router.post("/auth/register")
def register(body: AuthBody = Body(...)):
    """Register a new user and return an auth token."""
    try:
        record = auth_utils.create_user(BASE_DIR, body.username, body.password)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    token = auth_utils.create_token(BASE_DIR, record["user_id"], record["username"])
    return {"user_id": record["user_id"], "username": record["username"], "token": token}


@router.post("/auth/login")
def login(body: AuthBody = Body(...)):
    """Login and return an auth token."""
    record = auth_utils.authenticate_user(BASE_DIR, body.username, body.password)
    if not record:
        raise HTTPException(status_code=401, detail="Invalid username or password.")
    token = auth_utils.create_token(BASE_DIR, record["user_id"], record["username"])
    return {"user_id": record["user_id"], "username": record["username"], "token": token}


@router.get("/auth/me")
def get_me(user: Dict[str, str] = Depends(_require_user)):
    """Return the current authenticated user."""
    return {"user_id": user["user_id"], "username": user["username"]}


@router.get("/sessions")
def list_sessions(user: Dict[str, str] = Depends(_require_user)):
    """List sessions for the current user."""
    sessions_dir = _user_sessions_root(user["user_id"])
    if not sessions_dir.exists():
        return []
    results = []
    def infer_group(state: SessionState) -> str:
        if any((step.unit or "").startswith("sc_") for step in state.steps):
            return "sc"
        if state.steps:
            return "bulk"
        if any(name.lower().endswith((".tsv", ".tsv.gz")) for name in (state.aux_files or [])):
            return "sc"
        if any((art.path or "").lower().endswith((".tsv", ".tsv.gz")) for art in state.artifacts.values()):
            return "sc"
        if "SC_TABLE" in (state.current or {}):
            return "sc"
        if any(ch in (state.current or {}) for ch in ("R1", "R2")):
            return "bulk"
        if any(art.kind in ("fastq", "fasta") for art in state.artifacts.values()):
            return "bulk"
        return "unknown"
    for entry in sorted(sessions_dir.iterdir(), key=lambda p: p.name):
        if not entry.is_dir():
            continue
        state_file = entry / "state.json"
        if not state_file.exists():
            results.append({"session_id": entry.name, "group": "unknown"})
            continue
        try:
            state = SessionState.model_validate_json(state_file.read_text())
        except Exception:
            results.append({"session_id": entry.name, "group": "unknown"})
            continue
        if not state.steps:
            continue
        group = infer_group(state)
        results.append({
            "session_id": state.session_id,
            "created_at": state.created_at,
            "updated_at": state.updated_at,
            "steps": len(state.steps),
            "artifacts": len(state.artifacts),
            "group": group,
        })
    return results


@router.post("/session/start")
def start_session(user: Dict[str, str] = Depends(_require_user)):
    """Create a new processing session."""
    session_id = str(uuid.uuid4())
    session_dir = _session_dir_for_user(user["user_id"], session_id)
    session_dir.mkdir(parents=True, exist_ok=True)
    state = SessionState(
        session_id=session_id,
        owner_user_id=user["user_id"],
        owner_username=user["username"],
    )
    save_state(session_dir, state)
    return {"session_id": session_id}


@router.get("/session/{session_id}/units")
def list_units(session_id: str, user: Dict[str, str] = Depends(_require_user)):
    """
    List all available processing units.
    
    Args:
        session_id: Session identifier
        
    Returns:
        List of unit definitions
    """
    _ = _load_session_for_user(user, session_id)  # Validate session exists

    def get_group(unit):
        """Get unit group, with fallback for legacy units."""
        try:
            return unit.group
        except Exception:
            # Fallback if any instance lacks 'group'
            return "sc" if (getattr(unit, "id", "") or "").startswith("sc_") else "bulk"

    return [
        {
            "id": unit.id,
            "label": unit.label,
            "requires": unit.requires,
            "params_schema": unit.params_schema,
            "group": get_group(unit),
        }
        for unit in UNITS.values()
    ]


@router.post("/session/{session_id}/upload")
async def upload_reads(
    session_id: str,
    r1: UploadFile = File(...),
    r2: Optional[UploadFile] = File(None),
    user: Dict[str, str] = Depends(_require_user),
):
    """
    Upload read files (R1 and optional R2).
    
    Args:
        session_id: Session identifier
        r1: R1 read file
        r2: Optional R2 read file
        
    Returns:
        Upload status and current artifacts
    """
    session_dir, session = _load_session_for_user(user, session_id)
    
    artifact1 = save_upload_canonical(r1, "R1", session_dir)
    session.artifacts[artifact1.name] = artifact1
    session.current["R1"] = artifact1.name
    
    if r2:
        artifact2 = save_upload_canonical(r2, "R2", session_dir)
        session.artifacts[artifact2.name] = artifact2
        session.current["R2"] = artifact2.name
    
    save_state(session_dir, session)
    return {
        "ok": True,
        "current": session.current,
        "artifacts": list(session.artifacts.keys())
    }


@router.post("/session/{session_id}/upload-aux")
async def upload_aux_file(
    session_id: str,
    file: UploadFile = File(...),
    name: Optional[str] = Form(None),
    user: Dict[str, str] = Depends(_require_user),
):
    """
    Upload auxiliary file (primers, reference, etc.).
    
    Args:
        session_id: Session identifier
        file: File to upload
        name: Optional custom filename
        
    Returns:
        Storage information and detected role
    """
    session_dir, session = _load_session_for_user(user, session_id)
    
    filename = name or file.filename
    file_path = session_dir / filename
    
    with open(file_path, "wb") as f:
        import shutil
        shutil.copyfileobj(file.file, f)
    
    role = guess_aux_role(filename)
    if role in ("v_primers", "c_primers"):
        session.aux[role] = filename
        save_state(session_dir, session)
    
    return {"stored_as": filename, "role": role}


@router.post("/session/{session_id}/run")
def run_unit(
    session_id: str,
    body: RunBody = Body(...),
    user: Dict[str, str] = Depends(_require_user),
):
    """
    Execute a processing unit.
    
    Args:
        session_id: Session identifier
        body: Unit ID and parameters
        
    Returns:
        Step result and updated session state
        
    Raises:
        HTTPException: If unit not found or execution fails
    """
    session_dir, session = _load_session_for_user(user, session_id)
    
    unit = UNITS.get(body.unit_id)
    if not unit:
        raise HTTPException(404, f"Unknown unit_id '{body.unit_id}'")
    
    # Check required channels
    for channel in unit.requires:
        if channel not in session.current:
            raise HTTPException(
                400,
                f"Unit '{unit.id}' requires channel {channel} to be available."
            )
    
    step_index = len(session.steps)
    try:
        step = unit.run(session, session_dir, body.params)
        session.steps.append(step)
        save_state(session_dir, session)
        return {
            "step": step.model_dump(),
            "current": session.current,
            "artifacts": {k: v.model_dump() for k, v in session.artifacts.items()}
        }
    except Exception as e:
        # Collect log tail for error reporting
        prefix = f"{step_index:03d}_"
        logs = sorted([
            p for p in session_dir.iterdir()
            if p.name.startswith(prefix) and p.suffix == ".log"
        ])
        log_tail = ""
        for log_file in logs:
            try:
                log_tail += log_file.read_text(errors="ignore") + "\n\n"
            except Exception:
                pass
        if len(log_tail) > 5000:
            log_tail = log_tail[-5000:]
        
        raise HTTPException(
            status_code=500,
            detail={"error": str(e), "log_tail": log_tail}
        )


@router.get("/session/{session_id}/state")
def get_state(session_id: str, user: Dict[str, str] = Depends(_require_user)):
    """
    Get current session state.
    
    Args:
        session_id: Session identifier
        
    Returns:
        Session state as dictionary
    """
    _, state = _load_session_for_user(user, session_id)
    return state.model_dump()


@router.get("/session/{session_id}/download/{artifact_name}")
def download_artifact(
    session_id: str,
    artifact_name: str,
    user: Dict[str, str] = Depends(_require_user),
):
    """
    Download a session artifact.
    
    Args:
        session_id: Session identifier
        artifact_name: Name of artifact to download
        
    Returns:
        File response
        
    Raises:
        HTTPException: If artifact not found
    """
    session_dir, session = _load_session_for_user(user, session_id)
    
    artifact = session.artifacts.get(artifact_name)
    if not artifact:
        raise HTTPException(404, "Artifact not found")
    
    file_path = session_dir / artifact.path
    if not file_path.exists():
        raise HTTPException(404, "File missing on disk")
    
    return FileResponse(file_path, filename=file_path.name)


@router.get("/session/{session_id}/log/{step_index}", response_class=PlainTextResponse)
def get_log(
    session_id: str,
    step_index: int,
    user: Dict[str, str] = Depends(_require_user),
):
    """
    Get log file for a processing step.
    
    Args:
        session_id: Session identifier
        step_index: Step index
        
    Returns:
        Log file contents as plain text
        
    Raises:
        HTTPException: If log not found
    """
    session_dir, _ = _load_session_for_user(user, session_id)
    prefix = f"{int(step_index):03d}_"
    logs = sorted([
        p for p in session_dir.iterdir()
        if p.name.startswith(prefix) and p.suffix == ".log"
    ])
    
    if not logs:
        raise HTTPException(404, "Log not found")
    
    return "\n\n".join(log_file.read_text(errors="ignore") for log_file in logs)

