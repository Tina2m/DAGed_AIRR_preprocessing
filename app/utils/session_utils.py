"""Session state management utilities."""
import pathlib
from datetime import datetime, timezone
from app.models import SessionState


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_state(sess_dir: pathlib.Path) -> SessionState:
    """
    Load session state from disk.
    
    Args:
        sess_dir: Session directory
        
    Returns:
        SessionState object
    """
    state_file = sess_dir / "state.json"
    if state_file.exists():
        return SessionState.model_validate_json(state_file.read_text())
    
    # Create new state
    state = SessionState(
        session_id=sess_dir.name,
        created_at=_now_iso(),
        updated_at=_now_iso(),
    )
    save_state(sess_dir, state)
    return state


def save_state(sess_dir: pathlib.Path, state: SessionState) -> None:
    """
    Save session state to disk.
    
    Args:
        sess_dir: Session directory
        state: SessionState to save
    """
    if not state.created_at:
        state.created_at = _now_iso()
    state.updated_at = _now_iso()
    state_file = sess_dir / "state.json"
    state_file.write_text(state.model_dump_json(indent=2))


def get_next_step_index(state: SessionState) -> int:
    """
    Get the next step index for a session.
    
    Args:
        state: Session state
        
    Returns:
        Next step index
    """
    return len(state.steps)

