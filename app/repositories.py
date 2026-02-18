"""
Repository layer for users, auth tokens, and sessions.

All persistence goes through these functions so main and auth can use DB.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db_models import AuthTokenModel, SessionModel, UserModel


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------- Users ----------


def user_create(
    db: Session,
    username: str,
    password_salt: str,
    password_hash: str,
) -> UserModel:
    user = UserModel(
        username=username,
        password_salt=password_salt,
        password_hash=password_hash,
    )
    db.add(user)
    db.flush()
    return user


def user_get_by_username(db: Session, username: str) -> Optional[UserModel]:
    result = db.execute(
        select(UserModel).where(UserModel.username == username)
    )
    return result.scalars().first()


def user_get_by_id(db: Session, user_id: uuid.UUID) -> Optional[UserModel]:
    return db.get(UserModel, user_id)


# ---------- Auth tokens ----------


def token_create(db: Session, user_id: uuid.UUID, token: str) -> AuthTokenModel:
    row = AuthTokenModel(token=token, user_id=user_id)
    db.add(row)
    db.flush()
    return row


def token_get(db: Session, token: str) -> Optional[AuthTokenModel]:
    return db.get(AuthTokenModel, token)


# ---------- Sessions ----------


def session_create(
    db: Session,
    user_id: uuid.UUID,
    owner_username: Optional[str] = None,
    session_id: Optional[uuid.UUID] = None,
    state_json: Optional[Dict[str, Any]] = None,
) -> SessionModel:
    sid = session_id or uuid.uuid4()
    state = state_json or {
        "session_id": str(sid),
        "owner_user_id": str(user_id),
        "owner_username": owner_username,
        "created_at": _now_iso(),
        "updated_at": _now_iso(),
        "steps": [],
        "artifacts": {},
        "current": {},
        "aux": {},
        "aux_files": [],
        "stats": {},
    }
    row = SessionModel(
        id=sid,
        user_id=user_id,
        state_json=state,
    )
    db.add(row)
    db.flush()
    return row


def session_get(db: Session, session_id: uuid.UUID) -> Optional[SessionModel]:
    return db.get(SessionModel, session_id)


def session_get_for_user(
    db: Session, session_id: uuid.UUID, user_id: uuid.UUID
) -> Optional[SessionModel]:
    row = db.get(SessionModel, session_id)
    if row is None or row.user_id != user_id:
        return None
    return row


def session_list_by_user(
    db: Session, user_id: uuid.UUID
) -> List[SessionModel]:
    result = db.execute(
        select(SessionModel).where(SessionModel.user_id == user_id)
    )
    return list(result.scalars().all())


def session_update_state(
    db: Session, session_id: uuid.UUID, state_json: Dict[str, Any]
) -> bool:
    row = db.get(SessionModel, session_id)
    if row is None:
        return False
    row.state_json = state_json
    row.updated_at = datetime.now(timezone.utc)
    db.flush()
    return True


def session_update_display_name(
    db: Session, session_id: uuid.UUID, display_name: Optional[str]
) -> bool:
    row = db.get(SessionModel, session_id)
    if row is None:
        return False
    row.display_name = display_name
    db.flush()
    return True


def session_delete(db: Session, session_id: uuid.UUID) -> bool:
    row = db.get(SessionModel, session_id)
    if row is None:
        return False
    db.delete(row)
    db.flush()
    return True
