"""Authentication and user storage utilities (database-backed)."""
from __future__ import annotations

import hashlib
import hmac
import secrets
import uuid
from datetime import datetime, timezone
from typing import Dict, Optional

from sqlalchemy.orm import Session

from app.repositories import (
    token_create,
    token_get,
    user_create,
    user_get_by_id,
    user_get_by_username,
)

PBKDF2_ROUNDS = 200_000


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_username(username: str) -> str:
    return (username or "").strip().lower()


def _hash_password(password: str, salt_hex: str) -> str:
    salt = bytes.fromhex(salt_hex)
    digest = hashlib.pbkdf2_hmac(
        "sha256", password.encode("utf-8"), salt, PBKDF2_ROUNDS
    )
    return digest.hex()


def _user_record(user_id: uuid.UUID, username: str) -> Dict[str, str]:
    return {
        "user_id": str(user_id),
        "username": username,
    }


def create_user(db: Session, username: str, password: str) -> Dict[str, str]:
    username_norm = _normalize_username(username)
    if not username_norm:
        raise ValueError("Username is required.")
    if len(password or "") < 6:
        raise ValueError("Password must be at least 6 characters.")

    if user_get_by_username(db, username_norm) is not None:
        raise ValueError("Username already exists.")

    salt = secrets.token_bytes(16).hex()
    password_hash = _hash_password(password, salt)
    user = user_create(db, username_norm, salt, password_hash)
    return _user_record(user.id, user.username)


def authenticate_user(
    db: Session, username: str, password: str
) -> Optional[Dict[str, str]]:
    username_norm = _normalize_username(username)
    user = user_get_by_username(db, username_norm)
    if not user:
        return None
    candidate = _hash_password(password, user.password_salt)
    if not hmac.compare_digest(candidate, user.password_hash):
        return None
    return _user_record(user.id, user.username)


def create_token(db: Session, user_id: str, username: str) -> str:
    token = secrets.token_urlsafe(32)
    uid = uuid.UUID(user_id) if isinstance(user_id, str) else user_id
    token_create(db, uid, token)
    return token


def get_user_by_token(db: Session, token: str) -> Optional[Dict[str, str]]:
    if not token:
        return None
    entry = token_get(db, token)
    if not entry:
        return None
    user = user_get_by_id(db, entry.user_id)
    if not user:
        return None
    return _user_record(user.id, user.username)
