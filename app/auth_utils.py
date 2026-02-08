"""Authentication and user storage utilities."""
from __future__ import annotations

import hashlib
import hmac
import json
import pathlib
import secrets
import uuid
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple

PBKDF2_ROUNDS = 200_000
AUTH_DIRNAME = "_auth"
USERS_FILENAME = "users.json"
TOKENS_FILENAME = "tokens.json"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _auth_paths(base_dir: pathlib.Path) -> Tuple[pathlib.Path, pathlib.Path]:
    auth_dir = base_dir / AUTH_DIRNAME
    auth_dir.mkdir(parents=True, exist_ok=True)
    return auth_dir / USERS_FILENAME, auth_dir / TOKENS_FILENAME


def _load_json(path: pathlib.Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError:
        return default


def _save_json(path: pathlib.Path, data) -> None:
    path.write_text(json.dumps(data, indent=2))


def _normalize_username(username: str) -> str:
    return (username or "").strip().lower()


def _hash_password(password: str, salt_hex: str) -> str:
    salt = bytes.fromhex(salt_hex)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, PBKDF2_ROUNDS)
    return digest.hex()


def _load_users(base_dir: pathlib.Path) -> Dict[str, Dict[str, str]]:
    users_path, _ = _auth_paths(base_dir)
    data = _load_json(users_path, {"users": {}})
    return data.get("users", {})


def _save_users(base_dir: pathlib.Path, users: Dict[str, Dict[str, str]]) -> None:
    users_path, _ = _auth_paths(base_dir)
    _save_json(users_path, {"users": users})


def _load_tokens(base_dir: pathlib.Path) -> Dict[str, Dict[str, str]]:
    _, tokens_path = _auth_paths(base_dir)
    data = _load_json(tokens_path, {"tokens": {}})
    return data.get("tokens", {})


def _save_tokens(base_dir: pathlib.Path, tokens: Dict[str, Dict[str, str]]) -> None:
    _, tokens_path = _auth_paths(base_dir)
    _save_json(tokens_path, {"tokens": tokens})


def create_user(base_dir: pathlib.Path, username: str, password: str) -> Dict[str, str]:
    username_norm = _normalize_username(username)
    if not username_norm:
        raise ValueError("Username is required.")
    if len(password or "") < 6:
        raise ValueError("Password must be at least 6 characters.")

    users = _load_users(base_dir)
    if username_norm in users:
        raise ValueError("Username already exists.")

    salt = secrets.token_bytes(16).hex()
    password_hash = _hash_password(password, salt)
    user_id = str(uuid.uuid4())
    record = {
        "user_id": user_id,
        "username": username_norm,
        "password_salt": salt,
        "password_hash": password_hash,
        "created_at": _now_iso(),
    }
    users[username_norm] = record
    _save_users(base_dir, users)
    return record


def authenticate_user(base_dir: pathlib.Path, username: str, password: str) -> Optional[Dict[str, str]]:
    username_norm = _normalize_username(username)
    users = _load_users(base_dir)
    record = users.get(username_norm)
    if not record:
        return None
    expected = record.get("password_hash", "")
    salt = record.get("password_salt", "")
    if not salt or not expected:
        return None
    candidate = _hash_password(password, salt)
    if not hmac.compare_digest(candidate, expected):
        return None
    return record


def _find_user_by_id(users: Dict[str, Dict[str, str]], user_id: str) -> Optional[Dict[str, str]]:
    for record in users.values():
        if record.get("user_id") == user_id:
            return record
    return None


def create_token(base_dir: pathlib.Path, user_id: str, username: str) -> str:
    tokens = _load_tokens(base_dir)
    token = secrets.token_urlsafe(32)
    tokens[token] = {
        "user_id": user_id,
        "username": username,
        "created_at": _now_iso(),
    }
    _save_tokens(base_dir, tokens)
    return token


def get_user_by_token(base_dir: pathlib.Path, token: str) -> Optional[Dict[str, str]]:
    if not token:
        return None
    tokens = _load_tokens(base_dir)
    entry = tokens.get(token)
    if not entry:
        return None
    users = _load_users(base_dir)
    record = _find_user_by_id(users, entry.get("user_id", ""))
    if not record:
        return None
    return record
