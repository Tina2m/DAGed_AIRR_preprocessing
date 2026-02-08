"""Move legacy /data/<session_id> runs into a user's session folder.

Usage:
  python scripts/migrate_sessions.py --user-id <uuid> [--username <name>]
  python scripts/migrate_sessions.py --user-id <uuid> --dry-run
  python scripts/migrate_sessions.py --user-id <uuid> --overwrite
  python scripts/migrate_sessions.py --user-id <uuid> --base-dir /data
"""
from __future__ import annotations

import argparse
import json
import pathlib
import shutil
from datetime import datetime, timezone
from typing import Optional


SKIP_DIRS = {"users", "_auth"}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_state(path: pathlib.Path) -> Optional[dict]:
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def update_state(state: dict, user_id: str, username: Optional[str]) -> dict:
    state["owner_user_id"] = user_id
    if username:
        state["owner_username"] = username
    if not state.get("created_at"):
        state["created_at"] = now_iso()
    state["updated_at"] = now_iso()
    return state


def migrate_one(
    src_dir: pathlib.Path,
    dest_dir: pathlib.Path,
    user_id: str,
    username: Optional[str],
    dry_run: bool,
    overwrite: bool,
) -> bool:
    state_path = src_dir / "state.json"
    if not state_path.exists():
        return False

    if dest_dir.exists():
        if not overwrite:
            print(f"Skip {src_dir.name}: destination exists.")
            return False
        if not dry_run:
            shutil.rmtree(dest_dir)

    if dry_run:
        print(f"Would move {src_dir} -> {dest_dir}")
        return True

    dest_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(src_dir), str(dest_dir))

    moved_state_path = dest_dir / "state.json"
    state = load_state(moved_state_path)
    if state is None:
        print(f"Warning: moved {dest_dir.name} but could not read state.json.")
        return True

    update_state(state, user_id, username)
    moved_state_path.write_text(json.dumps(state, indent=2))
    print(f"Moved {dest_dir.name}")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="Migrate legacy sessions to a user folder.")
    parser.add_argument("--base-dir", default="/data", help="Base data directory (default: /data)")
    parser.add_argument("--user-id", required=True, help="Target user_id (required)")
    parser.add_argument("--username", default=None, help="Optional username to store in state.json")
    parser.add_argument("--dry-run", action="store_true", help="Show actions without moving files")
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite destination if it exists",
    )
    args = parser.parse_args()

    base_dir = pathlib.Path(args.base_dir)
    if not base_dir.exists():
        print(f"Base directory not found: {base_dir}")
        return 1

    sessions_root = base_dir / "users" / args.user_id / "sessions"
    migrated = 0
    scanned = 0

    for entry in sorted(base_dir.iterdir(), key=lambda p: p.name):
        if not entry.is_dir():
            continue
        if entry.name in SKIP_DIRS:
            continue
        scanned += 1
        dest_dir = sessions_root / entry.name
        if migrate_one(
            entry,
            dest_dir,
            args.user_id,
            args.username,
            args.dry_run,
            args.overwrite,
        ):
            migrated += 1

    print(f"Scanned {scanned} directories, migrated {migrated}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
