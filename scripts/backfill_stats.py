"""Backfill per-step read statistics into session state_json.

Usage:
  python scripts/backfill_stats.py
  python scripts/backfill_stats.py --session-id <uuid>
  python scripts/backfill_stats.py --force
  python scripts/backfill_stats.py --dry-run
  python scripts/backfill_stats.py --session-files-dir /data/session_files
"""
from __future__ import annotations

import argparse
import pathlib
import re
import sys
import uuid
from datetime import datetime, timezone
from typing import Dict, Optional

from sqlalchemy import select

# Ensure repo root is on sys.path so "app" imports work when running as a script.
REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.config import SESSION_FILES_BASE
from app.database import get_db
from app.db_models import SessionModel


COUNT_KEYS = {
    "PASS", "PASSED", "PASSING",
    "FAIL", "FAILED", "FAILING",
    "SEQUENCE", "SEQUENCES",
    "READ", "READS",
    "INPUT", "TOTAL",
    "UNIQUE", "RETAINED", "CONSENSUS",
    "MERGED", "PAIRED",
}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def guess_channel_from_name(name: str) -> Optional[str]:
    upper = name.upper()
    if "R2" in upper:
        return "R2"
    if "R1" in upper:
        return "R1"
    return None


def extract_first_number(text: str) -> Optional[int]:
    if not text:
        return None
    match = re.search(r"(\d[\d,]*)", str(text))
    if not match:
        return None
    raw = match.group(1).replace(",", "")
    try:
        return int(raw)
    except ValueError:
        return None


def record_log_count(counts: Dict[str, int], raw_key: str, raw_value: str) -> None:
    key = str(raw_key or "").upper()
    if key not in COUNT_KEYS:
        return
    value = extract_first_number(raw_value)
    if value is None:
        return
    if key.startswith("PASS"):
        counts["PASS"] = value
        return
    if key.startswith("FAIL"):
        counts["FAIL"] = value
        return
    if key in ("READ", "READS"):
        counts["READS"] = value
        return
    if key in ("SEQUENCE", "SEQUENCES"):
        counts["SEQUENCES"] = value
        return
    if key == "INPUT":
        counts["INPUT"] = value
        return
    if key in ("UNIQUE", "RETAINED", "CONSENSUS", "MERGED", "PAIRED"):
        counts[key] = value
        return
    if key == "TOTAL":
        counts["TOTAL"] = value


def parse_log_counts(log_text: str) -> Dict[str, Optional[int]]:
    if not log_text:
        return {"pass": None, "total": None, "counts": {}}
    counts: Dict[str, int] = {}
    lines = str(log_text).splitlines()
    for line in lines:
        if not line:
            continue
        for match in re.finditer(r"([A-Z][A-Z0-9_-]*)>\s*([^\r\n]+)", line):
            record_log_count(counts, match.group(1), match.group(2))
        for match in re.finditer(
            r"\b(pass(?:ed|ing)?|fail(?:ed|ing)?|total|reads?|sequences?|input)\b\s*[:=]\s*(\d[\d,]*)",
            line,
            re.IGNORECASE,
        ):
            record_log_count(counts, match.group(1), match.group(2))
        for match in re.finditer(
            r"\b(pass(?:ed|ing)?|fail(?:ed|ing)?|total|reads?|sequences?|input)\b\s+(\d[\d,]*)",
            line,
            re.IGNORECASE,
        ):
            record_log_count(counts, match.group(1), match.group(2))
        for match in re.finditer(
            r"(\d[\d,]*)\s+\b(pass(?:ed|ing)?|fail(?:ed|ing)?|total|reads?|sequences?|input)\b",
            line,
            re.IGNORECASE,
        ):
            record_log_count(counts, match.group(2), match.group(1))
    passed = counts.get("PASS")
    if passed is None:
        for alt in ("UNIQUE", "RETAINED", "CONSENSUS", "MERGED", "PAIRED"):
            if alt in counts:
                passed = counts[alt]
                break
    failed = counts.get("FAIL")
    total = counts.get("SEQUENCES") or counts.get("INPUT") or counts.get("READS") or counts.get("TOTAL")
    if total is None and passed is not None and failed is not None:
        total = passed + failed
    return {"pass": passed, "total": total, "counts": counts}


def detect_channel_from_log_text(log_text: str) -> Optional[str]:
    if not log_text:
        return None
    for line in str(log_text).splitlines():
        text = line.strip()
        if text.startswith("OUTPUT>") or text.startswith("INPUT>"):
            name = text.split(">", 1)[1].strip()
            channel = guess_channel_from_name(name)
            if channel:
                return channel
    upper = str(log_text).upper()
    has_r1 = re.search(r"\bR1\b|R1[_\.\-]", upper) is not None
    has_r2 = re.search(r"\bR2\b|R2[_\.\-]", upper) is not None
    if has_r1 and not has_r2:
        return "R1"
    if has_r2 and not has_r1:
        return "R2"
    return None


def collect_log_counts_for_step(
    session_dir: pathlib.Path,
    step_index: int,
) -> Dict[str, Dict[str, Optional[int]]]:
    prefix = f"{int(step_index):03d}_"
    logs = sorted([p for p in session_dir.iterdir() if p.name.startswith(prefix) and p.suffix == ".log"])
    results: Dict[str, Dict[str, Optional[int]]] = {}
    for log_path in logs:
        try:
            log_text = log_path.read_text(errors="ignore")
        except Exception:
            continue
        parsed = parse_log_counts(log_text)
        if parsed["pass"] is None and parsed["total"] is None:
            continue
        channel = None
        name_upper = log_path.name.upper()
        if name_upper.endswith("_R1.LOG"):
            channel = "R1"
        elif name_upper.endswith("_R2.LOG"):
            channel = "R2"
        if not channel:
            channel = detect_channel_from_log_text(log_text)
        key = channel or "single"
        results[key] = {"pass": parsed["pass"], "total": parsed["total"]}
    return results


def backfill_session(
    row: SessionModel,
    session_dir: pathlib.Path,
    force: bool,
    dry_run: bool,
    preserve_updated_at: bool,
) -> Optional[Dict[str, int]]:
    state = row.state_json or {}
    steps = state.get("steps") or []
    if not steps:
        return None
    if not session_dir.exists():
        return None

    stats = state.get("stats") or {}
    updated = False
    filled = 0
    skipped = 0

    for index, step in enumerate(steps):
        step_index = step.get("step_index")
        if step_index is None:
            step_index = index
        key = str(step_index)
        if not force and stats.get(key):
            skipped += 1
            continue
        counts = collect_log_counts_for_step(session_dir, int(step_index))
        if not counts:
            skipped += 1
            continue
        stats[key] = counts
        filled += 1
        updated = True

    if not updated:
        return {"filled": 0, "skipped": skipped, "updated": 0}

    if dry_run:
        return {"filled": filled, "skipped": skipped, "updated": 1}

    state["stats"] = stats
    if preserve_updated_at:
        preserved = row.updated_at
        row.state_json = state
        row.updated_at = preserved
    else:
        state["updated_at"] = state.get("updated_at") or now_iso()
        row.state_json = state
    return {"filled": filled, "skipped": skipped, "updated": 1}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill stats in session state_json.")
    parser.add_argument(
        "--session-id",
        action="append",
        help="Backfill only this session UUID (repeatable).",
    )
    parser.add_argument(
        "--session-files-dir",
        default=None,
        help="Override SESSION_FILES_DIR (default from app.config).",
    )
    parser.add_argument("--force", action="store_true", help="Recompute stats even if present.")
    parser.add_argument("--dry-run", action="store_true", help="Show actions without writing.")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of sessions to scan.")
    parser.add_argument(
        "--no-preserve-updated-at",
        action="store_true",
        help="Allow updated_at to change during backfill.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    session_files_base = pathlib.Path(args.session_files_dir) if args.session_files_dir else SESSION_FILES_BASE
    session_ids = None
    if args.session_id:
        session_ids = []
        for raw in args.session_id:
            try:
                session_ids.append(uuid.UUID(raw))
            except ValueError:
                print(f"Invalid session id: {raw}")
                return 2

    preserve_updated_at = not args.no_preserve_updated_at
    scanned = 0
    updated = 0
    filled_total = 0
    skipped_total = 0

    with get_db() as db:
        query = select(SessionModel)
        if session_ids:
            query = query.where(SessionModel.id.in_(session_ids))
        if args.limit:
            query = query.limit(args.limit)
        rows = list(db.execute(query).scalars().all())

        for row in rows:
            scanned += 1
            session_dir = session_files_base / str(row.id)
            result = backfill_session(
                row,
                session_dir,
                force=args.force,
                dry_run=args.dry_run,
                preserve_updated_at=preserve_updated_at,
            )
            if result is None:
                continue
            filled_total += result["filled"]
            skipped_total += result["skipped"]
            updated += result["updated"]

    print(
        f"Scanned {scanned} sessions, updated {updated}, "
        f"filled {filled_total}, skipped {skipped_total}."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
