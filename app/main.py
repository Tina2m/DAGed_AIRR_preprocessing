# app/main.py
import os
import uuid
import gzip
import pathlib
import shutil
import subprocess
import csv
from datetime import datetime, timezone
from typing import Optional, Dict, List, Literal, Any

from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Body, Depends
from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel

import auth_utils

# --------- sanity: ensure pRESTO tools exist on PATH ----------
import shutil as _shutil
_needed = ["FilterSeq.py","MaskPrimers.py","CollapseSeq.py","BuildConsensus.py"]
_missing = [t for t in _needed if not _shutil.which(t)]
if _missing:
    raise RuntimeError(f"pRESTO tools not found on PATH: {', '.join(_missing)}")

# --------- FastAPI app ----------
app = FastAPI(title="pRESTO Click-to-Run Backend")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# (Keep your UI files under app/ui)
app.mount("/ui", StaticFiles(directory="ui", html=True), name="ui")

BASE = pathlib.Path("/data")
BASE.mkdir(parents=True, exist_ok=True)

auth_scheme = HTTPBearer(auto_error=False)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _user_sessions_root(user_id: str) -> pathlib.Path:
    return BASE / "users" / user_id / "sessions"


def _session_dir_for_user(user_id: str, session_id: str) -> pathlib.Path:
    return _user_sessions_root(user_id) / session_id


def _require_user(
    creds: Optional[HTTPAuthorizationCredentials] = Depends(auth_scheme),
) -> Dict[str, str]:
    if not creds or not creds.credentials:
        raise HTTPException(status_code=401, detail="Missing auth token.")
    record = auth_utils.get_user_by_token(BASE, creds.credentials)
    if not record:
        raise HTTPException(status_code=401, detail="Invalid or expired token.")
    return record


def _load_session_for_user(user: Dict[str, str], session_id: str) -> tuple[pathlib.Path, "SessionState"]:
    session_dir = _session_dir_for_user(user["user_id"], session_id)
    if not session_dir.exists():
        raise HTTPException(status_code=404, detail="Session not found.")
    state = load_state(session_dir)
    if state.owner_user_id and state.owner_user_id != user["user_id"]:
        raise HTTPException(status_code=403, detail="Access denied for this session.")
    return session_dir, state

# --------- Models ----------
class UnitSpec(BaseModel):
    id: str
    label: str
    requires: List[str]
    params_schema: Dict[str, Any]
    # Tag units so the UI can filter bulk vs single-cell
    group: Literal["bulk", "sc"] = "bulk"

    # Quoted types avoid forward-ref problems if this class appears before the models
    def run(self, sess: "SessionState", sess_dir: pathlib.Path, params: Dict[str, Any]) -> "StepResult":
        raise NotImplementedError


class Artifact(BaseModel):
    name: str
    path: str
    kind: Literal["fastq","fasta","tab","log","plot","other"] = "other"
    channel: Optional[Literal["R1","R2"]] = None
    from_step: int

class StepResult(BaseModel):
    step_index: int
    unit: str
    params: Dict[str, Any]
    produced: List[Artifact]

class SessionState(BaseModel):
    session_id: str
    owner_user_id: Optional[str] = None
    owner_username: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    steps: List[StepResult] = []
    artifacts: Dict[str, Artifact] = {}
    current: Dict[str, str] = {}     # channel -> artifact-name
    aux: Dict[str, str] = {}         # e.g. {"v_primers": "Greiff2014_VPrimers.fasta"}
    aux_files: List[str] = []        # all uploaded aux filenames
    
def _ensure_uncompressed_path(path: pathlib.Path, dest: pathlib.Path) -> pathlib.Path:
    """If `path` endswith .gz, decompress to `dest` (overwrite) and return dest; else return path."""
    if str(path).lower().endswith(".gz"):
        dest.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(path, "rb") as src, open(dest, "wb") as dst:
            shutil.copyfileobj(src, dst)
        return dest
    return path

def _ensure_uncompressed_art(sess: SessionState, sdir: pathlib.Path, ch: str) -> pathlib.Path:
    """Return an uncompressed path for the current artifact of channel `ch`."""
    key = sess.current.get(ch)
    if not key:
        raise HTTPException(400, f"Channel '{ch}' is not available.")
    art = sess.artifacts[key]
    p = sdir / art.path
    if p.suffix.lower() == ".gz":
        # Decompress alongside with the same basename (without .gz)
        out = p.with_suffix("")  # drop .gz
        if not out.exists():
            with gzip.open(p, "rb") as src, open(out, "wb") as dst:
                shutil.copyfileobj(src, dst)
        return out
    return p

def _require_fastq(sess: SessionState, sdir: pathlib.Path, channel_key: str, for_what: str) -> pathlib.Path:
    """Ensure the current artifact is uncompressed FASTQ, else 400 with a helpful message."""
    p = _ensure_uncompressed_art(sess, sdir, channel_key)
    # quick, reliable check by peeking at first non-empty char
    first = _peek_first_nonempty_char(p, gz=False)
    if first != "@":
        raise HTTPException(
            400,
            f"{for_what} requires FASTQ (qualities), but '{p.name}' is not FASTQ. "
            "Upload FASTQ(.gz) or skip this unit."
        )
    return p


def load_state(sess_dir: pathlib.Path) -> SessionState:
    p = sess_dir / "state.json"
    if p.exists():
        return SessionState.model_validate_json(p.read_text())
    s = SessionState(session_id=sess_dir.name, created_at=_now_iso(), updated_at=_now_iso())
    p.write_text(s.model_dump_json(indent=2))
    return s

def save_state(sess_dir: pathlib.Path, s: SessionState):
    if not s.created_at:
        s.created_at = _now_iso()
    s.updated_at = _now_iso()
    (sess_dir / "state.json").write_text(s.model_dump_json(indent=2))

# --------- run_cmd: add --nproc when supported, retry w/o ----------
def run_cmd(cmd: List[str], cwd: pathlib.Path, log_file: pathlib.Path):
    nproc = os.cpu_count() or 2
    tool = pathlib.Path(cmd[0]).name
    NPROC_TOOLS = {
        "FilterSeq.py", "MaskPrimers.py",
        "CollapseSeq.py", "BuildConsensus.py",
    }
    final_cmd = list(cmd)
    print('CMD:',final_cmd)
    if tool in NPROC_TOOLS and "--nproc" not in final_cmd:
        final_cmd += ["--nproc", str(nproc)]

    with open(log_file, "ab") as log:
        log.write(("[CMD] " + " ".join(final_cmd) + "\n").encode())
        proc = subprocess.Popen(final_cmd, cwd=cwd, stdout=log, stderr=log)
        rc = proc.wait()

    if rc != 0 and "--nproc" in final_cmd:
        # auto-retry without --nproc if unrecognized
        try:
            txt = (log_file.read_text(errors="ignore") or "").lower()
            if "unrecognized arguments" in txt and "--nproc" in txt:
                retry = [x for x in final_cmd if x not in ("--nproc", str(nproc))]
                with open(log_file, "ab") as log:
                    log.write(b"[RETRY] removing --nproc\n")
                    p2 = subprocess.Popen(retry, cwd=cwd, stdout=log, stderr=log)
                    if p2.wait() == 0:
                        return
        except Exception:
            pass

    if rc != 0:
        raise RuntimeError(f"Command failed ({rc}): {' '.join(final_cmd)}")

# --------- FASTA/FASTQ helpers ----------
FASTQ_EXTS = {".fastq", ".fq"}
FASTA_EXTS = {".fasta", ".fa", ".fna"}

def _detect_kind_from_name(name: str) -> Optional[str]:
    """Infer kind from filename (case-insensitive), including *.gz combos."""
    low = name.lower()
    if low.endswith((".fastq.gz", ".fq.gz", ".fastq", ".fq")):
        return "fastq"
    if low.endswith((".fasta.gz", ".fa.gz", ".fna.gz", ".fasta", ".fa", ".fna")):
        return "fasta"
    return None

def _peek_first_nonempty_char(path: pathlib.Path, gz: bool) -> str:
    """Open (gzip/plain) and return first non-empty char ('@' or '>') or ''."""
    opener = gzip.open if gz else open
    try:
        with opener(path, "rt", errors="ignore") as fh:
            for _ in range(200):
                line = fh.readline()
                if not line:
                    break
                s = line.strip()
                if s:
                    return s[0]
    except Exception:
        pass
    return ""

MAX_PLOT_READS = 200000
QC_PLOT_DIRNAME = "plots"
MAX_SC_ROWS = 500000

def _open_text_maybe_gz(path: pathlib.Path):
    if path.suffix.lower() == ".gz":
        return gzip.open(path, "rt", errors="ignore")
    return open(path, "rt", errors="ignore")

def _iter_fastq_records(path: pathlib.Path, max_reads: Optional[int] = None):
    count = 0
    with _open_text_maybe_gz(path) as fh:
        while True:
            header = fh.readline()
            if not header:
                break
            seq = fh.readline()
            plus = fh.readline()
            qual = fh.readline()
            if not qual:
                break
            yield seq.strip(), qual.strip()
            count += 1
            if max_reads and count >= max_reads:
                break

def _iter_fasta_sequences(path: pathlib.Path, max_reads: Optional[int] = None):
    seq_parts: List[str] = []
    count = 0
    with _open_text_maybe_gz(path) as fh:
        for line in fh:
            if line.startswith(">"):
                if seq_parts:
                    yield "".join(seq_parts)
                    seq_parts = []
                    count += 1
                    if max_reads and count >= max_reads:
                        return
                continue
            seq_parts.append(line.strip())
        if seq_parts:
            yield "".join(seq_parts)
            count += 1
            if max_reads and count >= max_reads:
                return

def _iter_sequences(path: pathlib.Path, max_reads: Optional[int] = None):
    first = _peek_first_nonempty_char(path, gz=path.suffix.lower() == ".gz")
    if first == "@":
        for seq, _qual in _iter_fastq_records(path, max_reads=max_reads):
            yield seq
        return
    for seq in _iter_fasta_sequences(path, max_reads=max_reads):
        yield seq

def _iter_tsv_rows(path: pathlib.Path, max_rows: Optional[int] = None):
    count = 0
    with _open_text_maybe_gz(path) as fh:
        reader = csv.reader(fh, delimiter="\t")
        header = next(reader, None)
        if not header:
            return
        idx = {name: i for i, name in enumerate(header)}
        for row in reader:
            yield row, idx
            count += 1
            if max_rows and count >= max_rows:
                break

def _truthy(val: str) -> bool:
    if val is None:
        return False
    return str(val).strip().lower() in {"true", "t", "1", "yes", "y"}

def _get_cell_value(row: List[str], idx: Optional[int]) -> str:
    if idx is None:
        return ""
    if idx >= len(row):
        return ""
    return row[idx].strip()

def _quality_hist(path: pathlib.Path, max_reads: int = MAX_PLOT_READS) -> tuple[List[int], int]:
    counts = [0] * 41
    total = 0
    for _seq, qual in _iter_fastq_records(path, max_reads=max_reads):
        if not qual:
            continue
        total += 1
        qsum = sum((ord(ch) - 33) for ch in qual)
        mean_q = qsum / max(1, len(qual))
        idx = int(round(mean_q))
        if idx < 0:
            idx = 0
        if idx > 40:
            idx = 40
        counts[idx] += 1
    return counts, total

def _choose_length_bin_width(min_len: int, max_len: int) -> int:
    span = max_len - min_len
    if span <= 50:
        return 1
    if span <= 200:
        return 5
    if span <= 500:
        return 10
    return 25

def _length_hist(path: pathlib.Path, max_reads: int = MAX_PLOT_READS) -> tuple[List[int], List[int], int]:
    lengths: List[int] = []
    for seq in _iter_sequences(path, max_reads=max_reads):
        if seq:
            lengths.append(len(seq))
    if not lengths:
        return [], [], 0
    min_len = min(lengths)
    max_len = max(lengths)
    bin_width = _choose_length_bin_width(min_len, max_len)
    start = (min_len // bin_width) * bin_width
    end = ((max_len + bin_width - 1) // bin_width) * bin_width
    bins = list(range(start, end + bin_width, bin_width))
    counts = [0] * (len(bins) - 1)
    for length in lengths:
        idx = min((length - start) // bin_width, len(counts) - 1)
        counts[idx] += 1
    return bins, counts, len(lengths)

def _write_hist_svg(dest: pathlib.Path, title: str, x_label: str, bins: List[int], counts: List[int], total: int):
    width = 640
    height = 360
    margin = 48
    plot_w = width - margin * 2
    plot_h = height - margin * 2
    max_count = max(counts) if counts else 1
    bar_count = len(counts) if counts else 1
    bar_w = plot_w / bar_count
    lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        f'<rect x="0" y="0" width="{width}" height="{height}" fill="#ffffff"/>',
        f'<text x="{width/2:.1f}" y="24" text-anchor="middle" font-size="14" fill="#111827">{title}</text>',
        f'<text x="{width/2:.1f}" y="{height-8}" text-anchor="middle" font-size="12" fill="#6b7280">{x_label}</text>',
        f'<text x="10" y="{margin-8}" font-size="11" fill="#6b7280">n={total}</text>',
        f'<line x1="{margin}" y1="{height-margin}" x2="{width-margin}" y2="{height-margin}" stroke="#d1d5db"/>',
        f'<line x1="{margin}" y1="{margin}" x2="{margin}" y2="{height-margin}" stroke="#d1d5db"/>',
    ]
    if counts:
        for i, count in enumerate(counts):
            bar_h = (count / max_count) * plot_h if max_count else 0
            x = margin + i * bar_w
            y = margin + (plot_h - bar_h)
            lines.append(
                f'<rect x="{x:.2f}" y="{y:.2f}" width="{bar_w-1:.2f}" height="{bar_h:.2f}" fill="#60a5fa"/>'
            )
    if bins:
        lines.append(f'<text x="{margin}" y="{height-margin+16}" font-size="11" fill="#6b7280">{bins[0]}</text>')
        lines.append(f'<text x="{width-margin}" y="{height-margin+16}" text-anchor="end" font-size="11" fill="#6b7280">{bins[-1]}</text>')
    lines.append('</svg>')
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text("\n".join(lines))

def _write_hist_svg_dual(
    dest: pathlib.Path,
    title: str,
    x_label: str,
    bins: List[int],
    counts_a: List[int],
    counts_b: List[int],
    total_a: int,
    total_b: int,
    label_a: str,
    label_b: str,
):
    width = 640
    height = 360
    margin = 48
    plot_w = width - margin * 2
    plot_h = height - margin * 2
    max_count = max([1] + counts_a + counts_b)
    bar_count = len(counts_a) if counts_a else 1
    bar_w = plot_w / bar_count
    half_w = max(1, (bar_w - 2) / 2)
    lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        f'<rect x="0" y="0" width="{width}" height="{height}" fill="#ffffff"/>',
        f'<text x="{width/2:.1f}" y="24" text-anchor="middle" font-size="14" fill="#111827">{title}</text>',
        f'<text x="{width/2:.1f}" y="{height-8}" text-anchor="middle" font-size="12" fill="#6b7280">{x_label}</text>',
        f'<text x="10" y="{margin-8}" font-size="11" fill="#6b7280">n={total_a} / {total_b}</text>',
        f'<line x1="{margin}" y1="{height-margin}" x2="{width-margin}" y2="{height-margin}" stroke="#d1d5db"/>',
        f'<line x1="{margin}" y1="{margin}" x2="{margin}" y2="{height-margin}" stroke="#d1d5db"/>',
        # legend
        f'<rect x="{width-margin-120}" y="{margin-20}" width="10" height="10" fill="#60a5fa"/>',
        f'<text x="{width-margin-104}" y="{margin-11}" font-size="11" fill="#6b7280">{label_a}</text>',
        f'<rect x="{width-margin-60}" y="{margin-20}" width="10" height="10" fill="#fb7185"/>',
        f'<text x="{width-margin-44}" y="{margin-11}" font-size="11" fill="#6b7280">{label_b}</text>',
    ]
    if counts_a and counts_b:
        for i, (count_a, count_b) in enumerate(zip(counts_a, counts_b)):
            x = margin + i * bar_w
            bar_h_a = (count_a / max_count) * plot_h if max_count else 0
            bar_h_b = (count_b / max_count) * plot_h if max_count else 0
            y_a = margin + (plot_h - bar_h_a)
            y_b = margin + (plot_h - bar_h_b)
            lines.append(
                f'<rect x="{x:.2f}" y="{y_a:.2f}" width="{half_w:.2f}" height="{bar_h_a:.2f}" fill="#60a5fa"/>'
            )
            lines.append(
                f'<rect x="{x + half_w + 2:.2f}" y="{y_b:.2f}" width="{half_w:.2f}" height="{bar_h_b:.2f}" fill="#fb7185"/>'
            )
    if bins:
        lines.append(f'<text x="{margin}" y="{height-margin+16}" font-size="11" fill="#6b7280">{bins[0]}</text>')
        lines.append(f'<text x="{width-margin}" y="{height-margin+16}" text-anchor="end" font-size="11" fill="#6b7280">{bins[-1]}</text>')
    lines.append('</svg>')
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text("\n".join(lines))

def _write_stacked_bar_svg(
    dest: pathlib.Path,
    title: str,
    categories: List[str],
    segment_values: List[List[int]],
    segment_labels: List[str],
    colors: List[str],
    y_label: str,
    segment_counts: Optional[List[List[int]]] = None,
):
    width = 680
    height = 380
    margin = 60
    plot_w = width - margin * 2
    plot_h = height - margin * 2
    max_total = max([1] + [sum(vals) for vals in segment_values])
    bar_count = max(1, len(categories))
    bar_w = plot_w / bar_count
    lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        f'<rect x="0" y="0" width="{width}" height="{height}" fill="#ffffff"/>',
        f'<text x="{width/2:.1f}" y="24" text-anchor="middle" font-size="14" fill="#111827">{title}</text>',
        f'<text x="14" y="{margin-18}" font-size="11" fill="#6b7280">{y_label}</text>',
        f'<line x1="{margin}" y1="{height-margin}" x2="{width-margin}" y2="{height-margin}" stroke="#d1d5db"/>',
        f'<line x1="{margin}" y1="{margin}" x2="{margin}" y2="{height-margin}" stroke="#d1d5db"/>',
    ]
    # legend
    legend_x = width - margin - 10
    legend_y = margin - 24
    for i, label in enumerate(segment_labels):
        color = colors[i % len(colors)]
        lx = legend_x - (len(segment_labels) - i) * 110
        lines.append(f'<rect x="{lx}" y="{legend_y}" width="10" height="10" fill="{color}"/>')
        lines.append(f'<text x="{lx+14}" y="{legend_y+9}" font-size="11" fill="#6b7280">{label}</text>')

    for i, category in enumerate(categories):
        x = margin + i * bar_w
        base_y = height - margin
        total = max_total if max_total else 1
        vals = segment_values[i] if i < len(segment_values) else [0] * len(segment_labels)
        counts = segment_counts[i] if (segment_counts and i < len(segment_counts)) else None
        for j, value in enumerate(vals):
            bar_h = (value / total) * plot_h if total else 0
            y = base_y - bar_h
            color = colors[j % len(colors)]
            lines.append(
                f'<rect x="{x+6:.2f}" y="{y:.2f}" width="{bar_w-12:.2f}" height="{bar_h:.2f}" fill="{color}"/>'
            )
            if counts is not None and j < len(counts):
                label_val = counts[j]
                if label_val:
                    text_y = y + bar_h / 2
                    if bar_h < 14:
                        text_y = y - 4
                    lines.append(
                        f'<text x="{x + bar_w/2:.1f}" y="{text_y:.1f}" text-anchor="middle" font-size="11" fill="#111827">{label_val}</text>'
                    )
            base_y = y
        label = category if len(category) <= 10 else (category[:9] + "…")
        lines.append(
            f'<text x="{x + bar_w/2:.1f}" y="{height-margin+18}" text-anchor="middle" font-size="11" fill="#6b7280">{label}</text>'
        )
    lines.append('</svg>')
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text("\n".join(lines))

def _write_grouped_bar_svg(
    dest: pathlib.Path,
    title: str,
    categories: List[str],
    values_a: List[float],
    values_b: List[float],
    label_a: str,
    label_b: str,
    y_label: str,
    max_value: Optional[float] = None,
):
    width = 720
    height = 380
    margin = 60
    plot_w = width - margin * 2
    plot_h = height - margin * 2
    max_val = max_value if max_value is not None else max([1.0] + values_a + values_b)
    group_count = max(1, len(categories))
    group_w = plot_w / group_count
    bar_w = max(6, (group_w - 12) / 2)
    lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        f'<rect x="0" y="0" width="{width}" height="{height}" fill="#ffffff"/>',
        f'<text x="{width/2:.1f}" y="24" text-anchor="middle" font-size="14" fill="#111827">{title}</text>',
        f'<text x="14" y="{margin-18}" font-size="11" fill="#6b7280">{y_label}</text>',
        f'<line x1="{margin}" y1="{height-margin}" x2="{width-margin}" y2="{height-margin}" stroke="#d1d5db"/>',
        f'<line x1="{margin}" y1="{margin}" x2="{margin}" y2="{height-margin}" stroke="#d1d5db"/>',
        f'<rect x="{width-margin-120}" y="{margin-20}" width="10" height="10" fill="#60a5fa"/>',
        f'<text x="{width-margin-104}" y="{margin-11}" font-size="11" fill="#6b7280">{label_a}</text>',
        f'<rect x="{width-margin-60}" y="{margin-20}" width="10" height="10" fill="#fb7185"/>',
        f'<text x="{width-margin-44}" y="{margin-11}" font-size="11" fill="#6b7280">{label_b}</text>',
    ]
    for i, category in enumerate(categories):
        x = margin + i * group_w
        val_a = values_a[i] if i < len(values_a) else 0
        val_b = values_b[i] if i < len(values_b) else 0
        h_a = (val_a / max_val) * plot_h if max_val else 0
        h_b = (val_b / max_val) * plot_h if max_val else 0
        lines.append(
            f'<rect x="{x+6:.2f}" y="{height-margin-h_a:.2f}" width="{bar_w:.2f}" height="{h_a:.2f}" fill="#60a5fa"/>'
        )
        lines.append(
            f'<rect x="{x+6+bar_w+4:.2f}" y="{height-margin-h_b:.2f}" width="{bar_w:.2f}" height="{h_b:.2f}" fill="#fb7185"/>'
        )
        label = category if len(category) <= 10 else (category[:9] + "…")
        lines.append(
            f'<text x="{x + group_w/2:.1f}" y="{height-margin+18}" text-anchor="middle" font-size="11" fill="#6b7280">{label}</text>'
        )
    lines.append('</svg>')
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text("\n".join(lines))

def _plot_artifact_name(step_idx: int, unit_id: str, channel: str, stage: str) -> str:
    return f"plot_{step_idx:03d}_{unit_id}_{channel}_{stage}"

def _hist_uniform(values: List[float], bins: List[int]) -> List[int]:
    if not bins or len(bins) < 2:
        return []
    step = bins[1] - bins[0]
    if step <= 0:
        return []
    start = bins[0]
    counts = [0] * (len(bins) - 1)
    for value in values:
        idx = int((value - start) // step)
        if idx < 0:
            idx = 0
        if idx >= len(counts):
            idx = len(counts) - 1
        counts[idx] += 1
    return counts

def _percent_bins(step: int = 5) -> List[int]:
    return list(range(0, 101, step))

def _n_percent(seq: str) -> float:
    if not seq:
        return 0.0
    n_count = sum(1 for ch in seq if ch in ("N", "n"))
    return (n_count / len(seq)) * 100.0

def _max_homopolymer(seq: str) -> int:
    if not seq:
        return 0
    max_run = 1
    run = 1
    prev = seq[0].upper()
    for ch in seq[1:]:
        up = ch.upper()
        if up == prev:
            run += 1
        else:
            if run > max_run:
                max_run = run
            run = 1
            prev = up
    if run > max_run:
        max_run = run
    return max_run

def _percent_hist_from_sequences(
    path: pathlib.Path,
    bins: List[int],
    metric_fn,
    max_reads: int = MAX_PLOT_READS,
) -> tuple[List[int], int]:
    counts = [0] * (len(bins) - 1)
    total = 0
    step = bins[1] - bins[0] if len(bins) > 1 else 1
    start = bins[0] if bins else 0
    for seq in _iter_sequences(path, max_reads=max_reads):
        if not seq:
            continue
        value = metric_fn(seq)
        idx = int((value - start) // step)
        if idx < 0:
            idx = 0
        if idx >= len(counts):
            idx = len(counts) - 1
        counts[idx] += 1
        total += 1
    return counts, total

def _length_hist_dual(
    before_path: pathlib.Path,
    after_path: pathlib.Path,
    max_reads: int = MAX_PLOT_READS,
) -> tuple[List[int], List[int], List[int], int, int]:
    lengths_before = [len(seq) for seq in _iter_sequences(before_path, max_reads=max_reads)]
    lengths_after = [len(seq) for seq in _iter_sequences(after_path, max_reads=max_reads)]
    if not lengths_before and not lengths_after:
        return [], [], [], 0, 0
    all_lengths = lengths_before + lengths_after
    min_len = min(all_lengths)
    max_len = max(all_lengths)
    bin_width = _choose_length_bin_width(min_len, max_len)
    start = (min_len // bin_width) * bin_width
    end = ((max_len + bin_width - 1) // bin_width) * bin_width
    bins = list(range(start, end + bin_width, bin_width))
    counts_before = [0] * (len(bins) - 1)
    counts_after = [0] * (len(bins) - 1)
    for length in lengths_before:
        idx = min((length - start) // bin_width, len(counts_before) - 1)
        counts_before[idx] += 1
    for length in lengths_after:
        idx = min((length - start) // bin_width, len(counts_after) - 1)
        counts_after[idx] += 1
    return bins, counts_before, counts_after, len(lengths_before), len(lengths_after)

def _repeat_hist_dual(
    before_path: pathlib.Path,
    after_path: pathlib.Path,
    max_reads: int = MAX_PLOT_READS,
) -> tuple[List[int], List[int], List[int], int, int]:
    vals_before = [_max_homopolymer(seq) for seq in _iter_sequences(before_path, max_reads=max_reads)]
    vals_after = [_max_homopolymer(seq) for seq in _iter_sequences(after_path, max_reads=max_reads)]
    if not vals_before and not vals_after:
        return [], [], [], 0, 0
    all_vals = vals_before + vals_after
    min_val = min(all_vals)
    max_val = max(all_vals)
    bin_width = _choose_length_bin_width(min_val, max_val)
    start = (min_val // bin_width) * bin_width
    end = ((max_val + bin_width - 1) // bin_width) * bin_width
    bins = list(range(start, end + bin_width, bin_width))
    counts_before = [0] * (len(bins) - 1)
    counts_after = [0] * (len(bins) - 1)
    for value in vals_before:
        idx = min((value - start) // bin_width, len(counts_before) - 1)
        counts_before[idx] += 1
    for value in vals_after:
        idx = min((value - start) // bin_width, len(counts_after) - 1)
        counts_after[idx] += 1
    return bins, counts_before, counts_after, len(vals_before), len(vals_after)

def _percent_hist_dual(
    before_path: pathlib.Path,
    after_path: pathlib.Path,
    metric_fn,
    max_reads: int = MAX_PLOT_READS,
) -> tuple[List[int], List[int], List[int], int, int]:
    bins = _percent_bins(5)
    counts_before, total_before = _percent_hist_from_sequences(before_path, bins, metric_fn, max_reads=max_reads)
    counts_after, total_after = _percent_hist_from_sequences(after_path, bins, metric_fn, max_reads=max_reads)
    return bins, counts_before, counts_after, total_before, total_after

def _generate_quality_plots(
    sdir: pathlib.Path,
    step_idx: int,
    unit_id: str,
    channel: str,
    before_path: pathlib.Path,
    after_path: pathlib.Path,
    log_path: pathlib.Path,
) -> List[Artifact]:
    artifacts: List[Artifact] = []
    try:
        first_before = _peek_first_nonempty_char(before_path, gz=before_path.suffix.lower() == ".gz")
        first_after = _peek_first_nonempty_char(after_path, gz=after_path.suffix.lower() == ".gz")
        if first_before != "@" or first_after != "@":
            return artifacts
        counts_before, total_before = _quality_hist(before_path)
        counts_after, total_after = _quality_hist(after_path)
        if total_before == 0 and total_after == 0:
            return artifacts
        name = _plot_artifact_name(step_idx, unit_id, channel, "compare")
        rel = pathlib.Path(QC_PLOT_DIRNAME) / f"{name}.svg"
        _write_hist_svg_dual(
            sdir / rel,
            f"Quality scores ({channel})",
            "Mean quality (Phred)",
            list(range(41)),
            counts_before,
            counts_after,
            total_before,
            total_after,
            "before",
            "after",
        )
        artifacts.append(Artifact(name=name, path=str(rel), kind="plot", channel=channel, from_step=step_idx))
    except Exception as exc:
        try:
            with open(log_path, "a", encoding="utf-8") as log:
                log.write(f"[PLOT] quality plot failed for {channel} compare: {exc}\n")
        except Exception:
            pass
    return artifacts

def _generate_missing_plots(
    sdir: pathlib.Path,
    step_idx: int,
    unit_id: str,
    channel: str,
    before_path: pathlib.Path,
    after_path: pathlib.Path,
    log_path: pathlib.Path,
) -> List[Artifact]:
    artifacts: List[Artifact] = []
    try:
        bins, counts_before, counts_after, total_before, total_after = _percent_hist_dual(
            before_path, after_path, _n_percent
        )
        if total_before == 0 and total_after == 0:
            return artifacts
        name = _plot_artifact_name(step_idx, unit_id, channel, "compare")
        rel = pathlib.Path(QC_PLOT_DIRNAME) / f"{name}.svg"
        _write_hist_svg_dual(
            sdir / rel,
            f"Missing bases ({channel})",
            "Percent N per read",
            bins,
            counts_before,
            counts_after,
            total_before,
            total_after,
            "before",
            "after",
        )
        artifacts.append(Artifact(name=name, path=str(rel), kind="plot", channel=channel, from_step=step_idx))
    except Exception as exc:
        try:
            with open(log_path, "a", encoding="utf-8") as log:
                log.write(f"[PLOT] missing plot failed for {channel} compare: {exc}\n")
        except Exception:
            pass
    return artifacts

def _generate_repeats_plots(
    sdir: pathlib.Path,
    step_idx: int,
    unit_id: str,
    channel: str,
    before_path: pathlib.Path,
    after_path: pathlib.Path,
    log_path: pathlib.Path,
) -> List[Artifact]:
    artifacts: List[Artifact] = []
    try:
        bins, counts_before, counts_after, total_before, total_after = _repeat_hist_dual(before_path, after_path)
        if total_before == 0 and total_after == 0:
            return artifacts
        name = _plot_artifact_name(step_idx, unit_id, channel, "compare")
        rel = pathlib.Path(QC_PLOT_DIRNAME) / f"{name}.svg"
        _write_hist_svg_dual(
            sdir / rel,
            f"Max homopolymer ({channel})",
            "Max run length",
            bins,
            counts_before,
            counts_after,
            total_before,
            total_after,
            "before",
            "after",
        )
        artifacts.append(Artifact(name=name, path=str(rel), kind="plot", channel=channel, from_step=step_idx))
    except Exception as exc:
        try:
            with open(log_path, "a", encoding="utf-8") as log:
                log.write(f"[PLOT] repeats plot failed for {channel} compare: {exc}\n")
        except Exception:
            pass
    return artifacts

def _generate_maskqual_plots(
    sdir: pathlib.Path,
    step_idx: int,
    unit_id: str,
    channel: str,
    before_path: pathlib.Path,
    after_path: pathlib.Path,
    log_path: pathlib.Path,
) -> List[Artifact]:
    artifacts: List[Artifact] = []
    try:
        bins, counts_before, counts_after, total_before, total_after = _percent_hist_dual(
            before_path, after_path, _n_percent
        )
        if total_before == 0 and total_after == 0:
            return artifacts
        name = _plot_artifact_name(step_idx, unit_id, channel, "compare")
        rel = pathlib.Path(QC_PLOT_DIRNAME) / f"{name}.svg"
        _write_hist_svg_dual(
            sdir / rel,
            f"Masked bases ({channel})",
            "Percent N per read",
            bins,
            counts_before,
            counts_after,
            total_before,
            total_after,
            "before",
            "after",
        )
        artifacts.append(Artifact(name=name, path=str(rel), kind="plot", channel=channel, from_step=step_idx))
    except Exception as exc:
        try:
            with open(log_path, "a", encoding="utf-8") as log:
                log.write(f"[PLOT] maskqual plot failed for {channel} compare: {exc}\n")
        except Exception:
            pass
    return artifacts

def _sc_productive_counts(path: pathlib.Path, productive_field: str, fallback: bool) -> Dict[str, int]:
    counts = {"productive": 0, "nonproductive": 0, "unknown": 0}
    for row, idx in _iter_tsv_rows(path):
        if productive_field in idx:
            val = _get_cell_value(row, idx.get(productive_field))
            if _truthy(val):
                counts["productive"] += 1
            else:
                counts["nonproductive"] += 1
            continue
        if fallback and ("vj_in_frame" in idx) and ("stop_codon" in idx):
            vj_val = _get_cell_value(row, idx.get("vj_in_frame"))
            stop_val = _get_cell_value(row, idx.get("stop_codon"))
            if _truthy(vj_val) and not _truthy(stop_val):
                counts["productive"] += 1
            else:
                counts["nonproductive"] += 1
            continue
        counts["unknown"] += 1
    return counts

def _sc_heavy_count_by_cell(
    path: pathlib.Path,
    locus_field: str,
    heavy_values: List[str],
    cell_field: str,
    fallback: bool,
) -> Optional[Dict[str, int]]:
    counts: Dict[str, int] = {}
    saw_any = False
    for row, idx in _iter_tsv_rows(path):
        if not saw_any:
            saw_any = True
            if (locus_field not in idx) and not (fallback and ("v_call" in idx)):
                return None
        cell = _get_cell_value(row, idx.get(cell_field))
        if not cell:
            continue
        if cell not in counts:
            counts[cell] = 0
        is_heavy = False
        if locus_field in idx:
            locus_val = _get_cell_value(row, idx.get(locus_field))
            is_heavy = locus_val in heavy_values
        elif fallback and ("v_call" in idx):
            vcall = _get_cell_value(row, idx.get("v_call"))
            for hv in heavy_values:
                if vcall.startswith(hv):
                    is_heavy = True
                    break
        if is_heavy:
            counts[cell] += 1
    if not counts:
        return None
    return counts

def _sc_heavy_sets_by_sample(
    path: pathlib.Path,
    sample_field: Optional[str],
    locus_field: str,
    heavy_value: str,
    cell_field: str,
    fallback: bool,
) -> Optional[Dict[str, Dict[str, set]]]:
    all_cells: Dict[str, set] = {}
    heavy_cells: Dict[str, set] = {}
    saw_any = False
    for row, idx in _iter_tsv_rows(path):
        if not saw_any:
            saw_any = True
            if (locus_field not in idx) and not (fallback and ("v_call" in idx)):
                return None
        cell = _get_cell_value(row, idx.get(cell_field))
        if not cell:
            continue
        sample = "all"
        if sample_field and sample_field in idx:
            sample_val = _get_cell_value(row, idx.get(sample_field))
            if sample_val:
                sample = sample_val
        if sample not in all_cells:
            all_cells[sample] = set()
            heavy_cells[sample] = set()
        all_cells[sample].add(cell)

        is_heavy = False
        if locus_field in idx:
            locus_val = _get_cell_value(row, idx.get(locus_field))
            is_heavy = (locus_val == heavy_value)
        elif fallback and ("v_call" in idx):
            vcall = _get_cell_value(row, idx.get("v_call"))
            is_heavy = vcall.startswith(heavy_value)
        else:
            return None

        if is_heavy:
            heavy_cells[sample].add(cell)

    if not all_cells:
        return None
    out: Dict[str, Dict[str, set]] = {}
    for sample, cells in all_cells.items():
        out[sample] = {
            "all": cells,
            "heavy": heavy_cells.get(sample, set()),
        }
    return out

def _generate_sc_productive_plots(
    sdir: pathlib.Path,
    step_idx: int,
    unit_id: str,
    before_paths: List[pathlib.Path],
    productive_field: str,
    fallback: bool,
    log_path: pathlib.Path,
) -> List[Artifact]:
    artifacts: List[Artifact] = []
    try:
        counts = {"productive": 0, "nonproductive": 0, "unknown": 0}
        for p in before_paths:
            c = _sc_productive_counts(p, productive_field, fallback)
            for k in counts:
                counts[k] += c.get(k, 0)
        total = sum(counts.values())
        if total == 0:
            return artifacts
        name = f"plot_{step_idx:03d}_{unit_id}_ratio"
        rel = pathlib.Path(QC_PLOT_DIRNAME) / f"{name}.svg"
        _write_stacked_bar_svg(
            sdir / rel,
            f"Productive vs non-productive (n={total})",
            ["all"],
            [
                [counts["productive"], counts["nonproductive"], counts["unknown"]],
            ],
            ["productive", "non-productive", "unknown"],
            ["#22c55e", "#f59e0b", "#94a3b8"],
            "Rows",
            segment_counts=[[counts["productive"], counts["nonproductive"], counts["unknown"]]],
        )
        artifacts.append(Artifact(name=name, path=str(rel), kind="plot", from_step=step_idx))
    except Exception as exc:
        try:
            with open(log_path, "a", encoding="utf-8") as log:
                log.write(f"[PLOT] sc productive plot failed: {exc}\n")
        except Exception:
            pass
    return artifacts

def _generate_sc_multi_heavy_plots(
    sdir: pathlib.Path,
    step_idx: int,
    unit_id: str,
    before_paths: List[pathlib.Path],
    locus_field: str,
    heavy_values: List[str],
    cell_field: str,
    fallback: bool,
    log_path: pathlib.Path,
) -> List[Artifact]:
    artifacts: List[Artifact] = []
    try:
        total = 0
        c0 = 0
        c1 = 0
        c2 = 0
        for p in before_paths:
            counts = _sc_heavy_count_by_cell(p, locus_field, heavy_values, cell_field, fallback)
            if counts is None:
                continue
            total += len(counts)
            c0 += sum(1 for v in counts.values() if v == 0)
            c1 += sum(1 for v in counts.values() if v == 1)
            c2 += sum(1 for v in counts.values() if v > 1)
        if total == 0:
            return artifacts
        name = f"plot_{step_idx:03d}_{unit_id}_ratio"
        rel = pathlib.Path(QC_PLOT_DIRNAME) / f"{name}.svg"
        _write_stacked_bar_svg(
            sdir / rel,
            f"Heavy chains per cell (n={total})",
            ["all"],
            [
                [c0, c1, c2],
            ],
            ["0 heavy", "1 heavy", "2+ heavy"],
            ["#93c5fd", "#fbbf24", "#fb7185"],
            "Cells",
            segment_counts=[[c0, c1, c2]],
        )
        artifacts.append(Artifact(name=name, path=str(rel), kind="plot", from_step=step_idx))
    except Exception as exc:
        try:
            with open(log_path, "a", encoding="utf-8") as log:
                log.write(f"[PLOT] sc multi heavy plot failed: {exc}\n")
        except Exception:
            pass
    return artifacts

def _generate_sc_no_heavy_plots(
    sdir: pathlib.Path,
    step_idx: int,
    unit_id: str,
    before_paths: List[pathlib.Path],
    sample_field: Optional[str],
    locus_field: str,
    heavy_value: str,
    cell_field: str,
    fallback: bool,
    log_path: pathlib.Path,
) -> List[Artifact]:
    return []

def _generate_length_plots(
    sdir: pathlib.Path,
    step_idx: int,
    unit_id: str,
    channel: str,
    before_path: pathlib.Path,
    after_path: pathlib.Path,
    log_path: pathlib.Path,
) -> List[Artifact]:
    artifacts: List[Artifact] = []
    try:
        bins, counts_before, counts_after, total_before, total_after = _length_hist_dual(before_path, after_path)
        if total_before == 0 and total_after == 0:
            return artifacts
        name = _plot_artifact_name(step_idx, unit_id, channel, "compare")
        rel = pathlib.Path(QC_PLOT_DIRNAME) / f"{name}.svg"
        _write_hist_svg_dual(
            sdir / rel,
            f"Read length ({channel})",
            "Read length",
            bins,
            counts_before,
            counts_after,
            total_before,
            total_after,
            "before",
            "after",
        )
        artifacts.append(Artifact(name=name, path=str(rel), kind="plot", channel=channel, from_step=step_idx))
    except Exception as exc:
        try:
            with open(log_path, "a", encoding="utf-8") as log:
                log.write(f"[PLOT] length plot failed for {channel} compare: {exc}\n")
        except Exception:
            pass
    return artifacts

def make_canonical_name(channel: str, kind: str) -> str:
    return f"{channel}.fastq" if kind == "fastq" else f"{channel}.fasta"

def _save_upload_canonical(upload: UploadFile, channel: str, sdir: pathlib.Path) -> Artifact:
    """
    Save uploaded FASTA/FASTQ (.gz or plain) as an uncompressed canonical file:
      R1.fastq / R1.fasta, R2.fastq / R2.fasta.
    """
    tmp_path = sdir / f"__upload__{uuid.uuid4().hex}"
    with open(tmp_path, "wb") as f:
        shutil.copyfileobj(upload.file, f)

    # 1) Try filename-based detection (most reliable for gz)
    kind = _detect_kind_from_name(upload.filename)

    # 2) If still unknown, peek inside (handle gz/plain correctly)
    if kind is None:
        first = _peek_first_nonempty_char(tmp_path, gz=upload.filename.lower().endswith(".gz"))
        if first == ">":
            kind = "fasta"
        elif first == "@":
            kind = "fastq"

    if kind not in ("fastq", "fasta"):
        tmp_path.unlink(missing_ok=True)
        raise HTTPException(400, f"Unsupported upload type for '{upload.filename}'; expected FASTA/FASTQ(.gz).")

    out_name = make_canonical_name(channel, kind)
    out_path = sdir / out_name

    # 3) Decompress if needed, always store uncompressed canonical file
    if upload.filename.lower().endswith(".gz"):
        with gzip.open(tmp_path, "rb") as src, open(out_path, "wb") as dst:
            shutil.copyfileobj(src, dst)
        tmp_path.unlink(missing_ok=True)
    else:
        tmp_path.replace(out_path)

    return Artifact(name=f"{channel}_raw", path=out_name, kind=kind, channel=channel, from_step=-1)

# --------- misc helpers ----------
def file_existing(sess_dir: pathlib.Path, *candidates: str) -> str:
    for c in candidates:
        if (sess_dir / c).exists():
            return c
    raise HTTPException(500, f"Expected output not found. Tried: {candidates}")

def find_pass_for_prefix(sess_dir: pathlib.Path, prefix: str) -> str:
    for ext in ("fastq.gz","fastq","fasta.gz","fasta"):
        for tag in ("mask-pass","align-primers-pass","primers-pass","extract-pass","score-pass", "quality-pass",
                    "length-pass","missing-pass","repeats-pass","trimqual-pass","maskqual-pass",
                    "collapse-unique","collapse-pass","collapse-fail","collapse-failed"):
            p = sess_dir / f"{prefix}_{tag}.{ext}"
            if p.exists(): return p.name
    raise HTTPException(500, f"Expected output not found for prefix '{prefix}'.")

def _maskprimers_log_summary(log_path: pathlib.Path) -> str:
    if not log_path.exists():
        return ""
    try:
        lines = log_path.read_text(errors="ignore").splitlines()
    except Exception:
        return ""
    keys = ["OUTPUT>", "SEQUENCES>", "PASS>", "FAIL>", "END>"]
    found: Dict[str, str] = {}
    for line in reversed(lines):
        s = line.strip()
        for key in keys:
            if s.startswith(key):
                if key not in found:
                    found[key] = s
                break
        if len(found) == len(keys):
            break
    if not found:
        return ""
    return "\n".join(found.get(k) for k in keys if k in found)

def _maskprimers_no_output_message(log_path: pathlib.Path) -> str:
    msg = "MaskPrimers produced no passing reads. Adjust parameters and rerun."
    summary = _maskprimers_log_summary(log_path)
    if summary:
        msg += "\n" + summary
    return msg

def _default_outname_from_path(path: pathlib.Path) -> str:
    name = path.name
    if name.lower().endswith(".gz"):
        name = name[:-3]
    if "." in name:
        name = name.rsplit(".", 1)[0]
    return name

def _guess_channel_from_name(name: str) -> Optional[str]:
    upper = name.upper()
    if "R2" in upper:
        return "R2"
    if "R1" in upper:
        return "R1"
    return None

def _parse_files_param(files_param: str) -> Optional[str]:
    if not files_param:
        return None
    for entry in str(files_param).split(","):
        entry = entry.strip()
        if not entry:
            continue
        if ":" in entry:
            _, key = entry.split(":", 1)
        else:
            key = entry
        key = key.strip()
        if key:
            return key
    return None

def _resolve_input_sequence(sess: SessionState, sdir: pathlib.Path, params: Dict[str, Any]) -> tuple[pathlib.Path, str]:
    input_key = _parse_files_param(params.get("__files", ""))
    if not input_key:
        input_key = (params.get("input_artifact") or "").strip()

    input_channel = ""
    if input_key:
        if input_key in sess.artifacts:
            art = sess.artifacts[input_key]
            seq_path = sdir / art.path
            input_channel = art.channel or _guess_channel_from_name(art.path) or ""
        else:
            seq_path = sdir / input_key
            if not seq_path.exists():
                raise HTTPException(400, f"Input artifact not found: {input_key}")
            input_channel = (params.get("input_channel") or "").strip() or _guess_channel_from_name(seq_path.name) or ""
    else:
        channel_param = (params.get("input_channel") or "R1").strip().upper()
        _assert_channel(sess, channel_param)
        art = sess.artifacts[sess.current[channel_param]]
        seq_path = sdir / art.path
        input_channel = art.channel or channel_param

    return seq_path, input_channel

def _last_log_section(log_path: pathlib.Path, max_chars: int = 1500) -> str:
    if not log_path.exists():
        return ""
    try:
        lines = log_path.read_text(errors="ignore").splitlines()
    except Exception:
        return ""
    idx = len(lines) - 1
    while idx >= 0 and not lines[idx].strip():
        idx -= 1
    if idx < 0:
        return ""
    block = []
    while idx >= 0 and lines[idx].strip():
        block.append(lines[idx])
        idx -= 1
    block.reverse()
    text = "\n".join(block).strip()
    if max_chars and len(text) > max_chars:
        text = text[-max_chars:]
    return text

def _format_error_with_log(error_text: str, log_section: str) -> str:
    if not log_section:
        return error_text
    if log_section in error_text:
        return error_text
    return f"{error_text}\n\nLast log section:\n{log_section}"

def _assert_channel(sess: SessionState, ch: str):
    if ch not in sess.current:
        raise HTTPException(400, f"Required channel '{ch}' is not available.")

# --------- Units ----------
class UnitSpec(BaseModel):
    id: str
    label: str
    requires: List[str]
    params_schema: Dict[str, Any]
    def run(self, sess: SessionState, sess_dir: pathlib.Path, params: Dict[str, Any]) -> StepResult:
        raise NotImplementedError

def _next_idx(sess: SessionState) -> int: return len(sess.steps)

# FilterSeq units
class U_FilterQuality(UnitSpec):
    def run(self, sess, sdir, params):
        idx = _next_idx(sess); log = sdir / f"{idx:03d}_FilterSeq_quality.log"
        _assert_channel(sess, "R1"); r1 = sdir / sess.artifacts[sess.current["R1"]].path
        q = str(params.get("qmin", 20))
        run_cmd(["FilterSeq.py","quality","-s",str(r1),"-q",q,"--outname",f"R1_q{q}","--log",log.name], sdir, log)
        out_r1 = find_pass_for_prefix(sdir, f"R1_q{q}")
        produced = [Artifact(name="R1_quality", path=out_r1, kind="fastq", channel="R1", from_step=idx)]
        produced += _generate_quality_plots(
            sdir,
            idx,
            self.id,
            "R1",
            r1,
            sdir / out_r1,
            log,
        )
        if sess.current.get("R2"):
            r2 = sdir / sess.artifacts[sess.current["R2"]].path
            run_cmd(["FilterSeq.py","quality","-s",str(r2),"-q",q,"--outname",f"R2_q{q}","--log",log.name], sdir, log)
            out_r2 = find_pass_for_prefix(sdir, f"R2_q{q}")
            produced.append(Artifact(name="R2_quality", path=out_r2, kind="fastq", channel="R2", from_step=idx))
            produced += _generate_quality_plots(
                sdir,
                idx,
                self.id,
                "R2",
                r2,
                sdir / out_r2,
                log,
            )
            sess.current["R2"] = "R2_quality"
        sess.current["R1"] = "R1_quality"
        for a in produced: sess.artifacts[a.name] = a
        return StepResult(step_index=idx, unit=self.id, params=params, produced=produced)

class U_FilterLength(UnitSpec):
    def run(self, sess, sdir, params):
        idx = _next_idx(sess); log = sdir / f"{idx:03d}_FilterSeq_length.log"
        _assert_channel(sess, "R1"); r1 = sdir / sess.artifacts[sess.current["R1"]].path
        n = str(params.get("min_len", 100))
        cmd = ["FilterSeq.py","length","-s",str(r1),"-n",n,"--outname",f"R1_len{n}","--log",log.name]
        if str(params.get("inner","false")).lower() in ("1","true","yes","y"): cmd.append("--inner")
        run_cmd(cmd, sdir, log)
        out_r1 = find_pass_for_prefix(sdir, f"R1_len{n}")
        produced = [Artifact(name="R1_length", path=out_r1, kind="fastq", channel="R1", from_step=idx)]
        produced += _generate_length_plots(
            sdir,
            idx,
            self.id,
            "R1",
            r1,
            sdir / out_r1,
            log,
        )
        if sess.current.get("R2"):
            r2 = sdir / sess.artifacts[sess.current["R2"]].path
            cmd2 = ["FilterSeq.py","length","-s",str(r2),"-n",n,"--outname",f"R2_len{n}","--log",log.name]
            if str(params.get("inner","false")).lower() in ("1","true","yes","y"): cmd2.append("--inner")
            run_cmd(cmd2, sdir, log)
            out_r2 = find_pass_for_prefix(sdir, f"R2_len{n}")
            produced.append(Artifact(name="R2_length", path=out_r2, kind="fastq", channel="R2", from_step=idx))
            produced += _generate_length_plots(
                sdir,
                idx,
                self.id,
                "R2",
                r2,
                sdir / out_r2,
                log,
            )
            sess.current["R2"] = "R2_length"
        sess.current["R1"] = "R1_length"
        for a in produced: sess.artifacts[a.name] = a
        return StepResult(step_index=idx, unit=self.id, params=params, produced=produced)

class U_FilterMissing(UnitSpec):
    def run(self, sess, sdir, params):
        idx = _next_idx(sess); log = sdir / f"{idx:03d}_FilterSeq_missing.log"
        _assert_channel(sess, "R1"); r1 = sdir / sess.artifacts[sess.current["R1"]].path
        n = str(params.get("max_missing", 10))
        cmd = ["FilterSeq.py","missing","-s",str(r1),"-n",n,"--outname",f"R1_m{n}","--log",log.name]
        if str(params.get("inner","false")).lower() in ("1","true","yes","y"): cmd.append("--inner")
        run_cmd(cmd, sdir, log)
        out_r1 = find_pass_for_prefix(sdir, f"R1_m{n}")
        produced = [Artifact(name="R1_missing", path=out_r1, kind="fastq", channel="R1", from_step=idx)]
        produced += _generate_missing_plots(
            sdir,
            idx,
            self.id,
            "R1",
            r1,
            sdir / out_r1,
            log,
        )
        if sess.current.get("R2"):
            r2 = sdir / sess.artifacts[sess.current["R2"]].path
            cmd2 = ["FilterSeq.py","missing","-s",str(r2),"-n",n,"--outname",f"R2_m{n}","--log",log.name]
            if str(params.get("inner","false")).lower() in ("1","true","yes","y"): cmd2.append("--inner")
            run_cmd(cmd2, sdir, log)
            out_r2 = find_pass_for_prefix(sdir, f"R2_m{n}")
            produced.append(Artifact(name="R2_missing", path=out_r2, kind="fastq", channel="R2", from_step=idx))
            produced += _generate_missing_plots(
                sdir,
                idx,
                self.id,
                "R2",
                r2,
                sdir / out_r2,
                log,
            )
            sess.current["R2"] = "R2_missing"
        sess.current["R1"] = "R1_missing"
        for a in produced: sess.artifacts[a.name] = a
        return StepResult(step_index=idx, unit=self.id, params=params, produced=produced)

class U_FilterRepeats(UnitSpec):
    def run(self, sess, sdir, params):
        idx = _next_idx(sess); log = sdir / f"{idx:03d}_FilterSeq_repeats.log"
        _assert_channel(sess, "R1"); r1 = sdir / sess.artifacts[sess.current["R1"]].path
        n = str(params.get("max_repeat","0.8"))
        cmd = ["FilterSeq.py","repeats","-s",str(r1),"-n",n,"--outname",f"R1_rep{n}","--log",log.name]
        if str(params.get("missing","false")).lower() in ("1","true","yes","y"): cmd.append("--missing")
        if str(params.get("inner","false")).lower() in ("1","true","yes","y"): cmd.append("--inner")
        run_cmd(cmd, sdir, log)
        out_r1 = find_pass_for_prefix(sdir, f"R1_rep{n}")
        produced = [Artifact(name="R1_repeats", path=out_r1, kind="fastq", channel="R1", from_step=idx)]
        produced += _generate_repeats_plots(
            sdir,
            idx,
            self.id,
            "R1",
            r1,
            sdir / out_r1,
            log,
        )
        if sess.current.get("R2"):
            r2 = sdir / sess.artifacts[sess.current["R2"]].path
            cmd2 = ["FilterSeq.py","repeats","-s",str(r2),"-n",n,"--outname",f"R2_rep{n}","--log",log.name]
            if str(params.get("missing","false")).lower() in ("1","true","yes","y"): cmd2.append("--missing")
            if str(params.get("inner","false")).lower() in ("1","true","yes","y"): cmd2.append("--inner")
            run_cmd(cmd2, sdir, log)
            out_r2 = find_pass_for_prefix(sdir, f"R2_rep{n}")
            produced.append(Artifact(name="R2_repeats", path=out_r2, kind="fastq", channel="R2", from_step=idx))
            produced += _generate_repeats_plots(
                sdir,
                idx,
                self.id,
                "R2",
                r2,
                sdir / out_r2,
                log,
            )
            sess.current["R2"] = "R2_repeats"
        sess.current["R1"] = "R1_repeats"
        for a in produced: sess.artifacts[a.name] = a
        return StepResult(step_index=idx, unit=self.id, params=params, produced=produced)

class U_FilterTrimQual(UnitSpec):
    def run(self, sess, sdir, params):
        idx = _next_idx(sess); log = sdir / f"{idx:03d}_FilterSeq_trimqual.log"
        _assert_channel(sess, "R1"); r1 = sdir / sess.artifacts[sess.current["R1"]].path
        q = str(params.get("qmin", 20)); win = params.get("window", 10)
        cmd = ["FilterSeq.py","trimqual","-s",str(r1),"-q",q,"--outname",f"R1_tq{q}","--log",log.name]
        if win: cmd += ["--win", str(win)]
        if str(params.get("reverse","false")).lower() in ("1","true","yes","y"): cmd.append("--reverse")
        run_cmd(cmd, sdir, log)
        out_r1 = find_pass_for_prefix(sdir, f"R1_tq{q}")
        produced = [Artifact(name="R1_trimqual", path=out_r1, kind="fastq", channel="R1", from_step=idx)]
        produced += _generate_quality_plots(
            sdir,
            idx,
            self.id,
            "R1",
            r1,
            sdir / out_r1,
            log,
        )
        if sess.current.get("R2"):
            r2 = sdir / sess.artifacts[sess.current["R2"]].path
            cmd2 = ["FilterSeq.py","trimqual","-s",str(r2),"-q",q,"--outname",f"R2_tq{q}","--log",log.name]
            if win: cmd2 += ["--win", str(win)]
            if str(params.get("reverse","false")).lower() in ("1","true","yes","y"): cmd2.append("--reverse")
            run_cmd(cmd2, sdir, log)
            out_r2 = find_pass_for_prefix(sdir, f"R2_tq{q}")
            produced.append(Artifact(name="R2_trimqual", path=out_r2, kind="fastq", channel="R2", from_step=idx))
            produced += _generate_quality_plots(
                sdir,
                idx,
                self.id,
                "R2",
                r2,
                sdir / out_r2,
                log,
            )
            sess.current["R2"] = "R2_trimqual"
        sess.current["R1"] = "R1_trimqual"
        for a in produced: sess.artifacts[a.name] = a
        return StepResult(step_index=idx, unit=self.id, params=params, produced=produced)

class U_FilterMaskQual(UnitSpec):
    def run(self, sess, sdir, params):
        idx = _next_idx(sess); log = sdir / f"{idx:03d}_FilterSeq_maskqual.log"
        _assert_channel(sess, "R1"); r1 = sdir / sess.artifacts[sess.current["R1"]].path
        q = str(params.get("qmin", 20))
        run_cmd(["FilterSeq.py","maskqual","-s",str(r1),"-q",q,"--outname",f"R1_mq{q}","--log",log.name], sdir, log)
        out_r1 = find_pass_for_prefix(sdir, f"R1_mq{q}")
        produced = [Artifact(name="R1_maskqual", path=out_r1, kind="fastq", channel="R1", from_step=idx)]
        produced += _generate_maskqual_plots(
            sdir,
            idx,
            self.id,
            "R1",
            r1,
            sdir / out_r1,
            log,
        )
        if sess.current.get("R2"):
            r2 = sdir / sess.artifacts[sess.current["R2"]].path
            run_cmd(["FilterSeq.py","maskqual","-s",str(r2),"-q",q,"--outname",f"R2_mq{q}","--log",log.name], sdir, log)
            out_r2 = find_pass_for_prefix(sdir, f"R2_mq{q}")
            produced.append(Artifact(name="R2_maskqual", path=out_r2, kind="fastq", channel="R2", from_step=idx))
            produced += _generate_maskqual_plots(
                sdir,
                idx,
                self.id,
                "R2",
                r2,
                sdir / out_r2,
                log,
            )
            sess.current["R2"] = "R2_maskqual"
        sess.current["R1"] = "R1_maskqual"
        for a in produced: sess.artifacts[a.name] = a
        return StepResult(step_index=idx, unit=self.id, params=params, produced=produced)

class U_MaskPrimersScore(UnitSpec):
    def run(self, sess, sdir, params):
        idx = _next_idx(sess)
        log = sdir / f"{idx:03d}_MaskPrimers_score.log"

        def build_cmd(seq_path: pathlib.Path, primer_path: pathlib.Path, outname: str, primer_field: str) -> List[str]:
            mode = params.get("mode", "mask")
            cmd = [
                "MaskPrimers.py", "score",
                "-s", str(seq_path),
                "-p", str(primer_path),
                "--mode", mode,
                "--outname", outname,
                "--log", log.name,
            ]
            if primer_field:
                cmd += ["--pf", primer_field]
            start = params.get("start")
            if start not in (None, ""):
                cmd += ["--start", str(start)]
            max_error = params.get("max_error")
            if max_error not in (None, ""):
                cmd += ["--maxerror", str(max_error)]
            if str(params.get("revpr", "false")).lower() in ("1", "true", "yes", "y"):
                cmd.append("--revpr")
            if str(params.get("barcode", "false")).lower() in ("1", "true", "yes", "y"):
                cmd.append("--barcode")
                barcodelen = params.get("barcodelen")
                if barcodelen not in (None, ""):
                    cmd += ["--barcodelen", str(barcodelen)]
                barcode_field = params.get("barcode_field")
                if barcode_field not in (None, ""):
                    cmd += ["--bf", str(barcode_field)]
            if str(params.get("fasta", "false")).lower() in ("1", "true", "yes", "y"):
                cmd.append("--fasta")
            if str(params.get("failed", "false")).lower() in ("1", "true", "yes", "y"):
                cmd.append("--failed")
            return cmd

        produced: List[Artifact] = []
        seq_path, input_channel = _resolve_input_sequence(sess, sdir, params)

        primer_name = (params.get("primer_fname") or "").strip()
        if not primer_name:
            aux = load_state(sdir).aux
            if aux.get("v_primers") and not aux.get("c_primers"):
                primer_name = aux["v_primers"]
            elif aux.get("c_primers") and not aux.get("v_primers"):
                primer_name = aux["c_primers"]
        if not primer_name:
            raise HTTPException(400, "primer_fname is required for score.")
        primer_fa = sdir / primer_name
        if not primer_fa.exists():
            raise HTTPException(400, f"Primer file not found: {primer_name}")

        outname = (params.get("outname") or "").strip() or _default_outname_from_path(seq_path)
        primer_field = (params.get("primer_field") or "PRIMER").strip()
        cmd = build_cmd(seq_path, primer_fa, outname, primer_field)

        delim = (params.get("delim") or "").strip()
        if delim:
            parts = [p for p in delim.replace(",", " ").split() if p]
            if len(parts) != 3:
                raise HTTPException(400, "delim must contain exactly 3 values.")
            cmd += ["--delim"] + parts

        run_cmd(cmd, sdir, log)
        try:
            out_path = find_pass_for_prefix(sdir, outname)
        except HTTPException:
            raise RuntimeError(_maskprimers_no_output_message(log))
        kind = _detect_kind_from_name(out_path) or "fastq"
        channel = input_channel or _guess_channel_from_name(out_path) or "R1"
        artifact_name = f"{channel}_score"
        produced.append(Artifact(name=artifact_name, path=out_path, kind=kind, channel=channel, from_step=idx))
        sess.artifacts[artifact_name] = produced[-1]
        if channel:
            sess.current[channel] = artifact_name

        return StepResult(step_index=idx, unit=self.id, params=params, produced=produced)

class U_MaskPrimersAlign(UnitSpec):
    def run(self, sess, sdir, params):
        idx = _next_idx(sess)
        log = sdir / f"{idx:03d}_MaskPrimers_align.log"

        def build_cmd(seq_path: pathlib.Path, primer_path: pathlib.Path, outname: str, primer_field: str) -> List[str]:
            mode = params.get("mode", "mask")
            cmd = [
                "MaskPrimers.py", "align",
                "-s", str(seq_path),
                "-p", str(primer_path),
                "--mode", mode,
                "--outname", outname,
                "--log", log.name,
            ]
            if primer_field:
                cmd += ["--pf", primer_field]
            max_error = params.get("max_error")
            if max_error not in (None, ""):
                cmd += ["--maxerror", str(max_error)]
            max_len = params.get("max_len")
            if max_len not in (None, ""):
                cmd += ["--maxlen", str(max_len)]
            gap = (params.get("gap") or "").strip()
            if gap:
                parts = [p for p in gap.replace(",", " ").split() if p]
                if len(parts) != 2:
                    raise HTTPException(400, "gap must contain exactly 2 values.")
                cmd += ["--gap"] + parts
            if str(params.get("revpr", "false")).lower() in ("1", "true", "yes", "y"):
                cmd.append("--revpr")
            if str(params.get("skiprc", "false")).lower() in ("1", "true", "yes", "y"):
                cmd.append("--skiprc")
            if str(params.get("barcode", "false")).lower() in ("1", "true", "yes", "y"):
                cmd.append("--barcode")
                barcodelen = params.get("barcodelen")
                if barcodelen not in (None, ""):
                    cmd += ["--barcodelen", str(barcodelen)]
                barcode_field = params.get("barcode_field")
                if barcode_field not in (None, ""):
                    cmd += ["--bf", str(barcode_field)]
            if str(params.get("fasta", "false")).lower() in ("1", "true", "yes", "y"):
                cmd.append("--fasta")
            if str(params.get("failed", "false")).lower() in ("1", "true", "yes", "y"):
                cmd.append("--failed")
            return cmd

        produced: List[Artifact] = []
        seq_path, input_channel = _resolve_input_sequence(sess, sdir, params)

        primer_name = (params.get("primer_fname") or "").strip()
        if not primer_name:
            aux = load_state(sdir).aux
            if aux.get("v_primers") and not aux.get("c_primers"):
                primer_name = aux["v_primers"]
            elif aux.get("c_primers") and not aux.get("v_primers"):
                primer_name = aux["c_primers"]
        if not primer_name:
            raise HTTPException(400, "primer_fname is required for align.")
        primer_fa = sdir / primer_name
        if not primer_fa.exists():
            raise HTTPException(400, f"Primer file not found: {primer_name}")

        outname = (params.get("outname") or "").strip() or _default_outname_from_path(seq_path)
        primer_field = (params.get("primer_field") or "PRIMER").strip()
        cmd = build_cmd(seq_path, primer_fa, outname, primer_field)

        delim = (params.get("delim") or "").strip()
        if delim:
            parts = [p for p in delim.replace(",", " ").split() if p]
            if len(parts) != 3:
                raise HTTPException(400, "delim must contain exactly 3 values.")
            cmd += ["--delim"] + parts

        run_cmd(cmd, sdir, log)
        try:
            out_path = find_pass_for_prefix(sdir, outname)
        except HTTPException:
            raise RuntimeError(_maskprimers_no_output_message(log))
        kind = _detect_kind_from_name(out_path) or "fastq"
        channel = input_channel or _guess_channel_from_name(out_path) or "R1"
        artifact_name = f"{channel}_align"
        produced.append(Artifact(name=artifact_name, path=out_path, kind=kind, channel=channel, from_step=idx))
        sess.artifacts[artifact_name] = produced[-1]
        if channel:
            sess.current[channel] = artifact_name

        return StepResult(step_index=idx, unit=self.id, params=params, produced=produced)

class U_MaskPrimersExtract(UnitSpec):
    def run(self, sess, sdir, params):
        idx = _next_idx(sess)
        log = sdir / f"{idx:03d}_MaskPrimers_extract.log"

        def build_cmd(seq_path: pathlib.Path, outname: str, primer_field: str) -> List[str]:
            mode = params.get("mode", "mask")
            cmd = [
                "MaskPrimers.py", "extract",
                "-s", str(seq_path),
                "--mode", mode,
                "--outname", outname,
                "--log", log.name,
            ]
            if primer_field:
                cmd += ["--pf", primer_field]
            start = params.get("start")
            if start not in (None, ""):
                cmd += ["--start", str(start)]
            length = params.get("length")
            if length not in (None, ""):
                cmd += ["--len", str(length)]
            if str(params.get("revpr", "false")).lower() in ("1", "true", "yes", "y"):
                cmd.append("--revpr")
            if str(params.get("barcode", "false")).lower() in ("1", "true", "yes", "y"):
                cmd.append("--barcode")
                barcodelen = params.get("barcodelen")
                if barcodelen not in (None, ""):
                    cmd += ["--barcodelen", str(barcodelen)]
                barcode_field = params.get("barcode_field")
                if barcode_field not in (None, ""):
                    cmd += ["--bf", str(barcode_field)]
            if str(params.get("fasta", "false")).lower() in ("1", "true", "yes", "y"):
                cmd.append("--fasta")
            if str(params.get("failed", "false")).lower() in ("1", "true", "yes", "y"):
                cmd.append("--failed")
            return cmd

        produced: List[Artifact] = []
        seq_path, input_channel = _resolve_input_sequence(sess, sdir, params)

        length = params.get("length")
        if length in (None, ""):
            raise HTTPException(400, "length is required for extract.")

        outname = (params.get("outname") or "").strip() or _default_outname_from_path(seq_path)
        primer_field = (params.get("primer_field") or "").strip()
        cmd = build_cmd(seq_path, outname, primer_field)

        delim = (params.get("delim") or "").strip()
        if delim:
            parts = [p for p in delim.replace(",", " ").split() if p]
            if len(parts) != 3:
                raise HTTPException(400, "delim must contain exactly 3 values.")
            cmd += ["--delim"] + parts

        run_cmd(cmd, sdir, log)
        try:
            out_path = find_pass_for_prefix(sdir, outname)
        except HTTPException:
            raise RuntimeError(_maskprimers_no_output_message(log))
        kind = _detect_kind_from_name(out_path) or "fastq"
        channel = input_channel or _guess_channel_from_name(out_path) or "R1"
        artifact_name = f"{channel}_extract"
        produced.append(Artifact(name=artifact_name, path=out_path, kind=kind, channel=channel, from_step=idx))
        sess.artifacts[artifact_name] = produced[-1]
        if channel:
            sess.current[channel] = artifact_name

        return StepResult(step_index=idx, unit=self.id, params=params, produced=produced)

class U_CollapseSeq(UnitSpec):
    def run(self, sess, sdir, params):
        idx = _next_idx(sess)
        log_name = (params.get("log") or "").strip()
        if log_name:
            if os.path.isabs(log_name) or "/" in log_name or "\\" in log_name:
                raise HTTPException(400, "log must be a filename (no path separators).")
            log = sdir / log_name
        else:
            log = sdir / f"{idx:03d}_CollapseSeq.log"

        seq_path, input_channel = _resolve_input_sequence(sess, sdir, params)

        outdir_param = (params.get("outdir") or "").strip()
        outbase_dir = sdir
        if outdir_param:
            if os.path.isabs(outdir_param):
                raise HTTPException(400, "outdir must be a relative path within the session.")
            outbase_dir = (sdir / outdir_param).resolve()
            if not str(outbase_dir).startswith(str(sdir.resolve())):
                raise HTTPException(400, "outdir must be within the session directory.")
            outbase_dir.mkdir(parents=True, exist_ok=True)

        out_files_param = (params.get("out_files") or "").strip()
        if out_files_param:
            if outdir_param:
                raise HTTPException(400, "out_files cannot be used with outdir.")
            if str(params.get("failed", "false")).lower() in ("1", "true", "yes", "y"):
                raise HTTPException(400, "out_files cannot be used with failed.")

        cmd = ["CollapseSeq.py", "-s", str(seq_path), "--log", log.name]
        if out_files_param:
            out_files = [p for p in out_files_param.replace(",", " ").split() if p]
            if len(out_files) != 1:
                raise HTTPException(400, "out_files must contain exactly one filename.")
            out_file = out_files[0]
            out_file_path = (sdir / out_file).resolve()
            if out_file_path.is_absolute() and not str(out_file_path).startswith(str(sdir.resolve())):
                raise HTTPException(400, "out_files must be within the session directory.")
            out_file_path.parent.mkdir(parents=True, exist_ok=True)
            cmd += ["-o", out_file]
        else:
            outname = (params.get("outname") or "").strip()
            if outdir_param:
                cmd += ["--outdir", str(outbase_dir)]
            if outname:
                cmd += ["--outname", outname]

        if str(params.get("failed", "false")).lower() in ("1", "true", "yes", "y"):
            cmd += ["--failed"]
        if str(params.get("fasta", "false")).lower() in ("1", "true", "yes", "y"):
            cmd += ["--fasta"]
        if str(params.get("gzip_output", "false")).lower() in ("1", "true", "yes", "y"):
            cmd += ["--gzip-output"]

        delim = (params.get("delim") or "").strip()
        if delim:
            parts = [p for p in delim.replace(",", " ").split() if p]
            if len(parts) != 3:
                raise HTTPException(400, "delim must contain exactly 3 values.")
            cmd += ["--delim"] + parts

        max_missing = params.get("max_missing")
        if max_missing not in (None, ""):
            cmd += ["-n", str(max_missing)]

        uniq_fields = (params.get("uniq_fields") or "").strip()
        if uniq_fields:
            cmd += ["--uf"] + [x.strip() for x in uniq_fields.replace(",", " ").split() if x.strip()]

        copy_fields = (params.get("copy_fields") or "").strip()
        if copy_fields:
            cmd += ["--cf"] + [x.strip() for x in copy_fields.replace(",", " ").split() if x.strip()]

        act = (params.get("act") or "").strip()
        if act:
            actions = [x.strip() for x in act.replace(",", " ").split() if x.strip()]
            allowed = {"min", "max", "sum", "set"}
            for item in actions:
                if item not in allowed:
                    raise HTTPException(400, f"Invalid act value: {item}. Use min,max,sum,set.")
            cmd += ["--act"] + actions

        if str(params.get("inner", "false")).lower() in ("1", "true", "yes", "y"):
            cmd += ["--inner"]
        if str(params.get("keepmiss", "false")).lower() in ("1", "true", "yes", "y"):
            cmd += ["--keepmiss"]

        max_field = (params.get("max_field") or "").strip()
        min_field = (params.get("min_field") or "").strip()
        if max_field and min_field:
            raise HTTPException(400, "Choose either max_field or min_field (mutually exclusive).")
        if max_field:
            cmd += ["--maxf", max_field]
        if min_field:
            cmd += ["--minf", min_field]

        run_cmd(cmd, sdir, log)

        if out_files_param:
            out_rel = out_file
        else:
            prefix = (params.get("outname") or "").strip() or _default_outname_from_path(seq_path)
            out_name = find_pass_for_prefix(outbase_dir, prefix)
            rel_base = pathlib.Path(outdir_param) if outdir_param else pathlib.Path("")
            out_rel = str(rel_base / out_name)

        if not (sdir / out_rel).exists():
            raise HTTPException(500, f"Expected output not found: {out_rel}")

        kind = _detect_kind_from_name(out_rel) or _detect_kind_from_name(seq_path.name) or "fastq"
        a = Artifact(name="COLLAPSED", path=out_rel, kind=kind, channel=input_channel or None, from_step=idx)
        sess.artifacts[a.name] = a
        if input_channel:
            sess.current[input_channel] = a.name
        return StepResult(step_index=idx, unit=self.id, params=params, produced=[a])

class U_BuildConsensus(UnitSpec):
    def run(self, sess, sdir, params):
        idx = _next_idx(sess); log = sdir / f"{idx:03d}_BuildConsensus.log"
        key = sess.current.get("R1")
        if not key: raise HTTPException(400, "BuildConsensus needs a FASTQ/FASTA (R1).")
        src = sdir / sess.artifacts[key].path
        outdir_param = (params.get("outdir") or "").strip()
        outbase_dir = sdir
        if outdir_param:
            if os.path.isabs(outdir_param):
                raise HTTPException(400, "outdir must be a relative path within the session.")
            outbase_dir = (sdir / outdir_param).resolve()
            if not str(outbase_dir).startswith(str(sdir.resolve())):
                raise HTTPException(400, "outdir must be within the session directory.")
            outbase_dir.mkdir(parents=True, exist_ok=True)

        outprefix = (params.get("outname") or "CONS").strip() or "CONS"
        cmd = ["BuildConsensus.py","-s",str(src),"--outname",outprefix,"--log",log.name]
        if outdir_param:
            cmd += ["--outdir", str(outbase_dir)]
        if str(params.get("failed","false")).lower() in ("1","true","yes","y"):
            cmd += ["--failed"]
        if str(params.get("fasta","false")).lower() in ("1","true","yes","y"):
            cmd += ["--fasta"]
        delim = (params.get("delim") or "").strip()
        if delim:
            parts = [p for p in delim.replace(",", " ").split() if p]
            if len(parts) != 3:
                raise HTTPException(400, "delim must contain exactly 3 values.")
            cmd += ["--delim"] + parts
        if params.get("min_count"):
            cmd += ["-n", str(params["min_count"])]
        if params.get("barcode_field"):
            cmd += ["--bf", str(params["barcode_field"])]
        if params.get("qmin"):
            cmd += ["-q", str(params["qmin"])]
        if params.get("freq"):
            cmd += ["--freq", str(params["freq"])]
        if params.get("maxgap"):
            cmd += ["--maxgap", str(params["maxgap"])]
        if params.get("primer_field"):
            cmd += ["--pf", str(params["primer_field"])]
        if params.get("primer_freq"):
            cmd += ["--prcons", str(params["primer_freq"])]
        copy_fields = (params.get("copy_fields") or "").strip()
        if copy_fields:
            cmd += ["--cf"] + [x.strip() for x in copy_fields.replace(",", " ").split() if x.strip()]
        act = (params.get("act") or "").strip()
        if act:
            cmd += ["--act"] + [x.strip() for x in act.replace(",", " ").split() if x.strip()]
        if str(params.get("dep","false")).lower() in ("1","true","yes","y"):
            cmd += ["--dep"]
        if params.get("maxdiv") and params.get("maxerror"):
            raise HTTPException(400, "Choose either maxdiv or maxerror (mutually exclusive).")
        if params.get("maxdiv"):
            cmd += ["--maxdiv", str(params["maxdiv"])]
        if params.get("maxerror"):
            cmd += ["--maxerror", str(params["maxerror"])]

        run_cmd(cmd, sdir, log)
        # BuildConsensus creates multiple outputs; keep the consensus-pass.* as representative
        out = find_pass_for_prefix(outbase_dir, f"{outprefix}_consensus")
        rel_base = pathlib.Path(outdir_param) if outdir_param else pathlib.Path("")
        kind = _detect_kind_from_name(out) or "fastq"
        a = Artifact(name="CONSENSUS", path=str(rel_base / out), kind=kind, from_step=idx)
        sess.artifacts[a.name] = a
        return StepResult(step_index=idx, unit=self.id, params=params, produced=[a])
    
class U_MergeSamples(UnitSpec):
    """
    Merge multiple AIRR-C rearrangement tables using airr::read_rearrangement.
    - files: comma/space-separated list of filenames stored in this session (optional).
             If omitted, all *.tsv and *.tsv.gz in the session directory are used.
    - aux_types: key=type pairs (e.g. "v_germline_length=i, d_germline_length=i, j_germline_length=i, day=i").
                 Defaults to those four integers if omitted.
    - sample_field: column name to annotate each row with filename stem (default "sample_id"; set empty to skip).
    Output: MERGED.tsv
    """
    def run(self, sess, sess_dir, params):
        import re
        idx = _next_idx(sess)
        log = sess_dir / f"{idx:03d}_SC_MergeSamples.log"

        # Collect files
        files_param = (params.get("files") or "").strip()
        if files_param:
            # split by comma/space
            names = [n for n in re.split(r"[,\s]+", files_param) if n]
        else:
            # default: all TSV/TSV.GZ in session
            names = sorted([p.name for p in sess_dir.glob("*.tsv")] + [p.name for p in sess_dir.glob("*.tsv.gz")])

        if not names:
            raise HTTPException(400, "No input tables. Upload AIRR TSVs (use 'Upload aux') or provide 'files' list.")

        paths = []
        for n in names:
            p = sess_dir / n
            if not p.exists():
                raise HTTPException(400, f"File not found in session: {n}")
            paths.append(str(p))

        # aux_types mapping
        aux_default = "v_germline_length=i, d_germline_length=i, j_germline_length=i, day=i"
        aux_str = (params.get("aux_types") or aux_default).strip()

        # convert to R named vector literal: c('k'='i','k2'='i',...)
        pairs = []
        for part in re.split(r"[,\s]+", aux_str):
            if not part or "=" not in part: continue
            k,v = part.split("=",1)
            k = k.strip(); v = v.strip()
            if not k or not v: continue
            pairs.append(f"'{k}'='{v}'")
        r_aux_vec = "c(" + ",".join(pairs) + ")" if pairs else "c()"

        sample_field = (params.get("sample_field") if params.get("sample_field") is not None else "sample_id")
        sample_field = str(sample_field).strip()

        # Write a small R script into the session
        rfile = sess_dir / f"{idx:03d}_merge_samples.R"
        r_code = f"""
            args <- commandArgs(trailingOnly=TRUE)
            out <- args[1]
            files <- args[-1]
            suppressPackageStartupMessages(library(airr))
            aux_types <- {r_aux_vec}
            sfield <- {repr(sample_field)}

            read_one <- function(f) {{
            df <- airr::read_rearrangement(f, aux_types = aux_types)
            if (nchar(sfield) > 0) {{
                base <- basename(f)
                base <- sub("\\\\.[^.]+$", "", base)
                df[[sfield]] <- base
            }}
            df
            }}

            lst <- lapply(files, read_one)
            merged <- do.call(rbind, lst)
            write.table(merged, file=out, sep="\\t", quote=FALSE, row.names=FALSE)
        """
        rfile.write_text(r_code, encoding="utf-8")

        # Run Rscript
        out_path = sess_dir / "MERGED.tsv"
        cmd = ["Rscript", "--vanilla", rfile.name, out_path.name] + paths
        run_cmd(cmd, sess_dir, log)

        a = Artifact(name="SC_MERGED", path=out_path.name, kind="tab", from_step=idx)
        sess.artifacts[a.name] = a
        # Track a single-cell table "channel" for downstream SC units
        sess.current["SC_TABLE"] = a.name

        return StepResult(step_index=idx, unit=self.id, params=params, produced=[a])
    
class U_SC_FilterProductive(UnitSpec):
    """
    Single-cell: Remove non-productive sequences independently of other steps.

    Parameters
    ----------
    files : text (optional)
        Comma/space separated list of TSV/TSV.GZ files already uploaded to this session.
        If empty, all *.tsv / *.tsv.gz in the session directory are used.
    productive_field : text
        Column to test for truthy values (default: 'productive').
    fallback_from_airr : select {'true','false'}
        If productive_field is missing, try computing productivity as
        (vj_in_frame == TRUE) & (stop_codon == FALSE). Default true.
    mode : select {'merge','per_file'}
        'merge' produces one file (SC_productive.tsv) and sets SC_TABLE to it.
        'per_file' writes SC_prod_<basename>.tsv for each input file and sets SC_TABLE
        to the first produced file (so you can chain if desired).
    sample_field : text
        When mode='merge' and non-empty, a new column with this name is added containing
        the input filename stem.
    """
    def run(self, sess, sess_dir, params):
        import re
        idx = _next_idx(sess)
        log = sess_dir / f"{idx:03d}_SC_FilterProductive.log"

        # -------- inputs ----------
        files_param = (params.get("files") or "").strip()
        if files_param:
            names = [n for n in re.split(r"[,\s]+", files_param) if n]
        else:
            names = sorted([p.name for p in sess_dir.glob("*.tsv")] +
                           [p.name for p in sess_dir.glob("*.tsv.gz")])

        if not names:
            raise HTTPException(400, "No TSVs found. Upload AIRR TSV/TSV.GZ via 'Upload inputs' or specify 'files'.")

        for n in names:
            if not (sess_dir / n).exists():
                raise HTTPException(400, f"File not found in session: {n}")

        pf   = (params.get("productive_field") or "productive").strip() or "productive"
        fb   = str(params.get("fallback_from_airr", "true")).lower() in ("1","true","yes","y")
        mode = (params.get("mode") or "merge").strip().lower()
        if mode not in ("merge","per_file"):
            mode = "merge"
        sfield = (params.get("sample_field") or "sample_id").strip()

        # -------- R script ----------
        rfile = sess_dir / f"{idx:03d}_sc_filter_productive.R"
        out_merged = "SC_productive.tsv"
        # We pass: out_merged, mode, sfield, pf, fallbackFlag, then files...
        r_code = f"""
args <- commandArgs(trailingOnly=TRUE)
out_merged <- args[1]
mode <- args[2]
sfield <- args[3]
pf <- {repr(pf)}
fallbackFlag <- as.logical({str(fb).upper()})
files <- args[-(1:3)]

truthy <- c(TRUE, "TRUE", "T", "true", "True", 1, "1")

filter_one <- function(f){{
  df <- tryCatch({{
    read.delim(f, header=TRUE, sep="\\t", check.names=FALSE, stringsAsFactors=FALSE)
  }}, error=function(e) {{
    stop(paste("Failed to read:", f, "->", e$message))
  }})

  # compute keep mask
  if (pf %in% colnames(df)) {{
    keep <- df[[pf]] %in% truthy
  }} else if (fallbackFlag && all(c("vj_in_frame","stop_codon") %in% colnames(df))) {{
    # AIRR fallback: productive if in-frame and no stop codon
    keep <- (df[["vj_in_frame"]] %in% truthy) & !(df[["stop_codon"]] %in% truthy)
  }} else {{
    warning(paste("No productive field and no AIRR fallback columns; keeping all rows for", f))
    keep <- rep(TRUE, nrow(df))
  }}

  df2 <- df[keep, , drop=FALSE]
  df2
}}

if (mode == "per_file") {{
  for (f in files) {{
    df2 <- filter_one(f)
    base <- basename(f)
    base <- sub("\\\\.[^.]+$", "", base)
    out <- paste0("SC_prod_", base, ".tsv")
    write.table(df2, file=out, sep="\\t", quote=FALSE, row.names=FALSE)
    cat(paste("Wrote", out, "rows:", nrow(df2), "\\n"))
  }}
}} else {{
  lst <- lapply(files, filter_one)
  if (length(lst) == 0) {{
    stop("No input tables after filtering.")
  }}
  merged <- do.call(rbind, lst)
  if (nchar(sfield) > 0) {{
    # annotate origin by filename stem
    # We need to repeat the origin per row; rebuild using files and nrow of filtered fragments
    origins <- unlist(lapply(seq_along(files), function(i){{
      f <- files[[i]]
      base <- sub("\\\\.[^.]+$", "", basename(f))
      n <- nrow(lst[[i]])
      if (n <= 0) return(character(0))
      rep(base, n)
    }}))
    if (length(origins) == nrow(merged)) {{
      merged[[sfield]] <- origins
    }} else {{
      warning("Could not build origin column (row mismatch). Skipping.")
    }}
  }}
  write.table(merged, file=out_merged, sep="\\t", quote=FALSE, row.names=FALSE)
  cat(paste("Wrote", out_merged, "rows:", nrow(merged), "\\n"))
}}
"""
        rfile.write_text(r_code, encoding="utf-8")

        cmd = ["Rscript", "--vanilla", rfile.name, out_merged, mode, sfield] + names
        run_cmd(cmd, sess_dir, log)

        produced = []
        if mode == "per_file":
            # Register every produced SC_prod_<stem>.tsv
            for n in names:
                stem = re.sub(r"\\.[^.]+$", "", n)
                out = f"SC_prod_{stem}.tsv"
                if (sess_dir / out).exists():
                    a = Artifact(name=f"SC_PROD_{stem}", path=out, kind="tab", from_step=idx)
                    sess.artifacts[a.name] = a
                    produced.append(a)
            # set SC_TABLE to the first produced (if any)
            if produced:
                sess.current["SC_TABLE"] = produced[0].name
        else:
            # merge mode: one output
            a = Artifact(name="SC_PRODUCTIVE", path=out_merged, kind="tab", from_step=idx)
            sess.artifacts[a.name] = a
            produced.append(a)
            sess.current["SC_TABLE"] = a.name

        before_paths = [sess_dir / n for n in names if (sess_dir / n).exists()]
        produced += _generate_sc_productive_plots(
            sess_dir,
            idx,
            self.id,
            before_paths,
            pf,
            fb,
            log,
        )
        for a in produced:
            sess.artifacts[a.name] = a

        return StepResult(step_index=idx, unit=self.id, params=params, produced=produced)

class U_SC_RemoveMultiHeavy(UnitSpec):
    """
    Single-cell: remove cells that have multiple heavy-chain rearrangements.

    Parameters
    ----------
    files : text (optional)
        Comma/space-separated list of TSV/TSV.GZ files uploaded to this session.
        If empty, uses all *.tsv / *.tsv.gz in the session directory.
    locus_field : text
        Column that denotes chain locus (default: 'locus').
    heavy_value : text
        Select a locus (or the 'TRA + TRB' combo) treated as heavy. Defaults to IGH when left empty.
    cell_field : text
        Column with the cell identifier (default: 'cell_id')  — REQUIRED in input.
    fallback_from_vcall : select {'true','false'}
        If `locus_field` is missing, detect heavy loci via v_call prefixes (e.g., '^IGH', '^TRA',
        '^TRB') (default true).
    mode : select {'merge','per_file'}
        'merge' → one file SC_no_multi_heavy.tsv; 'per_file' → one file per input.
        In both cases SC_TABLE is set (first produced when per_file).
    sample_field : text
        When merging and non-empty, annotate each row with the filename stem.

    Output
    ------
    - merge: SC_no_multi_heavy.tsv
    - per_file: SC_noMH_<basename>.tsv per input
    """
    def run(self, sess, sess_dir, params):
        import re
        idx = _next_idx(sess)
        log = sess_dir / f"{idx:03d}_SC_RemoveMultiHeavy.log"

        # ---- inputs ----
        files_param = (params.get("files") or "").strip()
        if files_param:
            names = [n for n in re.split(r"[,\s]+", files_param) if n]
        else:
            names = sorted([p.name for p in sess_dir.glob("*.tsv")] +
                           [p.name for p in sess_dir.glob("*.tsv.gz")])
        if not names:
            raise HTTPException(400, "No TSVs found. Upload TSV/TSV.GZ or provide 'files'.")

        for n in names:
            if not (sess_dir / n).exists():
                raise HTTPException(400, f"File not found in session: {n}")

        locus_field = (params.get("locus_field") or "locus").strip() or "locus"
        heavy_value_text = (params.get("heavy_value") or "IGH").strip() or "IGH"
        cell_field  = (params.get("cell_field")  or "cell_id").strip() or "cell_id"
        fb          = str(params.get("fallback_from_vcall", "true")).lower() in ("1","true","yes","y")
        mode        = (params.get("mode") or "merge").strip().lower()
        if mode not in ("merge","per_file"):
            mode = "merge"
        sfield      = (params.get("sample_field") or "sample_id").strip()

        # ---- R script ----
        rfile = sess_dir / f"{idx:03d}_sc_remove_multi_heavy.R"
        out_merged = "SC_no_multi_heavy.tsv"
        heavy_values = [v for v in re.split(r"[,\s]+", heavy_value_text) if v]
        if not heavy_values:
            heavy_values = ["IGH"]
        hv_joined = ",".join(heavy_values)

        # pass: out_merged, mode, sfield, locus_field, heavy_values (comma-joined), cell_field, fallbackFlag, then files...
        r_code = f"""
args <- commandArgs(trailingOnly=TRUE)
out_merged <- args[1]
mode <- args[2]
sfield <- args[3]
locus_field <- {repr(locus_field)}
heavy_values <- unlist(strsplit({repr(hv_joined)}, ","))
heavy_values <- heavy_values[nchar(heavy_values) > 0]
if (length(heavy_values) == 0) {{
  heavy_values <- c("IGH")
}}
cell_field <- {repr(cell_field)}
fallbackFlag <- as.logical({str(fb).upper()})
files <- args[-(1:3)]

read_one <- function(f){{
  df <- tryCatch({{
    read.delim(f, header=TRUE, sep="\\t", check.names=FALSE, stringsAsFactors=FALSE)
  }}, error=function(e) {{
    stop(paste("Failed to read:", f, "->", e$message))
  }})
  if (!(cell_field %in% colnames(df))) {{
    stop(paste("Column", cell_field, "not found in", f))
  }}

  multi_cells <- character(0)
  collect_multi <- function(mask) {{
    if (!any(mask)) {{
      return(character(0))
    }}
    cells <- df[mask, cell_field]
    cells <- cells[!is.na(cells)]
    if (length(cells) == 0) {{
      return(character(0))
    }}
    tab <- table(cells)
    names(tab[tab > 1])
  }}

  if (locus_field %in% colnames(df)) {{
    loci_vals <- as.character(df[[locus_field]])
    for (hv in heavy_values) {{
      multi_cells <- union(multi_cells, collect_multi(loci_vals == hv))
    }}
  }} else if (fallbackFlag && ("v_call" %in% colnames(df))) {{
    vc <- as.character(df[["v_call"]])
    for (hv in heavy_values) {{
      pattern <- paste0("^", hv)
      multi_cells <- union(multi_cells, collect_multi(grepl(pattern, vc)))
    }}
  }} else {{
    warning(paste("No", locus_field, "and no v_call; assuming no heavy calls in", f))
    return(df)
  }}

  # Filter out those cells
  keep <- !(df[[cell_field]] %in% multi_cells)
  df2 <- df[keep, , drop=FALSE]
  df2
}}

if (mode == "per_file") {{
  for (f in files) {{
    df2 <- read_one(f)
    base <- sub("\\\\.[^.]+$", "", basename(f))
    out <- paste0("SC_noMH_", base, ".tsv")
    write.table(df2, file=out, sep="\\t", quote=FALSE, row.names=FALSE)
    cat(paste("Wrote", out, "rows:", nrow(df2), "\\n"))
  }}
}} else {{
  lst <- lapply(files, read_one)
  if (length(lst) == 0) {{
    stop("No input tables after filtering.")
  }}
  merged <- do.call(rbind, lst)
  if (nchar(sfield) > 0) {{
    origins <- unlist(lapply(seq_along(files), function(i){{
      base <- sub("\\\\.[^.]+$", "", basename(files[[i]]))
      n <- nrow(lst[[i]])
      if (n <= 0) return(character(0))
      rep(base, n)
    }}))
    if (length(origins) == nrow(merged)) {{
      merged[[sfield]] <- origins
    }} else {{
      warning("Could not add origin column (row mismatch).")
    }}
  }}
  write.table(merged, file=out_merged, sep="\\t", quote=FALSE, row.names=FALSE)
  cat(paste("Wrote", out_merged, "rows:", nrow(merged), "\\n"))
}}
"""
        rfile.write_text(r_code, encoding="utf-8")

        cmd = ["Rscript", "--vanilla", rfile.name, out_merged, mode, sfield] + names
        run_cmd(cmd, sess_dir, log)

        produced = []
        if mode == "per_file":
            for n in names:
                stem = re.sub(r"\.[^.]+$", "", n)
                out = f"SC_noMH_{stem}.tsv"
                if (sess_dir / out).exists():
                    a = Artifact(name=f"SC_NOMH_{stem}", path=out, kind="tab", from_step=idx)
                    sess.artifacts[a.name] = a
                    produced.append(a)
            if produced:
                sess.current["SC_TABLE"] = produced[0].name
        else:
            a = Artifact(name="SC_NO_MULTI_HEAVY", path=out_merged, kind="tab", from_step=idx)
            sess.artifacts[a.name] = a
            produced.append(a)
            sess.current["SC_TABLE"] = a.name

        before_paths = [sess_dir / n for n in names if (sess_dir / n).exists()]
        produced += _generate_sc_multi_heavy_plots(
            sess_dir,
            idx,
            self.id,
            before_paths,
            locus_field,
            heavy_values,
            cell_field,
            fb,
            log,
        )
        for a in produced:
            sess.artifacts[a.name] = a

        return StepResult(step_index=idx, unit=self.id, params=params, produced=produced)

class U_SC_RemoveNoHeavy(UnitSpec):
    """
    Single-cell: remove cells that have only light chains (no heavy).

    Parameters
    ----------
    files : text (optional)
        Comma/space-separated list of TSV/TSV.GZ files uploaded to this session.
        If empty, uses all *.tsv / *.tsv.gz in the session directory.
    locus_field : text
        Column that denotes chain locus (default: 'locus').
    heavy_value : text
        Value denoting heavy locus (default: 'IGH').
    light_values : text
        Comma/space-separated values denoting light loci (default: 'IGK, IGL').
    cell_field : text
        Column with cell identifier (default: 'cell_id') — required in input.
    fallback_from_vcall : select {'true','false'}
        If locus_field is missing, detect heavy via v_call =~ '^IGH' and light via v_call =~ '^IG[KL]'.
        Default true.
    mode : select {'merge','per_file'}
        'merge' → one file SC_no_heavy.tsv; 'per_file' → one file per input (SC_noH_<basename>.tsv).
        In both cases SC_TABLE is set (first produced when per_file).
    sample_field : text
        When merging and non-empty, annotate each row with the filename stem.
    """
    def run(self, sess, sess_dir, params):
        import re
        idx = _next_idx(sess)
        log = sess_dir / f"{idx:03d}_SC_RemoveNoHeavy.log"

        # ---- inputs ----
        files_param = (params.get("files") or "").strip()
        if files_param:
            names = [n for n in re.split(r"[,\s]+", files_param) if n]
        else:
            names = sorted([p.name for p in sess_dir.glob("*.tsv")] +
                           [p.name for p in sess_dir.glob("*.tsv.gz")])
        if not names:
            raise HTTPException(400, "No TSVs found. Upload TSV/TSV.GZ or provide 'files'.")

        for n in names:
            if not (sess_dir / n).exists():
                raise HTTPException(400, f"File not found in session: {n}")

        locus_field = (params.get("locus_field") or "locus").strip() or "locus"
        heavy_value = (params.get("heavy_value") or "IGH").strip() or "IGH"
        light_values_text = (params.get("light_values") or "IGK, IGL").strip()
        light_values = [v for v in re.split(r"[,\s]+", light_values_text) if v]
        cell_field  = (params.get("cell_field")  or "cell_id").strip() or "cell_id"
        fb          = str(params.get("fallback_from_vcall", "true")).lower() in ("1","true","yes","y")
        mode        = (params.get("mode") or "merge").strip().lower()
        if mode not in ("merge","per_file"):
            mode = "merge"
        sfield      = (params.get("sample_field") or "sample_id").strip()

        # ---- R script ----
        rfile = sess_dir / f"{idx:03d}_sc_remove_no_heavy.R"
        out_merged = "SC_no_heavy.tsv"
        # pass: out_merged, mode, sfield, locus_field, heavy_value, light_values (comma-joined), cell_field, fallbackFlag, then files...
        lv_joined = ",".join(light_values)
        r_code = f"""
args <- commandArgs(trailingOnly=TRUE)
out_merged <- args[1]
mode <- args[2]
sfield <- args[3]
locus_field <- {repr(locus_field)}
heavy_value <- {repr(heavy_value)}
light_values <- unlist(strsplit({repr(lv_joined)}, ","))
cell_field <- {repr(cell_field)}
fallbackFlag <- as.logical({str(fb).upper()})
files <- args[-(1:3)]

read_one <- function(f){{
  df <- tryCatch({{
    read.delim(f, header=TRUE, sep="\\t", check.names=FALSE, stringsAsFactors=FALSE)
  }}, error=function(e) {{
    stop(paste("Failed to read:", f, "->", e$message))
  }})
  if (!(cell_field %in% colnames(df))) {{
    stop(paste("Column", cell_field, "not found in", f))
  }}

  # Determine heavy vs light masks
  if (locus_field %in% colnames(df)) {{
    heavy_mask <- df[[locus_field]] == heavy_value
    light_mask <- df[[locus_field]] %in% light_values
  }} else if (fallbackFlag && ("v_call" %in% colnames(df))) {{
    vc <- as.character(df[["v_call"]])
    heavy_mask <- grepl("^IGH", vc)
    light_mask <- grepl("^IG(K|L)", vc)
  }} else {{
    warning(paste("No", locus_field, "and no v_call; cannot classify heavy/light in", f, "-- keeping all rows"))
    return(df)
  }}

  # Cells with only light (no heavy)
  heavy_cells <- unique(df[heavy_mask, cell_field])
  light_cells <- unique(df[light_mask, cell_field])
  no_heavy_cells <- setdiff(light_cells, heavy_cells)

  keep <- !(df[[cell_field]] %in% no_heavy_cells)
  df2 <- df[keep, , drop=FALSE]
  df2
}}

if (mode == "per_file") {{
  for (f in files) {{
    df2 <- read_one(f)
    base <- sub("\\\\.[^.]+$", "", basename(f))
    out <- paste0("SC_noH_", base, ".tsv")
    write.table(df2, file=out, sep="\\t", quote=FALSE, row.names=FALSE)
    cat(paste("Wrote", out, "rows:", nrow(df2), "\\n"))
  }}
}} else {{
  lst <- lapply(files, read_one)
  if (length(lst) == 0) {{
    stop("No input tables after filtering.")
  }}
  merged <- do.call(rbind, lst)
  if (nchar(sfield) > 0) {{
    origins <- unlist(lapply(seq_along(files), function(i){{
      base <- sub("\\\\.[^.]+$", "", basename(files[[i]]))
      n <- nrow(lst[[i]])
      if (n <= 0) return(character(0))
      rep(base, n)
    }}))
    if (length(origins) == nrow(merged)) {{
      merged[[sfield]] <- origins
    }} else {{
      warning("Could not add origin column (row mismatch).")
    }}
  }}
  write.table(merged, file=out_merged, sep="\\t", quote=FALSE, row.names=FALSE)
  cat(paste("Wrote", out_merged, "rows:", nrow(merged), "\\n"))
}}
"""
        rfile.write_text(r_code, encoding="utf-8")

        cmd = ["Rscript", "--vanilla", rfile.name, out_merged, mode, sfield] + names
        run_cmd(cmd, sess_dir, log)

        produced = []
        if mode == "per_file":
            for n in names:
                stem = re.sub(r"\.[^.]+$", "", n)
                out = f"SC_noH_{stem}.tsv"
                if (sess_dir / out).exists():
                    a = Artifact(name=f"SC_NOH_{stem}", path=out, kind="tab", from_step=idx)
                    sess.artifacts[a.name] = a
                    produced.append(a)
            if produced:
                sess.current["SC_TABLE"] = produced[0].name
        else:
            a = Artifact(name="SC_NO_HEAVY", path=out_merged, kind="tab", from_step=idx)
            sess.artifacts[a.name] = a
            produced.append(a)
            sess.current["SC_TABLE"] = a.name

        before_paths = [sess_dir / n for n in names if (sess_dir / n).exists()]
        produced += _generate_sc_no_heavy_plots(
            sess_dir,
            idx,
            self.id,
            before_paths,
            sfield if sfield else None,
            locus_field,
            heavy_value,
            cell_field,
            fb,
            log,
        )
        for a in produced:
            sess.artifacts[a.name] = a

        return StepResult(step_index=idx, unit=self.id, params=params, produced=produced)

# --------- Unit registry (bulk only here) ----------
UNITS: Dict[str, UnitSpec] = {
    "filter_quality": U_FilterQuality(
        id="filter_quality", label="FilterSeq: quality", requires=["R1"], group="bulk",
        params_schema={"qmin":{"type":"int","default":20,"min":0,"max":40}}
    ),
    "filter_length": U_FilterLength(
        id="filter_length", label="FilterSeq: length", requires=["R1"], group="bulk",
        params_schema={"min_len":{"type":"int","default":100,"min":1},"inner":{"type":"select","options":["false","true"],"default":"false"}}
    ),
    "filter_missing": U_FilterMissing(
        id="filter_missing", label="FilterSeq: missing", requires=["R1"], group="bulk",
        params_schema={"max_missing":{"type":"int","default":10,"min":0},"inner":{"type":"select","options":["false","true"],"default":"false"}}
    ),
    "filter_repeats": U_FilterRepeats(
        id="filter_repeats", label="FilterSeq: repeats", requires=["R1"], group="bulk",
        params_schema={"max_repeat":{"type":"text","default":"0.8"},"missing":{"type":"select","options":["false","true"],"default":"false"},"inner":{"type":"select","options":["false","true"],"default":"false"}}
    ),
    "filter_trimqual": U_FilterTrimQual(
        id="filter_trimqual", label="FilterSeq: trimqual", requires=["R1"], group="bulk",
        params_schema={"qmin":{"type":"int","default":20,"min":0,"max":40},"window":{"type":"int","default":10,"min":1},"reverse":{"type":"select","options":["false","true"],"default":"false"}}
    ),
    "filter_maskqual": U_FilterMaskQual(
        id="filter_maskqual", label="FilterSeq: maskqual", requires=["R1"], group="bulk",
        params_schema={"qmin":{"type":"int","default":20,"min":0,"max":40}}
    ),
    "mask_primers_score": U_MaskPrimersScore(
        id="mask_primers_score", label="MaskPrimers: score", requires=[], group="bulk",
        params_schema={
            "input_artifact":{"type":"text","label":"Input artifact","placeholder":"artifact key or filename (optional)"},
            "input_channel":{"type":"select","label":"Input channel","options":["R1","R2"],"default":"R1"},
            "primer_fname":{"type":"select","label":"Primer file","options":[],"help":"Select from uploaded aux files"},
            "outname":{"type":"text","label":"Outname","placeholder":"leave blank to use input name"},
            "start":{"type":"int","default":0,"min":0},
            "max_error":{"type":"text","label":"Max error","placeholder":"e.g. 0.1"},
            "mode":{"type":"select","options":["cut","mask","trim","tag"],"default":"mask"},
            "primer_field":{"type":"select","options":[{"value":"","label":"choose..."}, "MID", "VPRIMER", "CPRIMER"],"default":""},
            "revpr":{"type":"checkbox","default":False},
            "barcode":{"type":"checkbox","default":False},
            "barcodelen":{"type":"int","min":1,"placeholder":"use full if blank"},
            "barcode_field":{"type":"text","placeholder":"BARCODE"},
            "delim":{"type":"text","placeholder":"e.g. | : , (3 tokens)"},
            "fasta":{"type":"checkbox","default":False},
            "failed":{"type":"checkbox","default":False},
        }
    ),
    "mask_primers_align": U_MaskPrimersAlign(
        id="mask_primers_align", label="MaskPrimers: align", requires=[], group="bulk",
        params_schema={
            "input_artifact":{"type":"text","label":"Input artifact","placeholder":"artifact key or filename (optional)"},
            "input_channel":{"type":"select","label":"Input channel","options":["R1","R2"],"default":"R1"},
            "primer_fname":{"type":"select","label":"Primer file","options":[],"help":"Select from uploaded aux files"},
            "outname":{"type":"text","label":"Outname","placeholder":"leave blank to use input name"},
            "max_error":{"type":"text","label":"Max error","placeholder":"e.g. 0.1"},
            "max_len":{"type":"text","label":"Max length","placeholder":"e.g. 30"},
            "gap":{"type":"text","label":"Gap penalty","placeholder":"e.g. 5 2"},
            "mode":{"type":"select","options":["cut","mask","trim","tag"],"default":"mask"},
            "primer_field":{"type":"select","options":[{"value":"","label":"choose..."}, "MID", "VPRIMER", "CPRIMER"],"default":""},
            "revpr":{"type":"checkbox","default":False},
            "skiprc":{"type":"checkbox","default":False},
            "barcode":{"type":"checkbox","default":False},
            "barcodelen":{"type":"int","min":1,"placeholder":"use full if blank"},
            "barcode_field":{"type":"text","placeholder":"BARCODE"},
            "delim":{"type":"text","placeholder":"e.g. | : , (3 tokens)"},
            "fasta":{"type":"checkbox","default":False},
            "failed":{"type":"checkbox","default":False},
        }
    ),
    "mask_primers_extract": U_MaskPrimersExtract(
        id="mask_primers_extract", label="MaskPrimers: extract", requires=[], group="bulk",
        params_schema={
            "input_artifact":{"type":"text","label":"Input artifact","placeholder":"artifact key or filename (optional)"},
            "input_channel":{"type":"select","label":"Input channel","options":["R1","R2"],"default":"R1"},
            "outname":{"type":"text","label":"Outname","placeholder":"leave blank to use input name"},
            "start":{"type":"int","default":0,"min":0},
            "length":{"type":"int","label":"Length","min":1},
            "mode":{"type":"select","options":["cut","mask","trim","tag"],"default":"mask"},
            "primer_field":{"type":"select","options":[{"value":"","label":"choose..."}, "MID", "VPRIMER", "CPRIMER"],"default":""},
            "revpr":{"type":"checkbox","default":False},
            "barcode":{"type":"checkbox","default":False},
            "barcodelen":{"type":"int","min":1,"placeholder":"use full if blank"},
            "barcode_field":{"type":"text","placeholder":"BARCODE"},
            "delim":{"type":"text","placeholder":"e.g. | : , (3 tokens)"},
            "fasta":{"type":"checkbox","default":False},
            "failed":{"type":"checkbox","default":False},
        }
    ),
    "collapse_seq": U_CollapseSeq(
        id="collapse_seq", label="CollapseSeq (deduplicate)", requires=[], group="bulk",
        params_schema={
            "input_artifact":{"type":"text","label":"Input artifact","placeholder":"artifact key or filename (optional)"},
            "input_channel":{"type":"select","label":"Input channel","options":["R1","R2"],"default":"R1"},
            "outname":{"type":"text","label":"Outname","default":"COLLAPSE","placeholder":"leave blank to use input name"},
            "outdir":{"type":"text","label":"Outdir","placeholder":"relative folder (optional)"},
            "out_files":{"type":"text","label":"Out files","placeholder":"explicit filename (disables outdir/outname/failed)"},
            "log":{"type":"text","label":"Log file","placeholder":"optional log filename"},
            "failed":{"type":"checkbox","default":False},
            "fasta":{"type":"checkbox","default":False},
            "gzip_output":{"type":"checkbox","default":False},
            "delim":{"type":"text","placeholder":"e.g. | : , (3 tokens)"},
            "max_missing":{"type":"int","label":"Max missing","min":0},
            "uniq_fields":{"type":"text","label":"Uniq fields","placeholder":"field1 field2"},
            "copy_fields":{"type":"text","label":"Copy fields","placeholder":"field1 field2"},
            "act":{"type":"text","placeholder":"min,max,sum,set (comma sep)"},
            "inner":{"type":"checkbox","default":False},
            "keepmiss":{"type":"checkbox","default":False},
            "max_field":{"type":"text","label":"Max field","placeholder":"field name"},
            "min_field":{"type":"text","label":"Min field","placeholder":"field name"},
        }
    ),
    "build_consensus": U_BuildConsensus(
        id="build_consensus", label="BuildConsensus", requires=[], group="bulk",
        params_schema={
            "outdir":{"type":"text","label":"Outdir","placeholder":"relative folder (optional)"},
            "outname":{"type":"text","label":"Outname","default":"CONS"},
            "failed":{"type":"checkbox","default":False},
            "fasta":{"type":"checkbox","default":False},
            "delim":{"type":"text","placeholder":"e.g. | : , (3 tokens)"},
            "min_count":{"type":"int","label":"Min count","min":1},
            "barcode_field":{"type":"text","label":"Barcode field","placeholder":"BARCODE"},
            "qmin":{"type":"text","placeholder":"min quality"},
            "freq":{"type":"text","placeholder":"min freq"},
            "maxgap":{"type":"text","placeholder":"0..1"},
            "primer_field":{"type":"text","label":"Primer field","placeholder":"PRIMER"},
            "primer_freq":{"type":"text","placeholder":"e.g. 0.7"},
            "copy_fields":{"type":"text","placeholder":"field1 field2"},
            "act":{"type":"text","placeholder":"min,max,sum,set,majority (comma sep)"},
            "dep":{"type":"checkbox","default":False},
            "maxdiv":{"type":"text","placeholder":"e.g. 0.05"},
            "maxerror":{"type":"text","placeholder":"e.g. 0.05"},
        }
    ),
     "sc_merge_samples": U_MergeSamples(
        id="sc_merge_samples",
        label="Merge samples",
        requires=[],
        group="sc",
        params_schema={
            "files":{"type":"text","placeholder":"sample1.tsv, sample2.tsv (leave empty = all *.tsv in session)"},
            "aux_types":{"type":"text","placeholder":"v_germline_length=i, d_germline_length=i, j_germline_length=i, day=i"},
            "sample_field":{"type":"text","default":"sample_id","help":"Annotate each row with filename stem; empty to skip"}
        },
    ),
    "sc_filter_productive": U_SC_FilterProductive(
        id="sc_filter_productive",
        label="Keep productive sequences",
        requires=[],   # <-- no dependency on SC_TABLE
        group="sc",
        params_schema={
            "files": {"type":"text","placeholder":"file1.tsv file2.tsv (blank = all *.tsv/*.tsv.gz)"},
            "productive_field": {"type":"text","default":"productive","help":"Column with TRUE/T/1"},
            "fallback_from_airr": {"type":"select","options":["true","false"],"default":"true",
                                "help":"If 'productive' missing, use (vj_in_frame & !stop_codon)"},
            "mode": {"type":"select","options":["merge","per_file"],"default":"merge"},
            "sample_field": {"type":"text","default":"sample_id","help":"Add origin column when merging"}
        },
    ),
    "sc_remove_multi_heavy": U_SC_RemoveMultiHeavy(
        id="sc_remove_multi_heavy",
        label="Remove cells with multiple heavy chains",
        requires=[],  # fully independent
        group="sc",
        params_schema={
            "files": {"type":"text","placeholder":"file1.tsv file2.tsv (blank = all *.tsv/*.tsv.gz)"},
            "locus_field": {"type":"text","default":"locus","help":"Column with chain locus (IGH/IGK/IGL/TRA/TRB)"},
            "heavy_value": {
                "type":"select",
                "options":[
                    {"value":"","label":"choose..."},
                    "IGH",
                    "TRA",
                    "TRB",
                    {"value":"TRA, TRB","label":"TRA + TRB"}
                ],
                "default":"",
                "help":"Select the locus to treat as heavy (use 'TRA + TRB' to catch both)."
            },
            "cell_field": {"type":"text","default":"cell_id","help":"Cell identifier column (required)"},
            "fallback_from_vcall": {"type":"select","options":["true","false"],"default":"true",
                                    "help":"If locus missing, detect heavy via v_call prefixes (e.g., '^IGH' or '^TRA')"},
            "mode": {"type":"select","options":["merge","per_file"],"default":"merge"},
            "sample_field": {"type":"text","default":"sample_id","help":"Add origin column when merging"}
        },
    ),
    "sc_remove_no_heavy": U_SC_RemoveNoHeavy(
        id="sc_remove_no_heavy",
        label="Remove cells without heavy chains",
        requires=[],  # independent
        group="sc",
        params_schema={
            "files": {"type":"text","placeholder":"file1.tsv file2.tsv (blank = all *.tsv/*.tsv.gz)"},
            "locus_field": {"type":"text","default":"locus","help":"Column indicating locus (IGH/IGK/IGL)"},
            "heavy_value": {"type":"text","default":"IGH","help":"Value for heavy locus"},
            "light_values": {"type":"text","default":"IGK, IGL","help":"Values for light loci"},
            "cell_field": {"type":"text","default":"cell_id","help":"Cell identifier column"},
            "fallback_from_vcall": {"type":"select","options":["true","false"],"default":"true",
                                    "help":"If locus missing, infer heavy/light from v_call"},
            "mode": {"type":"select","options":["merge","per_file"],"default":"merge"},
            "sample_field": {"type":"text","default":"sample_id","help":"Add origin column when merging"}
        },
    ),
}

# --------- API ----------
class RunBody(BaseModel):
    unit_id: str
    params: Dict[str, Any] = {}


class AuthBody(BaseModel):
    username: str
    password: str


@app.post("/auth/register")
def register(body: AuthBody = Body(...)):
    try:
        record = auth_utils.create_user(BASE, body.username, body.password)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    token = auth_utils.create_token(BASE, record["user_id"], record["username"])
    return {"user_id": record["user_id"], "username": record["username"], "token": token}


@app.post("/auth/login")
def login(body: AuthBody = Body(...)):
    record = auth_utils.authenticate_user(BASE, body.username, body.password)
    if not record:
        raise HTTPException(status_code=401, detail="Invalid username or password.")
    token = auth_utils.create_token(BASE, record["user_id"], record["username"])
    return {"user_id": record["user_id"], "username": record["username"], "token": token}


@app.get("/auth/me")
def get_me(user: Dict[str, str] = Depends(_require_user)):
    return {"user_id": user["user_id"], "username": user["username"]}


@app.get("/sessions")
def list_sessions(user: Dict[str, str] = Depends(_require_user)):
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

@app.post("/session/start")
def start_session(user: Dict[str, str] = Depends(_require_user)):
    sid = str(uuid.uuid4())
    sdir = _session_dir_for_user(user["user_id"], sid)
    sdir.mkdir(parents=True, exist_ok=True)
    state = SessionState(
        session_id=sid,
        owner_user_id=user["user_id"],
        owner_username=user["username"],
        created_at=_now_iso(),
        updated_at=_now_iso(),
    )
    save_state(sdir, state)
    return {"session_id": sid}

@app.get("/session/{sid}/units")
def list_units(sid: str, user: Dict[str, str] = Depends(_require_user)):
    _ = _load_session_for_user(user, sid)

    def _group(u):
        try:
            return u.group
        except Exception:
            # Fallback if any instance lacks 'group'
            return "sc" if (getattr(u, "id", "") or "").startswith("sc_") else "bulk"

    return [
        {
            "id": u.id,
            "label": u.label,
            "requires": u.requires,
            "params_schema": u.params_schema,
            "group": _group(u),
        }
        for u in UNITS.values()
    ]
@app.post("/session/{sid}/upload")
async def upload_reads(
    sid: str,
    r1: UploadFile = File(...),
    r2: Optional[UploadFile] = File(None),
    user: Dict[str, str] = Depends(_require_user),
):
    sdir, sess = _load_session_for_user(user, sid)
    a1 = _save_upload_canonical(r1, "R1", sdir)
    sess.artifacts[a1.name] = a1; sess.current["R1"] = a1.name
    if r2:
        a2 = _save_upload_canonical(r2, "R2", sdir)
        sess.artifacts[a2.name] = a2; sess.current["R2"] = a2.name
    save_state(sdir, sess)
    return {"ok": True, "current": sess.current, "artifacts": list(sess.artifacts.keys())}

def _guess_aux_role(name: str) -> str:
    low = name.lower()
    # very simple heuristics; adjust if needed
    if "vprimer" in low or ("v_" in low and ".fa" in low): return "v_primers"
    if "cprimer" in low or "constant" in low: return "c_primers"
    if low.endswith(".fasta") or low.endswith(".fa"): return "other"
    return "other"

@app.post("/session/{sid}/upload-aux")
async def upload_aux_file(
    sid: str,
    file: UploadFile = File(...),
    name: Optional[str] = Form(None),
    user: Dict[str, str] = Depends(_require_user),
):
    sdir, sess = _load_session_for_user(user, sid)
    fname = name or file.filename
    with open(sdir / fname, "wb") as f:
        shutil.copyfileobj(file.file, f)
    role = _guess_aux_role(fname)
    if fname not in sess.aux_files:
        sess.aux_files.append(fname)
    if role in ("v_primers","c_primers"):
        sess.aux[role] = fname
    save_state(sdir, sess)
    return {"stored_as": fname, "role": role}

@app.post("/session/{sid}/run")
def run_unit(
    sid: str,
    body: RunBody = Body(...),
    user: Dict[str, str] = Depends(_require_user),
):
    sdir, sess = _load_session_for_user(user, sid)
    unit = UNITS.get(body.unit_id)
    if not unit:
        raise HTTPException(404, f"Unknown unit_id '{body.unit_id}'")
    # check required channels
    for ch in unit.requires:
        if ch not in sess.current:
            raise HTTPException(400, f"Unit '{unit.id}' requires channel {ch} to be available.")
    step_idx = len(sess.steps)
    try:
        step = unit.run(sess, sdir, body.params)
        sess.steps.append(step)
        save_state(sdir, sess)
        return {"step": step.model_dump(), "current": sess.current, "artifacts": {k:v.model_dump() for k,v in sess.artifacts.items()}}
    except Exception as e:
        prefix = f"{step_idx:03d}_"
        logs = sorted([p for p in sdir.iterdir() if p.name.startswith(prefix) and p.suffix == ".log"])
        tail = ""
        for p in logs:
            try: tail += p.read_text(errors="ignore") + "\n\n"
            except: pass
        if len(tail) > 5000: tail = tail[-5000:]
        section = _last_log_section(logs[-1]) if logs else ""
        err_text = _format_error_with_log(str(e), section)
        raise HTTPException(status_code=500, detail={"error": err_text, "log_tail": tail})

@app.get("/session/{sid}/state")
def get_state(sid: str, user: Dict[str, str] = Depends(_require_user)):
    _, s = _load_session_for_user(user, sid)
    return s.model_dump()

@app.get("/session/{sid}/download/{artifact_name}")
def download_artifact(
    sid: str,
    artifact_name: str,
    user: Dict[str, str] = Depends(_require_user),
):
    sdir, s = _load_session_for_user(user, sid)
    a = s.artifacts.get(artifact_name)
    if not a: raise HTTPException(404, "Artifact not found")
    path = sdir / a.path
    if not path.exists(): raise HTTPException(404, "File missing on disk")
    return FileResponse(path, filename=path.name)

@app.get("/session/{sid}/log/{step_index}", response_class=PlainTextResponse)
def get_log(
    sid: str,
    step_index: int,
    user: Dict[str, str] = Depends(_require_user),
):
    sdir, _ = _load_session_for_user(user, sid)
    prefix = f"{int(step_index):03d}_"
    logs = sorted([p for p in sdir.iterdir() if p.name.startswith(prefix) and p.suffix == ".log"])
    if not logs: raise HTTPException(404, "Log not found")
    return "\n\n".join(p.read_text(errors="ignore") for p in logs)
