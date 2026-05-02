"""Configuration settings for the application."""
import os
import pathlib
import shutil

# pRESTO tools required on PATH
REQUIRED_TOOLS = [
    "FilterSeq.py",
    "MaskPrimers.py",
    "CollapseSeq.py",
    "BuildConsensus.py",
    "PairSeq.py",
    "AssemblePairs.py",
]

# Base directory for legacy file-based data (only used if DB not configured)
BASE_DIR = pathlib.Path(os.environ.get("DATA_DIR", "/data"))
BASE_DIR.mkdir(parents=True, exist_ok=True)

# Session files (uploads, logs, pipeline outputs) live under this directory
# Path for a session: SESSION_FILES_BASE / <session_id>
SESSION_FILES_BASE = pathlib.Path(
    os.environ.get("SESSION_FILES_DIR", str(BASE_DIR / "session_files"))
)
SESSION_FILES_BASE.mkdir(parents=True, exist_ok=True)

# Tools that support --nproc flag
NPROC_TOOLS = {
    "FilterSeq.py",
    "MaskPrimers.py",
    "CollapseSeq.py",
    "BuildConsensus.py",
    "PairSeq.py",
    "AssemblePairs.py",
}


def validate_presto_tools() -> None:
    """Validate that all required pRESTO tools are available on PATH."""
    missing = [tool for tool in REQUIRED_TOOLS if not shutil.which(tool)]
    if missing:
        raise RuntimeError(f"pRESTO tools not found on PATH: {', '.join(missing)}")

