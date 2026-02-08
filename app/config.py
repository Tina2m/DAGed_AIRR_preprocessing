"""Configuration settings for the application."""
import pathlib
import shutil

# pRESTO tools required on PATH
REQUIRED_TOOLS = [
    "FilterSeq.py",
    "MaskPrimers.py",
    "CollapseSeq.py",
    "BuildConsensus.py",
]

# Base directory for session data
BASE_DIR = pathlib.Path("/data")
BASE_DIR.mkdir(parents=True, exist_ok=True)

# Tools that support --nproc flag
NPROC_TOOLS = {
    "FilterSeq.py",
    "MaskPrimers.py",
    "CollapseSeq.py",
    "BuildConsensus.py",
}


def validate_presto_tools() -> None:
    """Validate that all required pRESTO tools are available on PATH."""
    missing = [tool for tool in REQUIRED_TOOLS if not shutil.which(tool)]
    if missing:
        raise RuntimeError(f"pRESTO tools not found on PATH: {', '.join(missing)}")

