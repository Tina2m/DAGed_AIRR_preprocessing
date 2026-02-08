"""File handling utilities."""
import gzip
import pathlib
import shutil
from typing import Optional
from fastapi import UploadFile, HTTPException

from app.models import SessionState, Artifact


def ensure_uncompressed_path(
    path: pathlib.Path,
    dest: pathlib.Path
) -> pathlib.Path:
    """
    Decompress a gzipped file to destination if needed.
    
    Args:
        path: Source file path (may be .gz)
        dest: Destination path for uncompressed file
        
    Returns:
        Destination path if decompressed, original path otherwise
    """
    if str(path).lower().endswith(".gz"):
        dest.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(path, "rb") as src, open(dest, "wb") as dst:
            shutil.copyfileobj(src, dst)
        return dest
    return path


def ensure_uncompressed_artifact(
    sess: SessionState,
    sess_dir: pathlib.Path,
    channel: str
) -> pathlib.Path:
    """
    Get uncompressed path for current artifact of a channel.
    
    Args:
        sess: Session state
        sess_dir: Session directory
        channel: Channel name (e.g., "R1", "R2")
        
    Returns:
        Path to uncompressed artifact
        
    Raises:
        HTTPException: If channel is not available
    """
    key = sess.current.get(channel)
    if not key:
        raise HTTPException(400, f"Channel '{channel}' is not available.")
    
    artifact = sess.artifacts[key]
    artifact_path = sess_dir / artifact.path
    
    if artifact_path.suffix.lower() == ".gz":
        # Decompress alongside with the same basename (without .gz)
        out_path = artifact_path.with_suffix("")  # drop .gz
        if not out_path.exists():
            with gzip.open(artifact_path, "rb") as src, open(out_path, "wb") as dst:
                shutil.copyfileobj(src, dst)
        return out_path
    
    return artifact_path


def require_fastq(
    sess: SessionState,
    sess_dir: pathlib.Path,
    channel_key: str,
    for_what: str
) -> pathlib.Path:
    """
    Ensure the current artifact is uncompressed FASTQ.
    
    Args:
        sess: Session state
        sess_dir: Session directory
        channel_key: Channel name
        for_what: Description of what requires FASTQ (for error message)
        
    Returns:
        Path to uncompressed FASTQ file
        
    Raises:
        HTTPException: If file is not FASTQ format
    """
    path = ensure_uncompressed_artifact(sess, sess_dir, channel_key)
    # Quick, reliable check by peeking at first non-empty char
    first_char = peek_first_nonempty_char(path, gz=False)
    if first_char != "@":
        raise HTTPException(
            400,
            f"{for_what} requires FASTQ (qualities), but '{path.name}' is not FASTQ. "
            "Upload FASTQ(.gz) or skip this unit."
        )
    return path


# File extension sets
FASTQ_EXTENSIONS = {".fastq", ".fq"}
FASTA_EXTENSIONS = {".fasta", ".fa", ".fna"}


def detect_kind_from_name(name: str) -> Optional[str]:
    """
    Infer file kind from filename (case-insensitive), including *.gz combos.
    
    Args:
        name: Filename
        
    Returns:
        "fastq", "fasta", or None
    """
    name_lower = name.lower()
    if name_lower.endswith((".fastq.gz", ".fq.gz", ".fastq", ".fq")):
        return "fastq"
    if name_lower.endswith((".fasta.gz", ".fa.gz", ".fna.gz", ".fasta", ".fa", ".fna")):
        return "fasta"
    return None


def peek_first_nonempty_char(path: pathlib.Path, gz: bool) -> str:
    """
    Open (gzip/plain) and return first non-empty char ('@' or '>') or ''.
    
    Args:
        path: File path
        gz: Whether file is gzipped
        
    Returns:
        First non-empty character or empty string
    """
    opener = gzip.open if gz else open
    try:
        with opener(path, "rt", errors="ignore") as fh:
            for _ in range(200):
                line = fh.readline()
                if not line:
                    break
                stripped = line.strip()
                if stripped:
                    return stripped[0]
    except Exception:
        pass
    return ""


def make_canonical_name(channel: str, kind: str) -> str:
    """
    Create canonical filename for a channel and file kind.
    
    Args:
        channel: Channel name (e.g., "R1", "R2")
        kind: File kind ("fastq" or "fasta")
        
    Returns:
        Canonical filename
    """
    return f"{channel}.fastq" if kind == "fastq" else f"{channel}.fasta"


def save_upload_canonical(
    upload: UploadFile,
    channel: str,
    sess_dir: pathlib.Path
) -> Artifact:
    """
    Save uploaded FASTA/FASTQ (.gz or plain) as an uncompressed canonical file.
    
    Saves as R1.fastq / R1.fasta, R2.fastq / R2.fasta, etc.
    
    Args:
        upload: Uploaded file
        channel: Channel name
        sess_dir: Session directory
        
    Returns:
        Artifact object
        
    Raises:
        HTTPException: If file type is unsupported
    """
    import uuid
    
    tmp_path = sess_dir / f"__upload__{uuid.uuid4().hex}"
    with open(tmp_path, "wb") as f:
        shutil.copyfileobj(upload.file, f)

    # 1) Try filename-based detection (most reliable for gz)
    kind = detect_kind_from_name(upload.filename)

    # 2) If still unknown, peek inside (handle gz/plain correctly)
    if kind is None:
        is_gz = upload.filename.lower().endswith(".gz")
        first_char = peek_first_nonempty_char(tmp_path, gz=is_gz)
        if first_char == ">":
            kind = "fasta"
        elif first_char == "@":
            kind = "fastq"

    if kind not in ("fastq", "fasta"):
        tmp_path.unlink(missing_ok=True)
        raise HTTPException(
            400,
            f"Unsupported upload type for '{upload.filename}'; expected FASTA/FASTQ(.gz)."
        )

    out_name = make_canonical_name(channel, kind)
    out_path = sess_dir / out_name

    # 3) Decompress if needed, always store uncompressed canonical file
    if upload.filename.lower().endswith(".gz"):
        with gzip.open(tmp_path, "rb") as src, open(out_path, "wb") as dst:
            shutil.copyfileobj(src, dst)
        tmp_path.unlink(missing_ok=True)
    else:
        tmp_path.replace(out_path)

    return Artifact(
        name=f"{channel}_raw",
        path=out_name,
        kind=kind,
        channel=channel,
        from_step=-1
    )


def file_existing(sess_dir: pathlib.Path, *candidates: str) -> str:
    """
    Find first existing file from candidates.
    
    Args:
        sess_dir: Session directory
        *candidates: Candidate filenames
        
    Returns:
        First existing filename
        
    Raises:
        HTTPException: If no candidate exists
    """
    for candidate in candidates:
        if (sess_dir / candidate).exists():
            return candidate
    raise HTTPException(500, f"Expected output not found. Tried: {candidates}")


def find_pass_for_prefix(sess_dir: pathlib.Path, prefix: str) -> str:
    """
    Find a pass output file for a given prefix.
    
    Searches for files matching pattern: {prefix}_{tag}.{ext}
    where tag is one of the pass tags and ext is fastq/fasta (with optional .gz).
    
    Args:
        sess_dir: Session directory
        prefix: File prefix
        
    Returns:
        Filename of found file
        
    Raises:
        HTTPException: If no matching file found
    """
    pass_tags = (
        "mask-pass", "align-primers-pass", "primers-pass", "extract-pass",
        "quality-pass", "length-pass", "missing-pass", "repeats-pass",
        "trimqual-pass", "maskqual-pass", "collapse-pass"
    )
    
    for ext in ("fastq.gz", "fastq", "fasta.gz", "fasta"):
        for tag in pass_tags:
            candidate = sess_dir / f"{prefix}_{tag}.{ext}"
            if candidate.exists():
                return candidate.name
    
    raise HTTPException(500, f"Expected output not found for prefix '{prefix}'.")


def assert_channel(sess: SessionState, channel: str) -> None:
    """
    Assert that a required channel is available in session.
    
    Args:
        sess: Session state
        channel: Channel name
        
    Raises:
        HTTPException: If channel is not available
    """
    if channel not in sess.current:
        raise HTTPException(400, f"Required channel '{channel}' is not available.")


def guess_aux_role(name: str) -> str:
    """
    Guess the role of an auxiliary file from its name.
    
    Args:
        name: Filename
        
    Returns:
        Role: "v_primers", "c_primers", or "other"
    """
    name_lower = name.lower()
    if "vprimer" in name_lower or ("v_" in name_lower and ".fa" in name_lower):
        return "v_primers"
    if "cprimer" in name_lower or "constant" in name_lower:
        return "c_primers"
    if name_lower.endswith((".fasta", ".fa")):
        return "other"
    return "other"

