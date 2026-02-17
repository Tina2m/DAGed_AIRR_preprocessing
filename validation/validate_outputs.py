#!/usr/bin/env python3
"""
Validate pipeline outputs against pRESTO ground truth.

Compares "actual" outputs (from your app or a test run) to "expected" ground-truth
files. Uses normalized comparison so that ordering differences (e.g. from --nproc)
do not cause false failures.

Usage:
  python validation/validate_outputs.py expected_dir actual_dir
  python validation/validate_outputs.py expected_dir actual_dir --strict

  expected_dir: directory of ground-truth files (e.g. validation/ground_truth/step_002)
  actual_dir:   directory of outputs to validate (e.g. session dir or validation/actual)
  --strict:     require byte-identical normalized files (default: compare normalized content)
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import sys
from pathlib import Path


def _open_maybe_gz(path: Path, mode: str = "rt", **kwargs):
    if path.suffix == ".gz":
        return gzip.open(path, mode, **kwargs)
    return open(path, mode, **kwargs)


def _iter_fastq_blocks(path: Path):
    """Yield (id_line, block) for each FASTQ record. Block is 4 lines (id, seq, +, qual)."""
    with _open_maybe_gz(path, "rt", errors="replace") as f:
        lines = []
        for line in f:
            lines.append(line)
            if len(lines) == 4:
                yield (lines[0], "".join(lines))
                lines = []
    if lines:
        yield (lines[0], "".join(lines))


def _iter_fasta_blocks(path: Path):
    """Yield (id_line, block) for each FASTA record."""
    with _open_maybe_gz(path, "rt", errors="replace") as f:
        current_id = None
        current_lines = []
        for line in f:
            if line.startswith(">"):
                if current_id is not None:
                    yield (current_id, "".join(current_lines))
                current_id = line
                current_lines = [line]
            else:
                current_lines.append(line)
        if current_id is not None:
            yield (current_id, "".join(current_lines))


def _iter_tsv_rows(path: Path):
    """Yield header line and then data rows (as lines)."""
    with _open_maybe_gz(path, "rt", errors="replace") as f:
        header = f.readline()
        yield header
        for line in f:
            yield line


def normalize_fastq(path: Path) -> str:
    """Produce a canonical string for FASTQ: sorted by read ID, then concatenated."""
    blocks = list(_iter_fastq_blocks(path))
    blocks.sort(key=lambda x: x[0])
    return "".join(b[1] for b in blocks)


def normalize_fasta(path: Path) -> str:
    """Produce a canonical string for FASTA: sorted by sequence ID."""
    blocks = list(_iter_fasta_blocks(path))
    blocks.sort(key=lambda x: x[0])
    return "".join(b[1] for b in blocks)


def normalize_tsv(path: Path, sort_key_column: int = 0) -> str:
    """Produce a canonical string for TSV: header + rows sorted by key column."""
    lines = list(_iter_tsv_rows(path))
    if not lines:
        return ""
    header = lines[0]
    data = lines[1:]
    if not data:
        return header
    try:
        data.sort(key=lambda row: row.split("\t")[sort_key_column] if "\t" in row else row)
    except IndexError:
        data.sort()
    return header + "".join(data)


def normalize_file(path: Path) -> str | None:
    """Choose normalizer by extension. Returns None if unsupported."""
    suf = path.suffix.lower()
    name = path.name.lower()
    if ".gz" in name:
        suf = Path(name.replace(".gz", "")).suffix.lower()
    if suf in (".fastq", ".fq"):
        return normalize_fastq(path)
    if suf in (".fasta", ".fa", ".fna"):
        return normalize_fasta(path)
    if suf in (".tsv", ".tab") or "tsv" in name or "tab" in name:
        return normalize_tsv(path)
    return None


def compare_directories(
    expected_dir: Path,
    actual_dir: Path,
    strict: bool = False,
) -> tuple[list[str], list[str]]:
    """
    Compare contents of expected_dir and actual_dir.
    For each file in expected_dir, look for same name in actual_dir and compare
    normalized content (or hash if strict).
    Returns (list of success messages, list of error messages).
    """
    successes: list[str] = []
    errors: list[str] = []

    expected_files = sorted(expected_dir.iterdir()) if expected_dir.is_dir() else []
    if not expected_files:
        errors.append(f"No files found in expected dir: {expected_dir}")
        return successes, errors

    for exp_path in expected_files:
        if not exp_path.is_file():
            continue
        name = exp_path.name
        act_path = actual_dir / name
        if not act_path.exists():
            errors.append(f"Missing actual file: {name}")
            continue

        norm = normalize_file(exp_path)
        if norm is None:
            # Fallback: byte compare (e.g. logs)
            exp_bytes = exp_path.read_bytes() if exp_path.suffix != ".gz" else gzip.decompress(exp_path.read_bytes())
            act_bytes = act_path.read_bytes() if act_path.suffix != ".gz" else gzip.decompress(act_path.read_bytes())
            if exp_bytes != act_bytes:
                errors.append(f"Content mismatch (raw): {name}")
            else:
                successes.append(f"OK (raw): {name}")
            continue

        norm_actual = normalize_file(act_path)
        if norm_actual is None:
            errors.append(f"Cannot normalize actual file (type unsupported): {name}")
            continue

        exp_hash = hashlib.sha256(norm.encode()).hexdigest()
        act_hash = hashlib.sha256(norm_actual.encode()).hexdigest()
        if exp_hash != act_hash:
            errors.append(f"Normalized content mismatch: {name}")
        else:
            successes.append(f"OK: {name}" if not strict else f"OK (strict): {name}")

    return successes, errors


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate pipeline outputs against ground truth (normalized comparison)."
    )
    parser.add_argument("expected_dir", type=Path, help="Directory of ground-truth files")
    parser.add_argument("actual_dir", type=Path, help="Directory of actual output files")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Require byte-identical normalized content (default: same set of lines)",
    )
    args = parser.parse_args()

    if not args.expected_dir.is_dir():
        print(f"Expected dir not found: {args.expected_dir}", file=sys.stderr)
        return 1
    if not args.actual_dir.is_dir():
        print(f"Actual dir not found: {args.actual_dir}", file=sys.stderr)
        return 1

    successes, errors = compare_directories(args.expected_dir, args.actual_dir, strict=args.strict)
    for s in successes:
        print(s)
    for e in errors:
        print(f"ERROR: {e}", file=sys.stderr)
    return 0 if not errors else 1


if __name__ == "__main__":
    sys.exit(main())
