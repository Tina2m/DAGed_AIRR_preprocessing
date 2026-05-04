# compare_fastq_subset_pairseq.py
from dataclasses import dataclass
from pathlib import Path
import re

# ---- Hard-coded input paths ----
SCRIPT_DIR = Path(__file__).resolve().parent

R1_SMALL = SCRIPT_DIR / r"airr_session2\ex3_full\pairseq_006\R1_CONS_consensus-pass_pair-pass.fastq"
R1_LARGE = SCRIPT_DIR / r"airr_session\ex3_no_qc\pairseq_005\R1_CONS_consensus-pass_pair-pass.fastq"

R2_SMALL = SCRIPT_DIR / r"airr_session2\ex3_full\pairseq_006\R2_CONS_consensus-pass_pair-pass.fastq"
R2_LARGE = SCRIPT_DIR / r"airr_session\ex3_no_qc\pairseq_005\R2_CONS_consensus-pass_pair-pass.fastq"

R1_MISSING_LOG = SCRIPT_DIR / "subset_pairseq_R1_missing_in_large.log"
R1_MATCH_LOG = SCRIPT_DIR / "subset_pairseq_R1_matches.log"
R2_MISSING_LOG = SCRIPT_DIR / "subset_pairseq_R2_missing_in_large.log"
R2_MATCH_LOG = SCRIPT_DIR / "subset_pairseq_R2_matches.log"
# --------------------------------

BARCODE_RE = re.compile(r"BARCODE=([^|\s]+)")

@dataclass(frozen=True)
class FastqRecord:
    base_id: str
    header: str
    seq: str
    qual: str
    barcode: str

def extract_barcode(header: str) -> str:
    m = BARCODE_RE.search(header)
    return m.group(1) if m else ""

def base_read_id(header: str) -> str:
    # Take the first token, then drop any metadata after a pipe.
    token = header.split()[0]
    return token.split("|")[0]

def iter_fastq_records(path: Path):
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        while True:
            header = f.readline()
            if not header:
                break
            seq = f.readline()
            plus = f.readline()
            qual = f.readline()
            if not qual:
                raise ValueError(f"Incomplete FASTQ record in {path}")
            if not header.startswith("@") or not plus.startswith("+"):
                raise ValueError(f"Malformed FASTQ record in {path}: {header.strip()}")

            header = header[1:].strip()
            seq = seq.strip()
            qual = qual.strip()
            yield FastqRecord(
                base_id=base_read_id(header),
                header=header,
                seq=seq,
                qual=qual,
                barcode=extract_barcode(header),
            )

def load_fastq(path: Path):
    records = []
    by_base = {}
    for rec in iter_fastq_records(path):
        records.append(rec)
        by_base.setdefault(rec.base_id, []).append(rec)
    return records, by_base

def best_match(small: FastqRecord, candidates):
    # Priority: identical > same sequence > same barcode > found (different)
    for rec in candidates:
        if rec.header == small.header and rec.seq == small.seq and rec.qual == small.qual:
            return "IDENTICAL", rec
    for rec in candidates:
        if rec.seq == small.seq:
            return "SAME_SEQUENCE", rec
    if small.barcode:
        for rec in candidates:
            if rec.barcode == small.barcode:
                return "SAME_BARCODE", rec
    return "FOUND_DIFFERENT", candidates[0]

def compare_subset(small_path: Path, large_path: Path, missing_log: Path, match_log: Path, label: str):
    small_records, _ = load_fastq(small_path)
    large_records, large_by_base = load_fastq(large_path)

    missing = []
    matches = []

    for rec in small_records:
        candidates = large_by_base.get(rec.base_id, [])
        if not candidates:
            missing.append(rec)
            continue
        status, match_rec = best_match(rec, candidates)
        matches.append((status, rec, match_rec))

    with open(missing_log, "w", encoding="utf-8") as log:
        log.write(f"=== {label} ===\n")
        log.write(f"Small file: {small_path}\n")
        log.write(f"Large file: {large_path}\n\n")
        if not missing:
            log.write("All reads from the small file were found in the large file.\n")
        else:
            log.write(f"Missing reads (count={len(missing)}):\n")
            for rec in missing:
                log.write(f"@{rec.header}\n{rec.seq}\n+\n{rec.qual}\n")

    with open(match_log, "w", encoding="utf-8") as log:
        log.write(f"=== {label} ===\n")
        log.write(f"Small file: {small_path}\n")
        log.write(f"Large file: {large_path}\n\n")
        log.write("Matches (per read from the small file):\n")
        for status, small, large in matches:
            log.write(f"- {small.base_id}\n")
            log.write(f"  STATUS: {status}\n")
            log.write(f"  SMALL_HEADER: {small.header}\n")
            log.write(f"  LARGE_HEADER: {large.header}\n")
            if small.barcode or large.barcode:
                log.write(f"  SMALL_BARCODE: {small.barcode or '<none>'}\n")
                log.write(f"  LARGE_BARCODE: {large.barcode or '<none>'}\n")
            log.write("\n")

    is_subset = len(missing) == 0
    is_strict = is_subset and (len(small_records) < len(large_records))
    print(f"{label}:")
    print(f"  Small reads: {len(small_records)}")
    print(f"  Missing from large: {len(missing)}")
    print(f"  Subset: {'YES' if is_subset else 'NO'}")
    print(f"  Strict subset: {'YES' if is_strict else 'NO'}")
    print(f"  Missing log: {missing_log}")
    print(f"  Match log: {match_log}")

def main():
    compare_subset(R1_SMALL, R1_LARGE, R1_MISSING_LOG, R1_MATCH_LOG, "R1 PairSeq consensus subset check")
    compare_subset(R2_SMALL, R2_LARGE, R2_MISSING_LOG, R2_MATCH_LOG, "R2 PairSeq consensus subset check")

if __name__ == "__main__":
    main()
