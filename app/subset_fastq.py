#!/usr/bin/env python3
import argparse
import gzip
from collections import Counter
from pathlib import Path

def open_maybe_gz(path: Path):
    return gzip.open(path, "rt", errors="replace") if path.suffix == ".gz" else open(path, "rt", errors="replace")

def iter_fastq(path: Path):
    with open_maybe_gz(path) as f:
        while True:
            idl = f.readline()
            if not idl:
                break
            seq = f.readline()
            plus = f.readline()
            qual = f.readline()
            if not qual:
                break
            yield idl.rstrip(), seq.rstrip(), qual.rstrip()

def load_counter(path: Path, mode: str):
    c = Counter()
    for rid, seq, qual in iter_fastq(path):
        if mode == "record":
            key = (rid, seq, qual)
        elif mode == "id":
            key = rid
        else:
            key = seq
        c[key] += 1
    return c

def main():
    ap = argparse.ArgumentParser(description="Check if one FASTQ is a strict subset of another.")
    ap.add_argument("small", type=Path, help="FASTQ with fewer reads")
    ap.add_argument("large", type=Path, help="FASTQ with more reads")
    ap.add_argument("--mode", choices=["record", "id", "seq"], default="record",
                    help="Compare by full record, read ID only, or sequence only")
    ap.add_argument("--show-missing", type=int, default=5, help="Show up to N missing examples")
    args = ap.parse_args()

    small_c = load_counter(args.small, args.mode)
    large_c = load_counter(args.large, args.mode)

    missing = small_c - large_c
    missing_count = sum(missing.values())

    is_subset = (missing_count == 0)
    is_strict = is_subset and (sum(small_c.values()) < sum(large_c.values()))

    print(f"subset: {is_subset}")
    print(f"strict_subset: {is_strict}")
    print(f"missing_count: {missing_count}")

    if missing_count and args.show_missing:
        print("missing_examples:")
        for i, k in enumerate(missing):
            print(k)
            if i + 1 >= args.show_missing:
                break

if __name__ == "__main__":
    main()
