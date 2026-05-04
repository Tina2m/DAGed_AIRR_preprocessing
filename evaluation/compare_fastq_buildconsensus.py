# compare_fastq_buildconsensus.py
from collections import Counter
from pathlib import Path

# ---- Hard-coded input paths ----
SCRIPT_DIR = Path(__file__).resolve().parent

# R1_FILE1 = SCRIPT_DIR / r"presto_pipeline\MS12_R1_consensus-pass.fastq"
R1_FILE1 = SCRIPT_DIR / r"presto_pipeline\MS12_R1_consensus-pass.fastq"
R1_FILE2 = SCRIPT_DIR / r"airr_session2\ex3_full\pairseq_003\R1_CONS_consensus-pass.fastq"
R1_LOG_FILE = SCRIPT_DIR / "compare_fastq_buildconsensus_R1_differences.log"

R2_FILE1 = SCRIPT_DIR / r"presto_pipeline\MS12_R2_consensus-pass.fastq"
R2_FILE2 = SCRIPT_DIR / r"airr_session2\ex3_full\pairseq_003\R2_CONS_consensus-pass.fastq"
R2_LOG_FILE = SCRIPT_DIR / "compare_fastq_buildconsensus_R2_differences.log"
# --------------------------------

def iter_fastq_records(path):
    """
    Yield (read_id, seq, qual) for each FASTQ record.
    read_id is the full header line without the leading '@' and without trailing whitespace.
    """
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

            read_id = header[1:].strip()
            yield (read_id, seq.strip(), qual.strip())

def count_records(path):
    counts = Counter()
    for rec in iter_fastq_records(path):
        counts[rec] += 1
    return counts

def compare_fastq(file1, file2, log_file, label):
    counts1 = count_records(file1)
    counts2 = count_records(file2)

    only_in_1 = counts1 - counts2
    only_in_2 = counts2 - counts1

    with open(log_file, "w", encoding="utf-8") as log:
        def write(msg=""):
            print(msg)
            log.write(msg + "\n")

        write(f"=== {label} ===")

        if not only_in_1 and not only_in_2:
            write("Files match: all reads are identical in both files.")
            return

        if only_in_1:
            write(f"Reads present in {file1} but not in {file2}:")
            for (read_id, seq, qual), n in only_in_1.items():
                write(f"- {read_id}  (count {n})")
                write(f"  SEQ: {seq}")
                write(f"  QUAL: {qual}")

        if only_in_2:
            write(f"Reads present in {file2} but not in {file1}:")
            for (read_id, seq, qual), n in only_in_2.items():
                write(f"- {read_id}  (count {n})")
                write(f"  SEQ: {seq}")
                write(f"  QUAL: {qual}")

def main():
    compare_fastq(R1_FILE1, R1_FILE2, R1_LOG_FILE, "BuildConsensus R1 comparison")
    compare_fastq(R2_FILE1, R2_FILE2, R2_LOG_FILE, "BuildConsensus R2 comparison")

if __name__ == "__main__":
    main()
