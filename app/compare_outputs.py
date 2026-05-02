# run_subset.py
from pathlib import Path
from subset_fastq import load_counter

small = Path("data/session_files/c32b862f-8a0e-4d10-9326-ac1ba487f5b6/pairseq_005/R1_CONS_consensus-pass_pair-pass.fastq")
large = Path("data/session_files/3d5e8559-c691-43ca-b25a-ecf3b3eec7b3/pairseq_006/R1_CONS_consensus-pass_pair-pass.fastq")

small_c = load_counter(small, "record")
large_c = load_counter(large, "record")

missing = small_c - large_c
print("subset:", sum(missing.values()) == 0)
