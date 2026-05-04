#!/usr/bin/env python3
import shutil
import subprocess
import sys
from pathlib import Path
import gzip
import glob

SRR = "SRR1383456"
R1_STEM = f"{SRR}_1"
R2_STEM = f"{SRR}_2"

def run(cmd):
    print("+", " ".join(cmd))
    subprocess.run(cmd, check=True)

def ensure_tool(name):
    if shutil.which(name) is None:
        print(f"ERROR: Required tool not found on PATH: {name}", file=sys.stderr)
        sys.exit(1)

def resolve_fastq(stem):
    fastq = Path(f"{stem}.fastq")
    if fastq.exists():
        return fastq
    gz = Path(f"{stem}.fastq.gz")
    if gz.exists():
        print(f"Decompressing {gz} -> {fastq}")
        with gzip.open(gz, "rb") as fin, open(fastq, "wb") as fout:
            shutil.copyfileobj(fin, fout)
        return fastq
    print(f"ERROR: Missing input file {stem}.fastq or {stem}.fastq.gz", file=sys.stderr)
    sys.exit(1)

def main():
    # Check tools
    for tool in [
        "FilterSeq.py", "MaskPrimers.py", "PairSeq.py", "BuildConsensus.py",
        "AssemblePairs.py", "ParseHeaders.py", "CollapseSeq.py", "SplitSeq.py",
        "ParseLog.py"
    ]:
        ensure_tool(tool)

    # Resolve local FASTQ inputs (.fastq or .fastq.gz). If gz, decompress once.
    r1_path = resolve_fastq(R1_STEM)
    r2_path = resolve_fastq(R2_STEM)

    # pRESTO pipeline
    run(["FilterSeq.py", "quality", "-s", str(r1_path), "-q", "20", "--outname", "MS12_R1", "--log", "FS1.log"])
    run(["FilterSeq.py", "quality", "-s", str(r2_path), "-q", "20", "--outname", "MS12_R2", "--log", "FS2.log"])

    run([
        "MaskPrimers.py", "score", "-s", "MS12_R1_quality-pass.fastq",
        "-p", "Stern2014_CPrimers.fasta", "--start", "15", "--mode", "cut",
        "--barcode", "--outname", "MS12_R1", "--log", "MP1.log"
    ])
    run([
        "MaskPrimers.py", "score", "-s", "MS12_R2_quality-pass.fastq",
        "-p", "Stern2014_VPrimers.fasta", "--start", "0", "--mode", "mask",
        "--outname", "MS12_R2", "--log", "MP2.log"
    ])

    run([
        "PairSeq.py", "-1", "MS12_R1_primers-pass.fastq",
        "-2", "MS12_R2_primers-pass.fastq", "--1f", "BARCODE", "--coord", "sra"
    ])

    run([
        "BuildConsensus.py", "-s", "MS12_R1_primers-pass_pair-pass.fastq",
        "--bf", "BARCODE", "--pf", "PRIMER", "--prcons", "0.6",
        "--maxerror", "0.1", "--maxgap", "0.5", "--outname", "MS12_R1", "--log", "BC1.log"
    ])
    run([
        "BuildConsensus.py", "-s", "MS12_R2_primers-pass_pair-pass.fastq",
        "--bf", "BARCODE", "--pf", "PRIMER", "--maxerror", "0.1",
        "--maxgap", "0.5", "--outname", "MS12_R2", "--log", "BC2.log"
    ])

    run([
        "PairSeq.py", "-1", "MS12_R1_consensus-pass.fastq",
        "-2", "MS12_R2_consensus-pass.fastq", "--coord", "presto"
    ])

    run([
        "AssemblePairs.py", "align",
        "-1", "MS12_R2_consensus-pass_pair-pass.fastq",
        "-2", "MS12_R1_consensus-pass_pair-pass.fastq",
        "--coord", "presto", "--rc", "tail",
        "--1f", "CONSCOUNT", "--2f", "CONSCOUNT", "PRCONS",
        "--outname", "MS12", "--log", "AP.log"
    ])

    run(["ParseHeaders.py", "collapse", "-s", "MS12_assemble-pass.fastq", "-f", "CONSCOUNT", "--act", "min"])
    reheader_files = glob.glob("MS12*reheader.fastq")
    if not reheader_files:
        print("ERROR: No files matched MS12*reheader.fastq", file=sys.stderr)
        sys.exit(1)
    run(["CollapseSeq.py", "-s", *reheader_files, "-n", "20", "--inner", "--uf", "PRCONS",
         "--cf", "CONSCOUNT", "--act", "sum", "--outname", "MS12"])
    run(["SplitSeq.py", "group", "-s", "MS12_collapse-unique.fastq", "-f", "CONSCOUNT", "--num", "2", "--outname", "MS12"])
    run(["ParseHeaders.py", "table", "-s", "MS12_atleast-2.fastq", "-f", "ID", "PRCONS", "CONSCOUNT", "DUPCOUNT"])

    run(["ParseLog.py", "-l", "FS1.log", "FS2.log", "-f", "ID", "QUALITY"])
    run(["ParseLog.py", "-l", "MP1.log", "MP2.log", "-f", "ID", "PRIMER", "BARCODE", "ERROR"])
    run([
        "ParseLog.py", "-l", "BC1.log", "BC2.log", "-f", "BARCODE", "SEQCOUNT", "CONSCOUNT",
        "PRIMER", "PRCONS", "PRCOUNT", "PRFREQ", "ERROR"
    ])
    run(["ParseLog.py", "-l", "AP.log", "-f", "ID", "LENGTH", "OVERLAP", "ERROR", "PVALUE", "FIELDS1", "FIELDS2"])

if __name__ == "__main__":
    main()
