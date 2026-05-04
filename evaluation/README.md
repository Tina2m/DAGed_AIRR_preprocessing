# Evaluation of ImmunoStream Output Against a Reference pRESTO Pipeline

## Objective

This evaluation assessed whether the ImmunoStream bulk amplicon workflow reproduces the output of a manually executed pRESTO pipeline when both workflows are run with the same input data and equivalent parameters. The primary comparison focused on the quality-filtered R1 output produced by ImmunoStream:

`airr_session/ex3_full/R1_q20_quality-pass.fastq`

and the corresponding pRESTO output:

`presto_pipeline/MS12_R1_quality-pass.fastq`

Equivalent comparisons were also performed for R2 and for downstream intermediate files generated after primer masking, mate pairing, and consensus construction.

## Reference Pipeline

The reference pRESTO workflow was implemented in `presto_pipeline/presto_pipeline.py`. The pipeline used the paired-end FASTQ files from sample `SRR1383456` and applied the following processing steps:

1. `FilterSeq.py quality` with a Phred quality threshold of 20 for R1 and R2.
2. `MaskPrimers.py score` using `Stern2014_CPrimers.fasta` for R1 and `Stern2014_VPrimers.fasta` for R2.
3. `PairSeq.py` to retain successfully matched R1/R2 reads after primer masking.
4. `BuildConsensus.py` to generate barcode-based consensus sequences.
5. A second `PairSeq.py` step to retain paired consensus sequences.
6. Additional pRESTO post-processing steps, including assembly, header parsing, collapse, split, and tabular log extraction.

The ImmunoStream panel executed the same conceptual workflow through its graphical interface. The screenshots show the configured step order: quality filtering, primer scoring for both reads, mate pairing, consensus building for both reads, and final consensus pairing.

## Comparison Method

The comparison scripts use a record-level FASTQ comparison. Each script reads the FASTQ files in four-line records and represents each record as:

`(read header, nucleotide sequence, quality string)`

The records from each file are counted using Python `Counter` objects. This means that the comparison treats each FASTQ file as a multiset of complete records. The method detects:

- reads present only in the pRESTO output,
- reads present only in the ImmunoStream output,
- differences in read headers,
- differences in nucleotide sequences,
- differences in quality strings,
- duplicate-count differences.

This approach is stronger than comparing only read counts because two files are considered equivalent only if every complete FASTQ record is identical. It is also tolerant of record-order differences, because the scripts compare counted records rather than relying on line-by-line order.

For the primary FilterSeq comparison, `compare_fastq_filterseq.py` compared:

- R1: `presto_pipeline/MS12_R1_quality-pass.fastq` vs. `airr_session/ex3_full/R1_q20_quality-pass.fastq`
- R2: `presto_pipeline/MS12_R2_quality-pass.fastq` vs. `airr_session/ex3_full/R2_q20_quality-pass.fastq`

The same comparison strategy was used for the primer-masked outputs, the first paired-read outputs, the consensus outputs, and the final paired-consensus outputs.

## ImmunoStream Run Statistics

The ImmunoStream interface and logs report 63,073 input reads for each read channel. After quality filtering at Q20, 37,619 R1 reads passed and 6,445 R2 reads passed. This corresponds to 60% pass rate for R1 and 10% pass rate for R2. The lower R2 pass rate is also visible in the quality distribution plot, where the post-filtered R2 reads are concentrated in the high-quality range while many low-quality reads are removed.

After primer scoring, ImmunoStream retained 33,551 R1 reads and 6,184 R2 reads. The first mate-pairing step retained 5,229 paired reads. Consensus construction generated 1,274 R1 consensus records and 1,274 R2 consensus records, and the final consensus-pairing step retained all 1,274 consensus pairs.

## Results

The primary quality-filtering comparison showed exact record-level agreement between ImmunoStream and the reference pRESTO pipeline. The generated logs state:

`Files match: all reads are identical in both files.`

This result was obtained for both R1 and R2 quality-filtered outputs. The record counts were also identical:

| Step | pRESTO output | ImmunoStream output | Result |
|---|---:|---:|---|
| FilterSeq R1 | 37,619 | 37,619 | Identical records |
| FilterSeq R2 | 6,445 | 6,445 | Identical records |
| MaskPrimers R1 | 33,551 | 33,551 | Identical records |
| MaskPrimers R2 | 6,184 | 6,184 | Identical records |
| First PairSeq R1 | 5,229 | 5,229 | Identical records |
| First PairSeq R2 | 5,229 | 5,229 | Identical records |
| BuildConsensus R1 | 1,272 | 1,272 | Identical records |
| BuildConsensus R2 | 1,272 | 1,272 | Identical records |
| Final PairSeq R1 consensus | 1,270 | 1,270 | Identical records |
| Final PairSeq R2 consensus | 1,270 | 1,270 | Identical records |

In `compare_fastq_buildconsensus.py`, the consensus-generation comparison also showed complete agreement. The script compared `presto_pipeline/MS12_R1_consensus-pass.fastq` with `airr_session2/ex3_full/pairseq_003/R1_CONS_consensus-pass.fastq`, and `presto_pipeline/MS12_R2_consensus-pass.fastq` with `airr_session2/ex3_full/pairseq_003/R2_CONS_consensus-pass.fastq`. Both generated logs reported:

`Files match: all reads are identical in both files.`

The final paired-consensus comparison also showed identical outputs for both read directions, with 1,270 paired consensus records in both the pRESTO and ImmunoStream outputs.

## Interpretation

The evaluation demonstrates that ImmunoStream reproduces the reference pRESTO output exactly across the tested stages: quality filtering, primer masking, initial read pairing, consensus generation, and final consensus pairing. This is the most important validation point for the explicitly requested file pair, because `R1_q20_quality-pass.fastq` and `MS12_R1_quality-pass.fastq` were identical at the complete FASTQ-record level.

Overall, these results support the conclusion that the ImmunoStream panel correctly implements the pRESTO-compatible workflow and produces equivalent FASTQ outputs when run with the same inputs and parameters. No record-level differences were detected in the corrected comparison results.
