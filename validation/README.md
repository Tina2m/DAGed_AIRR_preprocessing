# Pipeline output validation

This directory supports **validating that each pipeline step (or full pipeline) produces output matching pRESTO “ground truth”** on the same inputs.

## Idea

1. **Ground truth**: Run pRESTO tools (manually or via a script) on a **fixed test dataset** and save the outputs into `ground_truth/` (e.g. one subdir per step or one for the full pipeline).
2. **Actual**: Run the same steps through your app (or the same commands in a test run) on the **same inputs** and capture outputs to a directory (e.g. `actual/` or a session dir).
3. **Compare**: Use `validate_outputs.py` to compare **normalized** outputs so that ordering differences (e.g. from `--nproc`) do not cause false failures.

## Quick start

```bash
# From repo root
python validation/validate_outputs.py validation/ground_truth/step_002 validation/actual/step_002
# Exit code 0 = all files match; 1 = one or more mismatches or missing files
```

## Generating ground truth

Use a **small, fixed input** (e.g. a few thousand reads) so that:

- Runs are fast and deterministic enough when single-threaded.
- You can commit ground-truth outputs or generate them once in CI.

### Option A: Run pRESTO manually

From a directory containing your test input (e.g. `R1.fastq`) and primers:

```bash
# Example: one step
FilterSeq.py length -s R1.fastq -n 300 --outname R1_len300 --log 000_FilterSeq_length.log
# Copy the *-pass output to validation/ground_truth/step_000/
```

Repeat for each step, using the **exact** command lines your app would run (see app logs `[CMD]` or `main.py`). Use **no `--nproc`** (or `--nproc 1`) for reproducible order if you want byte-identical comparison; otherwise the normalizer will sort by read ID and compare.

### Option B: Run via the app and copy

1. Create a session and upload the **same test inputs** (and aux files).
2. Run the pipeline (single step or full).
3. Copy the session output directory (or the files you care about) to `validation/ground_truth/full` (or per-step dirs).
4. Treat that as ground truth for future runs.

### Option C: Script that replays app commands

You can add a small script that:

- Reads a “recipe” (list of commands or step params) and runs them in order on a fixed input dir.
- Writes outputs into `ground_truth/`.  
Then run the same recipe again (e.g. from the app or from the script) and compare with `validate_outputs.py`.

## Directory layout (suggested)

```
validation/
  README.md
  validate_outputs.py
  ground_truth/
    step_000_filter_length/   # e.g. R1_len300_length-pass.fastq
    step_001_filter_quality/  # e.g. R1_q20_quality-pass.fastq
    step_002_mask_align/     # e.g. R1_*_primers-pass.fastq
    full/                    # or one dir with all outputs of full pipeline
  actual/                    # optional; or use a session path when validating
```

## How comparison works

- **FASTQ/FASTA**: Records are sorted by ID, then concatenated. Comparison is either “same set of lines” (default) or byte-identical normalized content (`--strict`). This makes multi-threaded pRESTO output (different order) still match.
- **TSV**: Header + rows sorted by first column, then compared.
- **Other files**: Raw byte comparison (e.g. logs); use only if you need exact match.

## CI

Run validation in CI after any change to pipeline logic or pRESTO usage:

```bash
# Generate actuals (e.g. run app pipeline or script on test data)
# then:
python validation/validate_outputs.py validation/ground_truth/full /path/to/actual/outputs
```

Exit code 1 will fail the job.
