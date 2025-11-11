import json, os, shutil, uuid, pathlib, subprocess, shutil as _shutil
from typing import Optional, Dict, List, Literal, Any

from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Body
from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# Tools that accept --nproc
SUPPORTED_NPROC_TOOLS = {"FilterSeq.py", "MaskPrimers.py", "AssemblePairs.py", "BuildConsensus.py"}

def _auto_nproc() -> int:
    """
    Return the maximum CPU count visible to the container.
    If PRESTO_NPROC is set to a positive integer, use that instead.
    """
    import os
    try:
        env = os.environ.get("PRESTO_NPROC")
        if env:
            n = int(env)
            if n >= 1:
                return n
    except Exception:
        pass
    # Respect cgroup/cpuset limits inside Docker if available
    try:
        import os as _os
        return max(1, len(_os.sched_getaffinity(0)))  # Linux/cgroups aware
    except Exception:
        pass
    n = os.cpu_count() or 1
    return max(1, n)

# ---- Ensure pRESTO CLI tools are available in PATH inside the container ----
_missing = [t for t in [
    "FilterSeq.py","MaskPrimers.py","PairSeq.py",
    "AssemblePairs.py","ParseLog.py","CollapseSeq.py",
    "BuildConsensus.py", "Rscript"
] if not _shutil.which(t)]
if _missing:
    raise RuntimeError(f"pRESTO tools not found on PATH: {', '.join(_missing)}")

PAIRSEQ_COORDS = {"illumina", "solexa", "sra", "454", "presto"}

app = FastAPI(title="pRESTO Click-to-Run Backend")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# Static UI (served from /app/ui)
app.mount("/ui", StaticFiles(directory="ui", html=True), name="ui")

BASE = pathlib.Path("/data")
BASE.mkdir(parents=True, exist_ok=True)

# =================== Models ===================
class Artifact(BaseModel):
    name: str
    path: str
    kind: Literal["fastq","tab","log","other"]
    channel: Optional[Literal["R1","R2","PAIR1","PAIR2","ASSEMBLED"]] = None
    from_step: int
    fields: Dict[str, bool] = {}

class StepResult(BaseModel):
    step_index: int
    unit: str
    params: Dict[str, Any]
    produced: List[Artifact]

class SessionState(BaseModel):
    session_id: str
    steps: List[StepResult] = []
    artifacts: Dict[str, Artifact] = {}
    current: Dict[str, str] = {}
    # New: remember uploaded primer files so we can auto-fill MaskPrimers params
    aux: Dict[str, str] = {}   # keys: 'v_primers', 'c_primers'

class PipelineStep(BaseModel):
    unit_id: str
    params: Dict[str, Any] = {}

class PipelineBody(BaseModel):
    steps: List[PipelineStep]
    start_from: Literal["current", "raw"] = "raw"
    halt_on_error: bool = True

# =================== Helpers ===================
def load_state(sess_dir: pathlib.Path) -> SessionState:
    p = sess_dir / "state.json"
    if p.exists():
        return SessionState.model_validate_json(p.read_text())
    s = SessionState(session_id=sess_dir.name)
    p.write_text(s.model_dump_json(indent=2))
    return s

def save_state(sess_dir: pathlib.Path, s: SessionState):
    (sess_dir / "state.json").write_text(s.model_dump_json(indent=2))

def run_cmd(cmd: List[str], cwd: pathlib.Path, log_file: pathlib.Path):
    # Auto-append --nproc when supported (FilterSeq, MaskPrimers, AssemblePairs, BuildConsensus)
    try:
        tool = pathlib.Path(cmd[0]).name  # e.g., "FilterSeq.py"
        if tool in SUPPORTED_NPROC_TOOLS and "--nproc" not in cmd:
            cmd = list(cmd) + ["--nproc", str(_auto_nproc())]
    except Exception:
        pass

    with open(log_file, "ab") as log:
        proc = subprocess.Popen(cmd, cwd=cwd, stdout=log, stderr=log)
        code = proc.wait()
    if code != 0:
        raise RuntimeError(f"Command failed ({code}): {' '.join(cmd)}")


def find_pass_generic(sess_dir: pathlib.Path, prefix: str, mid: str) -> str:
    patterns = [f"{prefix}*{mid}-pass.{ext}" for ext in ("fastq.gz","fastq","fasta.gz","fasta")]
    for pat in patterns:
        hits = sorted(sess_dir.glob(pat))
        if hits:
            return hits[0].name
    for ext in ("fastq.gz","fastq","fasta.gz","fasta"):
        fails = sorted(sess_dir.glob(f"{prefix}*{mid}-fail.{ext}"))
        if fails:
            raise HTTPException(400, f"No reads passed ({mid}). See log. Fail file: {fails[0].name}")
    raise HTTPException(500, f"Expected output not found for pattern '{prefix}*{mid}-pass.*'")

def find_maskprimers_pass(sess_dir: pathlib.Path, prefix: str) -> str:
    for ext in ("fastq.gz","fastq","fasta.gz","fasta"):
        hits = sorted(sess_dir.glob(f"{prefix}*pass.{ext}"))
        if hits:
            return hits[0].name
    for ext in ("fastq.gz","fastq","fasta.gz","fasta"):
        fails = sorted(sess_dir.glob(f"{prefix}*fail.{ext}"))
        if fails:
            raise HTTPException(400, f"No reads passed MaskPrimers for '{prefix}'. See log. Fail file: {fails[0].name}")
    raise HTTPException(500, f"Expected MaskPrimers output not found for prefix '{prefix}'.")

def find_pair_pass(sess_dir: pathlib.Path, which: int) -> str:
    for ext in ("fastq.gz","fastq"):
        hits = sorted(sess_dir.glob(f"PAIRED-{which}_*pass.{ext}")) or sorted(sess_dir.glob(f"PAIRED-{which}*pass.{ext}"))
        if hits:
            return hits[0].name
    raise HTTPException(500, "PairSeq outputs not found. Check logs.")

def find_assemble_pass(sess_dir: pathlib.Path) -> str:
    for ext in ("fastq.gz","fastq"):
        hits = sorted(sess_dir.glob(f"ASSEMBLED*assemble-pass.{ext}"))
        if hits:
            return hits[0].name
    raise HTTPException(500, "AssemblePairs output not found.")

def find_assemble_any_pass(sess_dir: pathlib.Path) -> str:
    for ext in ("fastq.gz","fastq"):
        for tag in ("sequential-pass", "join-pass", "assemble-pass"):
            hits = sorted(sess_dir.glob(f"ASSEMBLED*{tag}.{ext}"))
            if hits:
                return hits[0].name
    raise HTTPException(500, "AssemblePairs output not found (sequential/join/align). Check step log.")

def find_collapse_pass(sess_dir: pathlib.Path, outname: str) -> str:
    for ext in ("fastq.gz","fastq","fasta.gz","fasta"):
        hits = sorted(sess_dir.glob(f"{outname}*collapse-pass.{ext}"))
        if hits:
            return hits[0].name
    raise HTTPException(500, "CollapseSeq output not found.")

def _next_idx(sess: SessionState) -> int:
    return len(sess.steps)

def _assert_channel(sess: SessionState, ch: str):
    if ch not in sess.current:
        raise HTTPException(400, f"Required channel '{ch}' is not available.")

def _has(v) -> bool:
    return v is not None and str(v).strip() not in ("", "none", "None")

def _reset_to_raw(sess: SessionState):
    if "R1_raw" not in sess.artifacts:
        raise HTTPException(400, "No R1_raw found. Upload reads first.")
    sess.current["R1"] = "R1_raw"
    if "R2_raw" in sess.artifacts:
        sess.current["R2"] = "R2_raw"
    else:
        sess.current.pop("R2", None)
    for k in ("PAIR1","PAIR2","ASSEMBLED"):
        sess.current.pop(k, None)

# =================== Units ===================
class UnitSpec(BaseModel):
    id: str
    label: str
    requires: List[str]
    params_schema: Dict[str, Any]
    def run(self, sess: SessionState, sess_dir: pathlib.Path, params: Dict[str, Any]) -> StepResult: ...

# ---- FilterSeq.* ----
class U_FilterQuality(UnitSpec):
    def run(self, sess, sess_dir, params):
        idx = _next_idx(sess); log = sess_dir / f"{idx:03d}_FilterSeq_quality.log"
        _assert_channel(sess, "R1")
        r1 = sess_dir / sess.artifacts[sess.current["R1"]].path
        q = str(params.get("qmin", 20))
        run_cmd(["FilterSeq.py","quality","-s",str(r1),"-q",q,"--outname",f"R1_q{q}","--log",log.name], sess_dir, log)
        out_r1 = find_pass_generic(sess_dir, f"R1_q{q}", "quality")
        produced = [Artifact(name="R1_quality", path=out_r1, kind="fastq", channel="R1", from_step=idx)]
        if sess.current.get("R2"):
            r2 = sess_dir / sess.artifacts[sess.current["R2"]].path
            run_cmd(["FilterSeq.py","quality","-s",str(r2),"-q",q,"--outname",f"R2_q{q}","--log",log.name], sess_dir, log)
            out_r2 = find_pass_generic(sess_dir, f"R2_q{q}", "quality")
            produced.append(Artifact(name="R2_quality", path=out_r2, kind="fastq", channel="R2", from_step=idx))
            sess.current["R2"] = "R2_quality"
        sess.current["R1"] = "R1_quality"
        for a in produced: sess.artifacts[a.name] = a
        return StepResult(step_index=idx, unit=self.id, params=params, produced=produced)

class U_FilterLength(UnitSpec):
    def run(self, sess, sess_dir, params):
        idx = _next_idx(sess); log = sess_dir / f"{idx:03d}_FilterSeq_length.log"
        _assert_channel(sess, "R1")
        r1 = sess_dir / sess.artifacts[sess.current["R1"]].path
        n = str(params.get("min_len", 100))
        inner = str(params.get("inner","false")).lower() in ("1","true","yes","y")
        cmd = ["FilterSeq.py","length","-s",str(r1),"-n",n,"--outname",f"R1_len{n}","--log",log.name]
        if inner: cmd.append("--inner")
        run_cmd(cmd, sess_dir, log)
        out_r1 = find_pass_generic(sess_dir, f"R1_len{n}", "length")
        produced = [Artifact(name="R1_length", path=out_r1, kind="fastq", channel="R1", from_step=idx)]
        if sess.current.get("R2"):
            r2 = sess_dir / sess.artifacts[sess.current["R2"]].path
            cmd2 = ["FilterSeq.py","length","-s",str(r2),"-n",n,"--outname",f"R2_len{n}","--log",log.name]
            if inner: cmd2.append("--inner")
            run_cmd(cmd2, sess_dir, log)
            out_r2 = find_pass_generic(sess_dir, f"R2_len{n}", "length")
            produced.append(Artifact(name="R2_length", path=out_r2, kind="fastq", channel="R2", from_step=idx))
            sess.current["R2"] = "R2_length"
        sess.current["R1"] = "R1_length"
        for a in produced: sess.artifacts[a.name] = a
        return StepResult(step_index=idx, unit=self.id, params=params, produced=produced)

class U_FilterMissing(UnitSpec):
    def run(self, sess, sess_dir, params):
        idx = _next_idx(sess); log = sess_dir / f"{idx:03d}_FilterSeq_missing.log"
        _assert_channel(sess, "R1")
        r1 = sess_dir / sess.artifacts[sess.current["R1"]].path
        n = str(params.get("max_missing", 10))
        inner = str(params.get("inner","false")).lower() in ("1","true","yes","y")
        cmd = ["FilterSeq.py","missing","-s",str(r1),"-n",n,"--outname",f"R1_m{n}","--log",log.name]
        if inner: cmd.append("--inner")
        run_cmd(cmd, sess_dir, log)
        out_r1 = find_pass_generic(sess_dir, f"R1_m{n}", "missing")
        produced = [Artifact(name="R1_missing", path=out_r1, kind="fastq", channel="R1", from_step=idx)]
        if sess.current.get("R2"):
            r2 = sess_dir / sess.artifacts[sess.current["R2"]].path
            cmd2 = ["FilterSeq.py","missing","-s",str(r2),"-n",n,"--outname",f"R2_m{n}","--log",log.name]
            if inner: cmd2.append("--inner")
            run_cmd(cmd2, sess_dir, log)
            out_r2 = find_pass_generic(sess_dir, f"R2_m{n}", "missing")
            produced.append(Artifact(name="R2_missing", path=out_r2, kind="fastq", channel="R2", from_step=idx))
            sess.current["R2"] = "R2_missing"
        sess.current["R1"] = "R1_missing"
        for a in produced: sess.artifacts[a.name] = a
        return StepResult(step_index=idx, unit=self.id, params=params, produced=produced)

class U_FilterRepeats(UnitSpec):
    def run(self, sess, sess_dir, params):
        idx = _next_idx(sess); log = sess_dir / f"{idx:03d}_FilterSeq_repeats.log"
        _assert_channel(sess, "R1")
        r1 = sess_dir / sess.artifacts[sess.current["R1"]].path
        n = str(params.get("max_repeat","0.8"))
        use_missing = str(params.get("missing","false")).lower() in ("1","true","yes","y")
        inner = str(params.get("inner","false")).lower() in ("1","true","yes","y")
        cmd = ["FilterSeq.py","repeats","-s",str(r1),"-n",n,"--outname",f"R1_rep{n}","--log",log.name]
        if use_missing: cmd.append("--missing")
        if inner: cmd.append("--inner")
        run_cmd(cmd, sess_dir, log)
        out_r1 = find_pass_generic(sess_dir, f"R1_rep{n}", "repeats")
        produced = [Artifact(name="R1_repeats", path=out_r1, kind="fastq", channel="R1", from_step=idx)]
        sess.current["R1"] = "R1_repeats"
        if sess.current.get("R2"):
            r2 = sess_dir / sess.artifacts[sess.current["R2"]].path
            cmd2 = ["FilterSeq.py","repeats","-s",str(r2),"-n",n,"--outname",f"R2_rep{n}","--log",log.name]
            if use_missing: cmd2.append("--missing")
            if inner: cmd2.append("--inner")
            run_cmd(cmd2, sess_dir, log)
            out_r2 = find_pass_generic(sess_dir, f"R2_rep{n}", "repeats")
            produced.append(Artifact(name="R2_repeats", path=out_r2, kind="fastq", channel="R2", from_step=idx))
            sess.current["R2"] = "R2_repeats"
        for a in produced: sess.artifacts[a.name] = a
        return StepResult(step_index=idx, unit=self.id, params=params, produced=produced)

class U_FilterTrimQual(UnitSpec):
    def run(self, sess, sess_dir, params):
        idx = _next_idx(sess); log = sess_dir / f"{idx:03d}_FilterSeq_trimqual.log"
        _assert_channel(sess, "R1")
        r1 = sess_dir / sess.artifacts[sess.current["R1"]].path
        q = str(params.get("qmin", 20))
        win = params.get("window", 10)
        reverse = str(params.get("reverse","false")).lower() in ("1","true","yes","y")
        cmd = ["FilterSeq.py","trimqual","-s",str(r1),"-q",q,"--outname",f"R1_tq{q}","--log",log.name]
        if win: cmd += ["--win", str(win)]
        if reverse: cmd.append("--reverse")
        run_cmd(cmd, sess_dir, log)
        out_r1 = find_pass_generic(sess_dir, f"R1_tq{q}", "trimqual")
        produced = [Artifact(name="R1_trimqual", path=out_r1, kind="fastq", channel="R1", from_step=idx)]
        sess.current["R1"] = "R1_trimqual"
        if sess.current.get("R2"):
            r2 = sess_dir / sess.artifacts[sess.current["R2"]].path
            cmd2 = ["FilterSeq.py","trimqual","-s",str(r2),"-q",q,"--outname",f"R2_tq{q}","--log",log.name]
            if win: cmd2 += ["--win", str(win)]
            if reverse: cmd2.append("--reverse")
            run_cmd(cmd2, sess_dir, log)
            out_r2 = find_pass_generic(sess_dir, f"R2_tq{q}", "trimqual")
            produced.append(Artifact(name="R2_trimqual", path=out_r2, kind="fastq", channel="R2", from_step=idx))
            sess.current["R2"] = "R2_trimqual"
        for a in produced: sess.artifacts[a.name] = a
        return StepResult(step_index=idx, unit=self.id, params=params, produced=produced)

class U_FilterMaskQual(UnitSpec):
    def run(self, sess, sess_dir, params):
        idx = _next_idx(sess); log = sess_dir / f"{idx:03d}_FilterSeq_maskqual.log"
        _assert_channel(sess, "R1")
        r1 = sess_dir / sess.artifacts[sess.current["R1"]].path
        q = str(params.get("qmin", 20))
        cmd = ["FilterSeq.py","maskqual","-s",str(r1),"-q",q,"--outname",f"R1_mq{q}","--log",log.name]
        run_cmd(cmd, sess_dir, log)
        out_r1 = find_pass_generic(sess_dir, f"R1_mq{q}", "maskqual")
        produced = [Artifact(name="R1_maskqual", path=out_r1, kind="fastq", channel="R1", from_step=idx)]
        sess.current["R1"] = "R1_maskqual"
        if sess.current.get("R2"):
            r2 = sess_dir / sess.artifacts[sess.current["R2"]].path
            cmd2 = ["FilterSeq.py","maskqual","-s",str(r2),"-q",q,"--outname",f"R2_mq{q}","--log",log.name]
            run_cmd(cmd2, sess_dir, log)
            out_r2 = find_pass_generic(sess_dir, f"R2_mq{q}", "maskqual")
            produced.append(Artifact(name="R2_maskqual", path=out_r2, kind="fastq", channel="R2", from_step=idx))
            sess.current["R2"] = "R2_maskqual"
        for a in produced: sess.artifacts[a.name] = a
        return StepResult(step_index=idx, unit=self.id, params=params, produced=produced)

# ---- MaskPrimers ----
class U_MaskPrimers(UnitSpec):
    def run(self, sess, sess_dir, params):
        idx = _next_idx(sess)
        variant = params.get("variant","align")
        mode = params.get("mode","mask")
        revpr = str(params.get("revpr","false")).lower() in ("1","true","yes","y")
        log = sess_dir / f"{idx:03d}_MaskPrimers_{variant}.log"

        if not sess.current.get("R1"):
            raise HTTPException(400, "Need an R1 artifact (e.g., after FilterSeq).")
        r1 = sess_dir / sess.artifacts[sess.current["R1"]].path

        produced: List[Artifact] = []

        if variant in ("align","score"):
            # NEW: fall back to session-level remembered uploaded primer files
            v_fa_name = params.get("v_primers_fname") or sess.aux.get("v_primers")
            if not v_fa_name:
                raise HTTPException(400, "v_primers_fname is required for align/score.")
            v_fa = sess_dir / v_fa_name

            cmd = ["MaskPrimers.py", variant, "-s", str(r1), "-p", str(v_fa),
                   "--mode", mode, "--pf", "VPRIMER", "--outname", "R1", "--log", log.name]
            if revpr: cmd.append("--revpr")
            run_cmd(cmd, sess_dir, log)

            out_r1 = find_maskprimers_pass(sess_dir, "R1")
            produced.append(Artifact(name="R1_masked", path=out_r1, kind="fastq", channel="R1", from_step=idx))
            sess.current["R1"] = "R1_masked"

            if sess.current.get("R2"):
                c_fa_name = params.get("c_primers_fname") or sess.aux.get("c_primers")
                if c_fa_name:
                    r2 = sess_dir / sess.artifacts[sess.current["R2"]].path
                    c_fa = sess_dir / c_fa_name
                    cmd2 = ["MaskPrimers.py", variant, "-s", str(r2), "-p", str(c_fa),
                            "--mode", mode, "--pf", "CPRIMER", "--outname", "R2", "--log", log.name]
                    if revpr: cmd2.append("--revpr")
                    run_cmd(cmd2, sess_dir, log)
                    out_r2 = find_maskprimers_pass(sess_dir, "R2")
                    produced.append(Artifact(name="R2_masked", path=out_r2, kind="fastq", channel="R2", from_step=idx))
                    sess.current["R2"] = "R2_masked"

        elif variant == "extract":
            try:
                start = int(params.get("start"))
                length = int(params.get("length"))
            except Exception:
                raise HTTPException(400, "extract requires integer 'start' and 'length'.")

            pf = (params.get("pf") or "BARCODE").strip()
            cmd = ["MaskPrimers.py","extract","-s",str(r1),
                   "--start",str(start),"--len",str(length),
                   "--mode",mode,"--pf",pf,"--outname","R1","--log",log.name]
            if revpr: cmd.append("--revpr")
            run_cmd(cmd, sess_dir, log)
            out_r1 = find_maskprimers_pass(sess_dir, "R1")
            fields = {pf.upper(): True} if mode == "tag" else {}
            produced.append(Artifact(name="R1_extracted", path=out_r1, kind="fastq",
                                     channel="R1", from_step=idx, fields=fields))
            sess.current["R1"] = "R1_extracted"

            if sess.current.get("R2"):
                r2 = sess_dir / sess.artifacts[sess.current["R2"]].path
                cmd2 = ["MaskPrimers.py","extract","-s",str(r2),
                        "--start",str(start),"--len",str(length),
                        "--mode",mode,"--pf",pf,"--outname","R2","--log",log.name]
                if revpr: cmd2.append("--revpr")
                run_cmd(cmd2, sess_dir, log)
                out_r2 = find_maskprimers_pass(sess_dir, "R2")
                produced.append(Artifact(name="R2_extracted", path=out_r2, kind="fastq",
                                         channel="R2", from_step=idx, fields=fields))
                sess.current["R2"] = "R2_extracted"
        else:
            raise HTTPException(400, f"Unsupported variant '{variant}'. Choose from align, score, extract.")

        for a in produced: sess.artifacts[a.name] = a
        return StepResult(step_index=idx, unit=self.id, params=params, produced=produced)

# ---- Pair/Assemble/Collapse/Consensus ----
class U_PairSeq(UnitSpec):
    def run(self, sess, sess_dir, params):
        idx = _next_idx(sess); log = sess_dir / f"{idx:03d}_PairSeq.log"
        _assert_channel(sess, "R1"); _assert_channel(sess, "R2")
        r1 = sess_dir / sess.artifacts[sess.current["R1"]].path
        r2 = sess_dir / sess.artifacts[sess.current["R2"]].path
        coord = params.get("coord","illumina")
        if coord not in PAIRSEQ_COORDS:
            raise HTTPException(400, f"coord must be one of {sorted(PAIRSEQ_COORDS)}")
        run_cmd(["PairSeq.py","-1",str(r1),"-2",str(r2),"--coord",coord,"--outname","PAIRED"], sess_dir, log)
        a1 = Artifact(name="PAIR1", path=find_pair_pass(sess_dir, 1), kind="fastq", channel="PAIR1", from_step=idx)
        a2 = Artifact(name="PAIR2", path=find_pair_pass(sess_dir, 2), kind="fastq", channel="PAIR2", from_step=idx)
        sess.artifacts[a1.name] = a1; sess.artifacts[a2.name] = a2
        sess.current["PAIR1"] = a1.name; sess.current["PAIR2"] = a2.name
        return StepResult(step_index=idx, unit=self.id, params=params, produced=[a1, a2])

class U_AssembleAlign(UnitSpec):
    def run(self, sess, sess_dir, params):
        idx = _next_idx(sess)
        log = sess_dir / f"{idx:03d}_AssemblePairs_align.log"

        have_pair = sess.current.get("PAIR1") and sess.current.get("PAIR2")
        have_raw  = sess.current.get("R1")    and sess.current.get("R2")

        if have_pair:
            p1 = sess_dir / sess.artifacts[sess.current["PAIR1"]].path
            p2 = sess_dir / sess.artifacts[sess.current["PAIR2"]].path
            src_label = "PAIR1/PAIR2"
        elif have_raw:
            p1 = sess_dir / sess.artifacts[sess.current["R1"]].path
            p2 = sess_dir / sess.artifacts[sess.current["R2"]].path
            src_label = "R1/R2"
        else:
            have = {"PAIR1": bool(sess.current.get("PAIR1")),
                    "PAIR2": bool(sess.current.get("PAIR2")),
                    "R1":    bool(sess.current.get("R1")),
                    "R2":    bool(sess.current.get("R2"))}
            raise HTTPException(400, f"AssemblePairs needs BOTH reads. Present: {have}. Upload both FASTQs (R1 & R2) or run PairSeq first.")

        def has(v): return v is not None and str(v).strip() not in ("", "none", "None")

        coord = params.get("coord", "illumina")
        if coord not in {"illumina","solexa","sra","454","presto"}:
            raise HTTPException(400, "coord must be one of ['illumina','solexa','sra','454','presto']")

        rc = params.get("rc", "tail")
        if rc not in {"tail","head","both","none"}:
            raise HTTPException(400, "rc must be one of ['tail','head','both','none']")

        cmd = ["AssemblePairs.py","align",
               "-1", str(p1), "-2", str(p2),
               "--coord", coord, "--rc", rc,
               "--outname", "ASSEMBLED", "--log", log.name]

        if has(params.get("alpha")):     cmd += ["--alpha", str(params["alpha"])]
        if has(params.get("maxerror")):  cmd += ["--maxerror", str(params["maxerror"])]
        if has(params.get("minlen")):    cmd += ["--minlen", str(params["minlen"])]
        if has(params.get("maxlen")):    cmd += ["--maxlen", str(params["maxlen"])]
        try:
            if has(params.get("minlen")) and has(params.get("maxlen")):
                if int(params["maxlen"]) < int(params["minlen"]):
                    raise HTTPException(400, "maxlen must be ≥ minlen.")
        except ValueError:
            raise HTTPException(400, "minlen/maxlen must be integers.")

        run_cmd(cmd, sess_dir, log)
        run_cmd(["ParseLog.py","-l",log.name,"-f","ID","LENGTH","OVERLAP","ERROR","PVALUE","--outname","AP"], sess_dir, log)

        a = Artifact(name="ASSEMBLED", path=find_assemble_pass(sess_dir),
                     kind="fastq", channel="ASSEMBLED", from_step=idx)
        t = Artifact(name="AP_table", path="AP_table.tab", kind="tab", from_step=idx)
        sess.artifacts[a.name] = a; sess.artifacts[t.name] = t
        sess.current["ASSEMBLED"] = a.name

        return StepResult(step_index=idx, unit=self.id,
                          params={**params, "_source": src_label}, produced=[a, t])

class U_AssembleJoin(UnitSpec):
    def run(self, sess, sess_dir, params):
        idx = _next_idx(sess)
        log = sess_dir / f"{idx:03d}_AssemblePairs_join.log"

        have_pair = sess.current.get("PAIR1") and sess.current.get("PAIR2")
        have_raw  = sess.current.get("R1")    and sess.current.get("R2")

        if have_pair:
            p1 = sess_dir / sess.artifacts[sess.current["PAIR1"]].path
            p2 = sess_dir / sess.artifacts[sess.current["PAIR2"]].path
            src_label = "PAIR1/PAIR2"
        elif have_raw:
            p1 = sess_dir / sess.artifacts[sess.current["R1"]].path
            p2 = sess_dir / sess.artifacts[sess.current["R2"]].path
            src_label = "R1/R2"
        else:
            have = {"PAIR1": bool(sess.current.get("PAIR1")),
                    "PAIR2": bool(sess.current.get("PAIR")),
                    "R1":    bool(sess.current.get("R1")),
                    "R2":    bool(sess.current.get("R2"))}
            raise HTTPException(400, f"AssemblePairs (join) needs BOTH reads. Present: {have}.")

        def has(v): return v is not None and str(v).strip() not in ("", "none", "None")

        coord = params.get("coord", "illumina")
        if coord not in {"illumina","solexa","sra","454","presto"}:
            raise HTTPException(400, "coord must be one of ['illumina','solexa','sra','454','presto']")

        rc = params.get("rc", "tail")
        if rc not in {"tail","head","both","none"}:
            raise HTTPException(400, "rc must be one of ['tail','head','both','none']")

        cmd = ["AssemblePairs.py","join",
               "-1", str(p1), "-2", str(p2),
               "--coord", coord, "--rc", rc,
               "--outname", "ASSEMBLED", "--log", log.name]

        def parse_fields(x):
            if not has(x): return []
            if isinstance(x, list): return [str(v) for v in x if str(v).strip()]
            return [t for t in str(x).replace(",", " ").split() if t]

        one_fields = parse_fields(params.get("onef"))
        two_fields = parse_fields(params.get("twof"))
        if one_fields: cmd += ["--1f"] + one_fields
        if two_fields: cmd += ["--2f"] + two_fields

        if has(params.get("gap")):
            cmd += ["--gap", str(params["gap"])]

        run_cmd(cmd, sess_dir, log)

        out = find_assemble_any_pass(sess_dir)
        a = Artifact(name="ASSEMBLED", path=out, kind="fastq", channel="ASSEMBLED", from_step=idx)
        sess.artifacts[a.name] = a
        sess.current["ASSEMBLED"] = a.name
        return StepResult(step_index=idx, unit=self.id, params=params, produced=[a])

class U_AssembleSequential(UnitSpec):
    def run(self, sess, sess_dir, params):
        idx = _next_idx(sess)
        log = sess_dir / f"{idx:03d}_AssemblePairs_sequential.log"

        have_pair = sess.current.get("PAIR1") and sess.current.get("PAIR2")
        have_raw  = sess.current.get("R1")    and sess.current.get("R2")

        if have_pair:
            p1 = sess_dir / sess.artifacts[sess.current["PAIR1"]].path
            p2 = sess_dir / sess.artifacts[sess.current["PAIR2"]].path
            src_label = "PAIR1/PAIR2"
        elif have_raw:
            p1 = sess_dir / sess.artifacts[sess.current["R1"]].path
            p2 = sess_dir / sess.artifacts[sess.current["R2"]].path
            src_label = "R1/R2"
        else:
            have = {"PAIR1": bool(sess.current.get("PAIR1")),
                    "PAIR2": bool(sess.current.get("PAIR2")),
                    "R1":    bool(sess.current.get("R1")),
                    "R2":    bool(sess.current.get("R2"))}
            raise HTTPException(400, f"AssemblePairs (sequential) needs BOTH reads. Present: {have}.")

        def has(v): 
            return v is not None and str(v).strip() not in ("", "none", "None")

        coord = params.get("coord", "illumina")
        if coord not in {"illumina","solexa","sra","454","presto"}:
            raise HTTPException(400, "coord must be one of ['illumina','solexa','sra','454','presto']")

        rc = params.get("rc", "tail")
        if rc not in {"tail","head","both","none"}:
            raise HTTPException(400, "rc must be one of ['tail','head','both','none']")

        cmd = ["AssemblePairs.py", "sequential",
               "-1", str(p1), "-2", str(p2),
               "--coord", coord, "--rc", rc,
               "--outname", "ASSEMBLED", "--log", log.name]

        def parse_fields(x):
            if not has(x): return []
            if isinstance(x, list): return [str(v) for v in x if str(v).strip()]
            return [t for t in str(x).replace(",", " ").split() if t]
        one_fields = parse_fields(params.get("onef"))
        two_fields = parse_fields(params.get("twof"))
        if one_fields: cmd += ["--1f"] + one_fields
        if two_fields: cmd += ["--2f"] + two_fields

        if has(params.get("alpha")):     cmd += ["--alpha", str(params["alpha"])]
        if has(params.get("maxerror")):  cmd += ["--maxerror", str(params["maxerror"])]
        if has(params.get("minlen")):    cmd += ["--minlen", str(params["minlen"])]
        if has(params.get("maxlen")):    cmd += ["--maxlen", str(params["maxlen"])]
        if str(params.get("scanrev","false")).lower() in ("1","true","yes","y"):
            cmd.append("--scanrev")

        ref_fname = params.get("ref_fname")
        if not has(ref_fname):
            raise HTTPException(400, "AssemblePairs (sequential) requires a reference FASTA (-r). Upload it and set 'ref_fname'.")
        refp = sess_dir / ref_fname
        if not refp.exists():
            raise HTTPException(400, f"Reference file '{ref_fname}' not found in this session.")
        cmd += ["-r", str(refp)]

        if has(params.get("minident")):  cmd += ["--minident", str(params["minident"])]
        if has(params.get("evalue")):    cmd += ["--evalue", str(params["evalue"])]
        if has(params.get("maxhits")):   cmd += ["--maxhits", str(params["maxhits"])]
        aligner = params.get("aligner")
        if has(aligner):
            if aligner not in {"blastn","usearch"}:
                raise HTTPException(400, "aligner must be one of ['blastn','usearch']")
            cmd += ["--aligner", aligner]

        try:
            if has(params.get("minlen")) and has(params.get("maxlen")):
                if int(params["maxlen"]) < int(params["minlen"]):
                    raise HTTPException(400, "maxlen must be ≥ minlen.")
        except ValueError:
            raise HTTPException(400, "minlen/maxlen must be integers.")

        run_cmd(cmd, sess_dir, log)

        out = find_assemble_any_pass(sess_dir)
        a = Artifact(name="ASSEMBLED", path=out, kind="fastq", channel="ASSEMBLED", from_step=idx)
        sess.artifacts[a.name] = a
        sess.current["ASSEMBLED"] = a.name
        return StepResult(step_index=idx, unit=self.id,
                          params={**params, "_source": src_label}, produced=[a])

class U_CollapseSeq(UnitSpec):
    def run(self, sess, sess_dir, params):
        idx = _next_idx(sess); log = sess_dir / f"{idx:03d}_CollapseSeq.log"
        key = sess.current.get("ASSEMBLED") or sess.current.get("R1")
        if not key:
            raise HTTPException(400, "CollapseSeq needs a current single FASTQ (ASSEMBLED or R1).")
        src = sess_dir / sess.artifacts[key].path
        outname = params.get("outname","COLLAPSE")
        cmd = ["CollapseSeq.py","-s",str(src),"--outname",outname,"--log",log.name]
        act = params.get("act")
        if act and str(act).strip().lower() != "none":
            allowed = {"min","max","sum","set","majority"}
            if act not in allowed:
                raise HTTPException(400, f"Invalid act '{act}'. Allowed: {sorted(allowed)}")
            cmd += ["--act", act]
        run_cmd(cmd, sess_dir, log)
        a = Artifact(name="COLLAPSED",
                     path=find_collapse_pass(sess_dir, outname),
                     kind="fastq",
                     from_step=idx)
        sess.artifacts[a.name] = a
        sess.current["ASSEMBLED"] = a.name
        return StepResult(step_index=idx, unit=self.id, params=params, produced=[a])

class U_BuildConsensus(UnitSpec):
    def run(self, sess, sess_dir, params):
        idx = _next_idx(sess); log = sess_dir / f"{idx:03d}_BuildConsensus.log"
        key_name = "ASSEMBLED" if sess.current.get("ASSEMBLED") else ("R1" if sess.current.get("R1") else None)
        if not key_name:
            raise HTTPException(400, "BuildConsensus needs a single-read stream (e.g., after AssemblePairs or R1).")
        in_art = sess.artifacts[sess.current[key_name]]
        if not in_art.fields.get("BARCODE", False):
            raise HTTPException(400, "BuildConsensus requires a BARCODE field. Run MaskPrimers with variant=extract, mode=tag, pf=BARCODE first.")
        src = sess_dir / in_art.path
        cmd = ["BuildConsensus.py", "-s", str(src), "--outname", "CONS"]
        if _has(params.get("qmin")):   cmd += ["-q", str(params["qmin"])]
        if _has(params.get("freq")):   cmd += ["--freq", str(params["freq"])]
        if _has(params.get("maxgap")): cmd += ["--maxgap", str(params["maxgap"])]
        acts = params.get("act")
        if _has(acts):
            if isinstance(acts, str):
                acts = [a for a in acts.replace(",", " ").split() if a]
            allowed = {"min","max","sum","set","majority"}
            bad = [a for a in acts if a not in allowed]
            if bad:
                raise HTTPException(400, f"Invalid act value(s): {bad}. Allowed: {sorted(allowed)}")
            cmd += ["--act"] + acts
        if str(params.get("dep","false")).lower() in ("1","true","yes","y"):
            cmd.append("--dep")
        maxdiv = params.get("maxdiv"); maxerr = params.get("maxerror")
        if _has(maxdiv) and _has(maxerr):
            raise HTTPException(400, "Choose only one of maxdiv or maxerror, not both.")
        if _has(maxdiv): cmd += ["--maxdiv", str(maxdiv)]
        if _has(maxerr): cmd += ["--maxerror", str(maxerr)]
        run_cmd(cmd, sess_dir, log)
        out = find_pass_generic(sess_dir, "CONS", "consensus")
        kind = "fastq" if "fastq" in out else ("other")
        a = Artifact(name="CONSENSUS", path=out, kind=kind, channel=key_name, from_step=idx)
        sess.artifacts[a.name] = a
        sess.current[key_name] = a.name
        return StepResult(step_index=idx, unit=self.id, params=params, produced=[a])
## single cell preprocessing units
class U_MergeSamples(UnitSpec):
    """
    Merge multiple AIRR-C rearrangement tables using airr::read_rearrangement.
    - files: comma/space-separated list of filenames stored in this session (optional).
             If omitted, all *.tsv and *.tsv.gz in the session directory are used.
    - aux_types: key=type pairs (e.g. "v_germline_length=i, d_germline_length=i, j_germline_length=i, day=i").
                 Defaults to those four integers if omitted.
    - sample_field: column name to annotate each row with filename stem (default "sample_id"; set empty to skip).
    Output: MERGED.tsv
    """
    def run(self, sess, sess_dir, params):
        import re
        idx = _next_idx(sess)
        log = sess_dir / f"{idx:03d}_SC_MergeSamples.log"

        # Collect files
        files_param = (params.get("files") or "").strip()
        if files_param:
            # split by comma/space
            names = [n for n in re.split(r"[,\s]+", files_param) if n]
        else:
            # default: all TSV/TSV.GZ in session
            names = sorted([p.name for p in sess_dir.glob("*.tsv")] + [p.name for p in sess_dir.glob("*.tsv.gz")])

        if not names:
            raise HTTPException(400, "No input tables. Upload AIRR TSVs (use 'Upload aux') or provide 'files' list.")

        paths = []
        for n in names:
            p = sess_dir / n
            if not p.exists():
                raise HTTPException(400, f"File not found in session: {n}")
            paths.append(str(p))

        # aux_types mapping
        aux_default = "v_germline_length=i, d_germline_length=i, j_germline_length=i, day=i"
        aux_str = (params.get("aux_types") or aux_default).strip()

        # convert to R named vector literal: c('k'='i','k2'='i',...)
        pairs = []
        for part in re.split(r"[,\s]+", aux_str):
            if not part or "=" not in part: continue
            k,v = part.split("=",1)
            k = k.strip(); v = v.strip()
            if not k or not v: continue
            pairs.append(f"'{k}'='{v}'")
        r_aux_vec = "c(" + ",".join(pairs) + ")" if pairs else "c()"

        sample_field = (params.get("sample_field") if params.get("sample_field") is not None else "sample_id")
        sample_field = str(sample_field).strip()

        # Write a small R script into the session
        rfile = sess_dir / f"{idx:03d}_merge_samples.R"
        r_code = f"""
            args <- commandArgs(trailingOnly=TRUE)
            out <- args[1]
            files <- args[-1]
            suppressPackageStartupMessages(library(airr))
            aux_types <- {r_aux_vec}
            sfield <- {repr(sample_field)}

            read_one <- function(f) {{
            df <- airr::read_rearrangement(f, aux_types = aux_types)
            if (nchar(sfield) > 0) {{
                base <- basename(f)
                base <- sub("\\\\.[^.]+$", "", base)
                df[[sfield]] <- base
            }}
            df
            }}

            lst <- lapply(files, read_one)
            merged <- do.call(rbind, lst)
            write.table(merged, file=out, sep="\\t", quote=FALSE, row.names=FALSE)
        """
        rfile.write_text(r_code, encoding="utf-8")

        # Run Rscript
        out_path = sess_dir / "MERGED.tsv"
        cmd = ["Rscript", "--vanilla", rfile.name, out_path.name] + paths
        run_cmd(cmd, sess_dir, log)

        a = Artifact(name="SC_MERGED", path=out_path.name, kind="tab", from_step=idx)
        sess.artifacts[a.name] = a
        # Track a single-cell table "channel" for downstream SC units
        sess.current["SC_TABLE"] = a.name

        return StepResult(step_index=idx, unit=self.id, params=params, produced=[a])
    
class U_SC_FilterProductive(UnitSpec):
    """
    Single-cell: Remove non-productive sequences independently of other steps.

    Parameters
    ----------
    files : text (optional)
        Comma/space separated list of TSV/TSV.GZ files already uploaded to this session.
        If empty, all *.tsv / *.tsv.gz in the session directory are used.
    productive_field : text
        Column to test for truthy values (default: 'productive').
    fallback_from_airr : select {'true','false'}
        If productive_field is missing, try computing productivity as
        (vj_in_frame == TRUE) & (stop_codon == FALSE). Default true.
    mode : select {'merge','per_file'}
        'merge' produces one file (SC_productive.tsv) and sets SC_TABLE to it.
        'per_file' writes SC_prod_<basename>.tsv for each input file and sets SC_TABLE
        to the first produced file (so you can chain if desired).
    sample_field : text
        When mode='merge' and non-empty, a new column with this name is added containing
        the input filename stem.
    """
    def run(self, sess, sess_dir, params):
        import re
        idx = _next_idx(sess)
        log = sess_dir / f"{idx:03d}_SC_FilterProductive.log"

        # -------- inputs ----------
        files_param = (params.get("files") or "").strip()
        if files_param:
            names = [n for n in re.split(r"[,\s]+", files_param) if n]
        else:
            names = sorted([p.name for p in sess_dir.glob("*.tsv")] +
                           [p.name for p in sess_dir.glob("*.tsv.gz")])

        if not names:
            raise HTTPException(400, "No TSVs found. Upload AIRR TSV/TSV.GZ via 'Upload inputs' or specify 'files'.")

        for n in names:
            if not (sess_dir / n).exists():
                raise HTTPException(400, f"File not found in session: {n}")

        pf   = (params.get("productive_field") or "productive").strip() or "productive"
        fb   = str(params.get("fallback_from_airr", "true")).lower() in ("1","true","yes","y")
        mode = (params.get("mode") or "merge").strip().lower()
        if mode not in ("merge","per_file"):
            mode = "merge"
        sfield = (params.get("sample_field") or "sample_id").strip()

        # -------- R script ----------
        rfile = sess_dir / f"{idx:03d}_sc_filter_productive.R"
        out_merged = "SC_productive.tsv"
        # We pass: out_merged, mode, sfield, pf, fallbackFlag, then files...
        r_code = f"""
args <- commandArgs(trailingOnly=TRUE)
out_merged <- args[1]
mode <- args[2]
sfield <- args[3]
pf <- {repr(pf)}
fallbackFlag <- as.logical({str(fb).upper()})
files <- args[-(1:3)]

truthy <- c(TRUE, "TRUE", "T", "true", "True", 1, "1")

filter_one <- function(f){{
  df <- tryCatch({{
    read.delim(f, header=TRUE, sep="\\t", check.names=FALSE, stringsAsFactors=FALSE)
  }}, error=function(e) {{
    stop(paste("Failed to read:", f, "->", e$message))
  }})

  # compute keep mask
  if (pf %in% colnames(df)) {{
    keep <- df[[pf]] %in% truthy
  }} else if (fallbackFlag && all(c("vj_in_frame","stop_codon") %in% colnames(df))) {{
    # AIRR fallback: productive if in-frame and no stop codon
    keep <- (df[["vj_in_frame"]] %in% truthy) & !(df[["stop_codon"]] %in% truthy)
  }} else {{
    warning(paste("No productive field and no AIRR fallback columns; keeping all rows for", f))
    keep <- rep(TRUE, nrow(df))
  }}

  df2 <- df[keep, , drop=FALSE]
  df2
}}

if (mode == "per_file") {{
  for (f in files) {{
    df2 <- filter_one(f)
    base <- basename(f)
    base <- sub("\\\\.[^.]+$", "", base)
    out <- paste0("SC_prod_", base, ".tsv")
    write.table(df2, file=out, sep="\\t", quote=FALSE, row.names=FALSE)
    cat(paste("Wrote", out, "rows:", nrow(df2), "\\n"))
  }}
}} else {{
  lst <- lapply(files, filter_one)
  if (length(lst) == 0) {{
    stop("No input tables after filtering.")
  }}
  merged <- do.call(rbind, lst)
  if (nchar(sfield) > 0) {{
    # annotate origin by filename stem
    # We need to repeat the origin per row; rebuild using files and nrow of filtered fragments
    origins <- unlist(lapply(seq_along(files), function(i){{
      f <- files[[i]]
      base <- sub("\\\\.[^.]+$", "", basename(f))
      n <- nrow(lst[[i]])
      if (n <= 0) return(character(0))
      rep(base, n)
    }}))
    if (length(origins) == nrow(merged)) {{
      merged[[sfield]] <- origins
    }} else {{
      warning("Could not build origin column (row mismatch). Skipping.")
    }}
  }}
  write.table(merged, file=out_merged, sep="\\t", quote=FALSE, row.names=FALSE)
  cat(paste("Wrote", out_merged, "rows:", nrow(merged), "\\n"))
}}
"""
        rfile.write_text(r_code, encoding="utf-8")

        cmd = ["Rscript", "--vanilla", rfile.name, out_merged, mode, sfield] + names
        run_cmd(cmd, sess_dir, log)

        produced = []
        if mode == "per_file":
            # Register every produced SC_prod_<stem>.tsv
            for n in names:
                stem = re.sub(r"\\.[^.]+$", "", n)
                out = f"SC_prod_{stem}.tsv"
                if (sess_dir / out).exists():
                    a = Artifact(name=f"SC_PROD_{stem}", path=out, kind="tab", from_step=idx)
                    sess.artifacts[a.name] = a
                    produced.append(a)
            # set SC_TABLE to the first produced (if any)
            if produced:
                sess.current["SC_TABLE"] = produced[0].name
        else:
            # merge mode: one output
            a = Artifact(name="SC_PRODUCTIVE", path=out_merged, kind="tab", from_step=idx)
            sess.artifacts[a.name] = a
            produced.append(a)
            sess.current["SC_TABLE"] = a.name

        return StepResult(step_index=idx, unit=self.id, params=params, produced=produced)

class U_SC_RemoveMultiHeavy(UnitSpec):
    """
    Single-cell: remove cells that have multiple heavy-chain rearrangements.

    Parameters
    ----------
    files : text (optional)
        Comma/space-separated list of TSV/TSV.GZ files uploaded to this session.
        If empty, uses all *.tsv / *.tsv.gz in the session directory.
    locus_field : text
        Column that denotes chain locus (default: 'locus').
    heavy_value : text
        Value that denotes the heavy locus (default: 'IGH').
    cell_field : text
        Column with the cell identifier (default: 'cell_id')  — REQUIRED in input.
    fallback_from_vcall : select {'true','false'}
        If `locus_field` is missing, detect heavy with grepl('^IGH', v_call) (default true).
    mode : select {'merge','per_file'}
        'merge' → one file SC_no_multi_heavy.tsv; 'per_file' → one file per input.
        In both cases SC_TABLE is set (first produced when per_file).
    sample_field : text
        When merging and non-empty, annotate each row with the filename stem.

    Output
    ------
    - merge: SC_no_multi_heavy.tsv
    - per_file: SC_noMH_<basename>.tsv per input
    """
    def run(self, sess, sess_dir, params):
        import re
        idx = _next_idx(sess)
        log = sess_dir / f"{idx:03d}_SC_RemoveMultiHeavy.log"

        # ---- inputs ----
        files_param = (params.get("files") or "").strip()
        if files_param:
            names = [n for n in re.split(r"[,\s]+", files_param) if n]
        else:
            names = sorted([p.name for p in sess_dir.glob("*.tsv")] +
                           [p.name for p in sess_dir.glob("*.tsv.gz")])
        if not names:
            raise HTTPException(400, "No TSVs found. Upload TSV/TSV.GZ or provide 'files'.")

        for n in names:
            if not (sess_dir / n).exists():
                raise HTTPException(400, f"File not found in session: {n}")

        locus_field = (params.get("locus_field") or "locus").strip() or "locus"
        heavy_value = (params.get("heavy_value") or "IGH").strip() or "IGH"
        cell_field  = (params.get("cell_field")  or "cell_id").strip() or "cell_id"
        fb          = str(params.get("fallback_from_vcall", "true")).lower() in ("1","true","yes","y")
        mode        = (params.get("mode") or "merge").strip().lower()
        if mode not in ("merge","per_file"):
            mode = "merge"
        sfield      = (params.get("sample_field") or "sample_id").strip()

        # ---- R script ----
        rfile = sess_dir / f"{idx:03d}_sc_remove_multi_heavy.R"
        out_merged = "SC_no_multi_heavy.tsv"
        # pass: out_merged, mode, sfield, locus_field, heavy_value, cell_field, fallbackFlag, then files...
        r_code = f"""
args <- commandArgs(trailingOnly=TRUE)
out_merged <- args[1]
mode <- args[2]
sfield <- args[3]
locus_field <- {repr(locus_field)}
heavy_value <- {repr(heavy_value)}
cell_field <- {repr(cell_field)}
fallbackFlag <- as.logical({str(fb).upper()})
files <- args[-(1:3)]

read_one <- function(f){{
  df <- tryCatch({{
    read.delim(f, header=TRUE, sep="\\t", check.names=FALSE, stringsAsFactors=FALSE)
  }}, error=function(e) {{
    stop(paste("Failed to read:", f, "->", e$message))
  }})
  if (!(cell_field %in% colnames(df))) {{
    stop(paste("Column", cell_field, "not found in", f))
  }}

  # Identify heavy chains
  if (locus_field %in% colnames(df)) {{
    heavy_mask <- (df[[locus_field]] == heavy_value)
  }} else if (fallbackFlag && ("v_call" %in% colnames(df))) {{
    heavy_mask <- grepl("^IGH", as.character(df[["v_call"]]))
  }} else {{
    warning(paste("No", locus_field, "and no v_call; assuming no heavy calls in", f))
    heavy_mask <- rep(FALSE, nrow(df))
  }}

  # Find cells with >1 heavy
  heavy_cells <- df[heavy_mask, cell_field]
  tab <- table(heavy_cells)
  multi_cells <- names(tab[tab > 1])

  # Filter out those cells
  keep <- !(df[[cell_field]] %in% multi_cells)
  df2 <- df[keep, , drop=FALSE]
  df2
}}

if (mode == "per_file") {{
  for (f in files) {{
    df2 <- read_one(f)
    base <- sub("\\\\.[^.]+$", "", basename(f))
    out <- paste0("SC_noMH_", base, ".tsv")
    write.table(df2, file=out, sep="\\t", quote=FALSE, row.names=FALSE)
    cat(paste("Wrote", out, "rows:", nrow(df2), "\\n"))
  }}
}} else {{
  lst <- lapply(files, read_one)
  if (length(lst) == 0) {{
    stop("No input tables after filtering.")
  }}
  merged <- do.call(rbind, lst)
  if (nchar(sfield) > 0) {{
    origins <- unlist(lapply(seq_along(files), function(i){{
      base <- sub("\\\\.[^.]+$", "", basename(files[[i]]))
      n <- nrow(lst[[i]])
      if (n <= 0) return(character(0))
      rep(base, n)
    }}))
    if (length(origins) == nrow(merged)) {{
      merged[[sfield]] <- origins
    }} else {{
      warning("Could not add origin column (row mismatch).")
    }}
  }}
  write.table(merged, file=out_merged, sep="\\t", quote=FALSE, row.names=FALSE)
  cat(paste("Wrote", out_merged, "rows:", nrow(merged), "\\n"))
}}
"""
        rfile.write_text(r_code, encoding="utf-8")

        cmd = ["Rscript", "--vanilla", rfile.name, out_merged, mode, sfield] + names
        run_cmd(cmd, sess_dir, log)

        produced = []
        if mode == "per_file":
            for n in names:
                stem = re.sub(r"\.[^.]+$", "", n)
                out = f"SC_noMH_{stem}.tsv"
                if (sess_dir / out).exists():
                    a = Artifact(name=f"SC_NOMH_{stem}", path=out, kind="tab", from_step=idx)
                    sess.artifacts[a.name] = a
                    produced.append(a)
            if produced:
                sess.current["SC_TABLE"] = produced[0].name
        else:
            a = Artifact(name="SC_NO_MULTI_HEAVY", path=out_merged, kind="tab", from_step=idx)
            sess.artifacts[a.name] = a
            produced.append(a)
            sess.current["SC_TABLE"] = a.name

        return StepResult(step_index=idx, unit=self.id, params=params, produced=produced)

class U_SC_RemoveNoHeavy(UnitSpec):
    """
    Single-cell: remove cells that have only light chains (no heavy).

    Parameters
    ----------
    files : text (optional)
        Comma/space-separated list of TSV/TSV.GZ files uploaded to this session.
        If empty, uses all *.tsv / *.tsv.gz in the session directory.
    locus_field : text
        Column that denotes chain locus (default: 'locus').
    heavy_value : text
        Value denoting heavy locus (default: 'IGH').
    light_values : text
        Comma/space-separated values denoting light loci (default: 'IGK, IGL').
    cell_field : text
        Column with cell identifier (default: 'cell_id') — required in input.
    fallback_from_vcall : select {'true','false'}
        If locus_field is missing, detect heavy via v_call =~ '^IGH' and light via v_call =~ '^IG[KL]'.
        Default true.
    mode : select {'merge','per_file'}
        'merge' → one file SC_no_heavy.tsv; 'per_file' → one file per input (SC_noH_<basename>.tsv).
        In both cases SC_TABLE is set (first produced when per_file).
    sample_field : text
        When merging and non-empty, annotate each row with the filename stem.
    """
    def run(self, sess, sess_dir, params):
        import re
        idx = _next_idx(sess)
        log = sess_dir / f"{idx:03d}_SC_RemoveNoHeavy.log"

        # ---- inputs ----
        files_param = (params.get("files") or "").strip()
        if files_param:
            names = [n for n in re.split(r"[,\s]+", files_param) if n]
        else:
            names = sorted([p.name for p in sess_dir.glob("*.tsv")] +
                           [p.name for p in sess_dir.glob("*.tsv.gz")])
        if not names:
            raise HTTPException(400, "No TSVs found. Upload TSV/TSV.GZ or provide 'files'.")

        for n in names:
            if not (sess_dir / n).exists():
                raise HTTPException(400, f"File not found in session: {n}")

        locus_field = (params.get("locus_field") or "locus").strip() or "locus"
        heavy_value = (params.get("heavy_value") or "IGH").strip() or "IGH"
        light_values_text = (params.get("light_values") or "IGK, IGL").strip()
        light_values = [v for v in re.split(r"[,\s]+", light_values_text) if v]
        cell_field  = (params.get("cell_field")  or "cell_id").strip() or "cell_id"
        fb          = str(params.get("fallback_from_vcall", "true")).lower() in ("1","true","yes","y")
        mode        = (params.get("mode") or "merge").strip().lower()
        if mode not in ("merge","per_file"):
            mode = "merge"
        sfield      = (params.get("sample_field") or "sample_id").strip()

        # ---- R script ----
        rfile = sess_dir / f"{idx:03d}_sc_remove_no_heavy.R"
        out_merged = "SC_no_heavy.tsv"
        # pass: out_merged, mode, sfield, locus_field, heavy_value, light_values (comma-joined), cell_field, fallbackFlag, then files...
        lv_joined = ",".join(light_values)
        r_code = f"""
args <- commandArgs(trailingOnly=TRUE)
out_merged <- args[1]
mode <- args[2]
sfield <- args[3]
locus_field <- {repr(locus_field)}
heavy_value <- {repr(heavy_value)}
light_values <- unlist(strsplit({repr(lv_joined)}, ","))
cell_field <- {repr(cell_field)}
fallbackFlag <- as.logical({str(fb).upper()})
files <- args[-(1:3)]

read_one <- function(f){{
  df <- tryCatch({{
    read.delim(f, header=TRUE, sep="\\t", check.names=FALSE, stringsAsFactors=FALSE)
  }}, error=function(e) {{
    stop(paste("Failed to read:", f, "->", e$message))
  }})
  if (!(cell_field %in% colnames(df))) {{
    stop(paste("Column", cell_field, "not found in", f))
  }}

  # Determine heavy vs light masks
  if (locus_field %in% colnames(df)) {{
    heavy_mask <- df[[locus_field]] == heavy_value
    light_mask <- df[[locus_field]] %in% light_values
  }} else if (fallbackFlag && ("v_call" %in% colnames(df))) {{
    vc <- as.character(df[["v_call"]])
    heavy_mask <- grepl("^IGH", vc)
    light_mask <- grepl("^IG(K|L)", vc)
  }} else {{
    warning(paste("No", locus_field, "and no v_call; cannot classify heavy/light in", f, "-- keeping all rows"))
    return(df)
  }}

  # Cells with only light (no heavy)
  heavy_cells <- unique(df[heavy_mask, cell_field])
  light_cells <- unique(df[light_mask, cell_field])
  no_heavy_cells <- setdiff(light_cells, heavy_cells)

  keep <- !(df[[cell_field]] %in% no_heavy_cells)
  df2 <- df[keep, , drop=FALSE]
  df2
}}

if (mode == "per_file") {{
  for (f in files) {{
    df2 <- read_one(f)
    base <- sub("\\\\.[^.]+$", "", basename(f))
    out <- paste0("SC_noH_", base, ".tsv")
    write.table(df2, file=out, sep="\\t", quote=FALSE, row.names=FALSE)
    cat(paste("Wrote", out, "rows:", nrow(df2), "\\n"))
  }}
}} else {{
  lst <- lapply(files, read_one)
  if (length(lst) == 0) {{
    stop("No input tables after filtering.")
  }}
  merged <- do.call(rbind, lst)
  if (nchar(sfield) > 0) {{
    origins <- unlist(lapply(seq_along(files), function(i){{
      base <- sub("\\\\.[^.]+$", "", basename(files[[i]]))
      n <- nrow(lst[[i]])
      if (n <= 0) return(character(0))
      rep(base, n)
    }}))
    if (length(origins) == nrow(merged)) {{
      merged[[sfield]] <- origins
    }} else {{
      warning("Could not add origin column (row mismatch).")
    }}
  }}
  write.table(merged, file=out_merged, sep="\\t", quote=FALSE, row.names=FALSE)
  cat(paste("Wrote", out_merged, "rows:", nrow(merged), "\\n"))
}}
"""
        rfile.write_text(r_code, encoding="utf-8")

        cmd = ["Rscript", "--vanilla", rfile.name, out_merged, mode, sfield] + names
        run_cmd(cmd, sess_dir, log)

        produced = []
        if mode == "per_file":
            for n in names:
                stem = re.sub(r"\.[^.]+$", "", n)
                out = f"SC_noH_{stem}.tsv"
                if (sess_dir / out).exists():
                    a = Artifact(name=f"SC_NOH_{stem}", path=out, kind="tab", from_step=idx)
                    sess.artifacts[a.name] = a
                    produced.append(a)
            if produced:
                sess.current["SC_TABLE"] = produced[0].name
        else:
            a = Artifact(name="SC_NO_HEAVY", path=out_merged, kind="tab", from_step=idx)
            sess.artifacts[a.name] = a
            produced.append(a)
            sess.current["SC_TABLE"] = a.name

        return StepResult(step_index=idx, unit=self.id, params=params, produced=produced)

UNITS: Dict[str, UnitSpec] = {
    "filter_quality": U_FilterQuality(
        id="filter_quality", label="FilterSeq: quality", requires=["R1"],
        params_schema={"qmin":{"type":"int","default":20,"min":0,"max":40}}
    ),
    "filter_length": U_FilterLength(
        id="filter_length", label="FilterSeq: length", requires=["R1"],
        params_schema={"min_len":{"type":"int","default":100,"min":1},
                       "inner":{"type":"select","options":["false","true"],"default":"false"}}
    ),
    "filter_missing": U_FilterMissing(
        id="filter_missing", label="FilterSeq: missing", requires=["R1"],
        params_schema={"max_missing":{"type":"int","default":10,"min":0},
                       "inner":{"type":"select","options":["false","true"],"default":"false"}}
    ),
    "filter_repeats": U_FilterRepeats(
        id="filter_repeats", label="FilterSeq: repeats", requires=["R1"],
        params_schema={"max_repeat":{"type":"text","default":"0.8"},
                       "missing":{"type":"select","options":["false","true"],"default":"false"},
                       "inner":{"type":"select","options":["false","true"],"default":"false"}}
    ),
    "filter_trimqual": U_FilterTrimQual(
        id="filter_trimqual", label="FilterSeq: trimqual", requires=["R1"],
        params_schema={"qmin":{"type":"int","default":20,"min":0,"max":40},
                       "window":{"type":"int","default":10,"min":1},
                       "reverse":{"type":"select","options":["false","true"],"default":"false"}}
    ),
    "filter_maskqual": U_FilterMaskQual(
        id="filter_maskqual", label="FilterSeq: maskqual", requires=["R1"],
        params_schema={"qmin":{"type":"int","default":20,"min":0,"max":40}}
    ),
    "mask_primers":   U_MaskPrimers(
        id="mask_primers", label="MaskPrimers", requires=["R1"],
        params_schema={
            "variant":{"type":"select","options":["align","score","extract"],"default":"align"},
            "mode":{"type":"select","options":["cut","mask","trim","tag"],"default":"mask"},
            "revpr":{"type":"select","options":["false","true"],"default":"false"},
            "v_primers_fname":{"type":"file","accept":".fa,.fasta"},
            "c_primers_fname":{"type":"file","accept":".fa,.fasta","optional":True},
            "start":{"type":"int","default":0,"min":0},
            "length":{"type":"int","default":30,"min":1},
            "pf":{"type":"text","default":"BARCODE","placeholder":"annotation field when mode=tag"}
        }
    ),
    "pairseq":        U_PairSeq(
        id="pairseq", label="PairSeq", requires=["R1","R2"],
        params_schema={"coord":{"type":"select",
                                "options": ["illumina","solexa","sra","454","presto"],
                                "default":"illumina",
                                "help":"Read-ID format used to match paired ends."}}
    ),
    "assemble_align": U_AssembleAlign(
        id="assemble_align", label="AssemblePairs: align", requires=[],
        params_schema={
            "coord": {"type":"select","options":["illumina","solexa","sra","454","presto"],"default":"illumina",
                      "help":"Read-ID format used to match paired ends."},
            "rc": {"type":"select","options":["tail","head","both","none"],"default":"tail",
                   "help":"Which read(s) to reverse-complement before stitching."},
            "alpha": {"type":"text","placeholder":"e.g. 0.01","help":"Significance threshold for de novo assembly."},
            "maxerror": {"type":"text","placeholder":"e.g. 0.2","help":"Maximum allowed error rate for de novo assembly."},
            "minlen": {"type":"int","placeholder":"e.g. 50","help":"Minimum length to scan for overlap."},
            "maxlen": {"type":"int","placeholder":"e.g. 200","help":"Maximum length to scan for overlap."}
        }
    ),
    "assemble_join": U_AssembleJoin(
        id="assemble_join", label="AssemblePairs: join", requires=[],
        params_schema={
            "coord":{"type":"select","options":["illumina","solexa","sra","454","presto"],"default":"illumina",
                     "help":"Read-ID format used to match paired ends."},
            "rc":{"type":"select","options":["tail","head","both","none"],"default":"tail",
                  "help":"Which read(s) to reverse-complement before stitching."},
            "onef":{"type":"text","placeholder":"e.g. BARCODE UMI","help":"Fields to copy from head (R1)."},
            "twof":{"type":"text","placeholder":"e.g. CELL SAMPLE","help":"Fields to copy from tail (R2)."},
            "gap":{"type":"int","placeholder":"e.g. 0","help":"Number of 'N' characters to insert between ends."}
        }
    ),
    "assemble_sequential": U_AssembleSequential(
        id="assemble_sequential", label="AssemblePairs: sequential", requires=[],
        params_schema={
            "coord":{"type":"select","options":["illumina","solexa","sra","454","presto"],"default":"illumina",
                     "help":"Read-ID format used to match paired ends."},
            "rc":{"type":"select","options":["tail","head","both","none"],"default":"tail",
                  "help":"Which read(s) to reverse-complement before stitching."},
            "onef":{"type":"text","placeholder":"e.g. BARCODE UMI","help":"Fields to copy from head (R1)."},
            "twof":{"type":"text","placeholder":"e.g. CELL SAMPLE","help":"Fields to copy from tail (R2)."},
            "alpha":{"type":"text","placeholder":"e.g. 0.01","help":"De novo significance threshold."},
            "maxerror":{"type":"text","placeholder":"e.g. 0.2","help":"Max error rate in de novo step."},
            "minlen":{"type":"int","placeholder":"e.g. 50","help":"Min length to scan for overlap (de novo)."},
            "maxlen":{"type":"int","placeholder":"e.g. 200","help":"Max length to scan for overlap (de novo)."},
            "scanrev":{"type":"select","options":["false","true"],"default":"false",
                       "help":"Allow head to overhang tail (scan past tail end)."},
            "ref_fname":{"type":"file","accept":".fa,.fasta",
                         "help":"REQUIRED reference FASTA (upload, then set to stored name)."},
            "minident":{"type":"text","placeholder":"0..1","help":"Min identity for ref-guided assembly."},
            "evalue":{"type":"text","placeholder":"e.g. 1e-5","help":"Min E-value for ref alignments."},
            "maxhits":{"type":"int","placeholder":"e.g. 10","help":"Max reference hits to inspect."},
            "aligner":{"type":"select","options":["blastn","usearch"],"optional":True,
                       "help":"Local aligner to use for the reference step."}
        }
    ),
    "collapse_seq":   U_CollapseSeq(
        id="collapse_seq", label="CollapseSeq (deduplicate)", requires=[],
        params_schema={
            "act":{"type":"select","options":["min","max","sum","set","majority"],
                   "help":"How to combine per-position qualities when collapsing."}
        }
    ),
    "build_consensus": U_BuildConsensus(
        id="build_consensus", label="BuildConsensus", requires=[],
        params_schema={
            "qmin":{"type":"int"},
            "freq":{"type":"text","placeholder":"0..1"},
            "maxgap":{"type":"text","placeholder":"0..1"},
            "act":{"type":"text","placeholder":"min max sum set majority"},
            "dep":{"type":"select","options":["false","true"],"default":"false"},
            "maxdiv":{"type":"text","placeholder":"e.g. 0.1"},
            "maxerror":{"type":"text","placeholder":"e.g. 0.01"}
        }
    ),
    "sc_merge_samples": U_MergeSamples(
        id="sc_merge_samples",
        label="SC: Merge samples (AIRR TSV)",
        requires=[],
        params_schema={
            "files":{"type":"text","placeholder":"sample1.tsv, sample2.tsv (leave empty = all *.tsv in session)"},
            "aux_types":{"type":"text","placeholder":"v_germline_length=i, d_germline_length=i, j_germline_length=i, day=i"},
            "sample_field":{"type":"text","default":"sample_id","help":"Annotate each row with filename stem; empty to skip"}
        },
    ),
    "sc_filter_productive": U_SC_FilterProductive(
        id="sc_filter_productive",
        label="SC: Keep productive sequences (independent)",
        requires=[],   # <-- no dependency on SC_TABLE
        params_schema={
            "files": {"type":"text","placeholder":"file1.tsv file2.tsv (blank = all *.tsv/*.tsv.gz)"},
            "productive_field": {"type":"text","default":"productive","help":"Column with TRUE/T/1"},
            "fallback_from_airr": {"type":"select","options":["true","false"],"default":"true",
                                "help":"If 'productive' missing, use (vj_in_frame & !stop_codon)"},
            "mode": {"type":"select","options":["merge","per_file"],"default":"merge"},
            "sample_field": {"type":"text","default":"sample_id","help":"Add origin column when merging"}
        },
    ),
    "sc_remove_multi_heavy": U_SC_RemoveMultiHeavy(
        id="sc_remove_multi_heavy",
        label="SC: Remove cells with multiple heavy chains (independent)",
        requires=[],  # fully independent
        params_schema={
            "files": {"type":"text","placeholder":"file1.tsv file2.tsv (blank = all *.tsv/*.tsv.gz)"},
            "locus_field": {"type":"text","default":"locus","help":"Column with chain locus (IGH/IGK/IGL)"},
            "heavy_value": {"type":"text","default":"IGH","help":"Value indicating heavy locus"},
            "cell_field": {"type":"text","default":"cell_id","help":"Cell identifier column (required)"},
            "fallback_from_vcall": {"type":"select","options":["true","false"],"default":"true",
                                    "help":"If locus missing, detect heavy via v_call =~ '^IGH'"},
            "mode": {"type":"select","options":["merge","per_file"],"default":"merge"},
            "sample_field": {"type":"text","default":"sample_id","help":"Add origin column when merging"}
        },
    ),
    "sc_remove_no_heavy": U_SC_RemoveNoHeavy(
        id="sc_remove_no_heavy",
        label="SC: Remove cells without heavy chains (independent)",
        requires=[],  # independent
        params_schema={
            "files": {"type":"text","placeholder":"file1.tsv file2.tsv (blank = all *.tsv/*.tsv.gz)"},
            "locus_field": {"type":"text","default":"locus","help":"Column indicating locus (IGH/IGK/IGL)"},
            "heavy_value": {"type":"text","default":"IGH","help":"Value for heavy locus"},
            "light_values": {"type":"text","default":"IGK, IGL","help":"Values for light loci"},
            "cell_field": {"type":"text","default":"cell_id","help":"Cell identifier column"},
            "fallback_from_vcall": {"type":"select","options":["true","false"],"default":"true",
                                    "help":"If locus missing, infer heavy/light from v_call"},
            "mode": {"type":"select","options":["merge","per_file"],"default":"merge"},
            "sample_field": {"type":"text","default":"sample_id","help":"Add origin column when merging"}
        },
    ),

   

}

# =================== API ===================
@app.post("/session/start")
def start_session():
    sid = str(uuid.uuid4())
    sdir = BASE / sid
    sdir.mkdir(parents=True, exist_ok=True)
    save_state(sdir, SessionState(session_id=sid))
    return {"session_id": sid}

@app.get("/session/{sid}/units")
def list_units(sid: str):
    _ = load_state(BASE / sid)
    return [{"id": u.id, "label": u.label, "requires": u.requires, "params_schema": u.params_schema} for u in UNITS.values()]

@app.post("/session/{sid}/upload")
async def upload_reads(sid: str, r1: UploadFile = File(...), r2: Optional[UploadFile] = File(None)):
    sdir = BASE / sid
    sess = load_state(sdir)

    def save(upload: UploadFile, name: str) -> str:
        path = sdir / name
        with open(path, "wb") as f:
            shutil.copyfileobj(upload.file, f)
        return name

    r1_name = "R1.fastq.gz" if r1.filename.endswith(".gz") else "R1.fastq"
    save(r1, r1_name)
    a1 = Artifact(name="R1_raw", path=r1_name, kind="fastq", channel="R1", from_step=-1)
    sess.artifacts[a1.name] = a1
    sess.current["R1"] = a1.name

    if r2:
        r2_name = "R2.fastq.gz" if r2.filename.endswith(".gz") else "R2.fastq"
        save(r2, r2_name)
        a2 = Artifact(name="R2_raw", path=r2_name, kind="fastq", channel="R2", from_step=-1)
        sess.artifacts[a2.name] = a2
        sess.current["R2"] = a2.name

    save_state(sdir, sess)
    return {"ok": True, "current": sess.current, "artifacts": list(sess.artifacts.keys())}

def _infer_aux_role(fname: str, current_aux: Dict[str,str]) -> str:
    """Heuristic: try to guess whether a primer FASTA is V or C.
       If ambiguous, fill first fasta as V, second as C."""
    l = fname.lower()
    is_fasta = l.endswith((".fa",".fasta",".fna",".fas"))
    if not is_fasta: return "other"
    if any(t in l for t in ["vprimer","v_prim","v-prim","vprimers","_vprimer","-vprimer","ighv","trbv"]):
        return "v_primers"
    if any(t in l for t in ["cprimer","c_prim","c-prim","cprimers","_cprimer","-cprimer","ighc","trbc"]):
        return "c_primers"
    if "v_primers" not in current_aux: return "v_primers"
    if "c_primers" not in current_aux: return "c_primers"
    return "other"

@app.post("/session/{sid}/upload-aux")
async def upload_aux_file(sid: str, file: UploadFile = File(...), name: Optional[str] = Form(None)):
    """Store any helper file (FASTA) and remember primer roles so MaskPrimers auto-fills."""
    sdir = BASE / sid
    sess = load_state(sdir)
    fname = name or file.filename
    with open(sdir / fname, "wb") as f:
        shutil.copyfileobj(file.file, f)
    role = _infer_aux_role(fname, sess.aux or {})
    if role != "other":
        sess.aux[role] = fname
        save_state(sdir, sess)
    return {"stored_as": fname, "role": role, "aux": sess.aux}

class RunBody(BaseModel):
    unit_id: str
    params: Dict[str, Any] = {}

@app.post("/session/{sid}/run")
def run_unit(sid: str, body: RunBody = Body(...)):
    sdir = BASE / sid
    sess = load_state(sdir)
    unit = UNITS.get(body.unit_id)
    if not unit:
        raise HTTPException(404, f"Unknown unit_id '{body.unit_id}'")
    for ch in unit.requires:
        if ch not in sess.current:
            raise HTTPException(400, f"Unit '{unit.id}' requires channel {ch} to be available.")
    step_idx = len(sess.steps)
    try:
        step = unit.run(sess, sdir, body.params)
        sess.steps.append(step)
        save_state(sdir, sess)
        return {"step": step.model_dump(), "current": sess.current, "artifacts": {k:v.model_dump() for k,v in sess.artifacts.items()}}
    except Exception as e:
        prefix = f"{step_idx:03d}_"
        logs = sorted([p for p in sdir.iterdir() if p.name.startswith(prefix) and p.suffix == ".log"])
        tail = ""
        for p in logs:
            try: tail += p.read_text(errors="ignore") + "\n\n"
            except: pass
        if len(tail) > 4000: tail = tail[-4000:]
        raise HTTPException(status_code=500, detail={"error": str(e), "log_tail": tail})

@app.get("/session/{sid}/state")
def get_state(sid: str):
    s = load_state(BASE / sid)
    return s.model_dump()

@app.get("/session/{sid}/download/{artifact_name}")
def download_artifact(sid: str, artifact_name: str):
    sdir = BASE / sid
    s = load_state(sdir)
    a = s.artifacts.get(artifact_name)
    if not a:
        raise HTTPException(404, "Artifact not found")
    path = sdir / a.path
    if not path.exists():
        raise HTTPException(404, "File missing on disk")
    return FileResponse(path, filename=path.name)

@app.get("/session/{sid}/log/{step_index}", response_class=PlainTextResponse)
def get_log(sid: str, step_index: int):
    sdir = BASE / sid
    prefix = f"{int(step_index):03d}_"
    logs = sorted([p for p in sdir.iterdir() if p.name.startswith(prefix)])
    if not logs:
        raise HTTPException(404, "Log not found")
    return "\n\n".join(p.read_text(errors="ignore") for p in logs)
