"""Pydantic models for the application."""
from typing import Optional, Dict, List, Literal, Any
from pydantic import BaseModel
import pathlib


class Artifact(BaseModel):
    """Represents a file artifact produced by a processing step."""
    name: str
    path: str
    kind: Literal["fastq", "fasta", "tab", "log", "other"] = "other"
    channel: Optional[Literal["R1", "R2"]] = None
    from_step: int


class StepResult(BaseModel):
    """Result of executing a processing unit."""
    step_index: int
    unit: str
    params: Dict[str, Any]
    produced: List[Artifact]


class SessionState(BaseModel):
    """State of a processing session."""
    session_id: str
    owner_user_id: Optional[str] = None
    owner_username: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    steps: List[StepResult] = []
    artifacts: Dict[str, Artifact] = {}
    current: Dict[str, str] = {}  # channel -> artifact-name
    aux: Dict[str, str] = {}      # e.g. {"v_primers": "Greiff2014_VPrimers.fasta"}
    aux_files: List[str] = []
    stats: Dict[str, Dict[str, Dict[str, Optional[int]]]] = {}


class UnitSpec(BaseModel):
    """Base specification for a processing unit."""
    id: str
    label: str
    requires: List[str]
    params_schema: Dict[str, Any]
    group: Literal["bulk", "sc"] = "bulk"

    def run(
        self,
        sess: SessionState,
        sess_dir: pathlib.Path,
        params: Dict[str, Any]
    ) -> StepResult:
        """Execute the unit with given parameters."""
        raise NotImplementedError


class RunBody(BaseModel):
    """Request body for running a unit."""
    unit_id: str
    params: Dict[str, Any] = {}


class AuthBody(BaseModel):
    """Request body for registering/logging in."""
    username: str
    password: str

