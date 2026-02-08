"""Base class for processing units."""
import pathlib
from typing import Dict, Any, List, Literal
from pydantic import BaseModel

from app.models import SessionState, StepResult


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
        """
        Execute the unit with given parameters.
        
        Args:
            sess: Session state
            sess_dir: Session directory
            params: Unit parameters
            
        Returns:
            StepResult with produced artifacts
            
        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        raise NotImplementedError

