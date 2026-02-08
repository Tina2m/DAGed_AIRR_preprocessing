"""Command execution utilities."""
import os
import pathlib
import subprocess
from typing import List

from app.config import NPROC_TOOLS


def run_command(
    cmd: List[str],
    cwd: pathlib.Path,
    log_file: pathlib.Path
) -> None:
    """
    Run a command with automatic --nproc addition when supported.
    
    Automatically retries without --nproc if the tool doesn't support it.
    
    Args:
        cmd: Command and arguments
        cwd: Working directory
        log_file: Log file path
        
    Raises:
        RuntimeError: If command fails
    """
    nproc = os.cpu_count() or 2
    tool = pathlib.Path(cmd[0]).name
    final_cmd = list(cmd)
    
    # Add --nproc for supported tools
    if tool in NPROC_TOOLS and "--nproc" not in final_cmd:
        final_cmd += ["--nproc", str(nproc)]

    with open(log_file, "ab") as log:
        log.write(("[CMD] " + " ".join(final_cmd) + "\n").encode())
        proc = subprocess.Popen(final_cmd, cwd=cwd, stdout=log, stderr=log)
        return_code = proc.wait()

    # Auto-retry without --nproc if unrecognized
    if return_code != 0 and "--nproc" in final_cmd:
        try:
            log_text = log_file.read_text(errors="ignore") or ""
            if "unrecognized arguments" in log_text.lower() and "--nproc" in log_text.lower():
                retry_cmd = [x for x in final_cmd if x not in ("--nproc", str(nproc))]
                with open(log_file, "ab") as log:
                    log.write(b"[RETRY] removing --nproc\n")
                    proc2 = subprocess.Popen(retry_cmd, cwd=cwd, stdout=log, stderr=log)
                    if proc2.wait() == 0:
                        return
        except Exception:
            pass

    if return_code != 0:
        raise RuntimeError(f"Command failed ({return_code}): {' '.join(final_cmd)}")

