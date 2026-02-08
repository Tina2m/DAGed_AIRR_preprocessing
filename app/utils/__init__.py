"""Utility functions for the application."""
from .file_utils import (
    ensure_uncompressed_path,
    ensure_uncompressed_artifact,
    require_fastq,
    detect_kind_from_name,
    peek_first_nonempty_char,
    make_canonical_name,
    save_upload_canonical,
    file_existing,
    find_pass_for_prefix,
    assert_channel,
    guess_aux_role,
)
from .command_utils import run_command
from .session_utils import load_state, save_state, get_next_step_index

__all__ = [
    "ensure_uncompressed_path",
    "ensure_uncompressed_artifact",
    "require_fastq",
    "detect_kind_from_name",
    "peek_first_nonempty_char",
    "make_canonical_name",
    "save_upload_canonical",
    "file_existing",
    "find_pass_for_prefix",
    "assert_channel",
    "guess_aux_role",
    "run_command",
    "load_state",
    "save_state",
    "get_next_step_index",
]

