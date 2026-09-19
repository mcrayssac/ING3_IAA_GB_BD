"""Check that dashboard commands use the shared Python toolchain."""

import platform
from pathlib import Path
import sys


def test_pinned_cpython():
    """Keep runtime checks aligned with intentional Python patch updates."""
    project_root = Path(__file__).resolve().parents[1]
    expected = (project_root / ".python-version").read_text().strip()
    assert platform.python_implementation() == "CPython"
    assert platform.python_version() == expected


def test_virtual_environment():
    """Reject execution outside a project virtual environment."""
    assert sys.prefix != sys.base_prefix
