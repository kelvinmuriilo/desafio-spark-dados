"""Smoke test do script principal."""

from pathlib import Path
import runpy
import sys

import pytest

pytest.importorskip("pyspark.sql")


def test_main_executes_without_errors():
    """Verifica se o script principal executa sem lançar exceções."""
    src_dir = Path(__file__).resolve().parents[1] / "src"
    main_path = src_dir / "main.py"

    original_sys_path = list(sys.path)
    try:
        if str(src_dir) not in sys.path:
            sys.path.insert(0, str(src_dir))
        runpy.run_path(str(main_path), run_name="__main__")
    finally:
        sys.path[:] = original_sys_path
