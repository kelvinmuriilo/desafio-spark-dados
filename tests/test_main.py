"""Smoke test do script principal."""

from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
import runpy
import sys

import pytest

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

pytest.importorskip("pyspark.sql")


def test_main_executes_without_errors():
    """Verifica se o script principal executa e imprime as seções esperadas."""
    src_dir = Path(__file__).resolve().parents[1] / "src"
    main_path = src_dir / "main.py"

    original_sys_path = list(sys.path)
    buffer = StringIO()
    try:
        if str(src_dir) not in sys.path:
            sys.path.insert(0, str(src_dir))
        with redirect_stdout(buffer):
            runpy.run_path(str(main_path), run_name="__main__")
    finally:
        sys.path[:] = original_sys_path

    output = buffer.getvalue()
    assert "1. Pedidos com problemas de qualidade" in output
    assert "3. Estatísticas do valor total de pedidos por cliente" in output
    assert "4. Análise dos clientes com valor total de pedidos acima da média" in output
