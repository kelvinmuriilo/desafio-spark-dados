"""Testes para as configurações compartilhadas do projeto."""

from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from config import BASE_DIR


def test_base_dir_points_to_project_root():
    """Garante que BASE_DIR aponta para a raiz correta do desafio."""
    expected_dir = Path(__file__).resolve().parents[1]
    assert BASE_DIR == expected_dir
