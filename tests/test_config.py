"""Testes para as configurações compartilhadas do projeto."""

from pathlib import Path

from config import BASE_DIR


def test_base_dir_points_to_project_root():
    """Garante que BASE_DIR aponta para a raiz correta do desafio."""
    expected_dir = Path(__file__).resolve().parents[1]
    assert BASE_DIR == expected_dir
