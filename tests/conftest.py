"""Configurações compartilhadas da suíte de testes."""

from pathlib import Path
import sys

import pytest


# Garante que os módulos da pasta `code` possam ser importados nos testes.
CODE_DIR = Path(__file__).resolve().parents[1] / "code"
if str(CODE_DIR) not in sys.path:
    sys.path.insert(0, str(CODE_DIR))


@pytest.fixture(scope="session")
def spark():
    """Cria uma SparkSession local para os testes que dependem de PySpark."""
    pyspark_sql = pytest.importorskip("pyspark.sql")
    SparkSession = pyspark_sql.SparkSession
    spark_session = (
        SparkSession.builder
        .master("local[1]")
        .appName("teste-tecnico-unit-tests")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark_session.sparkContext.setLogLevel("ERROR")
    yield spark_session
    spark_session.stop()
