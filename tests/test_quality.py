"""Testes das regras de qualidade de dados."""

from contextlib import redirect_stdout
from decimal import Decimal
from io import StringIO
from pathlib import Path
import sys

import pytest

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

pytest.importorskip("pyspark.sql")

from pyspark.sql import functions as F

from quality import build_quality_report, get_invalid_order_ids, run_data_quality


def test_build_quality_report_returns_ids_with_reasons(spark):
    """Confirma que o relatório de qualidade contém id e motivo das falhas."""
    df_orders = spark.createDataFrame(
        [
            (1, Decimal("-5.00"), 100),
            (2, None, 100),
            (3, Decimal("10.00"), 999),
        ],
        ["id", "value", "client_id"],
    )
    df_clients = spark.createDataFrame([(100, "Ana")], ["id", "name"])

    result = build_quality_report(df_orders, df_clients, spark)

    rows = {(row["id"], row["motivo"]) for row in result.collect()}
    assert rows == {
        (1, "Preço inválido (menor que 0)"),
        (2, "Valor do pedido nulo"),
        (3, "Cliente não encontrado"),
    }


def test_get_invalid_order_ids_returns_union_of_quality_failures(spark):
    """Confirma que a consolidação reúne todos os IDs inválidos esperados."""
    df_orders = spark.createDataFrame(
        [
            (1, Decimal("10.00"), 101),
            (2, Decimal("-5.00"), 101),
            (3, None, 101),
            (None, Decimal("20.00"), 102),
            (4, Decimal("30.00"), None),
            (5, Decimal("40.00"), 201),
            (5, Decimal("50.00"), 202),
            (6, Decimal("60.00"), 301),
            (6, Decimal("60.00"), 301),
            (7, Decimal("70.00"), 999),
        ],
        ["id", "value", "client_id"],
    )
    df_clients = spark.createDataFrame(
        [(101, "Ana"), (102, "Bia"), (201, "Caio"), (202, "Duda"), (301, "Enzo")],
        ["id", "name"],
    )

    df_quality_report = build_quality_report(df_orders, df_clients, spark)
    result = get_invalid_order_ids(df_quality_report)

    invalid_ids = {row["id"] for row in result.collect()}
    assert invalid_ids == {None, 2, 3, 4, 5, 6, 7}


def test_get_invalid_order_ids_deduplicates_repeated_failures(spark):
    """Garante que um mesmo pedido inválido apareça apenas uma vez no resultado."""
    df_orders = spark.createDataFrame(
        [
            (10, Decimal("-1.00"), 1),
            (10, Decimal("-2.00"), 2),
        ],
        ["id", "value", "client_id"],
    )
    df_clients = spark.createDataFrame([(1, "Ana"), (2, "Bia")], ["id", "name"])

    df_quality_report = build_quality_report(df_orders, df_clients, spark)
    result = get_invalid_order_ids(df_quality_report)

    assert result.filter(F.col("id") == 10).count() == 1


def test_get_invalid_order_ids_flags_orders_with_unknown_client_fk(spark):
    """Garante que pedidos com client_id inexistente sejam marcados como inválidos."""
    df_orders = spark.createDataFrame(
        [
            (1, Decimal("10.00"), 100),
            (2, Decimal("20.00"), 999),
        ],
        ["id", "value", "client_id"],
    )
    df_clients = spark.createDataFrame([(100, "Ana")], ["id", "name"])

    df_quality_report = build_quality_report(df_orders, df_clients, spark)
    result = get_invalid_order_ids(df_quality_report)

    invalid_ids = {row["id"] for row in result.collect()}
    assert invalid_ids == {2}


def test_run_data_quality_prints_summary_with_quantidade_column(spark):
    """Garante que o resumo de qualidade exiba a coluna Quantidade."""
    df_quality_report = spark.createDataFrame(
        [
            (1, "Preço inválido (menor que 0)"),
            (2, "Cliente não encontrado"),
        ],
        ["id", "motivo"],
    )

    buffer = StringIO()
    with redirect_stdout(buffer):
        run_data_quality(df_quality_report)

    output = buffer.getvalue()
    assert "Problemas de qualidade encontrados:" in output
    assert "Quantidade" in output


def test_get_invalid_order_ids_flags_orders_with_null_value(spark):
    """Garante que pedidos com valor nulo sejam marcados como inválidos."""
    df_orders = spark.createDataFrame(
        [
            (1, Decimal("10.00"), 100),
            (2, None, 100),
        ],
        ["id", "value", "client_id"],
    )
    df_clients = spark.createDataFrame([(100, "Ana")], ["id", "name"])

    df_quality_report = build_quality_report(df_orders, df_clients, spark)
    result = get_invalid_order_ids(df_quality_report)

    invalid_ids = {row["id"] for row in result.collect()}
    assert invalid_ids == {2}
