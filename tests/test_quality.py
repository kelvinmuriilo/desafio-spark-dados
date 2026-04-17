"""Testes das regras de qualidade de dados."""

import pytest

pytest.importorskip("pyspark.sql")

from pyspark.sql import functions as F

from code.quality import get_invalid_order_ids


def test_get_invalid_order_ids_returns_union_of_quality_failures(spark):
    """Confirma que a consolidação reúne todos os IDs inválidos esperados."""
    df_orders = spark.createDataFrame(
        [
            (1, 10.0, 101),
            (2, -5.0, 101),
            (None, 20.0, 102),
            (4, 30.0, None),
            (5, 40.0, 201),
            (5, 50.0, 202),
            (6, 60.0, 301),
            (6, 60.0, 301),
        ],
        ["id", "value", "client_id"],
    )
    df_clients = spark.createDataFrame(
        [(101, "Ana"), (102, "Bia"), (201, "Caio"), (202, "Duda"), (301, "Enzo")],
        ["id", "name"],
    )

    result = get_invalid_order_ids(df_orders, df_clients)

    invalid_ids = {row["id"] for row in result.collect()}
    assert invalid_ids == {None, 2, 4, 5, 6}


def test_get_invalid_order_ids_deduplicates_repeated_failures(spark):
    """Garante que um mesmo pedido inválido apareça apenas uma vez no resultado."""
    df_orders = spark.createDataFrame(
        [
            (10, -1.0, 1),
            (10, -2.0, 2),
        ],
        ["id", "value", "client_id"],
    )
    df_clients = spark.createDataFrame([(1, "Ana"), (2, "Bia")], ["id", "name"])

    result = get_invalid_order_ids(df_orders, df_clients)

    assert result.filter(F.col("id") == 10).count() == 1
