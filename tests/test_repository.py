"""Testes das funções de leitura de dados."""

from decimal import Decimal
from pathlib import Path
import sys

import pytest

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

pytest.importorskip("pyspark.sql")

from repository import client_schema, get_clients, get_orders, order_schema


def test_get_orders_uses_expected_schema_and_loads_data(spark):
    """Valida o schema e o carregamento mínimo da base de pedidos."""
    df_orders = get_orders(spark)

    assert df_orders.schema == order_schema
    assert df_orders.count() > 0
    assert set(df_orders.columns) == {"id", "value", "client_id"}
    assert isinstance(df_orders.select("value").first()["value"], Decimal)


def test_get_clients_uses_expected_schema_and_loads_data(spark):
    """Valida o schema e o carregamento mínimo da base de clientes."""
    df_clients = get_clients(spark)

    assert df_clients.schema == client_schema
    assert df_clients.count() > 0
    assert set(df_clients.columns) == {"id", "name"}
