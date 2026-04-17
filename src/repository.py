"""Funções de acesso aos arquivos de entrada do desafio."""

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType
from config import BASE_DIR

# Schema esperado para a base de pedidos.
order_schema = StructType([
    StructField("id", LongType(), False),
    StructField("value", DoubleType(), False),
    StructField("client_id", LongType(), False)
])

# Schema esperado para a base de clientes.
client_schema = StructType([
    StructField("id", LongType(), False),
    StructField("name", StringType(), False)
])


def get_orders(spark) -> DataFrame:
    """Lê a base de pedidos aplicando o schema definido para o desafio."""
    json_path = BASE_DIR / "data" / "pedidos" / "data.json"

    return (
        spark.read
        .schema(order_schema)
        .option("multiline", "false")
        .json(str(json_path))
    )


def get_clients(spark) -> DataFrame:
    """Lê a base de clientes aplicando o schema definido para o desafio."""
    json_path = BASE_DIR / "data" / "clients" / "data.json"

    return (
        spark.read
        .schema(client_schema)
        .option("multiline", "false")
        .json(str(json_path))
    )
