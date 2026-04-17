"""Regras de qualidade de dados aplicadas à base de pedidos."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, StringType
from pyspark.sql import SparkSession


def run_data_quality(df_quality_report: DataFrame) -> None:
    """Exibe o relatório consolidado de problemas de qualidade encontrados."""
    df_quality_report.orderBy("motivo", "id").show(100, truncate=False)

    print("Problemas de qualidade encontrados: \n")
    df_quality_report.groupBy("motivo") \
        .count() \
        .withColumnRenamed("motivo", "Motivo") \
        .withColumnRenamed("count", "Quantidade") \
        .orderBy("Quantidade", ascending=False) \
        .show(truncate=False)


def build_quality_report(
    df_orders: DataFrame,
    df_clients: DataFrame,
    spark_session: SparkSession,
) -> DataFrame:
    """Monta o dataframe consolidado com todos os problemas de qualidade."""
    quality_checks = [
        (_get_invalid_order_value(df_orders), "Preço inválido (menor que 0)"),
        (_get_null_order_value(df_orders), "Valor do pedido nulo"),
        (_get_null_order_id(df_orders), "ID do pedido nulo"),
        (_get_null_client_id(df_orders), "ID do cliente nulo"),
        (_get_orders_with_invalid_client_fk(df_orders, df_clients), "Cliente não encontrado"),
        (_get_order_id_with_multiple_clients(df_orders), "Pedido vinculado a múltiplos clientes"),
        (_get_duplicated_order_id(df_orders), "ID do pedido duplicado"),
    ]

    df_quality_report = spark_session.createDataFrame([], _get_quality_report_schema())

    for df_check, motivo in quality_checks:
        df_quality_report = df_quality_report.union(
            df_check.withColumn("motivo", F.lit(motivo))
        )

    return df_quality_report.distinct()


def get_invalid_order_ids(df_quality_report: DataFrame) -> DataFrame:
    """Consolida os IDs de pedidos inválidos para exclusão da base analítica."""
    return df_quality_report.select("id").distinct()

def _get_invalid_order_value(data_frame: DataFrame) -> DataFrame:
    """Retorna pedidos com valor negativo."""
    invalid_prices = data_frame.filter((data_frame.value < 0))
    return invalid_prices.select("id")


def _get_null_order_value(data_frame: DataFrame) -> DataFrame:
    """Retorna pedidos cujo valor está nulo."""
    return (
        data_frame
        .filter(data_frame.value.isNull())
        .select("id")
    )

def _get_null_order_id(data_frame: DataFrame) -> DataFrame:
    """Retorna pedidos cujo identificador está nulo."""
    return (
        data_frame
        .filter(data_frame.id.isNull())
        .select("id")
    )

def _get_null_client_id(data_frame: DataFrame) -> DataFrame:
    """Retorna pedidos sem cliente associado."""
    return (
        data_frame
        .filter(data_frame.client_id.isNull())
        .select("id")
    )


def _get_orders_with_invalid_client_fk(
    df_orders: DataFrame,
    df_clients: DataFrame,
) -> DataFrame:
    """Retorna pedidos cujo cliente não existe na base de clientes."""
    valid_client_ids = df_clients.select(F.col("id").alias("client_id")).distinct()

    return (
        df_orders
        .filter(F.col("client_id").isNotNull())
        .join(valid_client_ids, on="client_id", how="left_anti")
        .select("id")
    )

def _get_order_id_with_multiple_clients(data_frame: DataFrame) -> DataFrame:
    """Retorna pedidos cujo mesmo ID aparece ligado a clientes diferentes."""
    return data_frame.groupBy("id").agg(
        F.countDistinct("client_id").alias("qtd_clientes_distintos"),
    ).filter(
        F.col("qtd_clientes_distintos") > 1
    ).select("id")

def _get_duplicated_order_id(data_frame: DataFrame) -> DataFrame:
    """Retorna IDs de pedido repetidos na base."""
    return (
        data_frame.groupBy("id")
        .count()
        .filter("count > 1")
        .select("id")
    )

def _get_quality_report_schema() -> StructType:
    """Define o schema do relatório consolidado de qualidade."""
    return StructType([
        StructField("id", LongType(), True),
        StructField("motivo", StringType(), True)
    ])
