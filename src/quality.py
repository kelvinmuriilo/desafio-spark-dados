"""Regras de qualidade de dados aplicadas à base de pedidos."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, StringType
from pyspark.sql import SparkSession


def run_data_quality(df_orders: DataFrame, spark_session: SparkSession) -> None:   
    """Exibe o relatório consolidado de problemas de qualidade encontrados."""
    quality_checks = [
        (_get_invalid_order_value(df_orders), "Preço inválido (menor que 0)"),
        (_get_null_order_id(df_orders), "ID do pedido nulo"),
        (_get_null_client_id(df_orders), "ID do cliente nulo"),
        (_get_order_id_with_multiple_clients(df_orders), "Pedido vinculado a múltiplos clientes"),
        (_get_duplicated_order_id(df_orders), "ID do pedido duplicado"),
    ]
    
    df_quality_report = spark_session.createDataFrame([], _get_quality_report_schema())
    
    for df_check, motivo in quality_checks:
        df_quality_report = df_quality_report.union(
            df_check.withColumn("motivo", F.lit(motivo))
        )

    df_quality_report.distinct().orderBy("motivo", "id").show(truncate=False)
    
    print("Problemas de qualidade encontrados: \n")
    df_quality_report.groupBy("motivo").count().orderBy("count", ascending=False).show(truncate=False)

def get_invalid_order_ids(df_orders: DataFrame, df_clients: DataFrame) -> DataFrame:
    """Consolida os IDs de pedidos inválidos para exclusão da base analítica."""
    return (
        _get_invalid_order_value(df_orders)
        .union(_get_null_order_id(df_orders))
        .union(_get_null_client_id(df_orders))
        .union(_get_order_id_with_multiple_clients(df_orders))
        .union(_get_duplicated_order_id(df_orders))
        .distinct()
    )

def _get_invalid_order_value(data_frame: DataFrame) -> DataFrame:
    """Retorna pedidos com valor negativo."""
    invalid_prices = data_frame.filter((data_frame.value < 0))
    return invalid_prices.select("id")

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
