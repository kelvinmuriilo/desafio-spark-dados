"""Script principal com a execução das análises do desafio técnico."""

import logging
import warnings
from decimal import Decimal, ROUND_HALF_UP

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, DecimalType

from repository import get_orders, get_clients
from quality import build_quality_report, get_invalid_order_ids, run_data_quality
from pyspark.sql import functions as F

# Inicializa a sessão Spark e reduz o ruído no terminal.
spark = SparkSession.builder \
    .appName("Teste Tecnico - Engenheiro de Dados PySpark") \
    .config("spark.ui.showConsoleProgress", "false") \
    .getOrCreate()

logging.getLogger("py4j").setLevel(logging.ERROR)
warnings.filterwarnings("ignore")
spark.sparkContext.setLogLevel("ERROR")
spark._jvm.org.apache.log4j.LogManager.getRootLogger().setLevel(
    spark._jvm.org.apache.log4j.Level.ERROR
)

# Carrega as bases de pedidos e clientes com schema explícito.
df_orders = get_orders(spark)
df_clients = get_clients(spark)

# Exibe o relatório consolidado de qualidade dos pedidos.
print("\n============================================\n")
print("1. Pedidos com problemas de qualidade")
print("============================================\n")
df_quality_report = build_quality_report(df_orders, df_clients, spark_session=spark)
run_data_quality(df_quality_report)

# Remove da análise todos os pedidos marcados como inválidos.
df_invalid_order_ids = get_invalid_order_ids(df_quality_report)
df_valid_orders = (
    df_orders
    .filter(F.col("id").isNotNull())
    .join(
        df_invalid_order_ids.filter(F.col("id").isNotNull()),
        on="id",
        how="left_anti",
    )
)
orders = df_valid_orders.alias("orders")
clients = df_clients.alias("clients")

# Consolida os indicadores por cliente para reutilização nas análises seguintes.
df_client_totals = (
    orders.join(clients, F.col("orders.client_id") == F.col("clients.id"))
    .groupBy(
        F.col("clients.id").alias("client_id"),
        F.col("clients.name").alias("nome_cliente"),
    )
    .agg(
        F.count(F.col("orders.id")).alias("quantidade_pedidos"),
        F.sum(F.col("orders.value")).cast("decimal(11,2)").alias("valor_total_pedidos")
    )
)

print("\n===============================================================\n")
print("2. ""Análise dos clientes com base no valor total de pedidos")
print("================================================================\n")

# Exibe a agregação principal: cliente, quantidade de pedidos e valor total.
df_client_totals.orderBy(F.col("valor_total_pedidos").desc()) \
    .select(
        "client_id",
        "nome_cliente",
        "quantidade_pedidos",
        F.format_number(F.col("valor_total_pedidos"), 2).alias("valor_total_pedidos")
    ) \
    .show(100, truncate=False)

# Mantém apenas os totais por cliente para os cálculos estatísticos.
df_client_totals_value = df_client_totals.select("valor_total_pedidos")

average_value = (
    df_client_totals_value
    .agg(F.avg("valor_total_pedidos").alias("average_value"))
    .collect()[0]["average_value"]
)
median_value = df_client_totals_value.approxQuantile("valor_total_pedidos", [0.5], 0)[0]
percentil_10 = df_client_totals_value.approxQuantile("valor_total_pedidos", [0.1], 0)[0]
percentil_90 = df_client_totals_value.approxQuantile("valor_total_pedidos", [0.9], 0)[0]

statistics_schema = StructType([
    StructField("Métrica", StringType(), False),
    StructField("Valor", DecimalType(11, 2), False),
])

df_statistics = spark.createDataFrame(
    [
        ("media_aritmetica", average_value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)),
        ("mediana", Decimal(str(median_value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)),
        ("percentil_10", Decimal(str(percentil_10)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)),
        ("percentil_90", Decimal(str(percentil_90)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)),
    ],
    schema=statistics_schema,
)

print("\n===============================================================\n")
print("3. Estatísticas do valor total de pedidos por cliente")
print("================================================================\n")

print("Estatísticas do valor total de pedidos por cliente:")
df_statistics.show(100, truncate=False)

print("\n================================================================\n")
print("4. Análise dos clientes com valor total de pedidos acima da média")
print("=================================================================\n")
# Lista os clientes cujo total de pedidos está acima da média aritmética.
df_clients_above_average = df_client_totals.filter(F.col("valor_total_pedidos") > average_value) \
    .orderBy(F.col("valor_total_pedidos").desc()) \
    .select(
        "client_id",
        "nome_cliente",
        F.format_number(F.col("valor_total_pedidos"), 2).alias("valor_total_pedidos")
    )
df_clients_above_average.show(100, truncate=False)
    

print("\n===============================================================================\n")
print("5. Análise dos clientes com valor total de pedidos entre os percentis 10 e 90")
print("================================================================================\n")
# Lista os clientes dentro do intervalo entre os percentis 10 e 90.
df_clients_between_percentiles = df_client_totals.filter(
    (F.col("valor_total_pedidos") >= percentil_10) & (F.col("valor_total_pedidos") <= percentil_90)
).orderBy(F.col("valor_total_pedidos").desc())
df_clients_between_percentiles.show(100, truncate=False)


# Finaliza a sessão Spark ao término da execução.
spark.stop()
