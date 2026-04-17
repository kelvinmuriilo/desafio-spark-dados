"""Script principal com a execução das análises do desafio técnico."""

import logging
import warnings
from pyspark.sql import SparkSession

from repository import get_orders, get_clients
from quality import get_invalid_order_ids, run_data_quality
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
print("============================================\n")
print("1. Pedidos com problemas de qualidade")
print("============================================\n")
run_data_quality(df_orders, spark_session=spark)

# Remove da análise todos os pedidos marcados como inválidos.
df_invalid_order_ids = get_invalid_order_ids(df_orders, df_clients)
df_valid_orders = df_orders.join(df_invalid_order_ids, on="id", how="left_anti")

# Consolida os indicadores por cliente para reutilização nas análises seguintes.
df_client_totals = (
    df_valid_orders.join(df_clients, df_valid_orders.client_id == df_clients.id)
    .groupBy(df_clients.id.alias("client_id"), df_clients.name.alias("nome_cliente"))
    .agg(
        F.count(df_valid_orders.id).alias("quantidade_pedidos"),
        F.sum(df_valid_orders.value).cast("decimal(11,2)").alias("valor_total_pedidos")
    )
)

# Exibe a agregação principal: cliente, quantidade de pedidos e valor total.
df_client_totals.orderBy(F.col("valor_total_pedidos").desc()) \
    .select(
        "client_id",
        "nome_cliente",
        "quantidade_pedidos",
        F.format_number(F.col("valor_total_pedidos"), 2).alias("valor_total_pedidos")
    ) \
    .show(truncate=False)

# Mantém apenas os totais por cliente para os cálculos estatísticos.
df_client_totals_value = df_client_totals.select("valor_total_pedidos").orderBy(F.col("valor_total_pedidos"))

print("Média aritmética do valor total de pedidos por cliente:")

average_value = (
    df_client_totals_value
    .agg(F.avg("valor_total_pedidos").alias("average_value"))
    .collect()[0]["average_value"]
)
print(f"Valor médio total de pedidos por cliente: {average_value}")


print("Mediana do valor total de pedidos por cliente:")
print(df_client_totals_value.approxQuantile("valor_total_pedidos", [0.5], 0)[0])


percentil_10 = df_client_totals_value.approxQuantile("valor_total_pedidos", [0.1], 0)[0]
print(f"Percentil 10: {percentil_10}")

percentil_90 = df_client_totals_value.approxQuantile("valor_total_pedidos", [0.9], 0)[0]
print(f"Percentil 90: {percentil_90}")


# Lista os clientes cujo total de pedidos está acima da média aritmética.
print("Clientes com valor total de pedidos acima da média:")
df_client_totals.filter(F.col("valor_total_pedidos") > average_value) \
    .orderBy(F.col("valor_total_pedidos").desc()) \
    .select(
        "client_id",
        "nome_cliente"
    ) \
    .show(truncate=False)
    

# Lista os clientes dentro do intervalo entre os percentis 10 e 90.
print(f"Clientes com valor total de pedidos entre o percentil 10 ({percentil_10}) e o percentil 90 ({percentil_90}):")
df_client_totals.filter((F.col("valor_total_pedidos") >= percentil_10) & (F.col("valor_total_pedidos") <= percentil_90)).show(truncate=False)   


# Finaliza a sessão Spark ao término da execução.
spark.stop()
