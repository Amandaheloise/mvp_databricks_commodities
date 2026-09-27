# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Ingestão Bronze
# MAGIC
# MAGIC **Etapa 4.2 do MVP — Coleta: trazendo os dados para a nuvem**
# MAGIC
# MAGIC Este notebook lê o arquivo bruto `commodity-prices.csv` (International Monetary Fund /
# MAGIC datasets.datahub.io, via mirror público em GitHub) exatamente como ele chegou da fonte,
# MAGIC sem nenhuma transformação, e persiste como tabela Delta na camada **Bronze**.
# MAGIC
# MAGIC ### Como subir o arquivo para o Databricks
# MAGIC 1. Baixe o CSV de: `https://raw.githubusercontent.com/datasets/commodity-prices/main/data/commodity-prices.csv`
# MAGIC 2. No Databricks, vá em **Catalog > (seu catálogo) > Create > Volume** (ou use um volume já existente).
# MAGIC 3. Dentro do volume, crie a pasta `bronze_raw` e faça upload do arquivo `commodity-prices.csv`
# MAGIC    (Catalog Explorer > seu volume > Upload to this volume).
# MAGIC 4. Ajuste a variável `VOLUME_PATH` abaixo para o caminho do seu volume.

# COMMAND ----------

# Ajuste estes três valores para o seu ambiente Databricks (Catalog > Schema > Volume)
CATALOG = "workspace"
SCHEMA = "mvp_vale_commodities"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/bronze_raw/commodity-prices.csv"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Leitura do CSV bruto
# MAGIC Nenhum tratamento é feito aqui: se uma coluna veio com nulos ou nomes estranhos, ela
# MAGIC entra exatamente assim na camada Bronze. O objetivo é rastreabilidade total até a fonte.

# COMMAND ----------

df_bronze_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(VOLUME_PATH)
)

print(f"Linhas: {df_bronze_raw.count()} | Colunas: {len(df_bronze_raw.columns)}")
df_bronze_raw.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Padronização mínima dos nomes de coluna
# MAGIC O Delta Lake não aceita espaços nem caracteres especiais (`,;{}()\n\t=`) em nomes de
# MAGIC coluna. Isso **não é uma transformação de valor** — os valores continuam exatamente
# MAGIC como vieram da fonte — apenas o nome técnico da coluna é normalizado (espaços e
# MAGIC caracteres especiais viram `_`), para que a tabela possa ser persistida como Delta.
# MAGIC O nome original de cada coluna fica registrado nos comentários abaixo, para
# MAGIC rastreabilidade.

# COMMAND ----------

import re

def normalize_col(colname):
    return re.sub(r"[^A-Za-z0-9]+", "_", colname).strip("_")

rename_map = {c: normalize_col(c) for c in df_bronze_raw.columns}
df_bronze = df_bronze_raw.toDF(*[rename_map[c] for c in df_bronze_raw.columns])

for original, novo in rename_map.items():
    if original != novo:
        print(f"'{original}' -> '{novo}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Metadados de controle
# MAGIC Adicionamos apenas metadados de ingestão (data de carga e fonte), sem alterar nenhum
# MAGIC valor original — é o único acréscimo permitido na camada Bronze.

# COMMAND ----------

from pyspark.sql import functions as F

df_bronze_ctrl = (
    df_bronze
    .withColumn("_ingestion_timestamp", F.current_timestamp())
    .withColumn("_source", F.lit("https://raw.githubusercontent.com/datasets/commodity-prices/main/data/commodity-prices.csv"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Persistência como tabela Delta (camada Bronze)

# COMMAND ----------

(
    df_bronze_ctrl.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.bronze_commodity_prices_raw")
)

print(f"Tabela criada: {CATALOG}.{SCHEMA}.bronze_commodity_prices_raw")
display(spark.table(f"{CATALOG}.{SCHEMA}.bronze_commodity_prices_raw").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC > **Evidência para o README (item 7/8 da especificação):** tire um screenshot desta
# MAGIC > célula (contagem de linhas + preview da tabela) e outro do Catalog Explorer mostrando
# MAGIC > a tabela `bronze_commodity_prices_raw` criada no schema.
