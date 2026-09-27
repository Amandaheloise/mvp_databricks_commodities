# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Transformação Silver
# MAGIC
# MAGIC **Etapa 4.4 do MVP — Carga: construindo o pipeline de ETL (parte 1: limpeza e padronização)**
# MAGIC
# MAGIC Lê a tabela Bronze e produz a camada **Silver**: dados selecionados (apenas as colunas
# MAGIC relevantes para o objetivo do MVP — commodities minerais ligadas ao negócio da Vale),
# MAGIC renomeados, tipados corretamente, sem duplicatas e com regras básicas de qualidade
# MAGIC aplicadas.

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "mvp_vale_commodities"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

df_bronze = spark.table(f"{CATALOG}.{SCHEMA}.bronze_commodity_prices_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Seleção e renomeação
# MAGIC Selecionamos apenas as colunas que respondem ao objetivo do MVP (Etapa 2): preços de
# MAGIC commodities minerais relevantes para as operações da Vale (cobre — produto da mina de
# MAGIC Salobo —, minério de ferro, alumínio, níquel e zinco), além do índice geral de metais
# MAGIC como referência de mercado.

# COMMAND ----------

COLUMN_MAP = {
    "Date": "data_referencia",
    "Metals_Price_Index": "indice_precos_metais",
    "Copper": "preco_cobre_usd_ton",
    "China_import_Iron_Ore_Fines_62_FE_spot": "preco_minerio_ferro_usd_ton",
    "Aluminum": "preco_aluminio_usd_ton",
    "Nickel": "preco_niquel_usd_ton",
    "Zinc": "preco_zinco_usd_ton",
}

# Databricks/Spark normaliza nomes de coluna com caracteres especiais ao inferir schema;
# selecionamos por posição/nome tolerante a variações de underline.
import re

def normalize(colname):
    return re.sub(r"[^A-Za-z0-9]+", "_", colname).strip("_")

norm_to_original = {normalize(c): c for c in df_bronze.columns}

select_exprs = []
for norm_name, final_name in COLUMN_MAP.items():
    original = norm_to_original.get(norm_name, norm_name)
    select_exprs.append(F.col(f"`{original}`").alias(final_name))

df_selected = df_bronze.select(*select_exprs)
df_selected.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tipagem e padronização
# MAGIC - `data_referencia` convertida para `date`
# MAGIC - Colunas de preço convertidas para `double`
# MAGIC - Colunas derivadas `ano` e `mes` para facilitar agregações na camada Gold

# COMMAND ----------

price_cols = [c for c in df_selected.columns if c != "data_referencia"]

df_typed = df_selected.withColumn("data_referencia", F.to_date("data_referencia"))
for c in price_cols:
    df_typed = df_typed.withColumn(c, F.col(c).cast(DoubleType()))

df_typed = (
    df_typed
    .withColumn("ano", F.year("data_referencia"))
    .withColumn("mes", F.month("data_referencia"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Regras de qualidade aplicadas nesta camada
# MAGIC 1. **Consistência**: remover linhas em que `data_referencia` não pôde ser convertida.
# MAGIC 2. **Unicidade**: remover datas duplicadas (mantendo a primeira ocorrência).
# MAGIC 3. **Acurácia**: preços não podem ser negativos — linhas com valor negativo em qualquer
# MAGIC    coluna de preço são descartadas (regra de domínio: preço de commodity é sempre >= 0).
# MAGIC
# MAGIC O detalhamento numérico de quantas linhas foram afetadas por cada regra está no
# MAGIC notebook `03_qualidade_dados`.

# COMMAND ----------

n_antes = df_typed.count()

df_clean = df_typed.filter(F.col("data_referencia").isNotNull())
df_clean = df_clean.dropDuplicates(["data_referencia"])

for c in price_cols:
    df_clean = df_clean.filter((F.col(c).isNull()) | (F.col(c) >= 0))

n_depois = df_clean.count()
print(f"Linhas antes da limpeza: {n_antes} | após limpeza: {n_depois}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Persistência como tabela Delta (camada Silver)

# COMMAND ----------

(
    df_clean.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.silver_commodity_prices")
)

print(f"Tabela criada: {CATALOG}.{SCHEMA}.silver_commodity_prices")
display(spark.table(f"{CATALOG}.{SCHEMA}.silver_commodity_prices").orderBy("data_referencia").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC > **Evidência para o README:** screenshot desta célula (contagem antes/depois da limpeza
# MAGIC > + preview da tabela Silver) e do Catalog Explorer com a tabela `silver_commodity_prices`.
