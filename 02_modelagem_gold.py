# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Modelagem Gold
# MAGIC
# MAGIC **Etapas 4.3 (Modelagem) e 4.4 (Carga: parte 2 — modelagem final) do MVP**
# MAGIC
# MAGIC Modelo escolhido: **Esquema Estrela (Star Schema)** — uma tabela fato central
# MAGIC (`fato_precos_mensais`) cercada por uma dimensão (`dim_commodity`), mais duas tabelas
# MAGIC fato agregadas (`fato_precos_anuais` e `fato_variacao_mensal`) que sustentam diretamente
# MAGIC as perguntas de negócio definidas na Etapa 2.
# MAGIC
# MAGIC O catálogo de dados completo (descrição de cada tabela e coluna) está documentado em
# MAGIC `docs/catalogo_de_dados.md`.

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "mvp_vale_commodities"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

df_silver = spark.table(f"{CATALOG}.{SCHEMA}.silver_commodity_prices")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_commodity
# MAGIC Dimensão com uma linha por commodity, explicando a relevância de cada uma para o
# MAGIC negócio da Vale.

# COMMAND ----------

dim_commodity_data = [
    (1, "Cobre", "preco_cobre_usd_ton", "USD/tonelada", "Principal produto da mina de Salobo (cobre)"),
    (2, "Minério de Ferro", "preco_minerio_ferro_usd_ton", "USD/tonelada", "Principal commodity da Vale globalmente"),
    (3, "Alumínio", "preco_aluminio_usd_ton", "USD/tonelada", "Metal industrial correlato, benchmark de mercado"),
    (4, "Níquel", "preco_niquel_usd_ton", "USD/tonelada", "Segundo maior produto da Vale (mineração de níquel)"),
    (5, "Zinco", "preco_zinco_usd_ton", "USD/tonelada", "Metal industrial correlato, benchmark de mercado"),
]
dim_commodity = spark.createDataFrame(
    dim_commodity_data,
    schema=["commodity_id", "commodity_nome", "coluna_origem", "unidade", "relevancia_vale"],
)

(
    dim_commodity.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.gold_dim_commodity")
)
display(dim_commodity)

# COMMAND ----------

# MAGIC %md
# MAGIC ## fato_precos_mensais
# MAGIC Tabela fato no grão **1 linha = 1 data x 1 commodity**, no formato *long*, o que
# MAGIC facilita filtros, joins com a dimensão e agregações.

# COMMAND ----------

commodity_cols = {
    1: "preco_cobre_usd_ton",
    2: "preco_minerio_ferro_usd_ton",
    3: "preco_aluminio_usd_ton",
    4: "preco_niquel_usd_ton",
    5: "preco_zinco_usd_ton",
}

fato_parts = []
for cid, colname in commodity_cols.items():
    part = (
        df_silver
        .select("data_referencia", "ano", "mes", F.col(colname).alias("preco_usd_ton"))
        .withColumn("commodity_id", F.lit(cid))
        .filter(F.col("preco_usd_ton").isNotNull())
    )
    fato_parts.append(part)

fato_precos_mensais = fato_parts[0]
for part in fato_parts[1:]:
    fato_precos_mensais = fato_precos_mensais.unionByName(part)

fato_precos_mensais = fato_precos_mensais.select(
    "data_referencia", "ano", "mes", "commodity_id", "preco_usd_ton"
)

(
    fato_precos_mensais.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.gold_fato_precos_mensais")
)
print(f"fato_precos_mensais: {fato_precos_mensais.count()} linhas")
display(fato_precos_mensais.orderBy("data_referencia").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## fato_precos_anuais
# MAGIC Agregação anual (média, mínimo, máximo, desvio padrão) por commodity — usada para
# MAGIC responder à pergunta sobre evolução dos preços ao longo do tempo.

# COMMAND ----------

fato_precos_anuais = (
    fato_precos_mensais.groupBy("ano", "commodity_id")
    .agg(
        F.mean("preco_usd_ton").alias("preco_medio"),
        F.min("preco_usd_ton").alias("preco_minimo"),
        F.max("preco_usd_ton").alias("preco_maximo"),
        F.stddev("preco_usd_ton").alias("desvio_padrao"),
    )
)

(
    fato_precos_anuais.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.gold_fato_precos_anuais")
)
print(f"fato_precos_anuais: {fato_precos_anuais.count()} linhas")
display(fato_precos_anuais.orderBy("ano").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## fato_variacao_mensal
# MAGIC Variação percentual mês a mês (`LAG` via window function) por commodity — usada para
# MAGIC responder à pergunta sobre volatilidade e maiores altas/quedas.

# COMMAND ----------

w = Window.partitionBy("commodity_id").orderBy("data_referencia")

fato_variacao_mensal = (
    fato_precos_mensais
    .withColumn("preco_mes_anterior", F.lag("preco_usd_ton").over(w))
    .withColumn(
        "variacao_pct_mom",
        (F.col("preco_usd_ton") - F.col("preco_mes_anterior")) / F.col("preco_mes_anterior") * 100,
    )
    .filter(F.col("variacao_pct_mom").isNotNull())
    .select("data_referencia", "ano", "mes", "commodity_id", "preco_usd_ton", "variacao_pct_mom")
)

(
    fato_variacao_mensal.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.gold_fato_variacao_mensal")
)
print(f"fato_variacao_mensal: {fato_variacao_mensal.count()} linhas")
display(fato_variacao_mensal.orderBy(F.desc("variacao_pct_mom")).limit(10))

# COMMAND ----------

