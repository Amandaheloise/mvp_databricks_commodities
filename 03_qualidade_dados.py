# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Qualidade de Dados
# MAGIC
# MAGIC **Etapa 4.5 (parte 1) do MVP — Qualidade de Dados**
# MAGIC
# MAGIC Verificação das 5 dimensões de qualidade pedidas na especificação (Completude,
# MAGIC Consistência, Unicidade, Acurácia e Outliers), comparando a camada Bronze (dado bruto)
# MAGIC com a Silver (dado tratado), para deixar explícito o que foi encontrado e como foi
# MAGIC corrigido.

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "mvp_vale_commodities"

# COMMAND ----------

from pyspark.sql import functions as F

df_bronze = spark.table(f"{CATALOG}.{SCHEMA}.bronze_commodity_prices_raw")
df_silver = spark.table(f"{CATALOG}.{SCHEMA}.silver_commodity_prices")

price_cols = [
    "indice_precos_metais", "preco_cobre_usd_ton", "preco_minerio_ferro_usd_ton",
    "preco_aluminio_usd_ton", "preco_niquel_usd_ton", "preco_zinco_usd_ton",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Completude
# MAGIC Percentual de valores nulos por coluna, na camada Silver (após seleção das colunas
# MAGIC relevantes ao objetivo do MVP).

# COMMAND ----------

total = df_silver.count()
completude = df_silver.select(
    [F.round((F.count(F.when(F.col(c).isNull(), c)) / total) * 100, 2).alias(c) for c in price_cols]
)
display(completude)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Consistência
# MAGIC Linhas da camada Bronze em que a coluna de data não pôde ser convertida para um tipo
# MAGIC `date` válido (e, portanto, foram descartadas na Silver).

# COMMAND ----------

n_bronze = df_bronze.count()
n_silver = df_silver.count()
print(f"Linhas Bronze: {n_bronze}")
print(f"Linhas Silver: {n_silver}")
print(f"Linhas descartadas por inconsistência de data ou preço negativo: {n_bronze - n_silver}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Unicidade
# MAGIC Verificação de datas duplicadas na Silver (esperado: zero, pois `dropDuplicates` foi
# MAGIC aplicado no notebook 01).

# COMMAND ----------

duplicatas = (
    df_silver.groupBy("data_referencia").count().filter(F.col("count") > 1).count()
)
print(f"Datas duplicadas remanescentes na Silver: {duplicatas}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Acurácia
# MAGIC Regra de domínio: preço de commodity nunca pode ser negativo. Contagem de violações
# MAGIC remanescentes na Silver (esperado: zero, pois o filtro já foi aplicado no notebook 01).

# COMMAND ----------

acuracia = df_silver.select(
    [F.sum(F.when(F.col(c) < 0, 1).otherwise(0)).alias(c) for c in price_cols]
)
display(acuracia)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Outliers
# MAGIC Identificação de variações mensais extremas (> 3 desvios-padrão da variação percentual
# MAGIC mês a mês) por commodity. Não removemos esses pontos — eles são picos/quedas reais de
# MAGIC mercado (ex.: crise de 2008), não erros de dado — mas é importante sinalizá-los antes da
# MAGIC análise, para não interpretá-los erroneamente como ruído.

# COMMAND ----------

from pyspark.sql.window import Window

w = Window.partitionBy("commodity_id").orderBy("data_referencia")
fato_precos_mensais = spark.table(f"{CATALOG}.{SCHEMA}.gold_fato_precos_mensais")

variacao = (
    fato_precos_mensais
    .withColumn("preco_mes_anterior", F.lag("preco_usd_ton").over(w))
    .withColumn("variacao_pct_mom", (F.col("preco_usd_ton") - F.col("preco_mes_anterior")) / F.col("preco_mes_anterior") * 100)
    .filter(F.col("variacao_pct_mom").isNotNull())
)

stats = variacao.groupBy("commodity_id").agg(
    F.mean("variacao_pct_mom").alias("media"), F.stddev("variacao_pct_mom").alias("desvio")
)

variacao_com_stats = variacao.join(stats, on="commodity_id")
outliers = variacao_com_stats.filter(
    F.abs(F.col("variacao_pct_mom") - F.col("media")) > 3 * F.col("desvio")
)

dim_commodity = spark.table(f"{CATALOG}.{SCHEMA}.gold_dim_commodity")
outliers_nomeados = outliers.join(dim_commodity, "commodity_id").select(
    "data_referencia", "commodity_nome", "preco_usd_ton", "variacao_pct_mom"
)

print(f"Outliers detectados (> 3 desvios-padrão): {outliers.count()}")
display(outliers_nomeados.orderBy(F.desc(F.abs("variacao_pct_mom"))))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo dos problemas encontrados e tratamento aplicado
# MAGIC
# MAGIC | Dimensão | Problema encontrado na Bronze | Tratamento aplicado na Silver |
# MAGIC |---|---|---|
# MAGIC | Completude | Nenhum nulo nas colunas selecionadas (base curada pelo FMI) | Nenhum tratamento necessário |
# MAGIC | Consistência | Nenhuma linha com data inválida | Filtro de segurança aplicado mesmo assim (`isNotNull`) |
# MAGIC | Unicidade | Nenhuma data duplicada na fonte | `dropDuplicates` aplicado como salvaguarda |
# MAGIC | Acurácia | Nenhum preço negativo encontrado | Filtro `>= 0` aplicado como regra de domínio |
# MAGIC | Outliers | Variações mensais extremas em 2008 (crise financeira) e 2005 (boom de commodities) | Mantidos e sinalizados — são eventos de mercado reais, não erros |
# MAGIC
# MAGIC A base do FMI já chega curada (é uma base oficial e amplamente utilizada), por isso os
# MAGIC problemas de completude/consistência/unicidade/acurácia encontrados foram mínimos ou
# MAGIC nulos. Ainda assim, todas as validações foram implementadas no pipeline (e não apenas
# MAGIC verificadas manualmente), para que o processo seja reprodutível caso a fonte de dados
# MAGIC mude no futuro.

# COMMAND ----------

# MAGIC %md
# MAGIC > **Evidência para o README:** screenshot dos resultados de cada célula acima
# MAGIC > (completude, consistência, unicidade, acurácia e outliers).
