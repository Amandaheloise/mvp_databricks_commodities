# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Análise de Dados — Respondendo às perguntas de negócio
# MAGIC
# MAGIC **Etapa 4.5 (parte 2) do MVP — Análise Final de Dados**
# MAGIC
# MAGIC Este notebook responde, uma a uma, às perguntas de negócio definidas na Etapa 2
# MAGIC (`docs/README.md`, seção "Contexto de Negócios e Perguntas"), usando as tabelas Gold.

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "mvp_vale_commodities"

# COMMAND ----------

from pyspark.sql import functions as F
import matplotlib.pyplot as plt

fato_precos_mensais = spark.table(f"{CATALOG}.{SCHEMA}.gold_fato_precos_mensais")
fato_precos_anuais = spark.table(f"{CATALOG}.{SCHEMA}.gold_fato_precos_anuais")
fato_variacao_mensal = spark.table(f"{CATALOG}.{SCHEMA}.gold_fato_variacao_mensal")
dim_commodity = spark.table(f"{CATALOG}.{SCHEMA}.gold_dim_commodity")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 1 — Como evoluíram os preços do cobre e do minério de ferro ao longo do tempo (1980–2017)?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT f.data_referencia, c.commodity_nome, f.preco_usd_ton
# MAGIC FROM workspace.mvp_vale_commodities.gold_fato_precos_mensais f
# MAGIC JOIN workspace.mvp_vale_commodities.gold_dim_commodity c USING (commodity_id)
# MAGIC WHERE c.commodity_nome IN ('Cobre', 'Minério de Ferro')
# MAGIC ORDER BY f.data_referencia

# COMMAND ----------

# MAGIC %md
# MAGIC **Resposta:** ambos os preços saem de patamares baixos no início dos anos 1980, ficam
# MAGIC relativamente estáveis até por volta de 2003/2004, e então disparam no chamado
# MAGIC *superciclo das commodities* (2004–2011), impulsionado principalmente pela demanda
# MAGIC chinesa. O cobre encerra o período coberto pela base (meados de 2017) cerca de **+96%** acima do
# MAGIC valor inicial da série, enquanto o minério de ferro sobe cerca de **+376%** — reflexo do
# MAGIC boom de infraestrutura chinesa, que impactou o minério de ferro de forma ainda mais
# MAGIC intensa que o cobre. *(gráfico de referência: `images/01_evolucao_cobre_minerio.png`)*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 2 — Existe correlação entre o preço do cobre e do minério de ferro?

# COMMAND ----------

pivot_pd = (
    fato_precos_mensais.join(dim_commodity, "commodity_id")
    .groupBy("data_referencia")
    .pivot("commodity_nome")
    .agg(F.first("preco_usd_ton"))
    .toPandas()
)
correlacao = pivot_pd[["Cobre", "Minério de Ferro", "Alumínio", "Níquel", "Zinco"]].corr()
print(correlacao.round(3))

# COMMAND ----------

# MAGIC %md
# MAGIC **Resposta:** sim, a correlação entre Cobre e Minério de Ferro é de **≈ 0,86** (forte e
# MAGIC positiva) no período analisado. Isso faz sentido do ponto de vista de negócio: ambos são
# MAGIC metais industriais cuja demanda é fortemente puxada pelos mesmos ciclos macroeconômicos
# MAGIC globais (crescimento industrial, construção civil, demanda chinesa), mesmo vindo de
# MAGIC cadeias produtivas distintas. A correlação de cada metal com o Índice de Preços de Metais
# MAGIC do FMI é ainda mais forte (Cobre ≈ **0,99**, Minério de Ferro ≈ **0,89**), confirmando que
# MAGIC o cobre é um dos principais componentes desse índice.
# MAGIC *(gráfico de referência: `images/02_correlacao_commodities.png`)*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 3 — Qual a volatilidade mensal de cada commodity, e em que períodos ela foi mais alta?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT c.commodity_nome, ROUND(STDDEV(f.variacao_pct_mom), 2) AS volatilidade_desvio_padrao_pct
# MAGIC FROM workspace.mvp_vale_commodities.gold_fato_variacao_mensal f
# MAGIC JOIN workspace.mvp_vale_commodities.gold_dim_commodity c USING (commodity_id)
# MAGIC GROUP BY c.commodity_nome
# MAGIC ORDER BY volatilidade_desvio_padrao_pct DESC

# COMMAND ----------

# MAGIC %md
# MAGIC **Resposta:** o **Níquel** é o mais volátil (desvio-padrão da variação mensal ≈ **8,7 p.p.**),
# MAGIC seguido por Minério de Ferro (≈ 6,9 p.p.), Cobre (≈ 6,3 p.p.), Zinco (≈ 6,2 p.p.) e
# MAGIC Alumínio, o mais estável (≈ 5,5 p.p.). Analisando a volatilidade por ano
# MAGIC (`images/03_volatilidade_anual.png`), os picos mais evidentes de Cobre e Minério de Ferro
# MAGIC coincidem com a crise financeira global de 2008.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 4 — Cobre e minério de ferro acompanham o Índice de Preços de Metais?

# COMMAND ----------

# MAGIC %md
# MAGIC **Resposta:** sim, ambos acompanham de perto o índice (ver Pergunta 2 para as
# MAGIC correlações e `images/04_vs_indice_metais.png` para a série normalizada em base 100).
# MAGIC O Cobre acompanha o índice quase perfeitamente (correlação ≈ 0,99), o que é esperado já
# MAGIC que o cobre tem peso relevante na composição do índice. O Minério de Ferro, apesar de
# MAGIC também correlacionado (≈ 0,89), mostra picos de valorização proporcionalmente maiores que
# MAGIC o índice geral em 2005 e 2008 — períodos de forte demanda siderúrgica chinesa.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 5 — Quais os maiores picos e quedas mensais, e é possível relacioná-los a eventos conhecidos?

# COMMAND ----------

# MAGIC %sql
# MAGIC (SELECT f.data_referencia, c.commodity_nome, ROUND(f.variacao_pct_mom, 1) AS variacao_pct
# MAGIC  FROM workspace.mvp_vale_commodities.gold_fato_variacao_mensal f
# MAGIC  JOIN workspace.mvp_vale_commodities.gold_dim_commodity c USING (commodity_id)
# MAGIC  ORDER BY f.variacao_pct_mom DESC LIMIT 5)
# MAGIC UNION ALL
# MAGIC (SELECT f.data_referencia, c.commodity_nome, ROUND(f.variacao_pct_mom, 1) AS variacao_pct
# MAGIC  FROM workspace.mvp_vale_commodities.gold_fato_variacao_mensal f
# MAGIC  JOIN workspace.mvp_vale_commodities.gold_dim_commodity c USING (commodity_id)
# MAGIC  ORDER BY f.variacao_pct_mom ASC LIMIT 5)

# COMMAND ----------

# MAGIC %md
# MAGIC **Resposta:** as maiores quedas mensais de toda a série se concentram em
# MAGIC **outubro/novembro de 2008** (Níquel -31,7%, Cobre -29,8%, Zinco -25,3%, Cobre novamente
# MAGIC -23,8% no mês seguinte) — coincide exatamente com o auge da crise financeira global
# MAGIC (colapso do Lehman Brothers em setembro de 2008), quando a demanda industrial global
# MAGIC despencou. Já as maiores altas aparecem em **janeiro de 2005 e janeiro de 2008** para o
# MAGIC Minério de Ferro (+71,5% e +66,0%), que correspondem aos reajustes anuais de contratos de
# MAGIC minério de ferro negociados no início de cada ano durante o auge do superciclo de
# MAGIC commodities, quando a demanda chinesa por aço pressionava fortemente os preços.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discussão geral
# MAGIC
# MAGIC A análise confirma a hipótese inicial do objetivo (Etapa 2): os preços de commodities
# MAGIC minerais relevantes para a Vale — especialmente Cobre e Minério de Ferro — se movem de
# MAGIC forma fortemente correlacionada entre si e com o índice geral de metais, respondendo aos
# MAGIC mesmos ciclos macroeconômicos globais. Dois eventos concentram a maior parte dos
# MAGIC movimentos extremos da série: o superciclo de commodities (2004–2011), puxado pela
# MAGIC industrialização chinesa, e a crise financeira de 2008, que provocou as maiores quedas
# MAGIC mensais registradas na base. Do ponto de vista de negócio, isso reforça que o
# MAGIC planejamento de produção e a gestão de risco de uma operação de mineração como a de Salobo
# MAGIC não podem olhar o preço do cobre isoladamente — ele está estruturalmente ligado ao
# MAGIC comportamento do mercado global de metais como um todo.
# MAGIC
# MAGIC Todas as 5 perguntas definidas na Etapa 2 foram respondidas com os dados disponíveis;
# MAGIC nenhuma pergunta ficou sem resposta.
