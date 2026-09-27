# Catálogo de Dados

Documentação de cada tabela e coluna produzidas pelo pipeline, organizadas pela
Arquitetura Medalhão (Bronze → Silver → Gold). Linhagem: todas as tabelas derivam de
`bronze_commodity_prices_raw`, que por sua vez vem da fonte externa descrita no
[README](README.md), seção "Contexto de Negócios e Perguntas".

## Camada Bronze

### `bronze_commodity_prices_raw`
Dado exatamente como recebido da fonte (FMI / datasets.datahub.io), sem nenhuma alteração de
valor. 449 linhas × 64 colunas originais + 2 colunas de controle de ingestão.

| Coluna | Tipo | Descrição | Domínio |
|---|---|---|---|
| `Date` | string | Data de referência do preço, formato `AAAA-MM-DD` (sempre dia 01, dado mensal) | 1980-02-01 a 2017-06-01 |
| `Metals Price Index` ... `Zinc` (61 colunas) | double | Preços/índices mensais de commodities, exatamente como publicados pelo FMI | Varia por commodity, nunca negativo |
| `_ingestion_timestamp` | timestamp | Data/hora em que o notebook de ingestão rodou (metadado de controle) | — |
| `_source` | string | URL da fonte de onde o arquivo foi baixado (metadado de controle) | — |

**Linhagem:** arquivo CSV público, baixado de `https://raw.githubusercontent.com/datasets/commodity-prices/main/data/commodity-prices.csv` (mirror do dataset oficial do FMI mantido pelo projeto Frictionless Data / datasets.datahub.io) e enviado via upload para um Volume do Databricks.

---

## Camada Silver

### `silver_commodity_prices`
Subconjunto das colunas relevantes ao objetivo do MVP (commodities minerais ligadas ao
negócio da Vale), tipadas, sem duplicatas, sem preços negativos. 449 linhas × 9 colunas.

| Coluna | Tipo | Descrição | Domínio |
|---|---|---|---|
| `data_referencia` | date | Data de referência do preço (mês) | 1980-02-01 a 2017-06-01 |
| `indice_precos_metais` | double | Índice de Preços de Metais do FMI (base 2005 = 100), usado como benchmark de mercado | ≥ 0 |
| `preco_cobre_usd_ton` | double | Preço do cobre | USD por tonelada, ≥ 0 |
| `preco_minerio_ferro_usd_ton` | double | Preço do minério de ferro (China import Iron Ore Fines 62% FE spot) | USD por tonelada, ≥ 0 |
| `preco_aluminio_usd_ton` | double | Preço do alumínio | USD por tonelada, ≥ 0 |
| `preco_niquel_usd_ton` | double | Preço do níquel | USD por tonelada, ≥ 0 |
| `preco_zinco_usd_ton` | double | Preço do zinco | USD por tonelada, ≥ 0 |
| `ano` | int | Ano extraído de `data_referencia` | 1980–2017 |
| `mes` | int | Mês extraído de `data_referencia` | 1–12 |

**Linhagem:** derivada de `bronze_commodity_prices_raw` por seleção de colunas, renomeação,
conversão de tipos (`to_date`, `cast double`), remoção de duplicatas de data e remoção de
linhas com preço negativo (notebook `01_transformacao_silver.py`).

---

## Camada Gold

### `gold_dim_commodity`
Dimensão com 1 linha por commodity analisada.

| Coluna | Tipo | Descrição | Domínio |
|---|---|---|---|
| `commodity_id` | int | Chave substituta da dimensão | 1 a 5 |
| `commodity_nome` | string | Nome da commodity em português | Cobre, Minério de Ferro, Alumínio, Níquel, Zinco |
| `coluna_origem` | string | Nome da coluna correspondente na Silver | — |
| `unidade` | string | Unidade de medida do preço | "USD/tonelada" |
| `relevancia_vale` | string | Por que essa commodity importa para o negócio da Vale | — |

**Linhagem:** dimensão criada manualmente no notebook `02_modelagem_gold.py`, mapeando as
5 colunas de commodity da Silver.

### `gold_fato_precos_mensais`
Tabela fato, grão = 1 linha por `data_referencia` × `commodity_id`. 2.245 linhas.

| Coluna | Tipo | Descrição | Domínio |
|---|---|---|---|
| `data_referencia` | date | Data de referência do preço | 1980-02-01 a 2017-06-01 |
| `ano` | int | Ano | 1980–2017 |
| `mes` | int | Mês | 1–12 |
| `commodity_id` | int (FK → `gold_dim_commodity`) | Commodity a que o preço se refere | 1 a 5 |
| `preco_usd_ton` | double | Preço da commodity na data | USD por tonelada, ≥ 0 |

**Linhagem:** derivada de `silver_commodity_prices` via unpivot das 5 colunas de preço para
formato long (notebook `02_modelagem_gold.py`).

### `gold_fato_precos_anuais`
Agregação anual por commodity. 190 linhas (5 commodities × ~38 anos).

| Coluna | Tipo | Descrição |
|---|---|---|
| `ano` | int | Ano de referência |
| `commodity_id` | int (FK → `gold_dim_commodity`) | Commodity |
| `preco_medio` | double | Média dos preços mensais do ano |
| `preco_minimo` | double | Menor preço mensal do ano |
| `preco_maximo` | double | Maior preço mensal do ano |
| `desvio_padrao` | double | Desvio-padrão dos preços mensais do ano |

**Linhagem:** agregação de `gold_fato_precos_mensais` por `ano` e `commodity_id`.

### `gold_fato_variacao_mensal`
Variação percentual mês a mês por commodity. 2.240 linhas (2.245 − 5, pois o primeiro mês de
cada commodity não tem mês anterior para comparação).

| Coluna | Tipo | Descrição |
|---|---|---|
| `data_referencia` | date | Data de referência |
| `ano` | int | Ano |
| `mes` | int | Mês |
| `commodity_id` | int (FK → `gold_dim_commodity`) | Commodity |
| `preco_usd_ton` | double | Preço no mês |
| `variacao_pct_mom` | double | Variação percentual em relação ao mês anterior (month-over-month) |

**Linhagem:** derivada de `gold_fato_precos_mensais` via window function (`LAG`) particionada
por `commodity_id` e ordenada por `data_referencia`.
