"""
Script de VALIDAÇÃO LOCAL do pipeline (pandas), espelhando exatamente a lógica
dos notebooks PySpark que serão executados no Databricks (pasta /notebooks).

Objetivo deste script: provar que a lógica bronze -> silver -> gold -> qualidade
-> análise funciona antes de rodar no Databricks, e gerar os gráficos/tabelas
que ilustram o README. Não é o entregável do MVP (o entregável são os notebooks
PySpark em /notebooks, executados no Databricks).
"""
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import json
import os

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BRONZE = os.path.join(BASE, "data", "bronze", "commodity_prices_raw.csv")
SILVER_DIR = os.path.join(BASE, "data", "silver")
GOLD_DIR = os.path.join(BASE, "data", "gold")
IMG_DIR = os.path.join(BASE, "images")
DOCS_DIR = os.path.join(BASE, "docs")
for d in [SILVER_DIR, GOLD_DIR, IMG_DIR, DOCS_DIR]:
    os.makedirs(d, exist_ok=True)

# ---------------------------------------------------------------------------
# BRONZE (lido exatamente como veio da fonte, sem alteração)
# ---------------------------------------------------------------------------
bronze = pd.read_csv(BRONZE)
print(f"[BRONZE] linhas={len(bronze)} colunas={len(bronze.columns)}")

# Colunas de interesse para o negócio da Vale (commodities minerais)
COLS_MAP = {
    "Date": "data_referencia",
    "Metals Price Index": "indice_precos_metais",
    "Copper": "preco_cobre_usd_ton",
    "China import Iron Ore Fines 62% FE spot": "preco_minerio_ferro_usd_ton",
    "Aluminum": "preco_aluminio_usd_ton",
    "Nickel": "preco_niquel_usd_ton",
    "Zinc": "preco_zinco_usd_ton",
}

# ---------------------------------------------------------------------------
# SILVER: seleção, renomeação, tipagem, limpeza e padronização
# ---------------------------------------------------------------------------
silver = bronze[list(COLS_MAP.keys())].rename(columns=COLS_MAP).copy()
silver["data_referencia"] = pd.to_datetime(silver["data_referencia"], errors="coerce")

quality_log = []

# Completude: nulos antes do tratamento
null_counts_before = silver.isna().sum().to_dict()

# Unicidade: duplicatas de data
dup_before = silver.duplicated(subset=["data_referencia"]).sum()
silver = silver.drop_duplicates(subset=["data_referencia"]).copy()

# Consistência: garantir tipos numéricos
for c in COLS_MAP.values():
    if c == "data_referencia":
        continue
    silver[c] = pd.to_numeric(silver[c], errors="coerce")

# Remover linhas sem data válida (não é possível ordenar/analisar série temporal sem data)
n_before_date_drop = len(silver)
silver = silver.dropna(subset=["data_referencia"]).copy()
n_after_date_drop = len(silver)

silver = silver.sort_values("data_referencia").reset_index(drop=True)
silver["ano"] = silver["data_referencia"].dt.year
silver["mes"] = silver["data_referencia"].dt.month

# Acurácia / Outliers: preços não podem ser negativos (regra de domínio)
neg_counts = {}
for c in ["preco_cobre_usd_ton", "preco_minerio_ferro_usd_ton", "preco_aluminio_usd_ton",
          "preco_niquel_usd_ton", "preco_zinco_usd_ton", "indice_precos_metais"]:
    neg = (silver[c] < 0).sum()
    neg_counts[c] = int(neg)

null_counts_after = silver.isna().sum().to_dict()

silver.to_csv(os.path.join(SILVER_DIR, "commodity_prices_silver.csv"), index=False)
print(f"[SILVER] linhas={len(silver)} colunas={len(silver.columns)} "
      f"duplicatas_removidas={dup_before} linhas_sem_data_removidas={n_before_date_drop - n_after_date_drop}")

# ---------------------------------------------------------------------------
# GOLD: modelagem dimensional (fato + dimensão) para responder as perguntas
# ---------------------------------------------------------------------------
# dim_commodity
dim_commodity = pd.DataFrame([
    {"commodity_id": 1, "commodity_nome": "Cobre", "coluna_origem": "preco_cobre_usd_ton", "unidade": "USD/tonelada", "relevancia_vale": "Principal produto da mina de Salobo (cobre)"},
    {"commodity_id": 2, "commodity_nome": "Minério de Ferro", "coluna_origem": "preco_minerio_ferro_usd_ton", "unidade": "USD/tonelada", "relevancia_vale": "Principal commodity da Vale globalmente"},
    {"commodity_id": 3, "commodity_nome": "Alumínio", "coluna_origem": "preco_aluminio_usd_ton", "unidade": "USD/tonelada", "relevancia_vale": "Metal industrial correlato, benchmark de mercado"},
    {"commodity_id": 4, "commodity_nome": "Níquel", "coluna_origem": "preco_niquel_usd_ton", "unidade": "USD/tonelada", "relevancia_vale": "Segundo maior produto da Vale (mineração de níquel)"},
    {"commodity_id": 5, "commodity_nome": "Zinco", "coluna_origem": "preco_zinco_usd_ton", "unidade": "USD/tonelada", "relevancia_vale": "Metal industrial correlato, benchmark de mercado"},
])
dim_commodity.to_csv(os.path.join(GOLD_DIR, "dim_commodity.csv"), index=False)

# fato_precos_mensais (formato long: uma linha por data x commodity)
long_rows = []
name_to_id = dict(zip(dim_commodity["coluna_origem"], dim_commodity["commodity_id"]))
for col, cid in name_to_id.items():
    tmp = silver[["data_referencia", "ano", "mes", col]].rename(columns={col: "preco_usd_ton"})
    tmp["commodity_id"] = cid
    long_rows.append(tmp)
fato_precos_mensais = pd.concat(long_rows, ignore_index=True)
fato_precos_mensais = fato_precos_mensais.dropna(subset=["preco_usd_ton"])
fato_precos_mensais = fato_precos_mensais[["data_referencia", "ano", "mes", "commodity_id", "preco_usd_ton"]]
fato_precos_mensais.to_csv(os.path.join(GOLD_DIR, "fato_precos_mensais.csv"), index=False)

# fato_precos_anuais: agregações (média, min, max, desvio padrão) por ano e commodity
fato_precos_anuais = (
    fato_precos_mensais.groupby(["ano", "commodity_id"])["preco_usd_ton"]
    .agg(preco_medio="mean", preco_minimo="min", preco_maximo="max", desvio_padrao="std")
    .reset_index()
)
fato_precos_anuais.to_csv(os.path.join(GOLD_DIR, "fato_precos_anuais.csv"), index=False)

# fato_volatilidade_mensal: variação percentual mês a mês por commodity
fato_precos_mensais_sorted = fato_precos_mensais.sort_values(["commodity_id", "data_referencia"])
fato_precos_mensais_sorted["variacao_pct_mom"] = (
    fato_precos_mensais_sorted.groupby("commodity_id")["preco_usd_ton"].pct_change() * 100
)
fato_volatilidade = fato_precos_mensais_sorted.dropna(subset=["variacao_pct_mom"])
fato_volatilidade.to_csv(os.path.join(GOLD_DIR, "fato_variacao_mensal.csv"), index=False)

print(f"[GOLD] fato_precos_mensais={len(fato_precos_mensais)} linhas | "
      f"fato_precos_anuais={len(fato_precos_anuais)} linhas | "
      f"fato_variacao_mensal={len(fato_volatilidade)} linhas")

# ---------------------------------------------------------------------------
# QUALIDADE DE DADOS: relatório
# ---------------------------------------------------------------------------
quality_report = {
    "completude_nulos_antes_tratamento": null_counts_before,
    "completude_nulos_apos_tratamento": null_counts_after,
    "unicidade_duplicatas_de_data_removidas": int(dup_before),
    "consistencia_linhas_sem_data_valida_removidas": int(n_before_date_drop - n_after_date_drop),
    "acuracia_valores_negativos_encontrados": neg_counts,
    "periodo_coberto": {
        "inicio": str(silver["data_referencia"].min().date()),
        "fim": str(silver["data_referencia"].max().date()),
    },
    "total_linhas_bronze": int(len(bronze)),
    "total_linhas_silver": int(len(silver)),
}
with open(os.path.join(DOCS_DIR, "quality_report.json"), "w", encoding="utf-8") as f:
    json.dump(quality_report, f, indent=2, ensure_ascii=False, default=str)
print("[QUALIDADE] relatório salvo em docs/quality_report.json")
print(json.dumps(quality_report, indent=2, ensure_ascii=False, default=str))

# ---------------------------------------------------------------------------
# ANÁLISE: gráficos para responder as perguntas de negócio
# ---------------------------------------------------------------------------
plt.style.use("default")
COMMODITY_COLORS = {1: "#B87333", 2: "#4A4A4A", 3: "#A9A9A9", 4: "#5B9BD5", 5: "#7F8C8D"}
id_to_name = dict(zip(dim_commodity["commodity_id"], dim_commodity["commodity_nome"]))

# Gráfico 1: evolução dos preços de cobre e minério de ferro
fig, ax = plt.subplots(figsize=(11, 5.5))
for cid in [1, 2]:
    sub = fato_precos_mensais[fato_precos_mensais["commodity_id"] == cid]
    ax.plot(sub["data_referencia"], sub["preco_usd_ton"], label=id_to_name[cid], color=COMMODITY_COLORS[cid], linewidth=1.6)
ax.set_title("Evolução dos preços de Cobre e Minério de Ferro (1980-2017)")
ax.set_xlabel("Data")
ax.set_ylabel("Preço (USD/tonelada)")
ax.legend()
ax.grid(alpha=0.3)
fig.tight_layout()
fig.savefig(os.path.join(IMG_DIR, "01_evolucao_cobre_minerio.png"), dpi=130)
plt.close(fig)

# Gráfico 2: correlação entre cobre e minério de ferro
pivot = fato_precos_mensais.pivot_table(index="data_referencia", columns="commodity_id", values="preco_usd_ton")
pivot = pivot.rename(columns=id_to_name)
corr_matrix = pivot[["Cobre", "Minério de Ferro", "Alumínio", "Níquel", "Zinco"]].corr()
corr_matrix.to_csv(os.path.join(DOCS_DIR, "correlacao_commodities.csv"))

fig, ax = plt.subplots(figsize=(6.5, 5.5))
im = ax.imshow(corr_matrix, cmap="RdYlBu_r", vmin=-1, vmax=1)
ax.set_xticks(range(len(corr_matrix.columns)))
ax.set_xticklabels(corr_matrix.columns, rotation=45, ha="right")
ax.set_yticks(range(len(corr_matrix.columns)))
ax.set_yticklabels(corr_matrix.columns)
for i in range(len(corr_matrix)):
    for j in range(len(corr_matrix)):
        ax.text(j, i, f"{corr_matrix.iloc[i, j]:.2f}", ha="center", va="center", fontsize=9)
ax.set_title("Matriz de correlação entre commodities minerais")
fig.colorbar(im, ax=ax, fraction=0.046)
fig.tight_layout()
fig.savefig(os.path.join(IMG_DIR, "02_correlacao_commodities.png"), dpi=130)
plt.close(fig)

# Gráfico 3: volatilidade mensal (desvio padrão da variação % por ano) - cobre x minério
vol_by_year = (
    fato_volatilidade[fato_volatilidade["commodity_id"].isin([1, 2])]
    .assign(commodity_nome=lambda d: d["commodity_id"].map(id_to_name))
    .groupby(["ano", "commodity_nome"])["variacao_pct_mom"]
    .std()
    .reset_index()
)
fig, ax = plt.subplots(figsize=(11, 5))
for name, color in [("Cobre", COMMODITY_COLORS[1]), ("Minério de Ferro", COMMODITY_COLORS[2])]:
    sub = vol_by_year[vol_by_year["commodity_nome"] == name]
    ax.plot(sub["ano"], sub["variacao_pct_mom"], label=name, color=color, marker="o", markersize=3, linewidth=1.4)
ax.set_title("Volatilidade anual (desvio-padrão da variação % mensal)")
ax.set_xlabel("Ano")
ax.set_ylabel("Volatilidade (p.p.)")
ax.legend()
ax.grid(alpha=0.3)
fig.tight_layout()
fig.savefig(os.path.join(IMG_DIR, "03_volatilidade_anual.png"), dpi=130)
plt.close(fig)

# Gráfico 4: cobre e minério de ferro vs índice de metais (normalizado, base 100)
fig, ax = plt.subplots(figsize=(11, 5.5))
base = silver.dropna(subset=["indice_precos_metais", "preco_cobre_usd_ton", "preco_minerio_ferro_usd_ton"]).iloc[0]
norm = silver.dropna(subset=["indice_precos_metais"]).copy()
norm["indice_norm"] = norm["indice_precos_metais"] / norm["indice_precos_metais"].iloc[0] * 100
norm_cu = silver.dropna(subset=["preco_cobre_usd_ton"]).copy()
norm_cu["cobre_norm"] = norm_cu["preco_cobre_usd_ton"] / norm_cu["preco_cobre_usd_ton"].iloc[0] * 100
norm_fe = silver.dropna(subset=["preco_minerio_ferro_usd_ton"]).copy()
norm_fe["fe_norm"] = norm_fe["preco_minerio_ferro_usd_ton"] / norm_fe["preco_minerio_ferro_usd_ton"].iloc[0] * 100
ax.plot(norm["data_referencia"], norm["indice_norm"], label="Índice de Preços de Metais", color="#2C3E50", linewidth=1.8)
ax.plot(norm_cu["data_referencia"], norm_cu["cobre_norm"], label="Cobre", color=COMMODITY_COLORS[1], linewidth=1.2, alpha=0.8)
ax.plot(norm_fe["data_referencia"], norm_fe["fe_norm"], label="Minério de Ferro", color=COMMODITY_COLORS[2], linewidth=1.2, alpha=0.8)
ax.set_title("Cobre e Minério de Ferro vs. Índice de Preços de Metais (base 100 = início da série)")
ax.set_xlabel("Data")
ax.set_ylabel("Índice (base 100)")
ax.legend()
ax.grid(alpha=0.3)
fig.tight_layout()
fig.savefig(os.path.join(IMG_DIR, "04_vs_indice_metais.png"), dpi=130)
plt.close(fig)

# ---------------------------------------------------------------------------
# Resultados numéricos para responder as perguntas no README
# ---------------------------------------------------------------------------
results = {}
corr_cu_fe = corr_matrix.loc["Cobre", "Minério de Ferro"]
results["correlacao_cobre_minerio_ferro"] = round(float(corr_cu_fe), 3)

vol_geral = fato_volatilidade.assign(commodity_nome=lambda d: d["commodity_id"].map(id_to_name)).groupby("commodity_nome")["variacao_pct_mom"].std().sort_values(ascending=False)
results["volatilidade_geral_desvio_padrao_pct"] = vol_geral.round(2).to_dict()

maiores_altas = fato_volatilidade.assign(commodity_nome=lambda d: d["commodity_id"].map(id_to_name)).nlargest(5, "variacao_pct_mom")[["data_referencia", "commodity_nome", "variacao_pct_mom"]]
maiores_quedas = fato_volatilidade.assign(commodity_nome=lambda d: d["commodity_id"].map(id_to_name)).nsmallest(5, "variacao_pct_mom")[["data_referencia", "commodity_nome", "variacao_pct_mom"]]
results["maiores_altas_mensais"] = maiores_altas.to_dict(orient="records")
results["maiores_quedas_mensais"] = maiores_quedas.to_dict(orient="records")

corr_cu_idx = pivot["Cobre"].corr(silver.set_index("data_referencia")["indice_precos_metais"].reindex(pivot.index))
corr_fe_idx = pivot["Minério de Ferro"].corr(silver.set_index("data_referencia")["indice_precos_metais"].reindex(pivot.index))
results["correlacao_cobre_indice_metais"] = round(float(corr_cu_idx), 3)
results["correlacao_minerio_indice_metais"] = round(float(corr_fe_idx), 3)

crescimento_cobre = (silver["preco_cobre_usd_ton"].iloc[-1] / silver.dropna(subset=["preco_cobre_usd_ton"])["preco_cobre_usd_ton"].iloc[0] - 1) * 100
crescimento_fe = (silver.dropna(subset=["preco_minerio_ferro_usd_ton"])["preco_minerio_ferro_usd_ton"].iloc[-1] / silver.dropna(subset=["preco_minerio_ferro_usd_ton"])["preco_minerio_ferro_usd_ton"].iloc[0] - 1) * 100
results["crescimento_pct_periodo_cobre"] = round(float(crescimento_cobre), 1)
results["crescimento_pct_periodo_minerio_ferro"] = round(float(crescimento_fe), 1)

with open(os.path.join(DOCS_DIR, "analysis_results.json"), "w", encoding="utf-8") as f:
    json.dump(results, f, indent=2, ensure_ascii=False, default=str)

print("\n[ANÁLISE] resultados:")
print(json.dumps(results, indent=2, ensure_ascii=False, default=str))
print("\nPipeline de validação local concluído com sucesso.")
