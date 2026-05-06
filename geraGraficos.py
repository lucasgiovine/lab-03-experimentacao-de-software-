import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

sns.set_theme(style="whitegrid")

# Arquivos gerados pelo cálculo
FILE_STATUS = "mediana_por_status.csv"
FILE_REVIEW = "mediana_por_review.csv"
FILE_CORR = "correlacoes_spearman.csv"

# ---------------------------
# GRUPOS DEFINIDOS (balanceados)
# ---------------------------
GRUPOS = {
    "tamanho": [
        "files_changed",
        "lines_changed"
    ],

    "interacoes": [
        "comment_count",
        "participants"
    ],

    "tempo_descricao": [
        "analysis_time_hours",
        "description_length"
    ]
}

# ---------------------------
# 1. STATUS (MERGED vs CLOSED)
# ---------------------------
def plot_status():
    df = pd.read_csv(FILE_STATUS)

    for nome, metrics in GRUPOS.items():
        subset = df[df["metric"].isin(metrics)]

        pivot = subset.pivot(
            index="metric",
            columns="status",
            values="median"
        )

        plt.figure()
        pivot.plot(kind="bar")
        plt.title(f"Status (MERGED vs CLOSED) - {nome}")
        plt.ylabel("Mediana")
        plt.xlabel("Métrica")
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f"status_{nome}.png")
        plt.close()

# ---------------------------
# 2. REVIEW BUCKETS
# ---------------------------
def plot_reviews():
    df = pd.read_csv(FILE_REVIEW)

    for nome, metrics in GRUPOS.items():
        subset = df[df["metric"].isin(metrics)]

        pivot = subset.pivot(
            index="metric",
            columns="review_bucket",
            values="median"
        )

        plt.figure()
        pivot.plot(kind="bar")
        plt.title(f"Faixa de Revisões - {nome}")
        plt.ylabel("Mediana")
        plt.xlabel("Métrica")
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f"reviews_{nome}.png")
        plt.close()

# ---------------------------
# 3. HEATMAP DE CORRELAÇÃO
# ---------------------------
def plot_heatmap():
    df = pd.read_csv(FILE_CORR)

    pivot = df.pivot(
        index="variable",
        columns="target",
        values="spearman"
    )

    plt.figure()
    sns.heatmap(pivot, annot=True)
    plt.title("Heatmap de Correlações (Spearman)")
    plt.tight_layout()
    plt.savefig("heatmap_correlacao.png")
    plt.close()

# ---------------------------
# MAIN
# ---------------------------
def main():
    plot_status()
    plot_reviews()
    plot_heatmap()
    print("Gráficos finais gerados com sucesso!")

if __name__ == "__main__":
    main()