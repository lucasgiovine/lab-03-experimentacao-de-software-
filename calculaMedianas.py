import csv
import statistics
from collections import defaultdict
from scipy.stats import spearmanr

CSV_FILE = "prs_dataset.csv"

NUMERIC_COLUMNS = [
    "files_changed",
    "additions",
    "deletions",
    "lines_changed",
    "description_length",
    "review_count",
    "comment_count",
    "participants",
    "analysis_time_hours"
]

def safe_float(value):
    try:
        return float(value)
    except:
        return None

def read_data(filepath):
    rows = []
    with open(filepath, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            parsed = {"status": row["status"]}
            for col in NUMERIC_COLUMNS:
                parsed[col] = safe_float(row[col])
            rows.append(parsed)
    return rows

def median(values):
    values = [v for v in values if v is not None]
    return statistics.median(values) if values else None

# ---------------------------
# Medianas
# ---------------------------
def overall_medians(rows):
    return {
        col: median([r[col] for r in rows])
        for col in NUMERIC_COLUMNS
    }

def medians_by_status(rows):
    grouped = defaultdict(list)
    for r in rows:
        grouped[r["status"]].append(r)

    return {
        status: {
            col: median([r[col] for r in items])
            for col in NUMERIC_COLUMNS
        }
        for status, items in grouped.items()
    }

def review_bucket(review_count):
    if review_count == 1:
        return "1"
    elif 2 <= review_count <= 3:
        return "2-3"
    else:
        return "4+"

def medians_by_review_bucket(rows):
    grouped = defaultdict(list)
    for r in rows:
        if r["review_count"] is not None:
            grouped[review_bucket(r["review_count"])].append(r)

    return {
        bucket: {
            col: median([r[col] for r in items])
            for col in NUMERIC_COLUMNS
        }
        for bucket, items in grouped.items()
    }

# ---------------------------
# Correlação
# ---------------------------
def spearman_with_p(x, y):
    x_clean, y_clean = [], []

    for a, b in zip(x, y):
        if a is not None and b is not None:
            x_clean.append(a)
            y_clean.append(b)

    if len(x_clean) < 2:
        return None, None

    return spearmanr(x_clean, y_clean)

def compute_correlations(rows):
    results = []

    status_numeric = [1 if r["status"] == "MERGED" else 0 for r in rows]

    variables = [
        "files_changed",
        "lines_changed",
        "analysis_time_hours",
        "description_length",
        "comment_count",
        "participants"
    ]

    # vs status
    for var in variables:
        corr, p = spearman_with_p(
            [r[var] for r in rows],
            status_numeric
        )
        results.append({
            "variable": var,
            "target": "status",
            "spearman": corr,
            "p_value": p
        })

    # vs review_count
    for var in variables:
        corr, p = spearman_with_p(
            [r[var] for r in rows],
            [r["review_count"] for r in rows]
        )
        results.append({
            "variable": var,
            "target": "review_count",
            "spearman": corr,
            "p_value": p
        })

    return results

# ---------------------------
# EXPORTAÇÃO
# ---------------------------
def export_overall(data):
    with open("mediana_geral.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["metric", "median"])
        for k, v in data.items():
            writer.writerow([k, v])

def export_by_status(data):
    with open("mediana_por_status.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["status", "metric", "median"])
        for status, metrics in data.items():
            for k, v in metrics.items():
                writer.writerow([status, k, v])

def export_by_review(data):
    with open("mediana_por_review.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["review_bucket", "metric", "median"])
        for bucket, metrics in data.items():
            for k, v in metrics.items():
                writer.writerow([bucket, k, v])

def export_correlations(data):
    with open("correlacoes_spearman.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["variable", "target", "spearman", "p_value"])
        writer.writeheader()
        writer.writerows(data)

# ---------------------------
# MAIN
# ---------------------------
def main():
    rows = read_data(CSV_FILE)

    overall = overall_medians(rows)
    by_status = medians_by_status(rows)
    by_review = medians_by_review_bucket(rows)
    correlations = compute_correlations(rows)

    # Exportar tudo
    export_overall(overall)
    export_by_status(by_status)
    export_by_review(by_review)
    export_correlations(correlations)

    print("Arquivos exportados com sucesso!")

if __name__ == "__main__":
    main()