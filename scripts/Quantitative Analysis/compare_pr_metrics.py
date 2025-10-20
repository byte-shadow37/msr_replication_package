import argparse
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

try:
    from scipy.stats import mannwhitneyu
    _HAS_SCIPY = True
except Exception:
    _HAS_SCIPY = False

# one chart per figure, no specific colors/styles
NUMERIC_METRICS = [
    "time_to_close_hours",
    "time_to_close_days",
    "body_length",
    "commits",
    "changed_files",
    "additions",
    "deletions",
    "code_churn",
    "review_iterations",
    "total_comments",
    "reviewer_workload_hours",
]

BOOL_COLS = ["is_closed", "is_merged"]

def _quantiles(x):
    x = pd.Series(x).dropna()
    if x.empty:
        return {"min": np.nan, "q1": np.nan, "median": np.nan, "mean": np.nan, "q3": np.nan, "max": np.nan}
    return {
        "min": float(x.min()),
        "q1": float(x.quantile(0.25)),
        "median": float(x.median()),
        "mean": float(x.mean()),
        "q3": float(x.quantile(0.75)),
        "max": float(x.max()),
    }

def cliffs_delta(a, b):
    a = pd.Series(a).dropna().values
    b = pd.Series(b).dropna().values
    if len(a) == 0 or len(b) == 0:
        return np.nan
    # Efficient Cliff's delta: count pairwise comparisons using sorting
    a_sorted = np.sort(a)
    b_sorted = np.sort(b)
    i = j = more = less = 0
    na, nb = len(a_sorted), len(b_sorted)
    while i < na:
        while j < nb and b_sorted[j] < a_sorted[i]:
            j += 1
        less += j
        i += 1
    i = j = 0
    while j < nb:
        while i < na and a_sorted[i] < b_sorted[j]:
            i += 1
        more += i
        j += 1
    # pairs = na*nb; delta = (more - less)/pairs
    pairs = na * nb
    if pairs == 0:
        return np.nan
    return float((more - less) / pairs)

def label_effect_size(delta_abs):
    if np.isnan(delta_abs):
        return "N/A"
    # thresholds from Romano et al. (2006): 0.147, 0.33, 0.474
    if delta_abs < 0.147:
        return "Negligible"
    if delta_abs < 0.33:
        return "Small"
    if delta_abs < 0.474:
        return "Medium"
    return "Large"

def coerce_numeric(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def coerce_bool(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip().str.lower().map({"true": True, "false": False})
    return df

def summarize_dataset(df: pd.DataFrame, name: str) -> pd.Series:
    s = pd.Series(dtype="float64")
    closed = df["is_closed"] if "is_closed" in df.columns else pd.Series([np.nan] * len(df))
    merged = df["is_merged"] if "is_merged" in df.columns else pd.Series([np.nan] * len(df))
    acc = (closed.fillna(False) | merged.fillna(False)).mean() if len(df) else np.nan
    s["acceptance_rate"] = acc

    for m in NUMERIC_METRICS:
        s[f"avg_{m}"] = df[m].mean() if m in df.columns else np.nan
    for m in NUMERIC_METRICS:
        s[f"median_{m}"] = df[m].median() if m in df.columns else np.nan

    s["num_prs"] = len(df)
    s.name = name
    return s

def make_boxplots(df_all: pd.DataFrame, out_dir: Path, dataset_col: str = "dataset"):
    out_dir.mkdir(parents=True, exist_ok=True)
    # generate one plot per metric
    for m in NUMERIC_METRICS:
        if m not in df_all.columns:
            continue
        if df_all[m].dropna().empty:
            continue
        groups = list(df_all.groupby(dataset_col))
        data = [g[1][m].dropna().values for g in groups]
        labels = [g[0] for g in groups]
        if sum(len(arr) for arr in data) == 0:
            continue
        plt.figure()
        bp = plt.boxplot(
            data,
            labels=labels,
            showfliers=False,          # HIDE outliers to avoid extreme scaling
            showmeans=True,            # still show the mean explicitly
            meanline=False,            # mean as a marker (not a line)
            meanprops={"marker": "x", "markersize": 8, "markeredgewidth": 1.5, "markerfacecolor": "tab:orange", "markeredgecolor": "tab:orange"},
            medianprops={"color": "green", "linewidth": 1.5},  # distinguish median from mean
        )
        # Legend with proxy artists so the meaning is clear
        handles = [
            Line2D([], [], color="green", linestyle="-", label="Median"),
            Line2D([], [], marker="x", color="tab:orange", linestyle="None", label="Mean"),
        ]
        plt.legend(handles=handles, loc="best", frameon=False)

        plt.ylabel(m)
        plt.title(f"Distribution of {m} by dataset")
        fig_path = out_dir / f"box_{m}.png"
        plt.tight_layout()
        plt.savefig(fig_path)
        plt.close()

def load_and_prepare(path: Path, dataset_name: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["dataset"] = dataset_name
    df = coerce_bool(df, BOOL_COLS)
    df = coerce_numeric(df, NUMERIC_METRICS)
    return df

def build_comparison_table(df_all: pd.DataFrame, label_a: str, label_b: str, dataset_col: str = "dataset") -> pd.DataFrame:
    rows = []
    # group data by dataset label
    g = {k: v for k, v in df_all.groupby(dataset_col)}
    A = g.get(label_a, pd.DataFrame())
    B = g.get(label_b, pd.DataFrame())
    for m in NUMERIC_METRICS:
        if m not in df_all.columns:
            continue
        a = A[m] if m in A.columns else pd.Series(dtype=float)
        b = B[m] if m in B.columns else pd.Series(dtype=float)
        qa = _quantiles(a)
        qb = _quantiles(b)
        # p-value via Mann-Whitney U (two-sided) if SciPy available and both non-empty
        if _HAS_SCIPY and a.dropna().size > 0 and b.dropna().size > 0:
            try:
                stat, p = mannwhitneyu(a.dropna(), b.dropna(), alternative="two-sided")
            except Exception:
                p = np.nan
        else:
            p = np.nan
        # Cliff's delta (directional)
        delta = cliffs_delta(a, b)
        eff_label = label_effect_size(abs(delta))
        rows.append({
            "Metric": m,
            f"{label_a} min": qa["min"],
            f"{label_a} Q1": qa["q1"],
            f"{label_a} Median": qa["median"],
            f"{label_a} Mean": qa["mean"],
            f"{label_a} Q3": qa["q3"],
            f"{label_a} Max": qa["max"],
            f"{label_b} min": qb["min"],
            f"{label_b} Q1": qb["q1"],
            f"{label_b} Median": qb["median"],
            f"{label_b} Mean": qb["mean"],
            f"{label_b} Q3": qb["q3"],
            f"{label_b} Max": qb["max"],
            "P-value": p,
            "Effect size (delta)": delta,
            "Effect label": eff_label,
        })
    return pd.DataFrame(rows)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file-a", required=True, help="Path to first metrics CSV")
    ap.add_argument("--file-b", required=True, help="Path to second metrics CSV")
    ap.add_argument("--label-a", default=None, help="Label for dataset A (defaults to filename)")
    ap.add_argument("--label-b", default=None, help="Label for dataset B (defaults to filename)")
    ap.add_argument("--out-summary", default="metrics_summary_by_file.csv", help="Output CSV for per-file summary")
    ap.add_argument("--out-long", default="metrics_long_concat.csv", help="Output CSV for concatenated long form")
    ap.add_argument("--out-plots", default="plots", help="Output directory for box plots")
    ap.add_argument("--out-table", default="metrics_comparison_table.csv", help="Output CSV for side-by-side comparison table")
    args = ap.parse_args()

    pA = Path(args.file_a)
    pB = Path(args.file_b)
    label_a = args.label_a or pA.name
    label_b = args.label_b or pB.name

    df_a = load_and_prepare(pA, label_a)
    df_b = load_and_prepare(pB, label_b)

    df_all = pd.concat([df_a, df_b], ignore_index=True)

    sum_a = summarize_dataset(df_a, label_a)
    sum_b = summarize_dataset(df_b, label_b)
    summary_df = pd.DataFrame([sum_a, sum_b])

    out_summary = Path(args.out_summary)
    out_long = Path(args.out_long)
    out_plots = Path(args.out_plots)

    summary_df.to_csv(out_summary, index=True)
    df_all.to_csv(out_long, index=False)

    make_boxplots(df_all, out_plots)

    comp_df = build_comparison_table(df_all, label_a, label_b)
    comp_out = Path(args.out_table)
    comp_df.to_csv(comp_out, index=False)
    print(f"[OK] Wrote summary: {out_summary.resolve()}")
    print(f"[OK] Wrote long form: {out_long.resolve()}")
    print(f"[OK] Plots in: {out_plots.resolve()}")
    print(f"[OK] Wrote comparison table: {comp_out.resolve()}")

if __name__ == "__main__":
    main()
