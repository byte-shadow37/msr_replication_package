#!/usr/bin/env python3
"""
run_rminer.py

Read a human_pull_request CSV, filter a set of PR IDs, and either:
  - print the RefactoringMiner commands to run for each PR, or
  - actually run RefactoringMiner for each PR (if --rminer is provided).

Usage examples:
  python run_rminer.py \
    --csv human_pull_request.csv \
    --ids 2625652637,2474497456,2303745279 \
    --out /tmp/rminer_commands.tsv

  # Actually run RefactoringMiner (path to binary or jar wrapper)
  python run_rminer.py \
    --csv human_pull_request.csv \
    --ids-file pr_ids.txt \
    --rminer /path/to/RefactoringMiner \
    --results-dir ./rminer_results
"""
import argparse
import csv
import os
import subprocess
import sys
from dataclasses import dataclass
from typing import List, Optional, Tuple

import pandas as pd
import shutil

# ---- Heuristics for column detection -----------------------------------------------------------

REPO_CANDIDATES = [
    "repo_url", "repository_url", "base_repo_url", "head_repo_url",
    "html_url_repo", "url_repo", "repo_html_url", "repo_api_url",
    "repo_full_name", "full_name", "repo", "repository", "base_repo_full_name",
    "head_repo_full_name"
]

PRNUM_CANDIDATES = [
    "number", "pr_number", "pull_number", "pr", "pr_num"
]

ID_CANDIDATES = [
    "id", "pr_id", "pull_request_id", "human_pr_id"
]

def first_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    # case-insensitive fallback
    lower = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c in lower:
            return lower[c]
    return None

def normalize_repo_url(row, repo_col: str) -> Optional[str]:
    val = str(row[repo_col]).strip()
    if not val or val.lower() == "nan":
        return None
    # Case 1: owner/repo shorthand -> https://github.com/owner/repo.git
    if "/" in val and not val.startswith("http"):
        owner, repo = val.split("/", 1)
        owner, repo = owner.strip(), repo.strip()
        if owner and repo:
            return f"https://github.com/{owner}/{repo}.git"
        return None
    # Case 2: URL inputs
    if val.startswith("http"):
        v = val.rstrip("/")
        # API-style endpoints -> map to github.com/owner/repo.git
        for marker in ("api.github.com/repos/", "apihub.com/repos/"):
            if marker in v:
                tail = v.split(marker, 1)[1].strip("/")
                parts = tail.split("/")
                if len(parts) >= 2:
                    owner, repo = parts[0], parts[1]
                    return f"https://github.com/{owner}/{repo}.git"
        # Direct github.com link -> normalize to https://github.com/owner/repo.git
        if "github.com" in v:
            parts = v.split("github.com/", 1)
            if len(parts) == 2:
                tail = parts[1]
                tail = tail.split("?", 1)[0].split("#", 1)[0]
                tail = tail.strip("/")
                if "/" in tail:
                    owner, repo = tail.split("/", 1)
                    repo = repo.replace(".git", "")
                    return f"https://github.com/{owner}/{repo}.git"
            # Fallback: if it's already a repo root, ensure .git
            if v.endswith(".git"):
                return v
            return v + ".git"
        # Unknown host -> reject
        return None
    return None

def find_repo_and_number(df: pd.DataFrame) -> Tuple[str, str]:
    repo_col = first_col(df, REPO_CANDIDATES)
    prnum_col = first_col(df, PRNUM_CANDIDATES)
    if not repo_col or not prnum_col:
        raise ValueError(
            f"Unable to find required columns. "
            f"Repo candidates tried: {REPO_CANDIDATES}, PR number candidates tried: {PRNUM_CANDIDATES}. "
            f"Found columns: {df.columns.tolist()}"
        )
    return repo_col, prnum_col

def find_id_col(df: pd.DataFrame) -> str:
    id_col = first_col(df, ID_CANDIDATES)
    if not id_col:
        # If 'number' exists without a dedicated 'id', allow using it as identifier
        if "number" in df.columns:
            return "number"
        raise ValueError(f"Unable to find PR id column. Tried: {ID_CANDIDATES}. Found: {df.columns.tolist()}")
    return id_col

# ---- Core --------------------------------------------------------------------------------------

@dataclass
class PRItem:
    id_val: str
    repo_url: str
    pr_number: str

def load_and_filter(csv_path: str, requested_ids: List[str]) -> Tuple[List[PRItem], str, str, str]:
    df = pd.read_csv(csv_path, low_memory=False)
    id_col = find_id_col(df)
    repo_col, prnum_col = find_repo_and_number(df)

    # Normalize id col to str for matching
    df["_id_match"] = df[id_col].astype(str).str.strip()
    requested_ids = [str(x).strip() for x in requested_ids if str(x).strip()]
    filt = df[df["_id_match"].isin(requested_ids)].copy()

    if filt.empty:
        raise SystemExit(f"No rows matched the provided IDs in {csv_path}. "
                         f"Ensure the PR IDs correspond to column '{id_col}'.")

    # Normalize repo URLs
    filt["_repo_url_norm"] = filt.apply(lambda r: normalize_repo_url(r, repo_col), axis=1)
    missing_repo = filt["_repo_url_norm"].isna().sum()
    if missing_repo > 0:
        print(f"[WARN] {missing_repo} row(s) missing/invalid repo URL; they will be skipped.", file=sys.stderr)
        filt = filt[~filt["_repo_url_norm"].isna()].copy()

    # Normalize PR number
    filt["_pr_number_str"] = filt[prnum_col].astype(str).str.strip()
    filt = filt[filt["_pr_number_str"].str.isdigit()].copy()

    items: List[PRItem] = []
    for _, row in filt.iterrows():
        items.append(PRItem(
            id_val=str(row["_id_match"]),
            repo_url=str(row["_repo_url_norm"]),
            pr_number=str(row["_pr_number_str"]),
        ))
    return items, id_col, repo_col, prnum_col

def write_commands(items: List[PRItem], out_tsv: str) -> None:
    dirpath = os.path.dirname(out_tsv)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)
    with open(out_tsv, "w", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(["id", "repo_url", "pr_number", "rminer_cmd"])
        for it in items:
            cmd = f'RefactoringMiner -gp "{it.repo_url}" {it.pr_number} 120'
            w.writerow([it.id_val, it.repo_url, it.pr_number, cmd])

def maybe_run_refactoringminer(items: List[PRItem], rminer: str, results_dir: str) -> None:
    os.makedirs(results_dir, exist_ok=True)
    rminer_dir = os.path.dirname(os.path.abspath(rminer)) or "."
    token_path = os.path.join(rminer_dir, "github-oauth.properties")
    if not os.path.exists(token_path):
        print(f"[FATAL] github-oauth.properties not found where RefactoringMiner runs: {token_path}", file=sys.stderr)
        print("        Create this file with a line 'OAuthToken=YOUR_TOKEN' (no quotes).", file=sys.stderr)
        return
    else:
        print(f"[INFO] Found token at: {token_path}")
    for it in items:
        print(f"[RUN] {it.id_val}  {it.repo_url}  #{it.pr_number}")
        # Each PR to its own subdir
        out_dir = os.path.join(results_dir, f"{it.id_val}_{it.pr_number}")
        os.makedirs(out_dir, exist_ok=True)
        # Typical usage: RefactoringMiner -gp <repo_url> <pr_number> 120 -json <file>
        json_path = os.path.join(out_dir, "refactorings.json")
        json_path_abs = os.path.abspath(json_path)
        cmd = [
            rminer, "-gp", it.repo_url, it.pr_number, "120", "-json", json_path_abs
        ]
        try:
            subprocess.run(cmd, check=True, cwd=rminer_dir)
        except subprocess.CalledProcessError as e:
            print(f"[ERROR] Failed on {it.id_val} #{it.pr_number}: {e}", file=sys.stderr)

def parse_ids(ids: Optional[str], ids_file: Optional[str]) -> List[str]:
    collected: List[str] = []
    if ids:
        collected += [x.strip() for x in ids.split(",") if x.strip()]
    if ids_file:
        with open(ids_file, "r") as f:
            for line in f:
                line = line.strip().split("#", 1)[0]  # allow comments
                if not line:
                    continue
                parts = [x.strip() for x in line.replace(" ", ",").split(",")]
                collected += [p for p in parts if p]
    # Deduplicate, keep order
    seen = set()
    uniq = []
    for x in collected:
        if x not in seen:
            uniq.append(x)
            seen.add(x)
    return uniq

def main():
    ap = argparse.ArgumentParser(description="Filter PRs and (optionally) run RefactoringMiner.")
    ap.add_argument("--csv", default="human_pull_request.csv",
                    help="Path to human_pull_request.csv (default: ./human_pull_request.csv)")
    ap.add_argument("--ids", default=None,
                    help="Comma-separated list of PR IDs to include")
    ap.add_argument("--ids-file", default=None,
                    help="Text file with PR IDs (comma/space/newline separated; # for comments)")
    ap.add_argument("--out", default="rminer_commands.tsv",
                    help="Where to write the commands TSV")
    ap.add_argument("--rminer", default=None,
                    help="Path to RefactoringMiner executable; if provided, will run it")
    ap.add_argument("--results-dir", default="rminer_results",
                    help="Where to store RefactoringMiner outputs (when --rminer is provided)")
    args = ap.parse_args()

    ids = parse_ids(args.ids, args.ids_file)
    if not ids:
        print("ERROR: No PR IDs supplied. Use --ids or --ids-file.", file=sys.stderr)
        sys.exit(2)

    items, id_col, repo_col, prnum_col = load_and_filter(args.csv, ids)
    print(f"[INFO] Matched {len(items)} PRs using id_col='{id_col}', repo_col='{repo_col}', prnum_col='{prnum_col}'.")
    if args.rminer:
        print(f"[INFO] RefactoringMiner path: {args.rminer}")
    print(f"[INFO] Current working dir: {os.getcwd()}")

    write_commands(items, args.out)
    print(f"[OK] Commands written to {args.out}")

    if args.rminer:
        print(f"[INFO] Running RefactoringMiner for {len(items)} PRs...")
        maybe_run_refactoringminer(items, args.rminer, args.results_dir)
        print(f"[DONE] Results at {args.results_dir}")

if __name__ == "__main__":
    main()
