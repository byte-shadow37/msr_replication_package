import argparse
import os
import re
import pandas as pd
import sqlite3

BASIC_KEYWORDS = [
    # English
    r"duplicate", r"dup code", r"duplicated", r"duplication", r"copy[-\s]?paste",
    r"clone(?:s|d|ing)?", r"code clone", r"clones?", r"remove duplication",
    r"dedup(?:lication)?", r"refactor(?:ing)?\s+(?:dup|duplicate|duplication|clones?)",
    r"\bDRY\b", r"redundant",

    # Added phrases requested
    r"duplicate(?:s|d)?",
    r"redundan(?:t|cy)",
    r"replicat(?:e|ed|ion|ing)",
    r"copy",
    r"duplicated\s+code",
    r"source\s+code\s+duplication",
    r"fragment\s+reus(?:e|ing)",
    r"redundant\s+implementation",
    r"copy[\-\s\u2012-\u2015]?\s*paste\s+code",
    r"program\s+duplication",

    # Chinese
    r"重复代码", r"去重", r"消除重复", r"抽取方法", r"抽取函数", r"提取方法", r"提取函数", r"克隆",
    # Other hints
    r"boilerplate\s+reduction", r"eliminat(?:e|ing)\s+duplication"
]

REGEX_RULES = [
    r"refactor(?:ing)?\s+(?:duplicate|duplication|clones?)",
    r"remove\s+(?:duplicate|duplication|clones?)\s+code",
    r"eliminat(?:e|ing)\s+(?:duplicate|duplication|clones?)",
    r"deduplicat(?:e|ion)\s+code",
    r"(?:extract|抽取|提取)\s+(?:method|function|方法|函数)\s+(?:to|以)?\s*(?:remove|消除)?\s*(?:duplicate|duplication|重复|克隆)",
    r"\bDRY\b.*\b(code|代码)\b",
]

EXCLUDE_PATTERNS = [
    r"\bgit\s+clone\b",
    r"clone\s+the\s+repo",
    r"docker\s+image\s+clone",
    r"vm\s+clone",
    r"k8s\s+clone",
]

def compile_patterns(patterns):
    return [re.compile(p, re.I) for p in patterns]

RE_BASIC = compile_patterns(BASIC_KEYWORDS)
RE_REGEX = compile_patterns(REGEX_RULES)
RE_EXCL  = compile_patterns(EXCLUDE_PATTERNS)

TEXT_COL_CANDIDATES = ["body"]

def pick_text_cols(df):
    cols = [c for c in TEXT_COL_CANDIDATES if c in df.columns]
    return cols if cols else [c for c in df.columns if df[c].dtype == "object"][:2]

def row_text(row, text_cols):
    return " \n ".join(str(row.get(c, "") or "") for c in text_cols)

def hit_any(text, regex_list):
    return next((r.pattern for r in regex_list if r.search(text)), None)

def should_exclude(text):
    return hit_any(text, RE_EXCL) is not None

def scan_chunk(df, source_tag):
    if df.empty:
        return pd.DataFrame()
    text_cols = pick_text_cols(df)
    out_rows = []
    for _, row in df.iterrows():
        t = row_text(row, text_cols).lower()
        if not t.strip():
            continue
        if should_exclude(t):
            continue
        rule = hit_any(t, RE_REGEX)
        kw   = hit_any(t, RE_BASIC) if rule is None else None
        if rule or kw:
            ctx = t[:500]
            out = row.to_dict()
            out["__source__"] = source_tag
            out["__match_type__"] = "regex" if rule else "keyword"
            out["__pattern__"] = rule or kw
            out["__context__"] = ctx
            out_rows.append(out)
    return pd.DataFrame(out_rows)

def scan_csv(in_path, source_tag, chunksize=100_000):
    hits = []
    for chunk in pd.read_csv(in_path, chunksize=chunksize, low_memory=False):
        hits.append(scan_chunk(chunk, source_tag))
    return pd.concat(hits, ignore_index=True) if hits else pd.DataFrame()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True, help="all_pull_request.csv")
    ap.add_argument("--out_dir", required=True, help="output directory")
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    clone_out = os.path.join(args.out_dir, "review_clone.csv")

    print("Scanning file ...")
    clone_hits = scan_csv(args.file, source_tag="file")

    clone_hits.to_csv(clone_out, index=False)
    print(f"Saved: {clone_out} ({len(clone_hits)})")

    # conn = sqlite3.connect("pr_clone_hits.db")
    # clone_hits.to_sql("pr_clone_hits", conn, if_exists='replace', index=False)

if __name__ == "__main__":
    main()