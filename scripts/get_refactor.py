import pandas as pd
import sqlite3
import re
import os

# Path to your CSV file
file_path = "pull_request.csv"

# Load CSV
df = pd.read_csv(file_path)

# Ensure columns exist
if 'title' not in df.columns or 'body' not in df.columns:
    raise ValueError("CSV must contain 'title' and 'body' columns.")

# Check for keyword presence
title_mask = df['title'].str.contains('refactor', case=False, na=False)
body_mask = df['body'].str.contains('refactor', case=False, na=False)

# Determine match type
title_only_mask = title_mask & ~body_mask
body_only_mask = ~title_mask & body_mask
both_mask = title_mask & body_mask

# Assign match type
df = df.copy()
df['match_type'] = None
df.loc[title_only_mask, 'match_type'] = 'title_only'
df.loc[body_only_mask, 'match_type'] = 'body_only'
df.loc[both_mask, 'match_type'] = 'both'

# Keep only rows with a match
result_df = df[df['match_type'].notna()].copy()

# Split into three DataFrames
title_only_df = result_df[result_df['match_type'] == 'title_only']
body_only_df = result_df[result_df['match_type'] == 'body_only']
both_df = result_df[result_df['match_type'] == 'both']

# ---- Fourth case: IDs whose task_type is 'refactor' ----
# Load task-type CSV and find the id columns in both files
TASK_TYPE_CSV = "pr_task_type.csv"

task_df = pd.read_csv(TASK_TYPE_CSV)
if 'type' not in task_df.columns:
    raise ValueError("Task-type CSV must contain a 'type' column.")

# Candidate ID column names to be robust to schema differences
id_candidates = ['id', 'pr_id', 'pull_request_id', 'number', 'PR_ID', 'prNumber']

# Choose an ID column from task_df
task_id_col = next((c for c in id_candidates if c in task_df.columns), None)
if task_id_col is None:
    raise ValueError("Could not find an ID column in task-type CSV. Tried: " + ", ".join(id_candidates))

# Choose an ID column from the PR dataframe (same candidate list)
pr_id_col = next((c for c in id_candidates if c in df.columns), None)
if pr_id_col is None:
    raise ValueError("Could not find an ID column in human_pull_request CSV. Tried: " + ", ".join(id_candidates))

# Filter task_df to rows where type == 'refactor' (case-insensitive)
ref_task_ids = (
    task_df[task_df['type'].astype(str).str.lower() == 'refactor'][task_id_col]
    .dropna()
    .astype(str)
    .unique()
)

# Match those IDs in the PR dataframe (cast to string for safe matching)
tasktype_refactor_df = df[df[pr_id_col].astype(str).isin(ref_task_ids)].copy()

# ---- Fifth case: From task-type refactor, further filter by keyword/regex rules in title/body ----
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

    # Other hints
    r"boilerplate\s+reduction", r"eliminat(?:e|ing)\s+duplication"
]

REGEX_RULES = [
    r"refactor(?:ing)?\s+(?:duplicate|duplication|clones?)",
    r"remove\s+(?:duplicate|duplication|clones?)\s+code",
    r"eliminat(?:e|ing)\s+(?:duplicate|duplication|clones?)",
    r"deduplicat(?:e|ion)\s+code",
    r"(?:extract)\s+(?:method|function)\s+(?:to|以)?\s*(?:remove)?\s*(?:duplicate|duplication)",
    r"\bDRY\b.*\b(code)\b",
]

EXCLUDE_PATTERNS = [
    r"\bgit\s+clone\b",
    r"clone\s+the\s+repo",
    r"docker\s+image\s+clone",
    r"vm\s+clone",
    r"k8s\s+clone",
]

include_pattern = r"(?:" + "|".join(BASIC_KEYWORDS + REGEX_RULES) + r")"
exclude_pattern = r"(?:" + "|".join(EXCLUDE_PATTERNS) + r")"

def _contains(series: pd.Series, pattern: str) -> pd.Series:
    return series.astype(str).str.contains(pattern, flags=re.IGNORECASE, regex=True, na=False)

inc_title = _contains(tasktype_refactor_df['title'], include_pattern)
inc_body = _contains(tasktype_refactor_df['body'], include_pattern)
exc_title = _contains(tasktype_refactor_df['title'], exclude_pattern)
exc_body = _contains(tasktype_refactor_df['body'], exclude_pattern)

tasktype_refactor_regex_df = tasktype_refactor_df[(inc_title | inc_body) & ~(exc_title | exc_body)].copy()

# Remove match_type column if it exists
tasktype_refactor_regex_df = tasktype_refactor_regex_df.drop(columns=['match_type'], errors='ignore')

# Helper to save a DataFrame into a SQLite DB file
def save_to_db(db_path: str, table_name: str, dataf: pd.DataFrame):
    with sqlite3.connect(db_path) as conn:
        dataf.to_sql(table_name, conn, if_exists='replace', index=False)


# Save each case to its own DB file (table name: pull_requests)
save_to_db('refactor_data/curated_agent/PRs_TitleRefactor.db', 'pull_requests', title_only_df)
save_to_db('refactor_data/curated_agent/PRs_DescriptionRefactor.db', 'pull_requests', body_only_df)
save_to_db('refactor_data/curated_agent/PRs_TitleDescriptionRefactor.db', 'pull_requests', both_df)
save_to_db('refactor_data/curated_agent/PRs_TagRefactor.db', 'pull_requests', tasktype_refactor_df)
save_to_db('refactor_data/curated_agent/PRs_TagRefactorKeywordClone.db', 'pull_requests', tasktype_refactor_regex_df)

# Also save to CSV files
title_only_df.to_csv('refactor_data/curated_agent/PRs_TitleRefactor.csv', index=False)
body_only_df.to_csv('refactor_data/curated_agent/PRs_DescriptionRefactor.csv', index=False)
both_df.to_csv('refactor_data/curated_agent/PRs_TitleDescriptionRefactor.csv', index=False)
tasktype_refactor_df.to_csv('refactor_data/curated_agent/PRs_TagRefactor.csv', index=False)
tasktype_refactor_regex_df.to_csv('refactor_data/curated_agent/PRs_TagRefactorKeywordClone.csv', index=False)

# Print summary
print(f"Total matches (title/body search): {len(result_df)}")
print("\nCounts by category (title/body search):")
print(result_df['match_type'].value_counts())
print(f"\nTask-type = 'refactor' matches by ID: {len(tasktype_refactor_df)}")
print(f"Task-type 'refactor' + regex keyword matches: {len(tasktype_refactor_regex_df)}")

print("\nSaved SQLite databases:")
print(" - refactor_data/curated_agent/PRs_TitleRefactor.db (table: pull_requests)")
print(" - refactor_data/curated_agent/PRs_DescriptionRefactor.db (table: pull_requests)")
print(" - refactor_data/curated_agent/PRs_TitleDescriptionRefactor.db (table: pull_requests)")
print(" - refactor_data/curated_agent/PRs_TagRefactor.db (table: pull_requests)")
print(" - refactor_data/curated_agent/PRs_TagRefactorKeywordClone.db (table: pull_requests)")

print("\nSaved CSV files:")
print(" - refactor_data/curated_agent/PRs_TitleRefactor.csv")
print(" - refactor_data/curated_agent/PRs_DescriptionRefactor.csv")
print(" - refactor_data/curated_agent/PRs_TitleDescriptionRefactor.csv")
print(" - refactor_data/curated_agent/PRs_TagRefactor.csv")
print(" - refactor_data/curated_agent/PRs_TagRefactorKeywordClone.csv")