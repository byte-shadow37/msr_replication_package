import pandas as pd

PRS_PATH = "pull_request.csv"
DETAILS_PATH = "pr_commit_details.csv"

prs = pd.read_csv(PRS_PATH, usecols=["id", "number", "repo_url"], low_memory=False)
details = pd.read_csv(DETAILS_PATH, usecols=["pr_id", "sha", "filename"], low_memory=False)

def as_int_series(s):
    out = pd.to_numeric(s, errors="coerce")
    return out.astype("Int64")

prs["id_i64"] = as_int_series(prs["id"])
details["pr_id_i64"] = as_int_series(details["pr_id"])

print("PRS rows:", len(prs), "DETAILS rows:", len(details))
print("PRS id NaN:", prs["id_i64"].isna().sum(), "DETAILS pr_id NaN:", details["pr_id_i64"].isna().sum())

ids_prs = set(prs["id_i64"].dropna().astype("int64").unique())
ids_det = set(details["pr_id_i64"].dropna().astype("int64").unique())
intersection = ids_prs & ids_det
print("ID intersection size:", len(intersection))

possible_number_cols = [c for c in details.columns if c.lower() in ("pr_number", "number", "pull_number")]
if len(intersection) == 0 and possible_number_cols:
    det_num_col = possible_number_cols[0]
    details["pr_number_i64"] = as_int_series(details[det_num_col])
    nums_prs = set(pd.to_numeric(prs["number"], errors="coerce").dropna().astype("int64").unique())
    nums_det = set(details["pr_number_i64"].dropna().astype("int64").unique())
    num_intersection = nums_prs & nums_det
    print(f"Number intersection size (using '{det_num_col}'):", len(num_intersection))
else:
    print("Details no pr_number")

print("\nSample PR ids:", prs['id'].head(5).tolist())
print("Sample DETAILS pr_id:", details['pr_id'].head(5).tolist())

if len(intersection) > 0:
    merged_preview = prs.merge(details, left_on="id_i64", right_on="pr_id_i64", how="inner")
    print("\nMerged preview:")
    print(merged_preview[["id","number","repo_url","pr_id","sha","filename"]].head(5))
else:
    print("\n⚠️ no intersection")