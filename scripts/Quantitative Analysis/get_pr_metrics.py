#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fetch comprehensive PR metrics (including code churn line counts) for a given set of PR IDs.

Inputs:
- CSV: /mnt/data/human_pull_request.csv  (must contain columns: id, number, repo_url, html_url, created_at, closed_at, merged_at, state, body)
- GitHub Token: set env var GITHUB_TOKEN="..."
- ID list: hardcoded below (can override via CLI --ids)

Outputs:
- /mnt/data/pr_metrics_full.csv     (row per PR with metrics)
- /mnt/data/pr_metrics_summary.csv  (overall summary: acceptance rate, averages, etc.)

Metrics per PR:
- acceptance (closed_or_merged: bool, merged: bool, state)
- time_to_close_hours / days (prefer merged_at else closed_at)
- body_length (characters)
- commits (count)          [from PR detail, field: "commits"]
- changed_files (count)    [from PR detail, field: "changed_files"]
- additions, deletions, code_churn (additions+deletions)  [from PR detail]
- review_iterations        [# of reviews from /pulls/{number}/reviews]
- total_comments           [issue comments + review comments]
- reviewer_workload_hours  [avg time from review_requested -> review submitted, per review]

Reviewer workload notes:
- We approximate by pairing review_requested events with subsequent reviews from the same reviewer.
- If no matching review_requested event is found, we fall back to (review.submitted_at - PR.created_at).
- This is an approximation due to REST API constraints. For exact timelines, consider GraphQL timeline queries.

CLI:
    python get_pr_metrics.py --ids 2625652637,2474497456,...
or  python get_pr_metrics.py              (uses hardcoded IDS below)

"""
import os
import sys
import time
import json
import math
import argparse
from datetime import datetime, timezone
from urllib.parse import urlparse
import requests
import pandas as pd

CSV_PATH = "/Users/xingqian/Desktop/MSR_Challenge/pull_request.csv"
OUT_FULL = "curated_pr_metrics_agent_created.csv"
OUT_SUMMARY = "curated_pr_metrics_agent_created_summary.csv"

# ---- Fill these 40 IDs by default (can be overridden via --ids) ----
DEFAULT_IDS = [
3129054890,
3123194825,
3230608495,
3218773894,
3211119439,
3141571114,
3252779862,
3189667414,
3271620469,
3078737490,
3090639461,
3200393827,
3189276011,
3088559123,
3154494868,
3276421841,
3097148026,
3110865105,
3146354845,
3158557487,
2858594058,
3116826149,
3043613876,
3221900086,
2997597213,
3173734154,
2951977419,
3196981192,
3072159278,
3205615159,
3205629566,
3038010445,
3151982889
]

ISO_FMT = "%Y-%m-%dT%H:%M:%SZ"

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ids", type=str, default=None,
                    help="Comma-separated PR IDs to include. If omitted, use DEFAULT_IDS in script.")
    ap.add_argument("--csv", type=str, default=CSV_PATH, help="Path to human_pull_request.csv")
    ap.add_argument("--out_full", type=str, default=OUT_FULL, help="Output CSV path for full metrics")
    ap.add_argument("--out_summary", type=str, default=OUT_SUMMARY, help="Output CSV path for summary metrics")
    return ap.parse_args()

def parse_owner_repo_from_repo_url(repo_url: str):
    # repo_url example: https://api.github.com/repos/getsentry/sentry
    try:
        parts = urlparse(repo_url)
        path = parts.path.strip("/")
        # path like 'repos/{owner}/{repo}'
        segs = path.split("/")
        idx = segs.index("repos")
        owner = segs[idx+1]
        repo = segs[idx+2]
        return owner, repo
    except Exception:
        return None, None

def gh_headers():
    raw = os.environ.get("GITHUB_TOKEN", "")
    # Strip whitespace and accidental quotes to avoid 401 Bad credentials
    token = raw.strip().strip('"').strip("'")
    hdrs = {"Accept": "application/vnd.github+json"}
    if token:
        hdrs["Authorization"] = f"Bearer {token}"
    return hdrs

def gh_get(url, params=None, max_retries=3):
    for attempt in range(max_retries):
        r = requests.get(url, headers=gh_headers(), params=params, timeout=30)
        # Fast-fail on invalid token to avoid noisy retries
        if r.status_code == 401:
            print(f"[ERROR] 401 Bad credentials for {url}. Check GITHUB_TOKEN (missing/expired/wrong scopes/SSO not authorized).", flush=True)
            return None
        if r.status_code == 403:
            # Handle rate limiting
            reset = r.headers.get("X-RateLimit-Reset")
            if reset:
                try:
                    wait = max(0, int(reset) - int(time.time()) + 2)
                    print(f"[RateLimit] Sleeping {wait}s until reset...", flush=True)
                    time.sleep(wait)
                    continue
                except Exception:
                    pass
            # Backoff fallback
            time.sleep(2 ** attempt)
        if r.status_code in (200, 201):
            return r.json()
        # Retry on 5xx
        if 500 <= r.status_code < 600:
            time.sleep(2 ** attempt)
            continue
        # Other errors: return None with warning
        print(f"[WARN] GET {url} failed: {r.status_code} {r.text[:200]}", flush=True)
        return None
    return None

def to_dt(s):
    if not s or (isinstance(s, float) and math.isnan(s)):
        return None
    try:
        # GitHub returns Zulu times like 2024-01-02T03:04:05Z
        return datetime.strptime(str(s), ISO_FMT).replace(tzinfo=timezone.utc)
    except Exception:
        try:
            # Try pandas parser style
            return pd.to_datetime(s, utc=True, errors="coerce").to_pydatetime()
        except Exception:
            return None

def hours_between(a, b):
    if not a or not b:
        return None
    return (b - a).total_seconds() / 3600.0

def collect_pr_detail(owner, repo, number):
    url = f"https://api.github.com/repos/{owner}/{repo}/pulls/{number}"
    data = gh_get(url)
    if not data:
        return {}
    # fields of interest
    return {
        "commits": data.get("commits"),
        "changed_files": data.get("changed_files"),
        "additions": data.get("additions"),
        "deletions": data.get("deletions"),
        "code_churn": (data.get("additions") or 0) + (data.get("deletions") or 0),
    }

def collect_reviews(owner, repo, number):
    url = f"https://api.github.com/repos/{owner}/{repo}/pulls/{number}/reviews"
    reviews = gh_get(url) or []
    # Normalize minimal fields
    items = []
    for rv in reviews:
        items.append({
            "id": rv.get("id"),
            "user_login": (rv.get("user") or {}).get("login"),
            "state": rv.get("state"),
            "submitted_at": rv.get("submitted_at"),
        })
    return items

def collect_issue_comments(owner, repo, number):
    # PRs are issues too
    url = f"https://api.github.com/repos/{owner}/{repo}/issues/{number}/comments"
    comments = gh_get(url) or []
    return [{
        "id": c.get("id"),
        "user_login": (c.get("user") or {}).get("login"),
        "created_at": c.get("created_at")
    } for c in comments]

def collect_review_comments(owner, repo, number):
    # review comments on code (diff)
    url = f"https://api.github.com/repos/{owner}/{repo}/pulls/{number}/comments"
    comments = gh_get(url) or []
    return [{
        "id": c.get("id"),
        "user_login": (c.get("user") or {}).get("login"),
        "created_at": c.get("created_at")
    } for c in comments]

def collect_issue_events(owner, repo, number):
    # To find "review_requested" timestamps
    url = f"https://api.github.com/repos/{owner}/{repo}/issues/{number}/events"
    events = gh_get(url) or []
    out = []
    for e in events:
        etype = e.get("event")
        if etype == "review_requested":
            out.append({
                "event": etype,
                "created_at": e.get("created_at"),
                "requested_reviewer": (e.get("requested_reviewer") or {}).get("login"),
            })
    return out

def estimate_reviewer_workload_hours(pr_created_at_dt, reviews, review_requested_events):
    """
    Approximate average time between a review request and the review submission.
    Pair by reviewer login; fallback to (submitted_at - pr_created_at).
    """
    # Map reviewer -> list of request times (sorted)
    req_map = {}
    for ev in review_requested_events:
        reviewer = ev.get("requested_reviewer")
        ts = to_dt(ev.get("created_at"))
        if reviewer and ts:
            req_map.setdefault(reviewer, []).append(ts)
    for k in req_map:
        req_map[k].sort()

    gaps = []
    for rv in reviews:
        reviewer = rv.get("user_login")
        sub_dt = to_dt(rv.get("submitted_at"))
        if not sub_dt:
            continue
        # find nearest prior request for this reviewer
        chosen_req = None
        if reviewer in req_map and req_map[reviewer]:
            for ts in reversed(req_map[reviewer]):
                if ts <= sub_dt:
                    chosen_req = ts
                    break
        if not chosen_req:
            chosen_req = pr_created_at_dt
        if chosen_req and sub_dt:
            diff_h = hours_between(chosen_req, sub_dt)
            if diff_h is not None and diff_h >= 0:
                gaps.append(diff_h)

    if gaps:
        return sum(gaps) / len(gaps)
    return None

def main():
    args = parse_args()
    ids = None
    if args.ids:
        ids = [int(x.strip()) for x in args.ids.split(",") if x.strip()]
    else:
        ids = DEFAULT_IDS

    if not os.path.exists(args.csv):
        print(f"[ERROR] CSV not found: {args.csv}", file=sys.stderr)
        sys.exit(1)

    df = pd.read_csv(args.csv)
    need_cols = {"id","number","repo_url","html_url","created_at","closed_at","merged_at","state","body","title","user","user_id"}
    missing = need_cols - set(df.columns)
    if missing:
        print(f"[ERROR] CSV missing columns: {missing}", file=sys.stderr)
        sys.exit(1)

    subset = df[df["id"].isin(ids)].copy()
    if subset.empty:
        print("[WARN] No rows matched given IDs.", file=sys.stderr)

    # Normalize datetimes
    subset["created_at_dt"] = pd.to_datetime(subset["created_at"], utc=True, errors="coerce")
    subset["closed_at_dt"]  = pd.to_datetime(subset["closed_at"],  utc=True, errors="coerce")
    subset["merged_at_dt"]  = pd.to_datetime(subset["merged_at"],  utc=True, errors="coerce")
    subset["body_length"]   = subset["body"].fillna("").astype(str).str.len()

    rows = []
    for _, row in subset.iterrows():
        pid = int(row["id"])
        number = int(row["number"])
        repo_url = row["repo_url"]
        owner, repo = parse_owner_repo_from_repo_url(repo_url)
        if not owner or not repo:
            # fallback: parse from html_url https://github.com/{owner}/{repo}/pull/{number}
            try:
                parts = urlparse(row["html_url"])
                segs = parts.path.strip("/").split("/")
                owner = segs[0]; repo = segs[1]
            except Exception:
                owner = None; repo = None

        created_dt = row["created_at_dt"].to_pydatetime() if pd.notna(row["created_at_dt"]) else None
        closed_dt  = row["closed_at_dt"].to_pydatetime() if pd.notna(row["closed_at_dt"]) else None
        merged_dt  = row["merged_at_dt"].to_pydatetime() if pd.notna(row["merged_at_dt"]) else None

        # Time to close/merge
        end_dt = merged_dt if merged_dt else closed_dt
        ttc_hours = hours_between(created_dt, end_dt) if end_dt else None
        ttc_days = (ttc_hours / 24.0) if ttc_hours is not None else None

        # Defaults
        commits = None
        changed_files = None
        additions = None
        deletions = None
        code_churn = None
        review_iterations = None
        total_comments = None
        reviewer_workload_hours = None

        if owner and repo:
            # PR detail
            detail = collect_pr_detail(owner, repo, number)
            commits = detail.get("commits")
            changed_files = detail.get("changed_files")
            additions = detail.get("additions")
            deletions = detail.get("deletions")
            code_churn = detail.get("code_churn")

            # Reviews
            reviews = collect_reviews(owner, repo, number)
            review_iterations = len(reviews) if reviews is not None else None

            # Comments (issue + review comments)
            icomments = collect_issue_comments(owner, repo, number)
            rcomments = collect_review_comments(owner, repo, number)
            if icomments is not None and rcomments is not None:
                total_comments = len(icomments) + len(rcomments)

            # Reviewer workload estimate
            issue_events = collect_issue_events(owner, repo, number)
            reviewer_workload_hours = estimate_reviewer_workload_hours(created_dt, reviews, issue_events)

        rows.append({
            "id": pid,
            "owner": owner,
            "repo": repo,
            "number": number,
            "title": row.get("title"),
            "user": row.get("user"),
            "user_id": row.get("user_id"),
            "state": row.get("state"),
            "created_at": row.get("created_at"),
            "closed_at": row.get("closed_at"),
            "merged_at": row.get("merged_at"),
            "is_closed": str(row.get("state","").lower() == "closed"),
            "is_merged": str(pd.notna(row.get("merged_at")) and len(str(row.get("merged_at"))) > 0),
            "time_to_close_hours": ttc_hours,
            "time_to_close_days": ttc_days,
            "body_length": int(row.get("body_length")) if pd.notna(row.get("body_length")) else None,
            "commits": commits,
            "changed_files": changed_files,
            "additions": additions,
            "deletions": deletions,
            "code_churn": code_churn,  # lines only
            "review_iterations": review_iterations,
            "total_comments": total_comments,
            "reviewer_workload_hours": reviewer_workload_hours,
            "repo_url": row.get("repo_url"),
            "html_url": row.get("html_url"),
        })

    out_df = pd.DataFrame(rows)
    out_df.to_csv(args.out_full, index=False)

    # Summary
    total = len(out_df)
    closed_or_merged = int(((out_df["state"].str.lower() == "closed") | out_df["is_merged"].astype(str).eq("True")).sum()) if total else 0
    merged_count = int(out_df["is_merged"].astype(str).eq("True").sum()) if total else 0
    acceptance_rate = (closed_or_merged / total) if total else float("nan")

    summary = {
        "total_prs": total,
        "closed_or_merged": closed_or_merged,
        "merged": merged_count,
        "acceptance_rate": acceptance_rate,
        "avg_time_to_close_hours": float(out_df["time_to_close_hours"].dropna().mean()) if total else None,
        "avg_time_to_close_days": float(out_df["time_to_close_days"].dropna().mean()) if total else None,
        "avg_body_length": float(out_df["body_length"].dropna().mean()) if total else None,
        "avg_commits": float(out_df["commits"].dropna().mean()) if total else None,
        "avg_changed_files": float(out_df["changed_files"].dropna().mean()) if total else None,
        "avg_code_churn": float(out_df["code_churn"].dropna().mean()) if total else None,
        "avg_review_iterations": float(out_df["review_iterations"].dropna().mean()) if total else None,
        "avg_total_comments": float(out_df["total_comments"].dropna().mean()) if total else None,
        "avg_reviewer_workload_hours": float(out_df["reviewer_workload_hours"].dropna().mean()) if total else None,
    }
    pd.DataFrame([summary]).to_csv(args.out_summary, index=False)

    print(f"[OK] Wrote: {args.out_full}")
    print(f"[OK] Wrote: {args.out_summary}")
    if not os.environ.get("GITHUB_TOKEN"):
        print("[NOTE] No GITHUB_TOKEN set; unauthenticated requests are heavily rate-limited.", file=sys.stderr)

if __name__ == "__main__":
    main()
