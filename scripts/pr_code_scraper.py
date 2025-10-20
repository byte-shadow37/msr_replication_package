from __future__ import annotations
import base64
import os
import time
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Iterable, Tuple, Dict, Any, Union

import pandas as pd
import requests


GITHUB_API = "https://api.github.com"
RAW_HOST = "https://raw.githubusercontent.com"
USER_AGENT = "pr-code-scraper/1.0"


def parse_owner_repo(repo_url: str) -> Tuple[str, str]:
    """
    Accepts either API URL (https://api.github.com/repos/OWNER/REPO) or HTML URL (https://github.com/OWNER/REPO)
    Returns (owner, repo)
    """
    m = re.search(r"github\.com/(?:repos/)?([^/]+)/([^/]+)", repo_url)
    if not m:
        raise ValueError(f"Unrecognized repo_url format: {repo_url}")
    owner, repo = m.group(1), m.group(2)
    return owner, repo


def is_textlike(path: str) -> bool:
    text_exts = {
        ".c",".cc",".cpp",".cxx",".c++",".h",".hpp",".hh",".hxx",
        ".java",".kt",".kts",".groovy",".scala",".go",".rs",".swift",".m",".mm",
        ".py",".rb",".php",".r",".jl",".lua",".pl",".pm",".sh",".bash",".zsh",".ps1",".bat",".cmd",
        ".ts",".tsx",".js",".jsx",".vue",".svelte",
        ".css",".scss",".sass",".less",".styl",".postcss",
        ".json",".yml",".yaml",".toml",".ini",".cfg",".conf",".xml",".xsd",".xsl",".xslt",".wsdl",".svg",
        ".md",".rst",".txt",".tex",".bib",".csv",".tsv",".proto",".gradle",".properties",".dockerfile",".env",
        ".sql",".psql"
    }
    p = path.lower()
    return any(p.endswith(ext) for ext in text_exts)


def save_bytes(target: Path, content: bytes) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    with open(target, "wb") as f:
        f.write(content)


def decode_base64_to_bytes(s: str) -> bytes:
    return base64.b64decode(s.encode("utf-8"), validate=True)


@dataclass
class HttpClient:
    token: Optional[str] = None
    retry: int = 3
    backoff: float = 2.0
    timeout: float = 30.0

    def _headers(self, accept: Optional[str] = None) -> Dict[str, str]:
        h = {
            "User-Agent": USER_AGENT,
        }
        if self.token:
            h["Authorization"] = f"token {self.token}"
        if accept:
            h["Accept"] = accept
        return h

    def get(self, url: str, params: Optional[Dict[str, Any]] = None, accept: Optional[str] = None) -> requests.Response:
        for attempt in range(1, self.retry + 1):
            r = requests.get(url, params=params, headers=self._headers(accept), timeout=self.timeout)
            if r.status_code == 403 and "rate limit" in r.text.lower():
                reset = r.headers.get("X-RateLimit-Reset")
                wait = self.backoff * attempt
                if reset and reset.isdigit():
                    # Sleep until reset if it's within a reasonable window, else backoff
                    now = time.time()
                    delta = int(reset) - now
                    if 0 < delta < 120:  # up to 2 minutes
                        time.sleep(delta + 1)
                        continue
                time.sleep(wait)
                continue
            if r.status_code in (429, 502, 503, 504):
                time.sleep(self.backoff * attempt)
                continue
            return r
        return r


def fetch_file_via_api(client: HttpClient, owner: str, repo: str, path: str, ref: str) -> Optional[bytes]:
    # GitHub Contents API (returns base64 for text, raw for binary with ?)
    url = f"{GITHUB_API}/repos/{owner}/{repo}/contents/{path}"
    r = client.get(url, params={"ref": ref}, accept="application/vnd.github+json")
    if r.status_code == 200:
        data = r.json()
        if isinstance(data, dict) and "content" in data and data.get("encoding") == "base64":
            try:
                return decode_base64_to_bytes(data["content"])
            except Exception:
                return None
        # If it's a directory or other type
        return None
    elif r.status_code == 404:
        return None
    else:
        return None


def fetch_file_via_raw(owner: str, repo: str, path: str, ref: str, client: HttpClient) -> Optional[bytes]:
    # Raw host fetch; often faster and works for large files.
    # Example: https://raw.githubusercontent.com/OWNER/REPO/REF/path/to/file
    url = f"{RAW_HOST}/{owner}/{repo}/{ref}/{path}"
    r = client.get(url)
    if r.status_code == 200:
        return r.content
    return None


def safe_path_segment(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", s)


def should_skip_file(path: str, include_binary: bool) -> bool:
    if include_binary:
        return False
    return not is_textlike(path)


def save_one(owner: str, repo: str, number: int, sha: str, filepath: str, out_dir: Path, client: HttpClient,
             overwrite: bool, include_binary: bool) -> bool:
    target = out_dir / f"{owner}__{repo}" / f"pr_{number}" / f"sha_{sha}" / filepath
    if target.exists() and not overwrite:
        return True
    if should_skip_file(filepath, include_binary):
        return False

    # Try raw first (works for large files)
    content = fetch_file_via_raw(owner, repo, filepath, sha, client)
    if content is None:
        # Fallback to API
        content = fetch_file_via_api(client, owner, repo, filepath, ref=sha)
    if content is None:
        return False

    target.parent.mkdir(parents=True, exist_ok=True)
    with open(target, "wb") as f:
        f.write(content)
    return True


def download_pr_code_join(
    prs_csv: Union[Path, str, pd.DataFrame],
    details_csv: Union[Path, str, pd.DataFrame],
    out_dir: Union[Path, str],
    token: Optional[str] = None,
    overwrite: bool = False,
    include_binary: bool = False,
    limit: Optional[int] = None,
) -> dict:
    """
    Download source files for commits associated with PRs using JOIN mode.

    Parameters:
    - prs_csv: path or DataFrame for human_pull_request.csv (must have columns: id, number, repo_url)
    - details_csv: path or DataFrame for pr_commit_details.csv (must have columns: pr_id, sha, filename)
    - out_dir: output directory path
    - token: GitHub token for authentication (optional)
    - overwrite: whether to overwrite existing files
    - include_binary: whether to include binary/unknown filetypes
    - limit: limit number of merged rows to process (optional)

    Returns:
    A dict summary with keys:
      - "saved_files": int number of files saved
      - "rows_processed": int number of rows processed
      - "out_dir": str output directory path
    """
    def _ensure_df(obj: Union[Path, str, pd.DataFrame], usecols: list[str]) -> pd.DataFrame:
        if isinstance(obj, pd.DataFrame):
            # Select only needed columns if present
            missing = [c for c in usecols if c not in obj.columns]
            if missing:
                raise ValueError(f"DataFrame missing required columns: {missing}")
            return obj.loc[:, usecols]
        else:
            return pd.read_csv(obj, usecols=usecols)

    prs = _ensure_df(prs_csv, ["id", "number", "repo_url"])
    det = _ensure_df(details_csv, ["pr_id", "sha", "filename"])
    merged = prs.merge(det, left_on="id", right_on="pr_id", how="inner")

    if limit:
        merged = merged.head(limit)

    # Parse owner/repo from repo_url (api or html)
    def parse(row):
        owner, repo = parse_owner_repo(row["repo_url"])
        return pd.Series({"owner": owner, "repo": repo})

    parsed = merged.apply(parse, axis=1)
    merged = pd.concat([merged, parsed], axis=1)

    client = HttpClient(token=token)

    success = 0
    total = len(merged)
    for i, row in merged.iterrows():
        ok = save_one(
            owner=row["owner"],
            repo=row["repo"],
            number=int(row["number"]),
            sha=row["sha"],
            filepath=row["filename"],
            out_dir=Path(out_dir),
            client=client,
            overwrite=overwrite,
            include_binary=include_binary,
        )
        if ok:
            success += 1
        if i % 200 == 0 and i > 0:
            print(f"[JOIN] Progress {i}/{total}, saved={success}")
    print(f"[JOIN] Done. Saved {success} files from {total} rows.")

    return {
        "saved_files": success,
        "rows_processed": total,
        "out_dir": str(out_dir),
    }


__all__ = ["download_pr_code_join"]

# Example usage:
# from pr_code_scraper import download_pr_code_join
summary = download_pr_code_join(
    prs_csv="human_pull_request.csv",
    details_csv="pr_commit_details.csv",
    out_dir="./out",
    token="",
    overwrite=False,
    include_binary=False,
    limit=None,
)
print(summary)
