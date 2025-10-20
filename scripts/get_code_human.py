# #!/usr/bin/env python3
# """
# Pull Request Change Scraper
# Read PR information from a CSV file and scrape commits, files, and diff information for each PR
# """
#
import pandas as pd
import requests
import json
import time
import os
from urllib.parse import urlparse
import logging
from typing import Dict, List, Optional
from datetime import datetime
import csv
#
# # Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pr_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class PullRequestScraper:
    def __init__(self, github_token: Optional[str] = None):
        """
        Initialize the scraper

        Args:
            github_token: GitHub API token (recommended to increase request limits)
        """
        self.session = requests.Session()
        if github_token:
            self.session.headers.update({
                'Authorization': f'token {github_token}',
                'Accept': 'application/vnd.github.v3+json'
            })
        else:
            logger.warning("No GitHub token, requests may face strict limitations")

        # Create output directories
        self.output_dir = 'pr_data'
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(f'{self.output_dir}/files', exist_ok=True)
        os.makedirs(f'{self.output_dir}/diffs', exist_ok=True)

    def parse_pr_url(self, html_url: str) -> tuple:
        """
        Parse owner and repo name from PR HTML URL

        Args:
            html_url: PR HTML URL

        Returns:
            (owner, repo, pr_number) tuple
        """
        # For example: https://github.com/getsentry/sentry/pull/85268
        parts = html_url.rstrip('/').split('/')
        owner = parts[-4]
        repo = parts[-3]
        pr_number = parts[-1]
        return owner, repo, pr_number

    def get_pr_commits(self, owner: str, repo: str, pr_number: str) -> List[Dict]:
        """
        Get all commits in a PR

        Args:
            owner: Repository owner
            repo: Repository name
            pr_number: PR number

        Returns:
            List of commits
        """
        url = f'https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}/commits'
        try:
            response = self.session.get(url)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to get PR {pr_number} commits: {e}")
            return []

    def get_pr_files(self, owner: str, repo: str, pr_number: str) -> List[Dict]:
        """
        Get the list of modified files in a PR

        Args:
            owner: Repository owner
            repo: Repository name
            pr_number: PR number

        Returns:
            List of files
        """
        url = f'https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}/files'
        try:
            response = self.session.get(url)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to get PR {pr_number} files: {e}")
            return []

    def get_file_content(self, owner: str, repo: str, file_path: str, ref: str) -> Optional[str]:
        """
        Get the full content of a file in a specific commit

        Args:
            owner: Repository owner
            repo: Repository name
            file_path: File path
            ref: commit SHA or branch name

        Returns:
            File content or None
        """
        url = f'https://api.github.com/repos/{owner}/{repo}/contents/{file_path}?ref={ref}'
        try:
            response = self.session.get(url)
            response.raise_for_status()
            data = response.json()

            # GitHub API returns content in base64 encoding
            import base64
            content = base64.b64decode(data['content']).decode('utf-8')
            return content
        except Exception as e:
            logger.error(f"Failed to get content from {file_path} @ {ref}: {e}")
            return None

    def save_pr_data(self, pr_info: Dict, commits: List[Dict], files: List[Dict],
                     file_contents: Dict[str, Dict]) -> None:
        """
        Save PR data to files

        Args:
            pr_info: Basic PR information
            commits: List of commits
            files: List of files
            file_contents: Dictionary of file contents
        """
        pr_number = pr_info['number']

        # Save PR basic information and commits
        pr_data = {
            'pr_info': pr_info,
            'commits': commits,
            'files': files,
            'scraped_at': datetime.now().isoformat()
        }

        with open(f'{self.output_dir}/pr_{pr_number}.json', 'w', encoding='utf-8') as f:
            json.dump(pr_data, f, indent=2, ensure_ascii=False)

        # Save each file’s content and diff
        for file_info in files:
            filename = file_info['filename'].replace('/', '_')
            safe_filename = f"pr_{pr_number}_{filename}"

            # Save diff
            if 'patch' in file_info:
                with open(f'{self.output_dir}/diffs/{safe_filename}.diff', 'w', encoding='utf-8') as f:
                    f.write(file_info['patch'])

            # Save full file content (if obtained)
            if file_info['filename'] in file_contents:
                content_data = file_contents[file_info['filename']]

                # Save content before modification
                if 'before' in content_data and content_data['before']:
                    with open(f'{self.output_dir}/files/{safe_filename}_before.txt', 'w', encoding='utf-8') as f:
                        f.write(content_data['before'])

                # Save content after modification
                if 'after' in content_data and content_data['after']:
                    with open(f'{self.output_dir}/files/{safe_filename}_after.txt', 'w', encoding='utf-8') as f:
                        f.write(content_data['after'])

    def scrape_pr(self, pr_info: Dict) -> bool:
        """
        Scrape all related information of a single PR

        Args:
            pr_info: PR information dictionary

        Returns:
            Success status
        """
        try:
            owner, repo, pr_number = self.parse_pr_url(pr_info['html_url'])
            logger.info(f"Start crawling PR {pr_number}: {pr_info['title']}")

            # Get commits
            commits = self.get_pr_commits(owner, repo, pr_number)
            if not commits:
                logger.warning(f"No commits in PR {pr_number}")
                return False

            # Get file list
            files = self.get_pr_files(owner, repo, pr_number)
            if not files:
                logger.warning(f"PR {pr_number} no modified files found")
                return False

            # Get file contents
            file_contents = {}
            for file_info in files:
                file_path = file_info['filename']

                # Skip binary and removed files
                if file_info.get('status') == 'removed':
                    continue

                content_data = {}

                # Use the parent of the first commit as the version before modification
                if file_info.get('status') != 'added' and commits:
                    first_commit = commits[0]
                    if first_commit.get('parents'):
                        parent_sha = first_commit['parents'][0]['sha']
                        before_content = self.get_file_content(owner, repo, file_path, parent_sha)
                        content_data['before'] = before_content

                # Get modified content (using the last commit)
                if commits:
                    last_commit_sha = commits[-1]['sha']
                    after_content = self.get_file_content(owner, repo, file_path, last_commit_sha)
                    content_data['after'] = after_content

                file_contents[file_path] = content_data

            # Save data
            self.save_pr_data(pr_info, commits, files, file_contents)

            logger.info(f"Crawl PR {pr_number} successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to crawl PR {pr_info.get('number', 'unknown')}: {e}")
            return False

    def scrape_from_csv(self, csv_file: str, max_prs: Optional[int] = None,
                        start_from: int = 0, delay: float = 1.0) -> None:
        """
        Read PR list from CSV file and scrape

        Args:
            csv_file: Path to CSV file
            max_prs: Maximum number of PRs to scrape (None means all)
            start_from: Start from which PR index
            delay: Delay between requests (seconds)
        """
        logger.info(f"Start read PR from {csv_file}")

        df = pd.read_csv(csv_file)
        logger.info(f"found {len(df)} PR")

        if start_from > 0:
            df = df.iloc[start_from:]
            logger.info(f"Start from PR {start_from} ")

        if max_prs:
            df = df.head(max_prs)
            logger.info(f"Limit scraping {max_prs} PRs")

        progress_file = 'scraper_progress.csv'
        processed_prs = set()

        if os.path.exists(progress_file):
            with open(progress_file, 'r') as f:
                reader = csv.reader(f)
                next(reader)
                for row in reader:
                    if row:
                        processed_prs.add(int(row[0]))
            logger.info(f"Found {len(processed_prs)} completed PR")

        success_count = 0
        error_count = 0

        for index, row in df.iterrows():
            pr_number = row['number']

            if pr_number in processed_prs:
                logger.info(f"Skip crawled PR {pr_number}")
                continue

            success = self.scrape_pr(row.to_dict())

            if success:
                success_count += 1

                with open(progress_file, 'a', newline='') as f:
                    writer = csv.writer(f)
                    if os.path.getsize(progress_file) == 0:
                        writer.writerow(['pr_number', 'status', 'timestamp'])
                    writer.writerow([pr_number, 'success', datetime.now().isoformat()])
            else:
                error_count += 1

            time.sleep(delay)

            if (success_count + error_count) % 10 == 0:
                logger.info(f"Progress: Success {success_count}, Fail {error_count}")

        logger.info(f"Complete crawling: success {success_count}, fail {error_count}")


def main():
    """
    Main function
    """

    github_token = ''

    if not github_token:
        print("Warning: no GITHUB_TOKEN")
        print("It is recommended to set GitHub token to get higher API request limits")
        print("export GITHUB_TOKEN=your_token_here")

    scraper = PullRequestScraper(github_token)


    csv_file = 'human_pull_request.csv'
    max_prs = None
    start_from = 0
    delay = 2.0

    try:
        scraper.scrape_from_csv(csv_file, max_prs=max_prs,
                                start_from=start_from, delay=delay)
    except KeyboardInterrupt:
        logger.info("User interrupted scraping")
    except Exception as e:
        logger.error(f"Error occurred during scraping: {e}")


if __name__ == '__main__':
    main()

def check_rate_limit(token=None):
    headers = {'Authorization': f'token {token}'} if token else {}
    response = requests.get('https://api.github.com/rate_limit', headers=headers)

    if response.status_code == 200:
        data = response.json()
        core = data['rate']
        print(f"API limit:")
        print(f"  Limit: {core['limit']}")
        print(f"  Remaining: {core['remaining']}")
        print(f"  Reset time: {datetime.fromtimestamp(core['reset'])}")

        if core['remaining'] == 0:
            wait_seconds = core['reset'] - int(time.time())
            print(f"Reach limits, need to wait {wait_seconds} seconds")
            return False
        return True
    else:
        print(f"Unable to get rate limit information: {response.status_code}")
        return False


# Run check