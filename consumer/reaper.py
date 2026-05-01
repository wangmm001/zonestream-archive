"""Watchdog: open an issue if no data has been committed for too long.

Runs from a workflow scheduled every hour. Finds the most recent commit by
zonestream-bot and compares its age against ZS_STALE_HOURS. Idempotent: if an
open issue with the same title already exists, it just adds a comment.

Env:
- ZS_STALE_HOURS : int, default 12
- GITHUB_TOKEN   : provided by GitHub Actions (gh CLI uses it via GH_TOKEN)
- GITHUB_REPOSITORY : provided by GitHub Actions
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time

ISSUE_TITLE = "zonestream consumer appears stalled"


def last_data_commit_unix() -> int | None:
    out = subprocess.check_output(
        [
            "git", "log", "-1", "--format=%ct",
            "--author=zonestream-bot",
            "--", "data/", "state/",
        ],
        text=True,
    ).strip()
    return int(out) if out else None


def find_open_issue() -> int | None:
    out = subprocess.check_output(
        [
            "gh", "issue", "list",
            "--state", "open",
            "--search", ISSUE_TITLE,
            "--json", "number,title",
        ],
        text=True,
    )
    for it in json.loads(out):
        if it["title"] == ISSUE_TITLE:
            return int(it["number"])
    return None


def main() -> int:
    stale_hours = int(os.environ.get("ZS_STALE_HOURS", "12"))
    last = last_data_commit_unix()
    now = int(time.time())

    if last is None:
        body = "No data commits found yet. Has the consumer ever run?"
        age_h = float("inf")
    else:
        age_h = (now - last) / 3600.0
        if age_h <= stale_hours:
            print(f"OK: last data commit {age_h:.1f}h ago (<= {stale_hours}h)")
            return 0
        body = (
            f"Last `data/` commit was {age_h:.1f}h ago, threshold is {stale_hours}h.\n\n"
            f"- last commit unix: `{last}`\n"
            f"- now unix: `{now}`\n"
            f"- run: <https://github.com/{os.environ['GITHUB_REPOSITORY']}"
            f"/actions/runs/{os.environ.get('GITHUB_RUN_ID', '?')}>\n\n"
            f"Triggering a `consume` workflow run automatically."
        )

    print(f"STALE: {body}")

    existing = find_open_issue()
    if existing is None:
        subprocess.check_call(
            ["gh", "issue", "create", "--title", ISSUE_TITLE,
             "--body", body, "--label", "stalled"]
        )
    else:
        subprocess.check_call(
            ["gh", "issue", "comment", str(existing), "--body", body]
        )

    subprocess.call(["gh", "workflow", "run", "consume.yml"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
