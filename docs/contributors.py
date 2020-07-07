"""
Get a list of contributors between now and a start date
"""

import datetime as dt
import os

from github import Github

# or using an access token
g = Github(os.environ.get("GITHUB_API_KEY"))

repo = g.get_repo("pydata/pandas-datareader")

start = dt.datetime(2019, 9, 26, 0, 0, 0)
pulls = repo.get_pulls(state="closed", sort="updated", base="master", direction="desc")

users = set()
for i, p in enumerate(pulls):
    print(f"{i}: {p.number}")
    if p.merged and p.merged_at >= start:
        users.update([p.user.name])
    if p.updated_at < start:
        break

contrib = sorted((c for c in users if c), key=lambda s: s.split(" ")[-1])

for c in contrib:
    print(f"- {c}")
