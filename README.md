# `ghfetcher` — High-Efficiency GitHub Artifact Fetcher for Research

`ghfetcher` is a single-binary Go CLI and `htop`-style terminal dashboard built for **Mining Software Repositories (MSR)** and empirical software engineering research. It extracts commits, line-level churn, pull requests, issues, code reviews, inline diff comments, reactions, and lifecycle timeline events while slashing GitHub API quota consumption by **90–99%**.

## How It Saves API Quota

1. **Zero-API Ephemeral Bare Git Extraction (`+refs/pull/*`)**: Clones bare Git repositories locally (with `+refs/pull/*/head`), streaming 100% of commits, commit messages, parent DAGs, file-level `+additions/-deletions` churn (`--numstat`), optional unified patches, and **PR-to-commit SHA mappings** (even for closed/unmerged PRs) for **0 REST/GraphQL calls**, then cleans up `.git` automatically.
2. **Dual-Pool Per-Token Parallelism**: Every GitHub PAT has **5,000 GraphQL points/hr** *and* **5,000 REST requests/hr** in separate buckets. `ghfetcher` schedules both concurrently across multiple tokens while keeping a configurable **100-quota safety reserve** per token.
3. **1-Point Bundled GraphQL + Repo-Wide Bulk REST**:
   - **GraphQL** fetches Issues & PRs ordered by `UPDATED_AT` with labels, assignees, reactions, reviews, comments, and timeline events bundled inline (~95% of items need zero follow-up calls).
   - **REST** sweeps `/repos/{owner}/{repo}/pulls/comments?per_page=100` repo-wide (100 inline diff comments per request instead of per-PR requests) with `ETag` (`If-None-Match`) caching (`304 Not Modified` costs **0** quota).
4. **Offline Continuation During API Cooldowns**: If all tokens reach their safety reserve on huge studies, API workers sleep until the earliest hourly reset and auto-resume—while local Git clone/churn workers keep indexing queued repos uninterrupted.

---

## Quick Start

### Install (Pre-Built Binary)

**Linux & macOS (one-liner with SHA256 verification):**

```bash
curl -fsSL https://raw.githubusercontent.com/imrmnabil/github-artifact-fetcher/main/install.sh | sh
```

**Windows / Manual Download:**
Download the pre-built archive for Linux (`amd64`/`arm64`), macOS (`amd64`/`arm64`), or Windows (`amd64`) from the [GitHub Releases](https://github.com/imrmnabil/github-artifact-fetcher/releases) page.

**Build from Source (Go 1.24+):**

```bash
CGO_ENABLED=0 go build -o ghfetcher ./cmd/ghfetcher
```

### 1. Configure Tokens (`ghfetcher config`)

```bash
# Interactive TUI setup wizard (saves chmod 0600 ~/.config/ghfetcher/config.toml)
ghfetcher config

# Or scriptable CLI commands (SSH-friendly)
ghfetcher config import-gh
ghfetcher config add-token --label lab-pat-2
ghfetcher config list --check
```

*(You can also pass comma-separated tokens via `GHFETCHER_TOKENS=ghp_1...,ghp_2...` or `GITHUB_TOKEN`.)*

### 2. Fetch All Research Artifacts (`ghfetcher fetch -all`)

```bash
# Launches the interactive htop-style TUI
ghfetcher fetch -all facebook/react vercel/next.js --out react_study.db

# Fetch from a corpus file with full unified commit patches
ghfetcher fetch -all --patches --from repos.txt --out corpus.db

# Run headless on an SSH research server, then attach the live htop TUI anytime!
nohup ghfetcher fetch -all kubernetes/kubernetes --out k8s.db --no-tui > fetch.log 2>&1 &
ghfetcher attach --db k8s.db
ghfetcher status --db k8s.db
```

### Interactive `htop` TUI Hotkeys
- `Space` / `p` — Pause / Resume pipeline cleanly
- `↑` / `↓` (`j` / `k`) — Select repository in process table
- `Enter` / `l` — Toggle detailed Repo Inspector vs. Live Event Log
- `+` / `-` — Dynamically increase or decrease concurrent repository workers
- `t` — Hot-reload tokens from `config.toml` / env without restarting
- `q` / `Ctrl+C` — Flush SQLite WAL checkpoints and exit cleanly (`ghfetcher fetch` resumes right where it left off!)

### 3. Export to Parquet or JSONL (`ghfetcher export`)

```bash
ghfetcher export --db react_study.db --format parquet --out ./parquet_data
ghfetcher export --db react_study.db --format jsonl   --out ./jsonl_data
```

Query directly in **DuckDB** or **Python / Polars**:

```python
import duckdb

con = duckdb.connect()
df = con.execute("""
    SELECT pr.number, pr.title, pr.merged, COUNT(pc.commit_sha) AS pr_commits, SUM(c.additions) AS lines_added
    FROM 'parquet_data/pull_requests.parquet' pr
    LEFT JOIN 'parquet_data/pr_commits.parquet' pc USING (repo_id, number)
    LEFT JOIN 'parquet_data/commits.parquet' c ON c.repo_id = pc.repo_id AND c.sha = pc.commit_sha
    GROUP BY ALL
    ORDER BY lines_added DESC
    LIMIT 10
""").df()
```
