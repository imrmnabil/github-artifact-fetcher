# `ghfetcher` — Project Handoff & Context

> **For the AI agent on a new machine**: Read this file first to recover the full architectural context, design decisions, completed work, and next steps for `ghfetcher` (`github.com/imrmnabil/github-artifact-fetcher`).

---

## 1. Project Goal
`ghfetcher` is a high-efficiency, crash-resumable CLI + `htop`-style TUI for Mining Software Repositories (MSR) and empirical software engineering research. Its #1 priority is **minimizing GitHub API quota usage** by combining zero-API local bare Git extraction with hybrid dual-pool (`GraphQL` + `REST`) scheduling across multiple tokens.

## 2. Locked-In Design Decisions (From Design Interview)
- **Language & Stack**: Go 1.24+ (`CGO_ENABLED=0` static binary), `cobra` CLI, `bubbletea` + `lipgloss` + `huh` TUI, `pelletier/go-toml/v2`, pure-Go `modernc.org/sqlite` (WAL mode), `parquet-go/parquet-go` (Snappy Parquet + JSONL export).
- **Artifact Scope**: Core Git & PR/Issue History — commits, commit messages, file-level `+additions/-deletions` churn (`--numstat`, optional `--patches`), branches/tags, PR→commit SHA links, issues, PRs, PR code reviews, inline diff review comments, issue/PR comments, reactions, and lifecycle timeline events.
- **Local Bare Git Strategy**:
  - **2-step bare fetch**: Full fetch of `+refs/heads/*` and `+refs/tags/*` (for 100% offline `--numstat` churn) with live `git fetch --progress` streaming into the TUI, followed by `--filter=blob:none` fetch of `+refs/pull/*/head` so 30k+ PR refs download in seconds without pulling abandoned PR file blobs.
  - **In-memory PR→Commit linking**: Walks commit parent DAG in Go memory (0 subprocesses, 0 API calls) to populate `pr_commits`.
  - **Ephemeral by default**: Deletes temp `.git` directory right after SQLite flush (`--keep-git` to retain; uses `--shallow-since` on incremental re-runs).
- **Hybrid API & Multi-Token Pool**:
  - Every token has **two independent buckets**: 5,000 GraphQL pts/hr + 5,000 REST reqs/hr.
  - Keeps a configurable **100-quota safety reserve** per token by default.
  - Auto-pauses API pools on quota exhaustion with a live countdown while **local Git workers keep running offline**, then auto-resumes.
  - **Single-repo parallelism**: GraphQL streams are state-partitioned (`OPEN` / `CLOSED` issues, `MERGED` / `OPEN+CLOSED` PRs; 45 issues / 35 PRs per page) while REST sweeps `/repos/{owner}/{repo}/pulls/comments` repo-wide (100/call + free `304` `ETag` caching).
- **Config & Storage**:
  - `ghfetcher config` interactive `huh` TUI + scriptable subcommands (`add-token`, `import-gh`, `list --check`, `remove-token`, `set`), stored in SSH-friendly `0600` `~/.config/ghfetcher/config.toml` + `GHFETCHER_TOKENS` / `GITHUB_TOKEN` env overrides.
  - Single unified SQLite WAL DB per study by default (`--out study.db`, optional `--per-repo-db`), with atomic `_sync_checkpoints` and remote `ghfetcher attach` / `status` via `_runtime_telemetry` and `_runtime_commands`.
  - Supports single-dash `-all` (`ghfetcher fetch -all <repo>...`) via arg normalization in `cmd/ghfetcher/main.go`.

---

## 3. Codebase Map

| Path | Responsibility |
| :--- | :--- |
| `cmd/ghfetcher/main.go` | Entrypoint + `-all` -> `--all` normalization |
| `internal/cli/` | Cobra commands: `config`, `fetch`, `attach`, `status`, `export`, `estimate` |
| `internal/config/` | `0600` TOML config, env token merge, `gh auth token` importer, unit tests |
| `internal/tokenpool/` | Dual-bucket (`GraphQL` + `REST`) load balancer, 100-reserve guard, hot-reload, unit tests |
| `internal/store/` | Pure-Go SQLite WAL schema (12 artifact tables + checkpoint/etag/telemetry tables) |
| `internal/gitlocal/` | 2-step bare Git fetch, live progress parser, process-group cancel, streaming `--numstat` & PR-commit linker, tests |
| `internal/github/` | HTTP/2 GraphQL + REST clients, `ETag` 304 cache, secondary rate-limit backoff, state-partitioned fetchers, tests |
| `internal/pipeline/` | Multi-repo orchestrator, parallel stream coordinator, blended EWMA+cumulative rate-limit-aware ETA, remote command polling |
| `internal/tui/` | Interactive `htop`-style Bubble Tea dashboard (`Space/p`, `+/-`, `t`, `Enter/l`, `q` + 2nd-press force quit) & `attach` controller |
| `internal/export/` | Streaming SQLite → Snappy Apache Parquet & JSONL exporter + tests |

---

## 4. Current Status & Recent Fixes (as of commit `9c24e7c`)
- All 5 core phases are implemented and all tests pass (`go test -v ./...`).
- **Real-world benchmarked on `huggingface/transformers`** (25k commits, 20k issues, 30k PRs), which uncovered and led to fixing:
  1. Single-repo GraphQL parallelism (state-partitioned 4-stream GraphQL + larger pages).
  2. Blobless `+refs/pull/*/head` fetch + live `git fetch --progress` display in TUI.
  3. Live TUI ETA math (removed broken `< 1.5 -> 3.0` rate clamp; excluded 0-commit clone wait from skewing API ETA).
  4. `CHECKPOINTING...` hang on `q` / `Ctrl+C` (fixed via `Setpgid: true` + `syscall.Kill(-pid, SIGKILL)` + `WaitDelay: 1s` on `git` subprocesses and 2nd-press immediate quit in TUI).
- **Note on `estimate`**: The user chose to **skip/defer** calibrating `ghfetcher estimate` for now and prioritize real-run speed and stability.

---

## 5. Setup on a New PC

```bash
git clone https://github.com/imrmnabil/github-artifact-fetcher.git
cd github-artifact-fetcher

# Requires Go 1.24+ and git
CGO_ENABLED=0 go build -o ghfetcher ./cmd/ghfetcher
go test -v ./...

# Configure GitHub token(s)
./ghfetcher config import-gh   # or ./ghfetcher config

# Run (or resume by copying an existing .db file here)
./ghfetcher fetch -all <owner/repo> --out study.db
```

---

## 6. Ideas / Optional Next Steps
- **Smart Git Email → GitHub Login Linker & Bot Tagger**: Offline `<id>+<login>@users.noreply.github.com` parsing + 1 sample commit lookup per remaining unique email + `is_bot` flag.
- **Time-Window & Corpus Filters**: `--since` / `--until` slicing, `--org <org>`, `--search "<query>"`.
- **Re-calibrate `ghfetcher estimate`**: When ready, update `estimate` to reflect real multi-stream GraphQL latency (~1.5–2.5s/page) and clone download time.
- **Academic Replication Manifest**: Auto-emit `manifest.json` (timestamps, HEAD SHAs, tool version) alongside Parquet exports.
