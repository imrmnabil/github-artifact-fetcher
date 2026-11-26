# CLI & TUI Reference

## Global Flags

| Flag | Default | Description |
| :--- | :--- | :--- |
| `--config <path>` | `~/.config/ghfetcher/config.toml` | Override configuration file path (also via `GHFETCHER_CONFIG`) |
| `--version` / `-v` | — | Print `ghfetcher` version, commit SHA, and build date |

## Environment Variables

| Variable | Description |
| :--- | :--- |
| `GHFETCHER_TOKENS` | Comma-, space-, or newline-separated GitHub PATs merged into the token pool |
| `GITHUB_TOKEN` | Fallback single GitHub token merged into the token pool |
| `GHFETCHER_CONFIG` | Custom path to `config.toml` |

---

## Commands

### `ghfetcher config`
Manage GitHub tokens and default runtime settings (saved atomically with `chmod 0600`).

- `ghfetcher config` — Opens the interactive TUI configuration menu (prints summary in non-TTY environments).
- `ghfetcher config add-token [token] [--label <name>]` — Prompts securely (masked) if `[token]` is omitted and adds it to the pool.
- `ghfetcher config import-gh [--label gh-cli]` — Imports your active token from `gh auth token`.
- `ghfetcher config list [--check]` — Lists masked tokens and defaults; `--check` probes live REST & GraphQL quota balances.
- `ghfetcher config remove-token <label-or-index>` — Removes a token from `config.toml`.
- `ghfetcher config set <key> <value>` — Updates defaults: `reserve`, `concurrency`, `git-concurrency`, `api-concurrency`, `db`, `keep-git`.

### `ghfetcher fetch`
Mine artifacts from one or more GitHub repositories (`owner/repo`, HTTPS URLs, or SSH URLs).

```bash
ghfetcher fetch -all facebook/react vercel/next.js --out react.db
```

| Flag | Default | Description |
| :--- | :--- | :--- |
| `-all`, `--all`, `-a` | `false` | Fetch all core research artifacts (default if no granular flags are passed) |
| `--commits` | `false` | Extract commits, messages, file churn, refs & PR commit mappings via bare Git |
| `--issues` | `false` | Fetch issues metadata via GraphQL |
| `--prs` | `false` | Fetch pull requests metadata via GraphQL |
| `--reviews` | `false` | Fetch PR code reviews & repo-wide inline diff review comments |
| `--comments` | `false` | Fetch issue & PR comments + reactions |
| `--timeline` | `false` | Fetch issue & PR lifecycle timeline events |
| `--patches` | `false` | Also store unified diff patches in `commit_files.patch` |
| `--keep-git` | `false` | Retain bare Git clones in `.ghfetcher/git-cache/` instead of auto-deleting |
| `-o, --out <file>` | `ghfetcher.db` | Output SQLite database path |
| `--per-repo-db` | `false` | Create an isolated `./ghfetcher_data/<owner>__<repo>.db` per repository |
| `--from <file>` | — | Read repository list from file (one per line, `#` comments ignored, `-` for stdin) |
| `-c, --concurrency <N>` | `3` | Maximum repositories processed concurrently |
| `--reserve <N>` | `100` | Per-token quota safety reserve left untouched |
| `--no-tui` | `false` | Disable interactive TUI and emit timestamped logs (for SSH/`nohup`/CI) |

### `ghfetcher attach` & `ghfetcher status`
Monitor or control a running `--no-tui` background fetch via SQLite WAL telemetry:

```bash
ghfetcher attach --db study.db   # Opens live htop TUI against running background job
ghfetcher status --db study.db   # One-shot table row counts & telemetry snapshot
```

### `ghfetcher export`
Stream normalized SQLite tables into columnar **Apache Parquet** (Snappy compressed) or **JSONL**:

```bash
ghfetcher export --db study.db --format parquet --out ./dataset_parquet
ghfetcher export --db study.db --format jsonl   --out ./dataset_jsonl
```

### `ghfetcher version`
Prints version, short commit SHA, and build timestamp.

---

## Interactive `htop` TUI Keybindings

| Key | Action |
| :--- | :--- |
| `Space` / `p` | Pause or resume new worker dispatches |
| `↑` / `↓` (`k` / `j`) | Move selection cursor across repositories |
| `Enter` / `l` / `Tab` | Toggle between Repo Inspector and Live Event Log |
| `+` / `-` | Dynamically increase or decrease concurrent repository workers (`1..16`) |
| `t` | Hot-reload tokens from `config.toml` & env vars without restarting |
| `q` / `Ctrl+C` | Graceful checkpoint & quit (press twice for instant exit; detaches in `attach` mode) |
| `X` *(attach mode only)* | Send remote graceful stop command to the headless background job |
