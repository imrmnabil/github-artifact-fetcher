package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"
)

// Store wraps a pure-Go SQLite database in WAL mode with a serialized batched writer
// so concurrent Git and API workers never contend on SQLITE_BUSY while readers run lock-free.
type Store struct {
	path        string
	db          *sql.DB
	writeMu     sync.Mutex
	rowsWritten atomic.Int64
}

// Open initializes (or opens) a SQLite database at path with WAL mode and runs schema migrations.
func Open(path string) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("database path cannot be empty")
	}
	if dir := filepath.Dir(path); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create db directory %s: %w", dir, err)
		}
	}

	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(15000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(ON)&_pragma=temp_store(MEMORY)&_pragma=cache_size(-64000)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite %s: %w", path, err)
	}
	db.SetMaxOpenConns(6)
	db.SetMaxIdleConns(4)

	s := &Store{path: path, db: db}
	if err := s.migrate(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate sqlite schema: %w", err)
	}
	return s, nil
}

func (s *Store) Path() string          { return s.path }
func (s *Store) DB() *sql.DB           { return s.db }
func (s *Store) RowsWritten() int64    { return s.rowsWritten.Load() }

func (s *Store) Close() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, _ = s.db.Exec(`PRAGMA wal_checkpoint(PASSIVE);`)
	return s.db.Close()
}

func (s *Store) FileSizeBytes() int64 {
	var total int64
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if info, err := os.Stat(s.path + suffix); err == nil {
			total += info.Size()
		}
	}
	return total
}

const schemaSQL = `
CREATE TABLE IF NOT EXISTS repositories (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	github_id INTEGER NOT NULL DEFAULT 0,
	owner TEXT NOT NULL,
	name TEXT NOT NULL,
	full_name TEXT NOT NULL UNIQUE,
	default_branch TEXT NOT NULL DEFAULT 'main',
	is_fork INTEGER NOT NULL DEFAULT 0,
	stars INTEGER NOT NULL DEFAULT 0,
	forks INTEGER NOT NULL DEFAULT 0,
	disk_kb INTEGER NOT NULL DEFAULT 0,
	total_commits_est INTEGER NOT NULL DEFAULT 0,
	total_issues_est INTEGER NOT NULL DEFAULT 0,
	total_prs_est INTEGER NOT NULL DEFAULT 0,
	cloned_head_sha TEXT NOT NULL DEFAULT '',
	last_commit_at TEXT NOT NULL DEFAULT '',
	last_synced_at TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS users (
	github_id INTEGER NOT NULL DEFAULT 0,
	login TEXT PRIMARY KEY,
	name TEXT NOT NULL DEFAULT '',
	email TEXT NOT NULL DEFAULT '',
	type TEXT NOT NULL DEFAULT 'User'
);

CREATE TABLE IF NOT EXISTS commits (
	repo_id INTEGER NOT NULL,
	sha TEXT NOT NULL,
	author_name TEXT NOT NULL DEFAULT '',
	author_email TEXT NOT NULL DEFAULT '',
	authored_at TEXT NOT NULL DEFAULT '',
	committer_name TEXT NOT NULL DEFAULT '',
	committer_email TEXT NOT NULL DEFAULT '',
	committed_at TEXT NOT NULL DEFAULT '',
	message TEXT NOT NULL DEFAULT '',
	parents_json TEXT NOT NULL DEFAULT '[]',
	files_changed INTEGER NOT NULL DEFAULT 0,
	additions INTEGER NOT NULL DEFAULT 0,
	deletions INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (repo_id, sha),
	FOREIGN KEY (repo_id) REFERENCES repositories(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_commits_repo_committed ON commits(repo_id, committed_at);

CREATE TABLE IF NOT EXISTS commit_files (
	repo_id INTEGER NOT NULL,
	commit_sha TEXT NOT NULL,
	file_path TEXT NOT NULL,
	old_path TEXT NOT NULL DEFAULT '',
	change_type TEXT NOT NULL DEFAULT 'M',
	additions INTEGER NOT NULL DEFAULT 0,
	deletions INTEGER NOT NULL DEFAULT 0,
	patch TEXT NOT NULL DEFAULT '',
	PRIMARY KEY (repo_id, commit_sha, file_path)
);

CREATE TABLE IF NOT EXISTS refs (
	repo_id INTEGER NOT NULL,
	ref_name TEXT NOT NULL,
	target_sha TEXT NOT NULL,
	ref_type TEXT NOT NULL,
	PRIMARY KEY (repo_id, ref_name)
);

CREATE TABLE IF NOT EXISTS pr_commits (
	repo_id INTEGER NOT NULL,
	pr_number INTEGER NOT NULL,
	commit_sha TEXT NOT NULL,
	ordinal INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (repo_id, pr_number, commit_sha)
);
CREATE INDEX IF NOT EXISTS idx_pr_commits_pr ON pr_commits(repo_id, pr_number);

CREATE TABLE IF NOT EXISTS issues (
	repo_id INTEGER NOT NULL,
	number INTEGER NOT NULL,
	github_id INTEGER NOT NULL DEFAULT 0,
	state TEXT NOT NULL DEFAULT '',
	state_reason TEXT NOT NULL DEFAULT '',
	title TEXT NOT NULL DEFAULT '',
	body TEXT NOT NULL DEFAULT '',
	author_login TEXT NOT NULL DEFAULT '',
	milestone TEXT NOT NULL DEFAULT '',
	labels_json TEXT NOT NULL DEFAULT '[]',
	assignees_json TEXT NOT NULL DEFAULT '[]',
	comment_count INTEGER NOT NULL DEFAULT 0,
	reactions_json TEXT NOT NULL DEFAULT '{}',
	created_at TEXT NOT NULL DEFAULT '',
	updated_at TEXT NOT NULL DEFAULT '',
	closed_at TEXT NOT NULL DEFAULT '',
	PRIMARY KEY (repo_id, number)
);
CREATE INDEX IF NOT EXISTS idx_issues_updated ON issues(repo_id, updated_at);

CREATE TABLE IF NOT EXISTS pull_requests (
	repo_id INTEGER NOT NULL,
	number INTEGER NOT NULL,
	github_id INTEGER NOT NULL DEFAULT 0,
	state TEXT NOT NULL DEFAULT '',
	is_draft INTEGER NOT NULL DEFAULT 0,
	merged INTEGER NOT NULL DEFAULT 0,
	title TEXT NOT NULL DEFAULT '',
	body TEXT NOT NULL DEFAULT '',
	author_login TEXT NOT NULL DEFAULT '',
	merged_by_login TEXT NOT NULL DEFAULT '',
	base_ref TEXT NOT NULL DEFAULT '',
	base_sha TEXT NOT NULL DEFAULT '',
	head_ref TEXT NOT NULL DEFAULT '',
	head_sha TEXT NOT NULL DEFAULT '',
	merge_commit_sha TEXT NOT NULL DEFAULT '',
	additions INTEGER NOT NULL DEFAULT 0,
	deletions INTEGER NOT NULL DEFAULT 0,
	changed_files INTEGER NOT NULL DEFAULT 0,
	comment_count INTEGER NOT NULL DEFAULT 0,
	review_count INTEGER NOT NULL DEFAULT 0,
	labels_json TEXT NOT NULL DEFAULT '[]',
	assignees_json TEXT NOT NULL DEFAULT '[]',
	closing_issues_json TEXT NOT NULL DEFAULT '[]',
	reactions_json TEXT NOT NULL DEFAULT '{}',
	created_at TEXT NOT NULL DEFAULT '',
	updated_at TEXT NOT NULL DEFAULT '',
	closed_at TEXT NOT NULL DEFAULT '',
	merged_at TEXT NOT NULL DEFAULT '',
	PRIMARY KEY (repo_id, number)
);
CREATE INDEX IF NOT EXISTS idx_prs_updated ON pull_requests(repo_id, updated_at);

CREATE TABLE IF NOT EXISTS issue_comments (
	repo_id INTEGER NOT NULL,
	comment_id INTEGER NOT NULL,
	item_number INTEGER NOT NULL DEFAULT 0,
	is_pr INTEGER NOT NULL DEFAULT 0,
	author_login TEXT NOT NULL DEFAULT '',
	body TEXT NOT NULL DEFAULT '',
	reactions_json TEXT NOT NULL DEFAULT '{}',
	created_at TEXT NOT NULL DEFAULT '',
	updated_at TEXT NOT NULL DEFAULT '',
	PRIMARY KEY (repo_id, comment_id)
);
CREATE INDEX IF NOT EXISTS idx_issue_comments_item ON issue_comments(repo_id, item_number);

CREATE TABLE IF NOT EXISTS pr_reviews (
	repo_id INTEGER NOT NULL,
	review_id INTEGER NOT NULL,
	pr_number INTEGER NOT NULL,
	author_login TEXT NOT NULL DEFAULT '',
	state TEXT NOT NULL DEFAULT '',
	commit_sha TEXT NOT NULL DEFAULT '',
	body TEXT NOT NULL DEFAULT '',
	submitted_at TEXT NOT NULL DEFAULT '',
	PRIMARY KEY (repo_id, review_id)
);
CREATE INDEX IF NOT EXISTS idx_pr_reviews_pr ON pr_reviews(repo_id, pr_number);

CREATE TABLE IF NOT EXISTS pr_review_comments (
	repo_id INTEGER NOT NULL,
	comment_id INTEGER NOT NULL,
	pr_number INTEGER NOT NULL DEFAULT 0,
	review_id INTEGER NOT NULL DEFAULT 0,
	in_reply_to_id INTEGER NOT NULL DEFAULT 0,
	author_login TEXT NOT NULL DEFAULT '',
	path TEXT NOT NULL DEFAULT '',
	commit_sha TEXT NOT NULL DEFAULT '',
	original_commit_sha TEXT NOT NULL DEFAULT '',
	diff_hunk TEXT NOT NULL DEFAULT '',
	line INTEGER NOT NULL DEFAULT 0,
	start_line INTEGER NOT NULL DEFAULT 0,
	side TEXT NOT NULL DEFAULT '',
	body TEXT NOT NULL DEFAULT '',
	reactions_json TEXT NOT NULL DEFAULT '{}',
	created_at TEXT NOT NULL DEFAULT '',
	updated_at TEXT NOT NULL DEFAULT '',
	PRIMARY KEY (repo_id, comment_id)
);
CREATE INDEX IF NOT EXISTS idx_pr_rev_comments_pr ON pr_review_comments(repo_id, pr_number);

CREATE TABLE IF NOT EXISTS timeline_events (
	repo_id INTEGER NOT NULL,
	event_key TEXT NOT NULL,
	item_number INTEGER NOT NULL,
	is_pr INTEGER NOT NULL DEFAULT 0,
	event_type TEXT NOT NULL,
	actor_login TEXT NOT NULL DEFAULT '',
	payload_json TEXT NOT NULL DEFAULT '{}',
	created_at TEXT NOT NULL DEFAULT '',
	PRIMARY KEY (repo_id, event_key)
);
CREATE INDEX IF NOT EXISTS idx_timeline_item ON timeline_events(repo_id, item_number);

CREATE TABLE IF NOT EXISTS _sync_checkpoints (
	repo_id INTEGER NOT NULL,
	stream_name TEXT NOT NULL,
	cursor TEXT NOT NULL DEFAULT '',
	watermark TEXT NOT NULL DEFAULT '',
	items_done INTEGER NOT NULL DEFAULT 0,
	completed INTEGER NOT NULL DEFAULT 0,
	updated_at TEXT NOT NULL DEFAULT '',
	PRIMARY KEY (repo_id, stream_name)
);

CREATE TABLE IF NOT EXISTS _etag_cache (
	cache_key TEXT PRIMARY KEY,
	etag TEXT NOT NULL,
	updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS _runtime_telemetry (
	id INTEGER PRIMARY KEY CHECK (id = 1),
	pid INTEGER NOT NULL,
	started_at TEXT NOT NULL,
	updated_at TEXT NOT NULL,
	payload_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS _runtime_commands (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	command TEXT NOT NULL,
	arg TEXT NOT NULL DEFAULT '',
	created_at TEXT NOT NULL,
	processed INTEGER NOT NULL DEFAULT 0
);
`

func (s *Store) migrate() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, err := s.db.Exec(schemaSQL)
	return err
}

func fmtTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

func parseTime(v string) time.Time {
	if v == "" {
		return time.Time{}
	}
	t, _ := time.Parse(time.RFC3339, v)
	return t
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func mustJSON(v any, fallback string) string {
	if v == nil {
		return fallback
	}
	b, err := json.Marshal(v)
	if err != nil {
		return fallback
	}
	return string(b)
}

// UpsertRepository creates or updates a repository row and returns its numeric ID.
func (s *Store) UpsertRepository(ctx context.Context, r *Repository) (int64, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	fullName := strings.ToLower(fmt.Sprintf("%s/%s", r.Owner, r.Name))
	if r.DefaultBranch == "" {
		r.DefaultBranch = "main"
	}

	_, err := s.db.ExecContext(ctx, `
		INSERT INTO repositories (
			github_id, owner, name, full_name, default_branch, is_fork, stars, forks,
			disk_kb, total_commits_est, total_issues_est, total_prs_est,
			cloned_head_sha, last_commit_at, last_synced_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(full_name) DO UPDATE SET
			github_id         = CASE WHEN excluded.github_id > 0 THEN excluded.github_id ELSE repositories.github_id END,
			default_branch    = CASE WHEN excluded.default_branch != '' THEN excluded.default_branch ELSE repositories.default_branch END,
			is_fork           = excluded.is_fork,
			stars             = CASE WHEN excluded.stars > 0 THEN excluded.stars ELSE repositories.stars END,
			forks             = CASE WHEN excluded.forks > 0 THEN excluded.forks ELSE repositories.forks END,
			disk_kb           = CASE WHEN excluded.disk_kb > 0 THEN excluded.disk_kb ELSE repositories.disk_kb END,
			total_commits_est = CASE WHEN excluded.total_commits_est > 0 THEN excluded.total_commits_est ELSE repositories.total_commits_est END,
			total_issues_est  = CASE WHEN excluded.total_issues_est > 0 THEN excluded.total_issues_est ELSE repositories.total_issues_est END,
			total_prs_est     = CASE WHEN excluded.total_prs_est > 0 THEN excluded.total_prs_est ELSE repositories.total_prs_est END,
			cloned_head_sha   = CASE WHEN excluded.cloned_head_sha != '' THEN excluded.cloned_head_sha ELSE repositories.cloned_head_sha END,
			last_commit_at    = CASE WHEN excluded.last_commit_at != '' THEN excluded.last_commit_at ELSE repositories.last_commit_at END,
			last_synced_at    = CASE WHEN excluded.last_synced_at != '' THEN excluded.last_synced_at ELSE repositories.last_synced_at END
	`,
		r.GitHubID, r.Owner, r.Name, fullName, r.DefaultBranch, boolToInt(r.IsFork), r.Stars, r.Forks,
		r.DiskKB, r.TotalCommitsEst, r.TotalIssuesEst, r.TotalPRsEst,
		r.ClonedHeadSHA, fmtTime(r.LastCommitAt), fmtTime(r.LastSyncedAt),
	)
	if err != nil {
		return 0, fmt.Errorf("upsert repository %s: %w", fullName, err)
	}

	var id int64
	if err := s.db.QueryRowContext(ctx, `SELECT id FROM repositories WHERE full_name = ?`, fullName).Scan(&id); err != nil {
		return 0, err
	}
	r.ID = id
	s.rowsWritten.Add(1)
	return id, nil
}

// GetRepository fetches existing repository state (e.g. LastCommitAt for incremental git clone).
func (s *Store) GetRepository(ctx context.Context, owner, name string) (*Repository, error) {
	fullName := strings.ToLower(fmt.Sprintf("%s/%s", owner, name))
	row := s.db.QueryRowContext(ctx, `
		SELECT id, github_id, owner, name, full_name, default_branch, is_fork, stars, forks,
		       disk_kb, total_commits_est, total_issues_est, total_prs_est,
		       cloned_head_sha, last_commit_at, last_synced_at
		FROM repositories WHERE full_name = ?
	`, fullName)

	var r Repository
	var isFork int
	var lastCommit, lastSync string
	err := row.Scan(
		&r.ID, &r.GitHubID, &r.Owner, &r.Name, &r.FullName, &r.DefaultBranch, &isFork, &r.Stars, &r.Forks,
		&r.DiskKB, &r.TotalCommitsEst, &r.TotalIssuesEst, &r.TotalPRsEst,
		&r.ClonedHeadSHA, &lastCommit, &lastSync,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	r.IsFork = isFork != 0
	r.LastCommitAt = parseTime(lastCommit)
	r.LastSyncedAt = parseTime(lastSync)
	return &r, nil
}

// UpsertCommitBatch writes a batch of commits and their file churn rows atomically.
func (s *Store) UpsertCommitBatch(ctx context.Context, commits []Commit) error {
	if len(commits) == 0 {
		return nil
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	commitStmt, err := tx.PrepareContext(ctx, `
		INSERT INTO commits (
			repo_id, sha, author_name, author_email, authored_at,
			committer_name, committer_email, committed_at, message,
			parents_json, files_changed, additions, deletions
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(repo_id, sha) DO UPDATE SET
			files_changed = excluded.files_changed,
			additions     = excluded.additions,
			deletions     = excluded.deletions
	`)
	if err != nil {
		return err
	}
	defer commitStmt.Close()

	fileStmt, err := tx.PrepareContext(ctx, `
		INSERT INTO commit_files (
			repo_id, commit_sha, file_path, old_path, change_type, additions, deletions, patch
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(repo_id, commit_sha, file_path) DO UPDATE SET
			additions = excluded.additions,
			deletions = excluded.deletions,
			patch     = CASE WHEN excluded.patch != '' THEN excluded.patch ELSE commit_files.patch END
	`)
	if err != nil {
		return err
	}
	defer fileStmt.Close()

	var written int64
	for _, c := range commits {
		if _, err := commitStmt.ExecContext(ctx,
			c.RepoID, c.SHA, c.AuthorName, c.AuthorEmail, fmtTime(c.AuthoredAt),
			c.CommitterName, c.CommitterEmail, fmtTime(c.CommittedAt), c.Message,
			mustJSON(c.ParentSHAs, "[]"), c.FilesChanged, c.Additions, c.Deletions,
		); err != nil {
			return err
		}
		written++

		for _, f := range c.Files {
			if _, err := fileStmt.ExecContext(ctx,
				c.RepoID, c.SHA, f.FilePath, f.OldPath, f.ChangeType, f.Additions, f.Deletions, f.Patch,
			); err != nil {
				return err
			}
			written++
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	s.rowsWritten.Add(written)
	return nil
}

// UpsertRefsAndPRCommits stores refs and zero-API PR-to-commit mappings in one transaction.
func (s *Store) UpsertRefsAndPRCommits(ctx context.Context, refs []GitRef, prCommits []PRCommit, headSHA string, latestCommitAt time.Time, repoID int64) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if len(refs) > 0 {
		stmt, err := tx.PrepareContext(ctx, `
			INSERT INTO refs (repo_id, ref_name, target_sha, ref_type)
			VALUES (?, ?, ?, ?)
			ON CONFLICT(repo_id, ref_name) DO UPDATE SET target_sha = excluded.target_sha
		`)
		if err != nil {
			return err
		}
		for _, r := range refs {
			if _, err := stmt.ExecContext(ctx, r.RepoID, r.RefName, r.TargetSHA, r.RefType); err != nil {
				_ = stmt.Close()
				return err
			}
		}
		_ = stmt.Close()
	}

	if len(prCommits) > 0 {
		stmt, err := tx.PrepareContext(ctx, `
			INSERT INTO pr_commits (repo_id, pr_number, commit_sha, ordinal)
			VALUES (?, ?, ?, ?)
			ON CONFLICT(repo_id, pr_number, commit_sha) DO UPDATE SET ordinal = excluded.ordinal
		`)
		if err != nil {
			return err
		}
		for _, pc := range prCommits {
			if _, err := stmt.ExecContext(ctx, pc.RepoID, pc.PRNumber, pc.CommitSHA, pc.Ordinal); err != nil {
				_ = stmt.Close()
				return err
			}
		}
		_ = stmt.Close()
	}

	if repoID > 0 && (headSHA != "" || !latestCommitAt.IsZero()) {
		_, _ = tx.ExecContext(ctx, `
			UPDATE repositories
			SET cloned_head_sha = CASE WHEN ? != '' THEN ? ELSE cloned_head_sha END,
			    last_commit_at  = CASE WHEN ? != '' THEN ? ELSE last_commit_at END
			WHERE id = ?
		`, headSHA, headSHA, fmtTime(latestCommitAt), fmtTime(latestCommitAt), repoID)
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	s.rowsWritten.Add(int64(len(refs) + len(prCommits)))
	return nil
}

// ArtifactBatch bundles API-fetched records + checkpoint update for atomic persistence.
type ArtifactBatch struct {
	Users          []User
	Issues         []Issue
	PullRequests   []PullRequest
	IssueComments  []IssueComment
	PRReviews      []PRReview
	ReviewComments []PRReviewComment
	TimelineEvents []TimelineEvent
	Checkpoint     *SyncCheckpoint
}

// SaveArtifactBatch persists mixed GitHub artifacts and advances the stream checkpoint atomically.
func (s *Store) SaveArtifactBatch(ctx context.Context, b ArtifactBatch) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	var written int64

	if len(b.Users) > 0 {
		stmt, err := tx.PrepareContext(ctx, `
			INSERT INTO users (github_id, login, name, email, type)
			VALUES (?, ?, ?, ?, ?)
			ON CONFLICT(login) DO UPDATE SET
				github_id = CASE WHEN excluded.github_id > 0 THEN excluded.github_id ELSE users.github_id END,
				name      = CASE WHEN excluded.name != '' THEN excluded.name ELSE users.name END,
				email     = CASE WHEN excluded.email != '' THEN excluded.email ELSE users.email END,
				type      = CASE WHEN excluded.type != '' THEN excluded.type ELSE users.type END
		`)
		if err != nil {
			return err
		}
		for _, u := range b.Users {
			if u.Login == "" {
				continue
			}
			if _, err := stmt.ExecContext(ctx, u.GitHubID, u.Login, u.Name, u.Email, u.Type); err != nil {
				_ = stmt.Close()
				return err
			}
			written++
		}
		_ = stmt.Close()
	}

	if len(b.Issues) > 0 {
		stmt, err := tx.PrepareContext(ctx, `
			INSERT INTO issues (
				repo_id, number, github_id, state, state_reason, title, body,
				author_login, milestone, labels_json, assignees_json,
				comment_count, reactions_json, created_at, updated_at, closed_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(repo_id, number) DO UPDATE SET
				state          = excluded.state,
				state_reason   = excluded.state_reason,
				title          = excluded.title,
				body           = excluded.body,
				milestone      = excluded.milestone,
				labels_json    = excluded.labels_json,
				assignees_json = excluded.assignees_json,
				comment_count  = excluded.comment_count,
				reactions_json = excluded.reactions_json,
				updated_at     = excluded.updated_at,
				closed_at      = excluded.closed_at
		`)
		if err != nil {
			return err
		}
		for _, is := range b.Issues {
			if _, err := stmt.ExecContext(ctx,
				is.RepoID, is.Number, is.GitHubID, is.State, is.StateReason, is.Title, is.Body,
				is.AuthorLogin, is.Milestone, mustJSON(is.Labels, "[]"), mustJSON(is.Assignees, "[]"),
				is.CommentCount, nonEmptyJSON(is.Reactions, "{}"),
				fmtTime(is.CreatedAt), fmtTime(is.UpdatedAt), fmtTime(is.ClosedAt),
			); err != nil {
				_ = stmt.Close()
				return err
			}
			written++
		}
		_ = stmt.Close()
	}

	if len(b.PullRequests) > 0 {
		stmt, err := tx.PrepareContext(ctx, `
			INSERT INTO pull_requests (
				repo_id, number, github_id, state, is_draft, merged, title, body,
				author_login, merged_by_login, base_ref, base_sha, head_ref, head_sha, merge_commit_sha,
				additions, deletions, changed_files, comment_count, review_count,
				labels_json, assignees_json, closing_issues_json, reactions_json,
				created_at, updated_at, closed_at, merged_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(repo_id, number) DO UPDATE SET
				state               = excluded.state,
				is_draft            = excluded.is_draft,
				merged              = excluded.merged,
				title               = excluded.title,
				body                = excluded.body,
				merged_by_login     = excluded.merged_by_login,
				head_sha            = excluded.head_sha,
				merge_commit_sha    = excluded.merge_commit_sha,
				additions           = excluded.additions,
				deletions           = excluded.deletions,
				changed_files       = excluded.changed_files,
				comment_count       = excluded.comment_count,
				review_count        = excluded.review_count,
				labels_json         = excluded.labels_json,
				assignees_json      = excluded.assignees_json,
				closing_issues_json = excluded.closing_issues_json,
				reactions_json      = excluded.reactions_json,
				updated_at          = excluded.updated_at,
				closed_at           = excluded.closed_at,
				merged_at           = excluded.merged_at
		`)
		if err != nil {
			return err
		}
		for _, pr := range b.PullRequests {
			if _, err := stmt.ExecContext(ctx,
				pr.RepoID, pr.Number, pr.GitHubID, pr.State, boolToInt(pr.IsDraft), boolToInt(pr.Merged), pr.Title, pr.Body,
				pr.AuthorLogin, pr.MergedByLogin, pr.BaseRef, pr.BaseSHA, pr.HeadRef, pr.HeadSHA, pr.MergeSHA,
				pr.Additions, pr.Deletions, pr.ChangedFiles, pr.CommentCount, pr.ReviewCount,
				mustJSON(pr.Labels, "[]"), mustJSON(pr.Assignees, "[]"), mustJSON(pr.ClosingIssues, "[]"), nonEmptyJSON(pr.Reactions, "{}"),
				fmtTime(pr.CreatedAt), fmtTime(pr.UpdatedAt), fmtTime(pr.ClosedAt), fmtTime(pr.MergedAt),
			); err != nil {
				_ = stmt.Close()
				return err
			}
			written++
		}
		_ = stmt.Close()
	}

	if len(b.IssueComments) > 0 {
		stmt, err := tx.PrepareContext(ctx, `
			INSERT INTO issue_comments (
				repo_id, comment_id, item_number, is_pr, author_login, body, reactions_json, created_at, updated_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(repo_id, comment_id) DO UPDATE SET
				body           = excluded.body,
				reactions_json = excluded.reactions_json,
				updated_at     = excluded.updated_at
		`)
		if err != nil {
			return err
		}
		for _, c := range b.IssueComments {
			if _, err := stmt.ExecContext(ctx,
				c.RepoID, c.CommentID, c.ItemNumber, boolToInt(c.IsPR), c.AuthorLogin, c.Body,
				nonEmptyJSON(c.Reactions, "{}"), fmtTime(c.CreatedAt), fmtTime(c.UpdatedAt),
			); err != nil {
				_ = stmt.Close()
				return err
			}
			written++
		}
		_ = stmt.Close()
	}

	if len(b.PRReviews) > 0 {
		stmt, err := tx.PrepareContext(ctx, `
			INSERT INTO pr_reviews (
				repo_id, review_id, pr_number, author_login, state, commit_sha, body, submitted_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(repo_id, review_id) DO UPDATE SET
				state        = excluded.state,
				body         = excluded.body,
				submitted_at = excluded.submitted_at
		`)
		if err != nil {
			return err
		}
		for _, r := range b.PRReviews {
			if _, err := stmt.ExecContext(ctx,
				r.RepoID, r.ReviewID, r.PRNumber, r.AuthorLogin, r.State, r.CommitSHA, r.Body, fmtTime(r.SubmittedAt),
			); err != nil {
				_ = stmt.Close()
				return err
			}
			written++
		}
		_ = stmt.Close()
	}

	if len(b.ReviewComments) > 0 {
		stmt, err := tx.PrepareContext(ctx, `
			INSERT INTO pr_review_comments (
				repo_id, comment_id, pr_number, review_id, in_reply_to_id, author_login,
				path, commit_sha, original_commit_sha, diff_hunk, line, start_line, side,
				body, reactions_json, created_at, updated_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(repo_id, comment_id) DO UPDATE SET
				body           = excluded.body,
				reactions_json = excluded.reactions_json,
				updated_at     = excluded.updated_at
		`)
		if err != nil {
			return err
		}
		for _, rc := range b.ReviewComments {
			if _, err := stmt.ExecContext(ctx,
				rc.RepoID, rc.CommentID, rc.PRNumber, rc.ReviewID, rc.InReplyToID, rc.AuthorLogin,
				rc.Path, rc.CommitSHA, rc.OriginalSHA, rc.DiffHunk, rc.Line, rc.StartLine, rc.Side,
				rc.Body, nonEmptyJSON(rc.Reactions, "{}"), fmtTime(rc.CreatedAt), fmtTime(rc.UpdatedAt),
			); err != nil {
				_ = stmt.Close()
				return err
			}
			written++
		}
		_ = stmt.Close()
	}

	if len(b.TimelineEvents) > 0 {
		stmt, err := tx.PrepareContext(ctx, `
			INSERT INTO timeline_events (
				repo_id, event_key, item_number, is_pr, event_type, actor_login, payload_json, created_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(repo_id, event_key) DO UPDATE SET
				payload_json = excluded.payload_json
		`)
		if err != nil {
			return err
		}
		for _, ev := range b.TimelineEvents {
			if _, err := stmt.ExecContext(ctx,
				ev.RepoID, ev.EventKey, ev.ItemNumber, boolToInt(ev.IsPR), ev.EventType,
				ev.ActorLogin, nonEmptyJSON(ev.Payload, "{}"), fmtTime(ev.CreatedAt),
			); err != nil {
				_ = stmt.Close()
				return err
			}
			written++
		}
		_ = stmt.Close()
	}

	if cp := b.Checkpoint; cp != nil {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO _sync_checkpoints (repo_id, stream_name, cursor, watermark, items_done, completed, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(repo_id, stream_name) DO UPDATE SET
				cursor     = excluded.cursor,
				watermark  = CASE WHEN excluded.watermark != '' THEN excluded.watermark ELSE _sync_checkpoints.watermark END,
				items_done = excluded.items_done,
				completed  = excluded.completed,
				updated_at = excluded.updated_at
		`, cp.RepoID, cp.StreamName, cp.Cursor, cp.Watermark, cp.ItemsDone, boolToInt(cp.Completed), fmtTime(time.Now()))
		if err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	s.rowsWritten.Add(written)
	return nil
}

func nonEmptyJSON(s, fallback string) string {
	if strings.TrimSpace(s) == "" {
		return fallback
	}
	return s
}

// GetCheckpoint retrieves saved cursor/watermark state for crash recovery and incremental runs.
func (s *Store) GetCheckpoint(ctx context.Context, repoID int64, streamName string) (*SyncCheckpoint, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT repo_id, stream_name, cursor, watermark, items_done, completed, updated_at
		FROM _sync_checkpoints WHERE repo_id = ? AND stream_name = ?
	`, repoID, streamName)

	var cp SyncCheckpoint
	var completed int
	var updated string
	if err := row.Scan(&cp.RepoID, &cp.StreamName, &cp.Cursor, &cp.Watermark, &cp.ItemsDone, &completed, &updated); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	cp.Completed = completed != 0
	cp.UpdatedAt = parseTime(updated)
	return &cp, nil
}

// GetETag / SetETag support zero-cost REST 304 Not Modified conditional requests.
func (s *Store) GetETag(ctx context.Context, key string) string {
	var etag string
	_ = s.db.QueryRowContext(ctx, `SELECT etag FROM _etag_cache WHERE cache_key = ?`, key).Scan(&etag)
	return etag
}

func (s *Store) SetETag(ctx context.Context, key, etag string) {
	if key == "" || etag == "" {
		return
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, _ = s.db.ExecContext(ctx, `
		INSERT INTO _etag_cache (cache_key, etag, updated_at)
		VALUES (?, ?, ?)
		ON CONFLICT(cache_key) DO UPDATE SET etag = excluded.etag, updated_at = excluded.updated_at
	`, key, etag, fmtTime(time.Now()))
}

// WriteTelemetry writes the current htop telemetry JSON for `ghfetcher attach` and `status`.
func (s *Store) WriteTelemetry(ctx context.Context, pid int, startedAt time.Time, payloadJSON string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO _runtime_telemetry (id, pid, started_at, updated_at, payload_json)
		VALUES (1, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			pid          = excluded.pid,
			started_at   = excluded.started_at,
			updated_at   = excluded.updated_at,
			payload_json = excluded.payload_json
	`, pid, fmtTime(startedAt), fmtTime(time.Now()), payloadJSON)
	return err
}

// ReadTelemetry reads the latest worker telemetry snapshot for `ghfetcher attach` / `status`.
func (s *Store) ReadTelemetry(ctx context.Context) (pid int, updatedAt time.Time, payloadJSON string, err error) {
	var started, updated string
	err = s.db.QueryRowContext(ctx, `
		SELECT pid, started_at, updated_at, payload_json FROM _runtime_telemetry WHERE id = 1
	`).Scan(&pid, &started, &updated, &payloadJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, time.Time{}, "", nil
	}
	return pid, parseTime(updated), payloadJSON, err
}

// EnqueueCommand pushes a remote control command (`pause`, `resume`, `reload-tokens`, `stop`) from `ghfetcher attach`.
func (s *Store) EnqueueCommand(ctx context.Context, command, arg string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO _runtime_commands (command, arg, created_at, processed)
		VALUES (?, ?, ?, 0)
	`, command, arg, fmtTime(time.Now()))
	return err
}

// PollCommands drains unprocessed remote control commands for the active coordinator.
func (s *Store) PollCommands(ctx context.Context) ([]RuntimeCommand, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	rows, err := s.db.QueryContext(ctx, `
		SELECT id, command, arg, created_at FROM _runtime_commands WHERE processed = 0 ORDER BY id ASC
	`)
	if err != nil {
		return nil, err
	}
	var cmds []RuntimeCommand
	for rows.Next() {
		var c RuntimeCommand
		var created string
		if err := rows.Scan(&c.ID, &c.Command, &c.Arg, &created); err != nil {
			_ = rows.Close()
			return nil, err
		}
		c.CreatedAt = parseTime(created)
		cmds = append(cmds, c)
	}
	_ = rows.Close()

	if len(cmds) > 0 {
		_, _ = s.db.ExecContext(ctx, `UPDATE _runtime_commands SET processed = 1 WHERE id <= ?`, cmds[len(cmds)-1].ID)
	}
	return cmds, nil
}
