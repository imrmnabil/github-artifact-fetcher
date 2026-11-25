package export

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/parquet-go/parquet-go"

	"ghfetcher/internal/store"
)

// Format identifies supported research dataset export formats.
type Format string

const (
	FormatParquet Format = "parquet"
	FormatJSONL   Format = "jsonl"
)

var ExportableTables = []string{
	"repositories",
	"users",
	"commits",
	"commit_files",
	"refs",
	"pr_commits",
	"issues",
	"pull_requests",
	"issue_comments",
	"pr_reviews",
	"pr_review_comments",
	"timeline_events",
}

// TableStats reports row count exported per table.
type TableStats struct {
	Table string
	File  string
	Rows  int64
}

// ExportAll streams normalized SQLite tables into columnar Apache Parquet or JSONL files.
func ExportAll(ctx context.Context, st *store.Store, outDir string, format Format) ([]TableStats, error) {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return nil, fmt.Errorf("create export dir %s: %w", outDir, err)
	}

	switch format {
	case FormatJSONL:
		return exportAllJSONL(ctx, st.DB(), outDir)
	case FormatParquet:
		return exportAllParquet(ctx, st.DB(), outDir)
	default:
		return nil, fmt.Errorf("unsupported export format %q (expected parquet or jsonl)", format)
	}
}

func exportAllJSONL(ctx context.Context, db *sql.DB, outDir string) ([]TableStats, error) {
	var stats []TableStats
	for _, tbl := range ExportableTables {
		outFile := filepath.Join(outDir, tbl+".jsonl")
		n, err := exportTableJSONL(ctx, db, tbl, outFile)
		if err != nil {
			return stats, fmt.Errorf("export %s to jsonl: %w", tbl, err)
		}
		stats = append(stats, TableStats{Table: tbl, File: outFile, Rows: n})
	}
	return stats, nil
}

func exportTableJSONL(ctx context.Context, db *sql.DB, table, outFile string) (int64, error) {
	rows, err := db.QueryContext(ctx, "SELECT * FROM "+table)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return 0, err
	}

	f, err := os.Create(outFile)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	bw := bufio.NewWriterSize(f, 512*1024)
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}

	var count int64
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return count, err
		}
		rec := make(map[string]any, len(cols))
		for i, col := range cols {
			if b, ok := vals[i].([]byte); ok {
				rec[col] = string(b)
			} else {
				rec[col] = vals[i]
			}
		}
		line, err := json.Marshal(rec)
		if err != nil {
			return count, err
		}
		if _, err := bw.Write(line); err != nil {
			return count, err
		}
		if err := bw.WriteByte('\n'); err != nil {
			return count, err
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return count, err
	}
	return count, bw.Flush()
}

// Strongly typed Parquet row structs for crisp columnar schemas in DuckDB / Polars / PyArrow.

type pqRepo struct {
	ID              int64  `parquet:"id"`
	GitHubID        int64  `parquet:"github_id"`
	Owner           string `parquet:"owner"`
	Name            string `parquet:"name"`
	FullName        string `parquet:"full_name"`
	DefaultBranch   string `parquet:"default_branch"`
	IsFork          int64  `parquet:"is_fork"`
	Stars           int64  `parquet:"stars"`
	Forks           int64  `parquet:"forks"`
	DiskKB          int64  `parquet:"disk_kb"`
	TotalCommitsEst int64  `parquet:"total_commits_est"`
	TotalIssuesEst  int64  `parquet:"total_issues_est"`
	TotalPRsEst     int64  `parquet:"total_prs_est"`
	ClonedHeadSHA   string `parquet:"cloned_head_sha"`
	LastCommitAt    string `parquet:"last_commit_at"`
	LastSyncedAt    string `parquet:"last_synced_at"`
}

type pqUser struct {
	GitHubID int64  `parquet:"github_id"`
	Login    string `parquet:"login"`
	Name     string `parquet:"name"`
	Email    string `parquet:"email"`
	Type     string `parquet:"type"`
}

type pqCommit struct {
	RepoID         int64  `parquet:"repo_id"`
	SHA            string `parquet:"sha"`
	AuthorName     string `parquet:"author_name"`
	AuthorEmail    string `parquet:"author_email"`
	AuthoredAt     string `parquet:"authored_at"`
	CommitterName  string `parquet:"committer_name"`
	CommitterEmail string `parquet:"committer_email"`
	CommittedAt    string `parquet:"committed_at"`
	Message        string `parquet:"message"`
	ParentsJSON    string `parquet:"parents_json"`
	FilesChanged   int64  `parquet:"files_changed"`
	Additions      int64  `parquet:"additions"`
	Deletions      int64  `parquet:"deletions"`
}

type pqCommitFile struct {
	RepoID     int64  `parquet:"repo_id"`
	CommitSHA  string `parquet:"commit_sha"`
	FilePath   string `parquet:"file_path"`
	OldPath    string `parquet:"old_path"`
	ChangeType string `parquet:"change_type"`
	Additions  int64  `parquet:"additions"`
	Deletions  int64  `parquet:"deletions"`
	Patch      string `parquet:"patch"`
}

type pqRef struct {
	RepoID    int64  `parquet:"repo_id"`
	RefName   string `parquet:"ref_name"`
	TargetSHA string `parquet:"target_sha"`
	RefType   string `parquet:"ref_type"`
}

type pqPRCommit struct {
	RepoID    int64  `parquet:"repo_id"`
	PRNumber  int64  `parquet:"pr_number"`
	CommitSHA string `parquet:"commit_sha"`
	Ordinal   int64  `parquet:"ordinal"`
}

type pqIssue struct {
	RepoID        int64  `parquet:"repo_id"`
	Number        int64  `parquet:"number"`
	GitHubID      int64  `parquet:"github_id"`
	State         string `parquet:"state"`
	StateReason   string `parquet:"state_reason"`
	Title         string `parquet:"title"`
	Body          string `parquet:"body"`
	AuthorLogin   string `parquet:"author_login"`
	Milestone     string `parquet:"milestone"`
	LabelsJSON    string `parquet:"labels_json"`
	AssigneesJSON string `parquet:"assignees_json"`
	CommentCount  int64  `parquet:"comment_count"`
	ReactionsJSON string `parquet:"reactions_json"`
	CreatedAt     string `parquet:"created_at"`
	UpdatedAt     string `parquet:"updated_at"`
	ClosedAt      string `parquet:"closed_at"`
}

type pqPR struct {
	RepoID            int64  `parquet:"repo_id"`
	Number            int64  `parquet:"number"`
	GitHubID          int64  `parquet:"github_id"`
	State             string `parquet:"state"`
	IsDraft           int64  `parquet:"is_draft"`
	Merged            int64  `parquet:"merged"`
	Title             string `parquet:"title"`
	Body              string `parquet:"body"`
	AuthorLogin       string `parquet:"author_login"`
	MergedByLogin     string `parquet:"merged_by_login"`
	BaseRef           string `parquet:"base_ref"`
	BaseSHA           string `parquet:"base_sha"`
	HeadRef           string `parquet:"head_ref"`
	HeadSHA           string `parquet:"head_sha"`
	MergeCommitSHA    string `parquet:"merge_commit_sha"`
	Additions         int64  `parquet:"additions"`
	Deletions         int64  `parquet:"deletions"`
	ChangedFiles      int64  `parquet:"changed_files"`
	CommentCount      int64  `parquet:"comment_count"`
	ReviewCount       int64  `parquet:"review_count"`
	LabelsJSON        string `parquet:"labels_json"`
	AssigneesJSON     string `parquet:"assignees_json"`
	ClosingIssuesJSON string `parquet:"closing_issues_json"`
	ReactionsJSON     string `parquet:"reactions_json"`
	CreatedAt         string `parquet:"created_at"`
	UpdatedAt         string `parquet:"updated_at"`
	ClosedAt          string `parquet:"closed_at"`
	MergedAt          string `parquet:"merged_at"`
}

type pqIssueComment struct {
	RepoID        int64  `parquet:"repo_id"`
	CommentID     int64  `parquet:"comment_id"`
	ItemNumber    int64  `parquet:"item_number"`
	IsPR          int64  `parquet:"is_pr"`
	AuthorLogin   string `parquet:"author_login"`
	Body          string `parquet:"body"`
	ReactionsJSON string `parquet:"reactions_json"`
	CreatedAt     string `parquet:"created_at"`
	UpdatedAt     string `parquet:"updated_at"`
}

type pqPRReview struct {
	RepoID      int64  `parquet:"repo_id"`
	ReviewID    int64  `parquet:"review_id"`
	PRNumber    int64  `parquet:"pr_number"`
	AuthorLogin string `parquet:"author_login"`
	State       string `parquet:"state"`
	CommitSHA   string `parquet:"commit_sha"`
	Body        string `parquet:"body"`
	SubmittedAt string `parquet:"submitted_at"`
}

type pqPRReviewComment struct {
	RepoID            int64  `parquet:"repo_id"`
	CommentID         int64  `parquet:"comment_id"`
	PRNumber          int64  `parquet:"pr_number"`
	ReviewID          int64  `parquet:"review_id"`
	InReplyToID       int64  `parquet:"in_reply_to_id"`
	AuthorLogin       string `parquet:"author_login"`
	Path              string `parquet:"path"`
	CommitSHA         string `parquet:"commit_sha"`
	OriginalCommitSHA string `parquet:"original_commit_sha"`
	DiffHunk          string `parquet:"diff_hunk"`
	Line              int64  `parquet:"line"`
	StartLine         int64  `parquet:"start_line"`
	Side              string `parquet:"side"`
	Body              string `parquet:"body"`
	ReactionsJSON     string `parquet:"reactions_json"`
	CreatedAt         string `parquet:"created_at"`
	UpdatedAt         string `parquet:"updated_at"`
}

type pqTimelineEvent struct {
	RepoID      int64  `parquet:"repo_id"`
	EventKey    string `parquet:"event_key"`
	ItemNumber  int64  `parquet:"item_number"`
	IsPR        int64  `parquet:"is_pr"`
	EventType   string `parquet:"event_type"`
	ActorLogin  string `parquet:"actor_login"`
	PayloadJSON string `parquet:"payload_json"`
	CreatedAt   string `parquet:"created_at"`
}

func writeParquetStream[T any](ctx context.Context, db *sql.DB, outFile, query string, scanRow func(*sql.Rows) (T, error)) (int64, error) {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	f, err := os.Create(outFile)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	pw := parquet.NewGenericWriter[T](f, parquet.Compression(&parquet.Snappy))
	batch := make([]T, 0, 512)
	var total int64

	for rows.Next() {
		item, err := scanRow(rows)
		if err != nil {
			_ = pw.Close()
			return total, err
		}
		batch = append(batch, item)
		if len(batch) >= 512 {
			n, werr := pw.Write(batch)
			total += int64(n)
			if werr != nil {
				_ = pw.Close()
				return total, werr
			}
			batch = batch[:0]
		}
	}
	if err := rows.Err(); err != nil {
		_ = pw.Close()
		return total, err
	}
	if len(batch) > 0 {
		n, werr := pw.Write(batch)
		total += int64(n)
		if werr != nil {
			_ = pw.Close()
			return total, werr
		}
	}
	if err := pw.Close(); err != nil {
		return total, err
	}
	return total, nil
}

func exportAllParquet(ctx context.Context, db *sql.DB, outDir string) ([]TableStats, error) {
	var stats []TableStats
	add := func(tbl string, fn func(string) (int64, error)) error {
		file := filepath.Join(outDir, tbl+".parquet")
		n, err := fn(file)
		if err != nil {
			return fmt.Errorf("export %s to parquet: %w", tbl, err)
		}
		stats = append(stats, TableStats{Table: tbl, File: file, Rows: n})
		return nil
	}

	if err := add("repositories", func(f string) (int64, error) {
		return writeParquetStream(ctx, db, f, `SELECT id, github_id, owner, name, full_name, default_branch, is_fork, stars, forks, disk_kb, total_commits_est, total_issues_est, total_prs_est, cloned_head_sha, last_commit_at, last_synced_at FROM repositories`, func(r *sql.Rows) (pqRepo, error) {
			var x pqRepo
			return x, r.Scan(&x.ID, &x.GitHubID, &x.Owner, &x.Name, &x.FullName, &x.DefaultBranch, &x.IsFork, &x.Stars, &x.Forks, &x.DiskKB, &x.TotalCommitsEst, &x.TotalIssuesEst, &x.TotalPRsEst, &x.ClonedHeadSHA, &x.LastCommitAt, &x.LastSyncedAt)
		})
	}); err != nil {
		return stats, err
	}

	if err := add("users", func(f string) (int64, error) {
		return writeParquetStream(ctx, db, f, `SELECT github_id, login, name, email, type FROM users`, func(r *sql.Rows) (pqUser, error) {
			var x pqUser
			return x, r.Scan(&x.GitHubID, &x.Login, &x.Name, &x.Email, &x.Type)
		})
	}); err != nil {
		return stats, err
	}

	if err := add("commits", func(f string) (int64, error) {
		return writeParquetStream(ctx, db, f, `SELECT repo_id, sha, author_name, author_email, authored_at, committer_name, committer_email, committed_at, message, parents_json, files_changed, additions, deletions FROM commits`, func(r *sql.Rows) (pqCommit, error) {
			var x pqCommit
			return x, r.Scan(&x.RepoID, &x.SHA, &x.AuthorName, &x.AuthorEmail, &x.AuthoredAt, &x.CommitterName, &x.CommitterEmail, &x.CommittedAt, &x.Message, &x.ParentsJSON, &x.FilesChanged, &x.Additions, &x.Deletions)
		})
	}); err != nil {
		return stats, err
	}

	if err := add("commit_files", func(f string) (int64, error) {
		return writeParquetStream(ctx, db, f, `SELECT repo_id, commit_sha, file_path, old_path, change_type, additions, deletions, patch FROM commit_files`, func(r *sql.Rows) (pqCommitFile, error) {
			var x pqCommitFile
			return x, r.Scan(&x.RepoID, &x.CommitSHA, &x.FilePath, &x.OldPath, &x.ChangeType, &x.Additions, &x.Deletions, &x.Patch)
		})
	}); err != nil {
		return stats, err
	}

	if err := add("refs", func(f string) (int64, error) {
		return writeParquetStream(ctx, db, f, `SELECT repo_id, ref_name, target_sha, ref_type FROM refs`, func(r *sql.Rows) (pqRef, error) {
			var x pqRef
			return x, r.Scan(&x.RepoID, &x.RefName, &x.TargetSHA, &x.RefType)
		})
	}); err != nil {
		return stats, err
	}

	if err := add("pr_commits", func(f string) (int64, error) {
		return writeParquetStream(ctx, db, f, `SELECT repo_id, pr_number, commit_sha, ordinal FROM pr_commits`, func(r *sql.Rows) (pqPRCommit, error) {
			var x pqPRCommit
			return x, r.Scan(&x.RepoID, &x.PRNumber, &x.CommitSHA, &x.Ordinal)
		})
	}); err != nil {
		return stats, err
	}

	if err := add("issues", func(f string) (int64, error) {
		return writeParquetStream(ctx, db, f, `SELECT repo_id, number, github_id, state, state_reason, title, body, author_login, milestone, labels_json, assignees_json, comment_count, reactions_json, created_at, updated_at, closed_at FROM issues`, func(r *sql.Rows) (pqIssue, error) {
			var x pqIssue
			return x, r.Scan(&x.RepoID, &x.Number, &x.GitHubID, &x.State, &x.StateReason, &x.Title, &x.Body, &x.AuthorLogin, &x.Milestone, &x.LabelsJSON, &x.AssigneesJSON, &x.CommentCount, &x.ReactionsJSON, &x.CreatedAt, &x.UpdatedAt, &x.ClosedAt)
		})
	}); err != nil {
		return stats, err
	}

	if err := add("pull_requests", func(f string) (int64, error) {
		return writeParquetStream(ctx, db, f, `SELECT repo_id, number, github_id, state, is_draft, merged, title, body, author_login, merged_by_login, base_ref, base_sha, head_ref, head_sha, merge_commit_sha, additions, deletions, changed_files, comment_count, review_count, labels_json, assignees_json, closing_issues_json, reactions_json, created_at, updated_at, closed_at, merged_at FROM pull_requests`, func(r *sql.Rows) (pqPR, error) {
			var x pqPR
			return x, r.Scan(&x.RepoID, &x.Number, &x.GitHubID, &x.State, &x.IsDraft, &x.Merged, &x.Title, &x.Body, &x.AuthorLogin, &x.MergedByLogin, &x.BaseRef, &x.BaseSHA, &x.HeadRef, &x.HeadSHA, &x.MergeCommitSHA, &x.Additions, &x.Deletions, &x.ChangedFiles, &x.CommentCount, &x.ReviewCount, &x.LabelsJSON, &x.AssigneesJSON, &x.ClosingIssuesJSON, &x.ReactionsJSON, &x.CreatedAt, &x.UpdatedAt, &x.ClosedAt, &x.MergedAt)
		})
	}); err != nil {
		return stats, err
	}

	if err := add("issue_comments", func(f string) (int64, error) {
		return writeParquetStream(ctx, db, f, `SELECT repo_id, comment_id, item_number, is_pr, author_login, body, reactions_json, created_at, updated_at FROM issue_comments`, func(r *sql.Rows) (pqIssueComment, error) {
			var x pqIssueComment
			return x, r.Scan(&x.RepoID, &x.CommentID, &x.ItemNumber, &x.IsPR, &x.AuthorLogin, &x.Body, &x.ReactionsJSON, &x.CreatedAt, &x.UpdatedAt)
		})
	}); err != nil {
		return stats, err
	}

	if err := add("pr_reviews", func(f string) (int64, error) {
		return writeParquetStream(ctx, db, f, `SELECT repo_id, review_id, pr_number, author_login, state, commit_sha, body, submitted_at FROM pr_reviews`, func(r *sql.Rows) (pqPRReview, error) {
			var x pqPRReview
			return x, r.Scan(&x.RepoID, &x.ReviewID, &x.PRNumber, &x.AuthorLogin, &x.State, &x.CommitSHA, &x.Body, &x.SubmittedAt)
		})
	}); err != nil {
		return stats, err
	}

	if err := add("pr_review_comments", func(f string) (int64, error) {
		return writeParquetStream(ctx, db, f, `SELECT repo_id, comment_id, pr_number, review_id, in_reply_to_id, author_login, path, commit_sha, original_commit_sha, diff_hunk, line, start_line, side, body, reactions_json, created_at, updated_at FROM pr_review_comments`, func(r *sql.Rows) (pqPRReviewComment, error) {
			var x pqPRReviewComment
			return x, r.Scan(&x.RepoID, &x.CommentID, &x.PRNumber, &x.ReviewID, &x.InReplyToID, &x.AuthorLogin, &x.Path, &x.CommitSHA, &x.OriginalCommitSHA, &x.DiffHunk, &x.Line, &x.StartLine, &x.Side, &x.Body, &x.ReactionsJSON, &x.CreatedAt, &x.UpdatedAt)
		})
	}); err != nil {
		return stats, err
	}

	if err := add("timeline_events", func(f string) (int64, error) {
		return writeParquetStream(ctx, db, f, `SELECT repo_id, event_key, item_number, is_pr, event_type, actor_login, payload_json, created_at FROM timeline_events`, func(r *sql.Rows) (pqTimelineEvent, error) {
			var x pqTimelineEvent
			return x, r.Scan(&x.RepoID, &x.EventKey, &x.ItemNumber, &x.IsPR, &x.EventType, &x.ActorLogin, &x.PayloadJSON, &x.CreatedAt)
		})
	}); err != nil {
		return stats, err
	}

	return stats, nil
}

func ParseFormat(s string) (Format, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "parquet", "pq":
		return FormatParquet, nil
	case "jsonl", "ndjson", "json":
		return FormatJSONL, nil
	default:
		return "", fmt.Errorf("unknown format %q: supported formats are parquet, jsonl", s)
	}
}
