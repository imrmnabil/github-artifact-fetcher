package export

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"ghfetcher/internal/store"
)

func TestExportParquetAndJSONL(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()

	st, err := store.Open(filepath.Join(tmp, "study.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, &store.Repository{
		GitHubID:      42,
		Owner:         "golang",
		Name:          "go",
		DefaultBranch: "master",
		Stars:         125000,
	})
	if err != nil {
		t.Fatal(err)
	}

	now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	if err := st.UpsertCommitBatch(ctx, []store.Commit{
		{
			RepoID:      repoID,
			SHA:         "1111222233334444555566667777888899990000",
			AuthorName:  "Rob Pike",
			AuthorEmail: "r@golang.org",
			AuthoredAt:  now,
			CommittedAt: now,
			Message:     "runtime: optimize scheduler",
			Additions:   45,
			Deletions:   12,
			Files: []store.CommitFile{
				{RepoID: repoID, CommitSHA: "1111222233334444555566667777888899990000", FilePath: "src/runtime/proc.go", Additions: 45, Deletions: 12},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	if err := st.SaveArtifactBatch(ctx, store.ArtifactBatch{
		Issues: []store.Issue{
			{RepoID: repoID, Number: 10, State: "OPEN", Title: "Improve GC latency", AuthorLogin: "rsc", CreatedAt: now, UpdatedAt: now},
		},
	}); err != nil {
		t.Fatal(err)
	}

	pqDir := filepath.Join(tmp, "out_pq")
	pqStats, err := ExportAll(ctx, st, pqDir, FormatParquet)
	if err != nil {
		t.Fatalf("ExportAll Parquet failed: %v", err)
	}
	if len(pqStats) != len(ExportableTables) {
		t.Fatalf("expected %d tables exported, got %d", len(ExportableTables), len(pqStats))
	}

	// Read back commits.parquet using parquet-go to verify valid columnar output
	rows, err := parquet.ReadFile[pqCommit](filepath.Join(pqDir, "commits.parquet"))
	if err != nil {
		t.Fatalf("ReadFile commits.parquet failed: %v", err)
	}
	if len(rows) != 1 || rows[0].Additions != 45 || rows[0].AuthorEmail != "r@golang.org" {
		t.Fatalf("unexpected parquet commit row: %+v", rows)
	}

	jsonlDir := filepath.Join(tmp, "out_jsonl")
	jsonlStats, err := ExportAll(ctx, st, jsonlDir, FormatJSONL)
	if err != nil {
		t.Fatalf("ExportAll JSONL failed: %v", err)
	}
	if len(jsonlStats) != len(ExportableTables) {
		t.Fatalf("expected %d jsonl files, got %d", len(ExportableTables), len(jsonlStats))
	}
}
