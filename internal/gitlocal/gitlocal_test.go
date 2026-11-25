package gitlocal

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"ghfetcher/internal/store"
)

func runGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	cmd.Env = append(os.Environ(),
		"GIT_AUTHOR_NAME=Alice Researcher",
		"GIT_AUTHOR_EMAIL=alice@example.edu",
		"GIT_COMMITTER_NAME=Alice Researcher",
		"GIT_COMMITTER_EMAIL=alice@example.edu",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v failed: %v\n%s", args, err, string(out))
	}
	return string(out)
}

func TestExtractRepositoryZeroAPIWithPRRefsAndChurn(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()

	// Create a synthetic upstream repo with main branch, a tag, and a simulated GitHub PR ref (refs/pull/7/head)
	upstream := filepath.Join(tmp, "upstream")
	if err := os.MkdirAll(upstream, 0o755); err != nil {
		t.Fatal(err)
	}
	runGit(t, upstream, "init", "-b", "main")
	if err := os.WriteFile(filepath.Join(upstream, "README.md"), []byte("line1\nline2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runGit(t, upstream, "add", "README.md")
	runGit(t, upstream, "commit", "-m", "Initial commit\n\nDetailed body here.")
	runGit(t, upstream, "tag", "v0.1.0")

	// Create a PR branch with 2 commits and expose it as refs/pull/7/head
	runGit(t, upstream, "checkout", "-b", "feature-pr")
	if err := os.WriteFile(filepath.Join(upstream, "README.md"), []byte("line1\nline2-modified\nline3\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runGit(t, upstream, "add", "README.md")
	runGit(t, upstream, "commit", "-m", "feat: update readme")

	if err := os.WriteFile(filepath.Join(upstream, "algo.go"), []byte("package algo\nfunc Add(a,b int) int { return a+b }\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runGit(t, upstream, "add", "algo.go")
	runGit(t, upstream, "commit", "-m", "feat: add algo")
	runGit(t, upstream, "update-ref", "refs/pull/7/head", "HEAD")
	runGit(t, upstream, "checkout", "main")

	dbPath := filepath.Join(tmp, "study.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, &store.Repository{
		Owner:         "lab",
		Name:          "demo",
		DefaultBranch: "main",
	})
	if err != nil {
		t.Fatalf("upsert repo: %v", err)
	}

	res, err := ExtractRepository(ctx, st, ExtractOptions{
		RepoID:         repoID,
		Owner:          "lab",
		Name:           "demo",
		CloneURL:       upstream,
		DefaultBranch:  "main",
		IncludePatches: true,
	})
	if err != nil {
		t.Fatalf("ExtractRepository failed: %v", err)
	}

	if res.CommitsExtracted != 3 {
		t.Fatalf("expected 3 commits extracted, got %d", res.CommitsExtracted)
	}
	if res.PRCommitsLinked != 2 {
		t.Fatalf("expected 2 PR commits linked for PR #7, got %d", res.PRCommitsLinked)
	}

	var fileCount int
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM commit_files WHERE repo_id = ? AND patch != ''`, repoID).Scan(&fileCount); err != nil {
		t.Fatal(err)
	}
	if fileCount != 3 {
		t.Fatalf("expected 3 commit_files rows with patches, got %d", fileCount)
	}
}
