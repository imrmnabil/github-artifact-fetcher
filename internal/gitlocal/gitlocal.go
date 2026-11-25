package gitlocal

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"ghfetcher/internal/store"
)

// ExtractOptions configures local bare-Git extraction for a repository.
type ExtractOptions struct {
	RepoID          int64
	Owner           string
	Name            string
	CloneURL        string // Optional override (e.g. local file:// path in unit tests); defaults to https://github.com/<owner>/<name>.git
	DefaultBranch   string
	Token           string // Optional PAT injected via GIT_CONFIG_* env vars (never written to disk or CLI args)
	KeepGit         bool
	CacheDir        string
	IncludePatches  bool
	SinceCommitTime time.Time // When set on incremental runs, uses --shallow-since to avoid re-downloading old history
	OnStage         func(stage string)
	OnProgress      func(commitsParsed int, filesParsed int)
}

// ExtractResult summarizes zero-API Git extraction metrics.
type ExtractResult struct {
	CommitsExtracted   int
	FilesExtracted     int
	RefsExtracted      int
	PRCommitsLinked    int
	HeadSHA            string
	LatestCommittedAt  time.Time
	EstimatedAPISaved  int
}

// ExtractRepository clones/fetches a bare Git repository (including +refs/pull/*),
// streams all commits + file-level line churn into SQLite, maps PRs to commit SHAs in memory,
// and cleans up the bare directory automatically unless KeepGit is enabled.
func ExtractRepository(ctx context.Context, st *store.Store, opts ExtractOptions) (*ExtractResult, error) {
	cloneURL := opts.CloneURL
	if cloneURL == "" {
		cloneURL = fmt.Sprintf("https://github.com/%s/%s.git", opts.Owner, opts.Name)
	}
	if opts.DefaultBranch == "" {
		opts.DefaultBranch = "main"
	}

	var repoDir string
	var cleanup func()

	if opts.KeepGit {
		cacheBase := opts.CacheDir
		if cacheBase == "" {
			cacheBase = filepath.Join(".ghfetcher", "git-cache")
		}
		if err := os.MkdirAll(cacheBase, 0o755); err != nil {
			return nil, fmt.Errorf("create git cache dir: %w", err)
		}
		repoDir = filepath.Join(cacheBase, fmt.Sprintf("%s__%s.git", opts.Owner, opts.Name))
		cleanup = func() {}
	} else {
		tmpDir, err := os.MkdirTemp("", fmt.Sprintf("ghfetcher-%s-%s-*", opts.Owner, opts.Name))
		if err != nil {
			return nil, fmt.Errorf("create ephemeral git dir: %w", err)
		}
		repoDir = tmpDir
		cleanup = func() { _ = os.RemoveAll(tmpDir) }
	}
	defer cleanup()

	if opts.OnStage != nil {
		opts.OnStage("Git Bare Clone")
	}

	if err := cloneOrFetchBare(ctx, cloneURL, repoDir, opts.Token, opts.KeepGit, opts.SinceCommitTime); err != nil {
		return nil, err
	}

	if opts.OnStage != nil {
		opts.OnStage("Git Refs & PR Map")
	}

	refs, prHeadSHAs, defaultHeadSHA, err := readAllRefs(ctx, repoDir, opts.RepoID, opts.DefaultBranch)
	if err != nil {
		return nil, err
	}

	if opts.OnStage != nil {
		opts.OnStage("Git Churn (--numstat)")
	}

	parentGraph := make(map[string][]string, 4096)
	var latestCommit time.Time
	totalCommits := 0
	totalFiles := 0

	err = streamCommitsAndChurn(ctx, repoDir, opts, func(batch []store.Commit) error {
		for _, c := range batch {
			parentGraph[c.SHA] = c.ParentSHAs
			if c.CommittedAt.After(latestCommit) {
				latestCommit = c.CommittedAt
			}
			totalCommits++
			totalFiles += len(c.Files)
		}
		if err := st.UpsertCommitBatch(ctx, batch); err != nil {
			return err
		}
		if opts.OnProgress != nil {
			opts.OnProgress(totalCommits, totalFiles)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Build zero-API PR -> Commit SHA links in Go memory using the parent DAG!
	prCommits := linkPRCommitsInMemory(opts.RepoID, defaultHeadSHA, prHeadSHAs, parentGraph)

	if err := st.UpsertRefsAndPRCommits(ctx, refs, prCommits, defaultHeadSHA, latestCommit, opts.RepoID); err != nil {
		return nil, err
	}

	// API calls saved: 1 per commit detail (for numstat/files) + 1 per PR commit list
	apiSaved := totalCommits + len(prHeadSHAs)

	return &ExtractResult{
		CommitsExtracted:  totalCommits,
		FilesExtracted:    totalFiles,
		RefsExtracted:     len(refs),
		PRCommitsLinked:   len(prCommits),
		HeadSHA:           defaultHeadSHA,
		LatestCommittedAt: latestCommit,
		EstimatedAPISaved: apiSaved,
	}, nil
}

func gitEnvWithToken(token string) []string {
	env := append(os.Environ(),
		"GIT_TERMINAL_PROMPT=0",
		"LC_ALL=C",
	)
	if strings.TrimSpace(token) != "" {
		basic := base64.StdEncoding.EncodeToString([]byte("x-access-token:" + strings.TrimSpace(token)))
		env = append(env,
			"GIT_CONFIG_COUNT=1",
			"GIT_CONFIG_KEY_0=http.https://github.com/.extraheader",
			"GIT_CONFIG_VALUE_0=Authorization: Basic "+basic,
		)
	}
	return env
}

func cloneOrFetchBare(ctx context.Context, cloneURL, repoDir, token string, keepGit bool, since time.Time) error {
	headFile := filepath.Join(repoDir, "HEAD")
	_, statErr := os.Stat(headFile)
	exists := statErr == nil

	if !exists {
		initArgs := []string{"init", "--bare", repoDir}
		initCmd := exec.CommandContext(ctx, "git", initArgs...)
		initCmd.Env = gitEnvWithToken("")
		if out, err := initCmd.CombinedOutput(); err != nil {
			return fmt.Errorf("git init --bare: %w (%s)", err, strings.TrimSpace(string(out)))
		}
	}

	fetchArgs := []string{
		"-C", repoDir,
		"-c", "fetch.parallel=4",
		"fetch", "--prune", "--no-write-fetch-head",
	}
	if !keepGit && !since.IsZero() {
		// Buffer 24 hours earlier to account for clock skew / author vs committer time
		cutoff := since.Add(-24 * time.Hour).UTC().Format(time.RFC3339)
		fetchArgs = append(fetchArgs, "--shallow-since="+cutoff)
	}
	fetchArgs = append(fetchArgs,
		cloneURL,
		"+refs/heads/*:refs/heads/*",
		"+refs/tags/*:refs/tags/*",
		"+refs/pull/*/head:refs/pull/*/head",
	)

	cmd := exec.CommandContext(ctx, "git", fetchArgs...)
	cmd.Env = gitEnvWithToken(token)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		// If --shallow-since fails on a repo with zero commits since cutoff, treat gracefully
		msg := stderr.String()
		if !since.IsZero() && (strings.Contains(msg, "no commits selected") || strings.Contains(msg, "shallow file has changed")) {
			return nil
		}
		return fmt.Errorf("git bare fetch %s: %w (%s)", cloneURL, err, strings.TrimSpace(msg))
	}
	return nil
}

func readAllRefs(ctx context.Context, repoDir string, repoID int64, defaultBranch string) ([]store.GitRef, map[int]string, string, error) {
	cmd := exec.CommandContext(ctx, "git", "-C", repoDir, "for-each-ref",
		"--format=%(refname)%00%(objectname)%00%(*objectname)")
	cmd.Env = gitEnvWithToken("")
	out, err := cmd.Output()
	if err != nil {
		return nil, nil, "", fmt.Errorf("git for-each-ref: %w", err)
	}

	var refs []store.GitRef
	prHeads := make(map[int]string)
	var defaultHeadSHA string
	wantBranchRef := "refs/heads/" + defaultBranch

	lines := bytes.Split(bytes.TrimSpace(out), []byte{'\n'})
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		parts := bytes.Split(line, []byte{0})
		if len(parts) < 2 {
			continue
		}
		refName := string(parts[0])
		sha := string(parts[1])
		if len(parts) >= 3 && len(parts[2]) > 0 {
			// Annotated tag peeled commit SHA
			sha = string(parts[2])
		}

		refType := "other"
		switch {
		case strings.HasPrefix(refName, "refs/heads/"):
			refType = "branch"
			if refName == wantBranchRef || defaultHeadSHA == "" {
				if refName == wantBranchRef {
					defaultHeadSHA = sha
				} else if defaultHeadSHA == "" {
					defaultHeadSHA = sha
				}
			}
		case strings.HasPrefix(refName, "refs/tags/"):
			refType = "tag"
		case strings.HasPrefix(refName, "refs/pull/") && strings.HasSuffix(refName, "/head"):
			refType = "pr_head"
			numStr := strings.TrimSuffix(strings.TrimPrefix(refName, "refs/pull/"), "/head")
			if prNum, nerr := strconv.Atoi(numStr); nerr == nil && prNum > 0 {
				prHeads[prNum] = sha
			}
		}

		refs = append(refs, store.GitRef{
			RepoID:    repoID,
			RefName:   refName,
			TargetSHA: sha,
			RefType:   refType,
		})
	}
	return refs, prHeads, defaultHeadSHA, nil
}

// streamCommitsAndChurn streams `git log --all --numstat -z` in constant O(1) memory,
// invoking flushBatch every 400 commits.
func streamCommitsAndChurn(ctx context.Context, repoDir string, opts ExtractOptions, flushBatch func([]store.Commit) error) error {
	// Check if repository is empty (zero refs)
	checkCmd := exec.CommandContext(ctx, "git", "-C", repoDir, "rev-parse", "--all")
	checkOut, _ := checkCmd.Output()
	if len(bytes.TrimSpace(checkOut)) == 0 {
		return nil
	}

	args := []string{
		"-C", repoDir,
		"log", "--all", "--date-order", "--numstat", "-z",
		"--format=%x1e%H%x00%P%x00%an%x00%ae%x00%aI%x00%cn%x00%ce%x00%cI%x00%B%x00",
	}
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Env = gitEnvWithToken("")

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start git log: %w", err)
	}

	reader := bufio.NewReaderSize(stdout, 1024*1024)
	batch := make([]store.Commit, 0, 400)

	// Read records delimited by 0x1e (Record Separator)
	// First chunk before initial 0x1e is empty.
	_, err = reader.ReadBytes(0x1e)
	if err != nil && err != io.EOF {
		_ = cmd.Wait()
		return err
	}

	for err == nil {
		rawRecord, rerr := reader.ReadBytes(0x1e)
		if len(rawRecord) > 0 {
			if rawRecord[len(rawRecord)-1] == 0x1e {
				rawRecord = rawRecord[:len(rawRecord)-1]
			}
			commit, perr := parseLogRecord(opts.RepoID, rawRecord)
			if perr == nil && commit.SHA != "" {
				if opts.IncludePatches && len(commit.Files) > 0 {
					populateCommitPatches(ctx, repoDir, &commit)
				}
				batch = append(batch, commit)
				if len(batch) >= 400 {
					if ferr := flushBatch(batch); ferr != nil {
						_ = cmd.Process.Kill()
						_ = cmd.Wait()
						return ferr
					}
					batch = batch[:0]
				}
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			_ = cmd.Wait()
			return rerr
		}
	}

	if len(batch) > 0 {
		if ferr := flushBatch(batch); ferr != nil {
			_ = cmd.Wait()
			return ferr
		}
	}

	if werr := cmd.Wait(); werr != nil {
		return fmt.Errorf("git log failed: %w (%s)", werr, strings.TrimSpace(stderr.String()))
	}
	return nil
}

func parseLogRecord(repoID int64, rec []byte) (store.Commit, error) {
	// Header has 9 NUL-terminated fields:
	// 0: SHA, 1: Parents, 2: AuthorName, 3: AuthorEmail, 4: AuthoredISO,
	// 5: CommitterName, 6: CommitterEmail, 7: CommittedISO, 8: Body
	var fields [9][]byte
	rest := rec
	for i := 0; i < 9; i++ {
		idx := bytes.IndexByte(rest, 0)
		if idx < 0 {
			return store.Commit{}, fmt.Errorf("malformed git log record at field %d", i)
		}
		fields[i] = rest[:idx]
		rest = rest[idx+1:]
	}

	var parents []string
	if len(fields[1]) > 0 {
		parents = strings.Fields(string(fields[1]))
	}
	authoredAt, _ := time.Parse(time.RFC3339, string(fields[4]))
	committedAt, _ := time.Parse(time.RFC3339, string(fields[7]))

	c := store.Commit{
		RepoID:         repoID,
		SHA:            string(fields[0]),
		ParentSHAs:     parents,
		AuthorName:     string(fields[2]),
		AuthorEmail:    string(fields[3]),
		AuthoredAt:     authoredAt,
		CommitterName:  string(fields[5]),
		CommitterEmail: string(fields[6]),
		CommittedAt:    committedAt,
		Message:        strings.TrimRight(string(fields[8]), "\r\n"),
	}

	// Remaining bytes contain optional newline + `-z` numstat entries:
	// Normal file: "<add>\t<del>\t<path>\x00"
	// Rename/Copy: "<add>\t<del>\t\x00<old_path>\x00<new_path>\x00"
	rest = bytes.TrimLeft(rest, "\r\n")
	for len(rest) > 0 {
		idx := bytes.IndexByte(rest, 0)
		if idx < 0 {
			break
		}
		token := rest[:idx]
		rest = rest[idx+1:]
		token = bytes.TrimLeft(token, "\r\n")
		if len(token) == 0 {
			continue
		}

		tabs := bytes.SplitN(token, []byte{'\t'}, 3)
		if len(tabs) < 3 {
			continue
		}
		adds, _ := strconv.Atoi(string(tabs[0])) // "-" on binary files -> 0
		dels, _ := strconv.Atoi(string(tabs[1]))

		var filePath, oldPath string
		changeType := "M"

		if len(tabs[2]) == 0 {
			// Rename entry: next two NUL-terminated tokens are oldPath and newPath
			idx1 := bytes.IndexByte(rest, 0)
			if idx1 < 0 {
				break
			}
			oldPath = string(rest[:idx1])
			rest = rest[idx1+1:]

			idx2 := bytes.IndexByte(rest, 0)
			if idx2 < 0 {
				break
			}
			filePath = string(rest[:idx2])
			rest = rest[idx2+1:]
			changeType = "R"
		} else {
			filePath = string(tabs[2])
		}

		c.Additions += adds
		c.Deletions += dels
		c.Files = append(c.Files, store.CommitFile{
			RepoID:     repoID,
			CommitSHA:  c.SHA,
			FilePath:   filePath,
			OldPath:    oldPath,
			ChangeType: changeType,
			Additions:  adds,
			Deletions:  dels,
		})
	}
	c.FilesChanged = len(c.Files)
	return c, nil
}

func populateCommitPatches(ctx context.Context, repoDir string, c *store.Commit) {
	cmd := exec.CommandContext(ctx, "git", "-C", repoDir, "show", "--format=", "--patch", "--unified=3", c.SHA)
	cmd.Env = gitEnvWithToken("")
	out, err := cmd.Output()
	if err != nil || len(out) == 0 {
		return
	}
	sections := strings.Split(string(out), "\ndiff --git ")
	patchByPath := make(map[string]string, len(sections))
	for i, sec := range sections {
		if i > 0 {
			sec = "diff --git " + sec
		}
		if !strings.HasPrefix(sec, "diff --git ") {
			continue
		}
		nl := strings.IndexByte(sec, '\n')
		if nl < 0 {
			continue
		}
		header := sec[:nl]
		if bIdx := strings.LastIndex(header, " b/"); bIdx > 0 {
			p := header[bIdx+3:]
			patchByPath[p] = sec
		}
	}
	for i := range c.Files {
		if p, ok := patchByPath[c.Files[i].FilePath]; ok {
			c.Files[i].Patch = p
		}
	}
}

// linkPRCommitsInMemory maps every refs/pull/<num>/head to its constituent commits
// in pure Go memory (0 git subprocesses, 0 API calls).
func linkPRCommitsInMemory(repoID int64, defaultHeadSHA string, prHeads map[int]string, parents map[string][]string) []store.PRCommit {
	if len(prHeads) == 0 {
		return nil
	}

	// 1. Mark all commits reachable on the first-parent trunk of defaultBranch,
	//    and also record merge commits M(p1, p2) where p2 == a PR head SHA!
	mainAncestors := make(map[string]struct{}, len(parents))
	mergeBaseParentForPRHead := make(map[string]string)

	if defaultHeadSHA != "" {
		stack := []string{defaultHeadSHA}
		for len(stack) > 0 {
			curr := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if _, seen := mainAncestors[curr]; seen {
				continue
			}
			mainAncestors[curr] = struct{}{}
			ps := parents[curr]
			if len(ps) >= 2 {
				// Merge commit: p[0] is base branch, p[1] is merged branch head
				if _, exists := mergeBaseParentForPRHead[ps[1]]; !exists {
					mergeBaseParentForPRHead[ps[1]] = ps[0]
				}
			}
			for _, p := range ps {
				if _, seen := mainAncestors[p]; !seen {
					stack = append(stack, p)
				}
			}
		}
	}

	var out []store.PRCommit
	const maxCommitsPerPR = 250

	for prNum, headSHA := range prHeads {
		if headSHA == "" {
			continue
		}

		// Determine stop boundary:
		// - If PR was merged via a merge commit M(base, headSHA), stop at ancestors of `base`
		// - If PR head is not on default branch (open, closed, squash-merged, rebase-merged), stop at `mainAncestors`
		// - If fast-forward merged directly onto main without a merge commit, link at least headSHA
		var stopSet map[string]struct{}
		if baseSHA, mergedViaCommit := mergeBaseParentForPRHead[headSHA]; mergedViaCommit {
			stopSet = collectAncestorsBounded(baseSHA, parents, 2000)
		} else if _, inMain := mainAncestors[headSHA]; !inMain {
			stopSet = mainAncestors
		}

		if stopSet == nil {
			out = append(out, store.PRCommit{
				RepoID:    repoID,
				PRNumber:  prNum,
				CommitSHA: headSHA,
				Ordinal:   1,
			})
			continue
		}

		// Walk backwards from headSHA until stopSet
		var chain []string
		visited := make(map[string]struct{}, 32)
		queue := []string{headSHA}
		for len(queue) > 0 && len(chain) < maxCommitsPerPR {
			curr := queue[0]
			queue = queue[1:]
			if _, v := visited[curr]; v {
				continue
			}
			visited[curr] = struct{}{}
			if _, stop := stopSet[curr]; stop {
				continue
			}
			if _, known := parents[curr]; !known && curr != headSHA {
				continue
			}
			chain = append(chain, curr)
			for _, p := range parents[curr] {
				if _, stop := stopSet[p]; !stop {
					queue = append(queue, p)
				}
			}
		}

		// Reverse so ordinal 1 is oldest commit in the PR and N is headSHA
		for i, j := 0, len(chain)-1; i < j; i, j = i+1, j-1 {
			chain[i], chain[j] = chain[j], chain[i]
		}
		for idx, sha := range chain {
			out = append(out, store.PRCommit{
				RepoID:    repoID,
				PRNumber:  prNum,
				CommitSHA: sha,
				Ordinal:   idx + 1,
			})
		}
	}

	return out
}

func collectAncestorsBounded(startSHA string, parents map[string][]string, maxDepth int) map[string]struct{} {
	anc := make(map[string]struct{}, 256)
	queue := []string{startSHA}
	for len(queue) > 0 && len(anc) < maxDepth {
		curr := queue[0]
		queue = queue[1:]
		if _, ok := anc[curr]; ok {
			continue
		}
		anc[curr] = struct{}{}
		for _, p := range parents[curr] {
			if _, ok := anc[p]; !ok {
				queue = append(queue, p)
			}
		}
	}
	return anc
}
