package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"ghfetcher/internal/config"
	"ghfetcher/internal/github"
	"ghfetcher/internal/gitlocal"
	"ghfetcher/internal/store"
	"ghfetcher/internal/tokenpool"
)

// ArtifactSelection specifies which GitHub artifacts to fetch.
type ArtifactSelection struct {
	Commits  bool `json:"commits"`
	Issues   bool `json:"issues"`
	PRs      bool `json:"prs"`
	Reviews  bool `json:"reviews"`
	Comments bool `json:"comments"`
	Timeline bool `json:"timeline"`
	Patches  bool `json:"patches"`
}

func (s ArtifactSelection) AnyAPI() bool {
	return s.Issues || s.PRs || s.Reviews || s.Comments || s.Timeline
}

func AllArtifacts(patches bool) ArtifactSelection {
	return ArtifactSelection{
		Commits:  true,
		Issues:   true,
		PRs:      true,
		Reviews:  true,
		Comments: true,
		Timeline: true,
		Patches:  patches,
	}
}

// RunOptions configures a multi-repository fetch session.
type RunOptions struct {
	ConfigPath      string
	Config          *config.Config
	Repos           [][2]string
	Selection       ArtifactSelection
	DBPath          string
	PerRepoDB       bool
	KeepGit         bool
	RepoConcurrency int
	GitConcurrency  int
	APIConcurrency  int
	SafetyReserve   int
	OnLog           func(msg string)
}

// RepoProgress represents real-time status of one repository for the htop TUI.
type RepoProgress struct {
	FullName       string    `json:"full_name"`
	Owner          string    `json:"owner"`
	Name           string    `json:"name"`
	State          string    `json:"state"` // queued, active, done, error
	Stage          string    `json:"stage"`
	ProgressPct    float64   `json:"progress_pct"`
	CommitsDone    int       `json:"commits_done"`
	CommitsTotal   int       `json:"commits_total"`
	IssuesDone     int       `json:"issues_done"`
	IssuesTotal    int       `json:"issues_total"`
	PRsDone        int       `json:"prs_done"`
	PRsTotal       int       `json:"prs_total"`
	RevCommDone    int       `json:"rev_comm_done"`
	OverflowQueued int       `json:"overflow_queued"`
	OverflowDone   int       `json:"overflow_done"`
	ItemsPerSec    float64   `json:"items_per_sec"`
	ETASeconds     int64     `json:"eta_seconds"`
	APISaved       int64     `json:"api_saved"`
	LastError      string    `json:"last_error,omitempty"`
	StartedAt      time.Time `json:"started_at"`
	CompletedAt    time.Time `json:"completed_at"`
}

// TelemetrySnapshot is the complete live state rendered by both local TUI and `ghfetcher attach`.
type TelemetrySnapshot struct {
	PID             int                        `json:"pid"`
	DBPath          string                     `json:"db_path"`
	DBSizeBytes     int64                      `json:"db_size_bytes"`
	RowsWritten     int64                      `json:"rows_written"`
	RowsPerSec      float64                    `json:"rows_per_sec"`
	Paused          bool                       `json:"paused"`
	Finished        bool                       `json:"finished"`
	StartedAt       time.Time                  `json:"started_at"`
	UpdatedAt       time.Time                  `json:"updated_at"`
	GitWorkersBusy  int                        `json:"git_workers_busy"`
	GitWorkersMax   int                        `json:"git_workers_max"`
	APIWorkersBusy  int                        `json:"api_workers_busy"`
	APIWorkersMax   int                        `json:"api_workers_max"`
	TargetWorkers   int                        `json:"target_workers"`
	TotalAPISaved   int64                      `json:"total_api_saved"`
	TotalETASeconds int64                      `json:"total_eta_seconds"`
	CooldownNote    string                     `json:"cooldown_note,omitempty"`
	APIMetrics      github.Metrics             `json:"api_metrics"`
	Tokens          []tokenpool.TokenTelemetry `json:"tokens"`
	Repos           []RepoProgress             `json:"repos"`
	RecentLogs      []string                   `json:"recent_logs"`
}

// ParseRepoSpecs normalizes CLI repo strings ("owner/repo", GitHub URLs, ".git") and deduplicates them.
func ParseRepoSpecs(inputs []string) ([][2]string, error) {
	seen := make(map[string]struct{})
	var out [][2]string

	for _, raw := range inputs {
		s := strings.TrimSpace(raw)
		if s == "" {
			continue
		}
		if strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://") {
			u, err := url.Parse(s)
			if err != nil {
				return nil, fmt.Errorf("invalid repository URL %q: %w", raw, err)
			}
			s = strings.Trim(u.Path, "/")
		} else if strings.HasPrefix(s, "git@github.com:") {
			s = strings.TrimPrefix(s, "git@github.com:")
		}
		s = strings.TrimSuffix(s, ".git")
		s = strings.Trim(s, "/")
		parts := strings.Split(s, "/")
		if len(parts) < 2 || parts[0] == "" || parts[1] == "" {
			return nil, fmt.Errorf("invalid repository %q: expected <owner>/<repo>", raw)
		}
		owner, repo := parts[0], parts[1]
		key := strings.ToLower(owner + "/" + repo)
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, [2]string{owner, repo})
	}
	if len(out) == 0 {
		return nil, errors.New("at least one <owner>/<repo> argument is required")
	}
	return out, nil
}

// Orchestrator runs and monitors the multi-repo hybrid pipeline.
type Orchestrator struct {
	opts       RunOptions
	store      *store.Store
	pool       *tokenpool.Pool
	gh         *github.Client
	startedAt  time.Time

	mu             sync.RWMutex
	repos          []*RepoProgress
	logs           []string
	finished       bool
	targetWorkers  atomic.Int32
	gitBusy        atomic.Int32
	apiBusy        atomic.Int32
	totalAPISaved  atomic.Int64

	lastRowsCheck  time.Time
	lastRowsCount  int64
	ewmaRowsPerSec float64
	ewmaGQLPerSec  float64
	lastGQLCount   int64

	cancelFn context.CancelFunc
}

// NewOrchestrator constructs a pipeline Orchestrator ready to run.
func NewOrchestrator(opts RunOptions) (*Orchestrator, error) {
	if opts.DBPath == "" {
		opts.DBPath = "ghfetcher.db"
	}
	st, err := store.Open(opts.DBPath)
	if err != nil {
		return nil, err
	}

	effTokens := opts.Config.EffectiveTokens()
	if opts.Selection.AnyAPI() && len(effTokens) == 0 {
		_ = st.Close()
		return nil, tokenpool.ErrNoTokensConfigured
	}

	pool := tokenpool.New(effTokens, tokenpool.Options{
		SafetyReserve:   opts.SafetyReserve,
		MaxGQLInFlight:  opts.Config.Defaults.MaxGQLPerToken,
		MaxRESTInFlight: opts.Config.Defaults.MaxRESTPerToken,
	})
	gh := github.NewClient(pool, st)

	if opts.RepoConcurrency <= 0 {
		opts.RepoConcurrency = 3
	}
	if opts.GitConcurrency <= 0 {
		opts.GitConcurrency = 2
	}
	if opts.APIConcurrency <= 0 {
		opts.APIConcurrency = 6
	}

	repos := make([]*RepoProgress, len(opts.Repos))
	for i, sp := range opts.Repos {
		repos[i] = &RepoProgress{
			FullName: sp[0] + "/" + sp[1],
			Owner:    sp[0],
			Name:     sp[1],
			State:    "queued",
			Stage:    "Queued",
		}
	}

	o := &Orchestrator{
		opts:          opts,
		store:         st,
		pool:          pool,
		gh:            gh,
		startedAt:     time.Now(),
		repos:         repos,
		lastRowsCheck: time.Now(),
	}
	o.targetWorkers.Store(int32(opts.RepoConcurrency))
	return o, nil
}

func (o *Orchestrator) Close() error {
	return o.store.Close()
}

func (o *Orchestrator) SetPaused(paused bool) {
	o.pool.SetPaused(paused)
	if paused {
		o.logf("Pipeline paused by user (in-flight ops completing cleanly)")
	} else {
		o.logf("Pipeline resumed")
	}
}

func (o *Orchestrator) TogglePause() bool {
	next := !o.pool.IsPaused()
	o.SetPaused(next)
	return next
}

func (o *Orchestrator) AdjustWorkers(delta int) int {
	next := int(o.targetWorkers.Load()) + delta
	if next < 1 {
		next = 1
	}
	if next > 16 {
		next = 16
	}
	o.targetWorkers.Store(int32(next))
	o.logf("Target repo worker concurrency set to %d", next)
	return next
}

func (o *Orchestrator) ReloadTokens() {
	cfg, _, err := config.Load(o.opts.ConfigPath)
	if err != nil {
		o.logf("Token reload error: %v", err)
		return
	}
	o.opts.Config = cfg
	n := o.pool.Reload(cfg.EffectiveTokens())
	o.logf("Hot-reloaded token pool (%d active tokens)", n)
}

func (o *Orchestrator) Stop() {
	if o.cancelFn != nil {
		o.logf("Saving checkpoints and stopping...")
		o.cancelFn()
	}
}

func (o *Orchestrator) logf(format string, args ...any) {
	msg := fmt.Sprintf("[%s] %s", time.Now().Format("15:04:05"), fmt.Sprintf(format, args...))
	o.mu.Lock()
	o.logs = append(o.logs, msg)
	if len(o.logs) > 50 {
		o.logs = o.logs[len(o.logs)-50:]
	}
	o.mu.Unlock()
	if o.opts.OnLog != nil {
		o.opts.OnLog(msg)
	}
}

// Run executes the full multi-repository pipeline until completion or cancellation.
func (o *Orchestrator) Run(parentCtx context.Context) error {
	ctx, cancel := context.WithCancel(parentCtx)
	o.cancelFn = cancel
	defer cancel()

	// Background telemetry publisher & remote command poller (`ghfetcher attach`)
	bgDone := make(chan struct{})
	go func() {
		defer close(bgDone)
		ticker := time.NewTicker(400 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				o.publishTelemetry(context.Background())
				return
			case <-ticker.C:
				o.pollRemoteCommands(ctx)
				o.publishTelemetry(ctx)
			}
		}
	}()

	// Step 1: 1-Point GraphQL Pre-Flight Estimate (populates exact totals for progress bars & ETA!)
	var estByFull map[string]github.RepoEstimate
	if o.pool.TokenCount() > 0 {
		o.logf("Running 1-point GraphQL pre-flight sizing for %d repo(s)...", len(o.opts.Repos))
		ests, cost, err := o.gh.BatchEstimateRepos(ctx, o.opts.Repos)
		if err != nil {
			o.logf("Pre-flight estimate warning (continuing): %v", err)
		} else {
			estByFull = make(map[string]github.RepoEstimate, len(ests))
			o.mu.Lock()
			for _, e := range ests {
				estByFull[strings.ToLower(e.FullName)] = e
				for _, rp := range o.repos {
					if strings.EqualFold(rp.FullName, e.FullName) {
						rp.CommitsTotal = e.CommitsCount
						rp.IssuesTotal = e.IssuesCount
						rp.PRsTotal = e.PRsCount
					}
				}
			}
			o.mu.Unlock()
			o.logf("Pre-flight complete (%d GQL pt used)", cost)
		}
	}

	// Semaphores for decoupled Git & API concurrency
	gitSem := make(chan struct{}, o.opts.GitConcurrency)
	apiSem := make(chan struct{}, o.opts.APIConcurrency)

	// Dynamic repo worker dispatcher respecting live `targetWorkers` (+/- in TUI)
	var wg sync.WaitGroup
	var nextRepoIdx atomic.Int32
	var firstErr error
	var errMu sync.Mutex

	recordErr := func(err error) {
		if err == nil || errors.Is(err, context.Canceled) {
			return
		}
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		errMu.Unlock()
	}

	maxWorkers := 16
	for w := 0; w < maxWorkers; w++ {
		workerID := int32(w)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if ctx.Err() != nil {
					return
				}
				// Throttle extra worker slots when user reduces concurrency via '-'
				if workerID >= o.targetWorkers.Load() {
					select {
					case <-ctx.Done():
						return
					case <-time.After(300 * time.Millisecond):
						continue
					}
				}

				idx := int(nextRepoIdx.Add(1) - 1)
				if idx >= len(o.repos) {
					return
				}
				err := o.processRepo(ctx, idx, estByFull, gitSem, apiSem)
				if err != nil {
					recordErr(err)
				}
			}
		}()
	}

	wg.Wait()
	o.mu.Lock()
	o.finished = true
	o.mu.Unlock()
	cancel()
	<-bgDone
	o.publishTelemetry(context.Background())

	return firstErr
}

func (o *Orchestrator) processRepo(
	ctx context.Context,
	idx int,
	estByFull map[string]github.RepoEstimate,
	gitSem chan struct{},
	apiSem chan struct{},
) error {
	o.mu.RLock()
	rp := o.repos[idx]
	owner, name, fullName := rp.Owner, rp.Name, rp.FullName
	o.mu.RUnlock()

	o.updateRepo(idx, func(r *RepoProgress) {
		r.State = "active"
		r.Stage = "Initializing"
		r.StartedAt = time.Now()
	})
	o.logf("Starting %s", fullName)

	targetStore := o.store
	var closePerRepo func()
	if o.opts.PerRepoDB {
		dbFile := filepath.Join("ghfetcher_data", fmt.Sprintf("%s__%s.db", owner, name))
		perSt, err := store.Open(dbFile)
		if err != nil {
			o.failRepo(idx, err)
			return err
		}
		targetStore = perSt
		closePerRepo = func() { _ = perSt.Close() }
		defer closePerRepo()
	}

	prevRepo, _ := targetStore.GetRepository(ctx, owner, name)
	repoMeta := &store.Repository{
		Owner:         owner,
		Name:          name,
		FullName:      strings.ToLower(fullName),
		DefaultBranch: "main",
	}
	if prevRepo != nil && prevRepo.DefaultBranch != "" {
		repoMeta.DefaultBranch = prevRepo.DefaultBranch
	}
	if est, ok := estByFull[strings.ToLower(fullName)]; ok {
		repoMeta.GitHubID = est.GitHubID
		repoMeta.DefaultBranch = est.DefaultBranch
		repoMeta.IsFork = est.IsFork
		repoMeta.Stars = est.Stars
		repoMeta.Forks = est.Forks
		repoMeta.DiskKB = est.DiskKB
		repoMeta.TotalCommitsEst = est.CommitsCount
		repoMeta.TotalIssuesEst = est.IssuesCount
		repoMeta.TotalPRsEst = est.PRsCount
	}

	repoID, err := targetStore.UpsertRepository(ctx, repoMeta)
	if err != nil {
		o.failRepo(idx, err)
		return err
	}

	// Run Local Bare Git Pipeline and Hybrid API Pipeline concurrently for this repo!
	// Crucially: Git pipeline does NOT acquire API tokens, so it proceeds even during API quota sleep!
	var subWG sync.WaitGroup
	errCh := make(chan error, 4)

	if o.opts.Selection.Commits {
		subWG.Add(1)
		go func() {
			defer subWG.Done()
			select {
			case gitSem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			o.gitBusy.Add(1)
			defer func() {
				o.gitBusy.Add(-1)
				<-gitSem
			}()

			var sinceCommit time.Time
			if prevRepo != nil && !prevRepo.LastCommitAt.IsZero() {
				sinceCommit = prevRepo.LastCommitAt
			}
			var gitToken string
			if eff := o.opts.Config.EffectiveTokens(); len(eff) > 0 {
				gitToken = eff[0].Token
			}

			res, gerr := gitlocal.ExtractRepository(ctx, targetStore, gitlocal.ExtractOptions{
				RepoID:          repoID,
				Owner:           owner,
				Name:            name,
				DefaultBranch:   repoMeta.DefaultBranch,
				Token:           gitToken,
				KeepGit:         o.opts.KeepGit,
				IncludePatches:  o.opts.Selection.Patches,
				SinceCommitTime: sinceCommit,
				OnStage: func(st string) {
					o.updateRepo(idx, func(r *RepoProgress) {
						r.Stage = st
					})
				},
				OnProgress: func(commitsParsed, _ int) {
					o.updateRepo(idx, func(r *RepoProgress) {
						r.CommitsDone = commitsParsed
						if r.CommitsTotal < commitsParsed {
							r.CommitsTotal = commitsParsed
						}
						o.recomputeRepoPctLocked(r)
					})
				},
			})
			if gerr != nil {
				errCh <- fmt.Errorf("%s git extract: %w", fullName, gerr)
				return
			}
			o.totalAPISaved.Add(int64(res.EstimatedAPISaved))
			o.updateRepo(idx, func(r *RepoProgress) {
				r.CommitsDone = res.CommitsExtracted
				if r.CommitsTotal == 0 {
					r.CommitsTotal = res.CommitsExtracted
				}
				r.APISaved += int64(res.EstimatedAPISaved)
				o.recomputeRepoPctLocked(r)
			})
			o.logf("%s: extracted %d commits & %d PR-commit links locally (0 API calls)", fullName, res.CommitsExtracted, res.PRCommitsLinked)
		}()
	}

	overflowCh := make(chan github.OverflowTask, 256)
	var apiProducersWG sync.WaitGroup

	// Stream A: GraphQL Issues (+ embedded comments & timeline)
	if o.opts.Selection.Issues {
		apiProducersWG.Add(1)
		subWG.Add(1)
		go func() {
			defer subWG.Done()
			defer apiProducersWG.Done()
			if err := o.runIssuesStream(ctx, idx, targetStore, repoID, owner, name, apiSem, overflowCh); err != nil {
				errCh <- fmt.Errorf("%s issues: %w", fullName, err)
			}
		}()
	}

	// Stream B: GraphQL Pull Requests (+ embedded comments, reviews & timeline)
	if o.opts.Selection.PRs || o.opts.Selection.Reviews {
		apiProducersWG.Add(1)
		subWG.Add(1)
		go func() {
			defer subWG.Done()
			defer apiProducersWG.Done()
			if err := o.runPRsStream(ctx, idx, targetStore, repoID, owner, name, apiSem, overflowCh); err != nil {
				errCh <- fmt.Errorf("%s prs: %w", fullName, err)
			}
		}()
	}

	// Stream C: REST Repo-Wide Inline PR Review Comments Sweep (runs on independent REST quota!)
	if o.opts.Selection.Reviews || o.opts.Selection.Comments {
		subWG.Add(1)
		go func() {
			defer subWG.Done()
			select {
			case apiSem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			o.apiBusy.Add(1)
			defer func() {
				o.apiBusy.Add(-1)
				<-apiSem
			}()

			fetched, rerr := o.gh.SweepRepoReviewCommentsREST(ctx, targetStore, repoID, owner, name, func(done int) {
				o.updateRepo(idx, func(r *RepoProgress) {
					r.RevCommDone = done
					r.Stage = "REST Review Comments"
					o.recomputeRepoPctLocked(r)
				})
			})
			if rerr != nil {
				errCh <- fmt.Errorf("%s review comments: %w", fullName, rerr)
				return
			}
			if fetched > 0 {
				o.logf("%s: swept %d inline PR review comments via bulk REST", fullName, fetched)
			}
		}()
	}

	// Close overflowCh once Issues & PRs producers finish
	go func() {
		apiProducersWG.Wait()
		close(overflowCh)
	}()

	// Stream D: Overflow consumer for mega-Issues/PRs (>20 comments/reviews or >25 timeline events)
	subWG.Add(1)
	go func() {
		defer subWG.Done()
		for task := range overflowCh {
			if ctx.Err() != nil {
				return
			}
			select {
			case apiSem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			o.apiBusy.Add(1)
			o.updateRepo(idx, func(r *RepoProgress) {
				r.Stage = fmt.Sprintf("Overflow #%d (%s)", task.Number, task.Kind)
			})
			derr := o.gh.DrainOverflowTask(ctx, targetStore, task)
			o.apiBusy.Add(-1)
			<-apiSem
			if derr != nil {
				errCh <- fmt.Errorf("%s overflow #%d: %w", fullName, task.Number, derr)
				return
			}
			o.updateRepo(idx, func(r *RepoProgress) {
				r.OverflowDone++
				o.recomputeRepoPctLocked(r)
			})
		}
	}()

	subWG.Wait()
	close(errCh)

	for e := range errCh {
		if e != nil && !errors.Is(e, context.Canceled) {
			o.failRepo(idx, e)
			return e
		}
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}

	repoMeta.LastSyncedAt = time.Now().UTC()
	_, _ = targetStore.UpsertRepository(ctx, repoMeta)

	o.updateRepo(idx, func(r *RepoProgress) {
		r.State = "done"
		r.Stage = "Completed"
		r.ProgressPct = 100
		r.ETASeconds = 0
		r.CompletedAt = time.Now()
	})
	o.logf("Completed %s", fullName)
	return nil
}

func (o *Orchestrator) runIssuesStream(
	ctx context.Context,
	idx int,
	st *store.Store,
	repoID int64,
	owner, name string,
	apiSem chan struct{},
	overflowCh chan<- github.OverflowTask,
) error {
	const stream = "gql_issues"
	cp, err := st.GetCheckpoint(ctx, repoID, stream)
	if err != nil {
		return err
	}

	cursor := ""
	sinceISO := ""
	var done int64
	if cp != nil {
		done = cp.ItemsDone
		if cp.Completed {
			sinceISO = cp.Watermark
		} else {
			cursor = cp.Cursor
			sinceISO = cp.Watermark
		}
	}

	maxUpdated := time.Time{}
	if sinceISO != "" {
		maxUpdated, _ = time.Parse(time.RFC3339, sinceISO)
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		select {
		case apiSem <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		o.apiBusy.Add(1)
		o.updateRepo(idx, func(r *RepoProgress) {
			r.Stage = "GQL Issues + Timeline"
		})

		page, perr := o.gh.FetchIssuesPage(
			ctx, repoID, owner, name, cursor, sinceISO,
			o.opts.Selection.Comments, o.opts.Selection.Timeline,
		)
		o.apiBusy.Add(-1)
		<-apiSem
		if perr != nil {
			return perr
		}

		if page.MaxUpdatedAt.After(maxUpdated) {
			maxUpdated = page.MaxUpdatedAt
		}
		done += int64(page.ItemsCount)

		wm := sinceISO
		if !maxUpdated.IsZero() {
			wm = maxUpdated.UTC().Format(time.RFC3339)
		}
		nextCur := ""
		if page.HasNextPage {
			nextCur = page.EndCursor
		}
		page.Batch.Checkpoint = &store.SyncCheckpoint{
			RepoID:     repoID,
			StreamName: stream,
			Cursor:     nextCur,
			Watermark:  wm,
			ItemsDone:  done,
			Completed:  !page.HasNextPage,
		}
		if err := st.SaveArtifactBatch(ctx, page.Batch); err != nil {
			return err
		}

		o.totalAPISaved.Add(int64(page.APISaved))
		o.updateRepo(idx, func(r *RepoProgress) {
			r.IssuesDone += page.ItemsCount
			if r.IssuesTotal < r.IssuesDone {
				r.IssuesTotal = r.IssuesDone
			}
			r.OverflowQueued += len(page.OverflowTasks)
			r.APISaved += int64(page.APISaved)
			o.recomputeRepoPctLocked(r)
		})

		for _, ot := range page.OverflowTasks {
			select {
			case overflowCh <- ot:
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		if !page.HasNextPage {
			break
		}
		cursor = page.EndCursor
	}
	return nil
}

func (o *Orchestrator) runPRsStream(
	ctx context.Context,
	idx int,
	st *store.Store,
	repoID int64,
	owner, name string,
	apiSem chan struct{},
	overflowCh chan<- github.OverflowTask,
) error {
	const stream = "gql_prs"
	cp, err := st.GetCheckpoint(ctx, repoID, stream)
	if err != nil {
		return err
	}

	cursor := ""
	descending := false
	var cutoff time.Time
	var done int64

	if cp != nil {
		done = cp.ItemsDone
		if cp.Completed && cp.Watermark != "" {
			// Incremental PR mode: walk UPDATED_AT DESC and stop as soon as updatedAt < watermark!
			descending = true
			cutoff, _ = time.Parse(time.RFC3339, cp.Watermark)
		} else {
			cursor = cp.Cursor
		}
	}

	maxUpdated := cutoff

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		select {
		case apiSem <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		o.apiBusy.Add(1)
		o.updateRepo(idx, func(r *RepoProgress) {
			r.Stage = "GQL PRs + Reviews"
		})

		page, perr := o.gh.FetchPRsPage(
			ctx, repoID, owner, name, cursor, descending, cutoff,
			o.opts.Selection.Comments, o.opts.Selection.Reviews, o.opts.Selection.Timeline,
		)
		o.apiBusy.Add(-1)
		<-apiSem
		if perr != nil {
			return perr
		}

		if page.MaxUpdatedAt.After(maxUpdated) {
			maxUpdated = page.MaxUpdatedAt
		}
		done += int64(page.ItemsCount)
		finished := !page.HasNextPage || page.ReachedCutoff

		wm := ""
		if cp != nil {
			wm = cp.Watermark
		}
		if !maxUpdated.IsZero() {
			wm = maxUpdated.UTC().Format(time.RFC3339)
		}
		nextCur := ""
		if !finished {
			nextCur = page.EndCursor
		}

		page.Batch.Checkpoint = &store.SyncCheckpoint{
			RepoID:     repoID,
			StreamName: stream,
			Cursor:     nextCur,
			Watermark:  wm,
			ItemsDone:  done,
			Completed:  finished,
		}
		if err := st.SaveArtifactBatch(ctx, page.Batch); err != nil {
			return err
		}

		o.totalAPISaved.Add(int64(page.APISaved))
		o.updateRepo(idx, func(r *RepoProgress) {
			r.PRsDone += page.ItemsCount
			if r.PRsTotal < r.PRsDone {
				r.PRsTotal = r.PRsDone
			}
			r.OverflowQueued += len(page.OverflowTasks)
			r.APISaved += int64(page.APISaved)
			o.recomputeRepoPctLocked(r)
		})

		for _, ot := range page.OverflowTasks {
			select {
			case overflowCh <- ot:
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		if finished {
			break
		}
		cursor = page.EndCursor
	}
	return nil
}

func (o *Orchestrator) updateRepo(idx int, fn func(*RepoProgress)) {
	o.mu.Lock()
	defer o.mu.Unlock()
	fn(o.repos[idx])
}

func (o *Orchestrator) failRepo(idx int, err error) {
	o.logf("ERROR on %s: %v", o.repos[idx].FullName, err)
	o.updateRepo(idx, func(r *RepoProgress) {
		r.State = "error"
		r.Stage = "Failed (Checkpointed)"
		r.LastError = err.Error()
	})
}

func (o *Orchestrator) recomputeRepoPctLocked(r *RepoProgress) {
	var done, total float64
	if o.opts.Selection.Commits {
		done += float64(r.CommitsDone)
		if r.CommitsTotal > 0 {
			total += float64(r.CommitsTotal)
		} else {
			total += float64(r.CommitsDone + 1)
		}
	}
	if o.opts.Selection.Issues {
		done += float64(r.IssuesDone) * 4 // Weight API items higher than local git commits
		total += float64(maxInt(r.IssuesTotal, r.IssuesDone)) * 4
	}
	if o.opts.Selection.PRs || o.opts.Selection.Reviews {
		done += float64(r.PRsDone) * 5
		total += float64(maxInt(r.PRsTotal, r.PRsDone)) * 5
	}
	if r.OverflowQueued > 0 {
		done += float64(r.OverflowDone) * 3
		total += float64(r.OverflowQueued) * 3
	}

	if total <= 0 {
		r.ProgressPct = 0
	} else {
		pct := (done / total) * 100.0
		if pct > 99.4 && r.State != "done" {
			pct = 99.4
		}
		r.ProgressPct = pct
	}

	if !r.StartedAt.IsZero() {
		elapsed := time.Since(r.StartedAt).Seconds()
		items := r.CommitsDone + r.IssuesDone + r.PRsDone + r.RevCommDone
		if elapsed > 0.5 {
			r.ItemsPerSec = float64(items) / elapsed
		}
		if r.ProgressPct > 2.0 && elapsed > 1.0 {
			remSec := elapsed * ((100.0 - r.ProgressPct) / r.ProgressPct)
			r.ETASeconds = int64(math.Ceil(remSec))
		}
	}
}

// Snapshot builds a consistent TelemetrySnapshot including Rate-Limit-Aware ETA.
func (o *Orchestrator) Snapshot() TelemetrySnapshot {
	now := time.Now()
	metrics := o.gh.Metrics()
	tokens := o.pool.Snapshot()
	rowsNow := o.store.RowsWritten()

	o.mu.Lock()
	dt := now.Sub(o.lastRowsCheck).Seconds()
	if dt >= 0.35 {
		instRows := float64(rowsNow-o.lastRowsCount) / dt
		instGQL := float64(metrics.GQLPoints-o.lastGQLCount) / dt
		if o.ewmaRowsPerSec == 0 {
			o.ewmaRowsPerSec = instRows
			o.ewmaGQLPerSec = instGQL
		} else {
			o.ewmaRowsPerSec = 0.7*o.ewmaRowsPerSec + 0.3*instRows
			o.ewmaGQLPerSec = 0.7*o.ewmaGQLPerSec + 0.3*instGQL
		}
		o.lastRowsCount = rowsNow
		o.lastGQLCount = metrics.GQLPoints
		o.lastRowsCheck = now
	}

	repoCopies := make([]RepoProgress, len(o.repos))
	var remGQLPages int
	var maxGitETASec int64
	for i, rp := range o.repos {
		repoCopies[i] = *rp
		if rp.State == "done" || rp.State == "error" {
			continue
		}
		remIssues := maxInt(0, rp.IssuesTotal-rp.IssuesDone)
		remPRs := maxInt(0, rp.PRsTotal-rp.PRsDone)
		remOverflow := maxInt(0, rp.OverflowQueued-rp.OverflowDone)
		remGQLPages += (remIssues+29)/30 + (remPRs+24)/25 + remOverflow
		if rp.ETASeconds > maxGitETASec {
			maxGitETASec = rp.ETASeconds
		}
	}

	logsCopy := make([]string, len(o.logs))
	copy(logsCopy, o.logs)
	finished := o.finished
	o.mu.Unlock()

	// Rate-Limit-Aware ETA calculation
	var totalETASec int64
	var cooldownNote string
	if !finished {
		usableGQL, hourlyCapGQL, earliestReset := o.pool.PooledAvailable(tokenpool.BucketGraphQL)
		reqRate := o.ewmaGQLPerSec
		if reqRate < 1.5 {
			reqRate = 3.0 // Conservative baseline 3 pages/sec
		}
		execSec := float64(remGQLPages) / reqRate
		var cooldownSec float64

		if remGQLPages > usableGQL && hourlyCapGQL > 0 {
			deficit := remGQLPages - usableGQL
			if !earliestReset.IsZero() && earliestReset.After(now) {
				waitFirst := earliestReset.Sub(now).Seconds()
				cooldownSec += waitFirst
				cooldownNote = fmt.Sprintf("Includes +%dm quota reset wait", int(math.Ceil(waitFirst/60.0)))
			}
			extraWindows := (deficit - 1) / hourlyCapGQL
			if extraWindows > 0 {
				cooldownSec += float64(extraWindows) * 3600.0
			}
		} else if usableGQL == 0 && o.opts.Selection.AnyAPI() && !earliestReset.IsZero() && earliestReset.After(now) {
			wait := earliestReset.Sub(now).Seconds()
			cooldownSec += wait
			cooldownNote = fmt.Sprintf("API sleeping %dm%02ds (Git active)", int(wait)/60, int(wait)%60)
		}

		apiETA := int64(math.Ceil(execSec + cooldownSec))
		totalETASec = apiETA
		if maxGitETASec > totalETASec && cooldownSec == 0 {
			totalETASec = maxGitETASec
		}
	}

	saved := o.totalAPISaved.Load() + metrics.ETagHits304

	return TelemetrySnapshot{
		PID:             os.Getpid(),
		DBPath:          o.store.Path(),
		DBSizeBytes:     o.store.FileSizeBytes(),
		RowsWritten:     rowsNow,
		RowsPerSec:      o.ewmaRowsPerSec,
		Paused:          o.pool.IsPaused(),
		Finished:        finished,
		StartedAt:       o.startedAt,
		UpdatedAt:       now,
		GitWorkersBusy:  int(o.gitBusy.Load()),
		GitWorkersMax:   o.opts.GitConcurrency,
		APIWorkersBusy:  int(o.apiBusy.Load()),
		APIWorkersMax:   o.opts.APIConcurrency,
		TargetWorkers:   int(o.targetWorkers.Load()),
		TotalAPISaved:   saved,
		TotalETASeconds: totalETASec,
		CooldownNote:    cooldownNote,
		APIMetrics:      metrics,
		Tokens:          tokens,
		Repos:           repoCopies,
		RecentLogs:      logsCopy,
	}
}

func (o *Orchestrator) publishTelemetry(ctx context.Context) {
	snap := o.Snapshot()
	data, err := json.Marshal(snap)
	if err != nil {
		return
	}
	_ = o.store.WriteTelemetry(ctx, snap.PID, o.startedAt, string(data))
}

func (o *Orchestrator) pollRemoteCommands(ctx context.Context) {
	cmds, err := o.store.PollCommands(ctx)
	if err != nil || len(cmds) == 0 {
		return
	}
	for _, c := range cmds {
		switch c.Command {
		case "pause":
			o.SetPaused(true)
		case "resume":
			o.SetPaused(false)
		case "toggle-pause":
			o.TogglePause()
		case "reload-tokens":
			o.ReloadTokens()
		case "workers-inc":
			o.AdjustWorkers(1)
		case "workers-dec":
			o.AdjustWorkers(-1)
		case "stop":
			o.Stop()
		}
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
