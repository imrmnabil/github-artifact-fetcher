package cli

import (
	"bufio"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"ghfetcher/internal/config"
	"ghfetcher/internal/pipeline"
	"ghfetcher/internal/tui"
)

func newFetchCmd(gf *globalFlags) *cobra.Command {
	var (
		flagAll       bool
		flagCommits   bool
		flagIssues    bool
		flagPRs       bool
		flagReviews   bool
		flagComments  bool
		flagTimeline  bool
		flagPatches   bool
		flagKeepGit   bool
		flagPerRepoDB bool
		flagNoTUI     bool
		flagOutDB     string
		flagFromFile  string
		flagWorkers   int
		flagReserve   int
	)

	cmd := &cobra.Command{
		Use:   "fetch -all [<owner/repo>...]",
		Short: "Fetch GitHub research artifacts with hybrid bare-Git + GraphQL/REST & htop TUI",
		Example: `  ghfetcher fetch -all facebook/react vercel/next.js
  ghfetcher fetch -all --from repos.txt --out study.db
  ghfetcher fetch --commits --prs golang/go --no-tui`,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, cfgPath, err := config.Load(gf.configPath)
			if err != nil {
				return err
			}

			inputs, err := collectRepoInputs(args, flagFromFile)
			if err != nil {
				return err
			}
			specs, err := pipeline.ParseRepoSpecs(inputs)
			if err != nil {
				return err
			}

			anyExplicit := flagCommits || flagIssues || flagPRs || flagReviews || flagComments || flagTimeline
			var sel pipeline.ArtifactSelection
			if flagAll || !anyExplicit {
				sel = pipeline.AllArtifacts(flagPatches)
			} else {
				sel = pipeline.ArtifactSelection{
					Commits:  flagCommits,
					Issues:   flagIssues,
					PRs:      flagPRs,
					Reviews:  flagReviews,
					Comments: flagComments,
					Timeline: flagTimeline,
					Patches:  flagPatches,
				}
			}

			dbPath := flagOutDB
			if dbPath == "" {
				dbPath = cfg.Defaults.DatabasePath
			}
			workers := cfg.Defaults.RepoConcurrency
			if flagWorkers > 0 {
				workers = flagWorkers
			}
			reserve := cfg.Defaults.SafetyReserve
			if cmd.Flags().Changed("reserve") {
				reserve = flagReserve
			}
			keepGit := cfg.Defaults.KeepGit || flagKeepGit

			headless := flagNoTUI || !isTerminal(os.Stdout) || !isTerminal(os.Stdin)

			runOpts := pipeline.RunOptions{
				ConfigPath:      cfgPath,
				Config:          cfg,
				Repos:           specs,
				Selection:       sel,
				DBPath:          dbPath,
				PerRepoDB:       flagPerRepoDB,
				KeepGit:         keepGit,
				RepoConcurrency: workers,
				GitConcurrency:  cfg.Defaults.GitConcurrency,
				APIConcurrency:  cfg.Defaults.APIConcurrency,
				SafetyReserve:   reserve,
			}
			if headless {
				runOpts.OnLog = func(msg string) {
					fmt.Fprintln(os.Stdout, msg)
				}
			}

			orch, err := pipeline.NewOrchestrator(runOpts)
			if err != nil {
				return err
			}
			defer orch.Close()

			ctx, stopSig := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stopSig()

			start := time.Now()
			var runErr error

			if headless {
				fmt.Printf("Starting ghfetcher in headless mode (%d repo(s), DB: %s). Tip: run `ghfetcher attach --db %s` to view live htop TUI.\n",
					len(specs), dbPath, dbPath)
				runErr = orch.Run(ctx)
			} else {
				doneCh := make(chan error, 1)
				go func() {
					doneCh <- orch.Run(ctx)
				}()
				runErr = tui.RunDashboard(&tui.LocalController{Orch: orch}, doneCh)
			}

			snap := orch.Snapshot()
			fmt.Printf("\n── ghfetcher Run Summary (%s) ──\n", time.Since(start).Round(time.Millisecond))
			fmt.Printf("Database         : %s (%.2f MB, %d rows written)\n",
				snap.DBPath, float64(snap.DBSizeBytes)/(1024*1024), snap.RowsWritten)
			fmt.Printf("API Quota Used   : %d GraphQL pts (%d reqs) + %d REST reqs (%d free 304 ETag hits)\n",
				snap.APIMetrics.GQLPoints, snap.APIMetrics.GQLRequests,
				snap.APIMetrics.RESTRequests, snap.APIMetrics.ETagHits304)
			fmt.Printf("API Calls Saved  : ~%d calls avoided via bare Git + PR refspecs + GraphQL bundling\n", snap.TotalAPISaved)

			return runErr
		},
	}

	cmd.Flags().BoolVarP(&flagAll, "all", "a", false, "Fetch all research artifacts (commits, churn, PRs, issues, reviews, comments, timeline)")
	cmd.Flags().BoolVar(&flagCommits, "commits", false, "Fetch commits, commit messages, file churn, refs & PR commit mappings via bare Git")
	cmd.Flags().BoolVar(&flagIssues, "issues", false, "Fetch issues metadata via GraphQL")
	cmd.Flags().BoolVar(&flagPRs, "prs", false, "Fetch pull requests metadata via GraphQL")
	cmd.Flags().BoolVar(&flagReviews, "reviews", false, "Fetch PR code reviews & inline diff review comments")
	cmd.Flags().BoolVar(&flagComments, "comments", false, "Fetch issue and PR discussion comments + reactions")
	cmd.Flags().BoolVar(&flagTimeline, "timeline", false, "Fetch issue and PR lifecycle timeline events")
	cmd.Flags().BoolVar(&flagPatches, "patches", false, "Store unified diff patches per commit file in commit_files.patch")
	cmd.Flags().BoolVar(&flagKeepGit, "keep-git", false, "Retain bare Git clones in .ghfetcher/git-cache/ instead of deleting")
	cmd.Flags().BoolVar(&flagPerRepoDB, "per-repo-db", false, "Store each repository in its own SQLite DB under ./ghfetcher_data/")
	cmd.Flags().BoolVar(&flagNoTUI, "no-tui", false, "Disable interactive htop TUI and print structured progress logs (for SSH/nohup)")
	cmd.Flags().StringVarP(&flagOutDB, "out", "o", "", "Output SQLite database path (default from config: ghfetcher.db)")
	cmd.Flags().StringVar(&flagFromFile, "from", "", "Read repositories from file (one per line, '#' comments ignored, '-' for stdin)")
	cmd.Flags().IntVarP(&flagWorkers, "concurrency", "c", 0, "Number of repositories processed concurrently")
	cmd.Flags().IntVar(&flagReserve, "reserve", 100, "Per-token quota safety reserve to leave untouched")

	return cmd
}

func collectRepoInputs(cliArgs []string, fromFile string) ([]string, error) {
	var all []string
	all = append(all, cliArgs...)

	if strings.TrimSpace(fromFile) != "" {
		var sc *bufio.Scanner
		if fromFile == "-" {
			sc = bufio.NewScanner(os.Stdin)
		} else {
			f, err := os.Open(fromFile)
			if err != nil {
				return nil, fmt.Errorf("open repo list %s: %w", fromFile, err)
			}
			defer f.Close()
			sc = bufio.NewScanner(f)
		}
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if idx := strings.IndexByte(line, '#'); idx >= 0 {
				line = strings.TrimSpace(line[:idx])
			}
			if line != "" {
				// Support whitespace or comma separated fields on a line
				fields := strings.FieldsFunc(line, func(r rune) bool {
					return r == ' ' || r == '\t' || r == ','
				})
				if len(fields) > 0 {
					all = append(all, fields[0])
				}
			}
		}
		if err := sc.Err(); err != nil {
			return nil, err
		}
	}
	return all, nil
}
