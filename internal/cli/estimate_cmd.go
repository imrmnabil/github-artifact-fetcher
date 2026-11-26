package cli

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"ghfetcher/internal/config"
	"ghfetcher/internal/github"
	"ghfetcher/internal/pipeline"
	"ghfetcher/internal/tokenpool"
)

func newEstimateCmd(gf *globalFlags) *cobra.Command {
	var fromFile string

	cmd := &cobra.Command{
		Use:    "estimate [<owner/repo>...]",
		Short:  "[experimental] 1-point GraphQL dry-run showing exact repo artifact counts and disk size",
		Hidden: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, _, err := config.Load(gf.configPath)
			if err != nil {
				return err
			}
			inputs, err := collectRepoInputs(args, fromFile)
			if err != nil {
				return err
			}
			specs, err := pipeline.ParseRepoSpecs(inputs)
			if err != nil {
				return err
			}

			eff := cfg.EffectiveTokens()
			if len(eff) == 0 {
				return tokenpool.ErrNoTokensConfigured
			}

			pool := tokenpool.New(eff, tokenpool.Options{
				SafetyReserve:   cfg.Defaults.SafetyReserve,
				MaxGQLInFlight:  cfg.Defaults.MaxGQLPerToken,
				MaxRESTInFlight: cfg.Defaults.MaxRESTPerToken,
			})
			gh := github.NewClient(pool, nil)

			start := time.Now()
			ests, cost, err := gh.BatchEstimateRepos(cmd.Context(), specs)
			if err != nil {
				return err
			}

			fmt.Printf("Pre-Flight Estimate (%d repo(s) queried in %s using %d GraphQL point(s)):\n\n",
				len(ests), time.Since(start).Round(time.Millisecond), cost)

			fmt.Printf("%-28s %10s %9s %9s %10s %9s %9s %12s\n",
				"REPOSITORY", "COMMITS", "ISSUES", "PRS", "TEMP GIT", "EST GQL", "EST REST", "CALLS SAVED")
			fmt.Println(strings.Repeat("─", 104))

			var totCommits, totIssues, totPRs, totDiskKB, totGQL, totREST, totSaved int
			var maxDiskKB int
			for _, e := range ests {
				totCommits += e.CommitsCount
				totIssues += e.IssuesCount
				totPRs += e.PRsCount
				totDiskKB += e.DiskKB
				if e.DiskKB > maxDiskKB {
					maxDiskKB = e.DiskKB
				}
				totGQL += e.EstGQLPoints
				totREST += e.EstRESTCalls
				totSaved += e.EstAPISaved

				fmt.Printf("%-28s %10d %9d %9d %10s %9d %9d %12s\n",
					truncateStr(e.FullName, 28),
					e.CommitsCount,
					e.IssuesCount,
					e.PRsCount,
					fmtKB(e.DiskKB),
					e.EstGQLPoints,
					e.EstRESTCalls,
					fmt.Sprintf("~%d", e.EstAPISaved),
				)
			}
			fmt.Println(strings.Repeat("─", 104))
			fmt.Printf("%-28s %10d %9d %9d %10s %9d %9d %12s\n\n",
				"TOTAL", totCommits, totIssues, totPRs, fmtKB(totDiskKB), totGQL, totREST, fmt.Sprintf("~%d", totSaved))

			usableGQL, hourlyGQL, earliestReset := pool.PooledAvailable(tokenpool.BucketGraphQL)
			usableREST, _, _ := pool.PooledAvailable(tokenpool.BucketREST)

			execSec := float64(totGQL)/3.5 + float64(totCommits)/2500.0
			var cooldownSec float64
			fitsNow := totGQL <= usableGQL && totREST <= usableREST
			if !fitsNow && hourlyGQL > 0 && totGQL > usableGQL {
				deficit := totGQL - usableGQL
				if !earliestReset.IsZero() && earliestReset.After(time.Now()) {
					cooldownSec += time.Until(earliestReset).Seconds()
				}
				cooldownSec += float64((deficit-1)/hourlyGQL) * 3600.0
			}
			etaDur := time.Duration(math.Ceil(execSec+cooldownSec)) * time.Second

			peakEphemeralKB := maxDiskKB * cfg.Defaults.GitConcurrency
			if peakEphemeralKB > totDiskKB {
				peakEphemeralKB = totDiskKB
			}

			fmt.Printf("Active Token Pool        : %d token(s) (%d GQL pts & %d REST calls immediately usable above %d reserve)\n",
				len(eff), usableGQL, usableREST, cfg.Defaults.SafetyReserve)
			fmt.Printf("Peak Ephemeral Git Disk  : ~%s (bare clones auto-deleted after commit indexing)\n", fmtKB(peakEphemeralKB))
			if fitsNow {
				fmt.Printf("Fits Current Window      : YES (0 rate-limit sleep needed)\n")
			} else {
				fmt.Printf("Fits Current Window      : NO (auto-sleep & resume will add ~%s of quota reset wait)\n",
					(time.Duration(cooldownSec) * time.Second).Round(time.Minute))
			}
			fmt.Printf("Projected Wall-Clock ETA : ~%s\n", etaDur.Round(time.Second))
			return nil
		},
	}

	cmd.Flags().StringVar(&fromFile, "from", "", "Read repository list from file (one <owner/repo> or URL per line, or '-' for stdin)")
	return cmd
}

func fmtKB(kb int) string {
	if kb >= 1024*1024 {
		return fmt.Sprintf("%.2f GB", float64(kb)/(1024.0*1024.0))
	}
	if kb >= 1024 {
		return fmt.Sprintf("%.1f MB", float64(kb)/1024.0)
	}
	return fmt.Sprintf("%d KB", kb)
}

func truncateStr(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max-2] + ".."
}
