package cli

import (
	"os"

	"github.com/spf13/cobra"
)

type globalFlags struct {
	configPath string
}

// NewRootCmd constructs the root `ghfetcher` CLI command tree.
func NewRootCmd() *cobra.Command {
	gf := &globalFlags{}

	root := &cobra.Command{
		Use:   "ghfetcher",
		Short: "High-efficiency GitHub artifact fetcher for MSR & software engineering research",
		Long: `ghfetcher mines GitHub repositories (commits, churn, PRs, issues, code reviews,
inline diff comments, reactions, and timeline events) with minimal API consumption by
combining zero-API ephemeral bare Git clones (+refs/pull/*) with dual-pool GraphQL + REST
scheduling, multi-token load balancing, SQLite WAL storage, and an htop-style TUI.`,
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	root.PersistentFlags().StringVar(&gf.configPath, "config", "", "Path to config.toml (default ~/.config/ghfetcher/config.toml)")

	root.AddCommand(
		newConfigCmd(gf),
		newEstimateCmd(gf),
		newFetchCmd(gf),
		newAttachCmd(gf),
		newStatusCmd(gf),
		newExportCmd(gf),
	)

	return root
}

func isTerminal(f *os.File) bool {
	info, err := f.Stat()
	if err != nil {
		return false
	}
	return (info.Mode() & os.ModeCharDevice) != 0
}
