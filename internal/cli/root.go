package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"ghfetcher/internal/version"
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
		Version:       version.Summary(),
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	root.PersistentFlags().StringVar(&gf.configPath, "config", "", "Path to config.toml (default ~/.config/ghfetcher/config.toml)")

	versionCmd := &cobra.Command{
		Use:   "version",
		Short: "Print ghfetcher version, commit, and build date",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(version.Summary())
		},
	}

	root.AddCommand(
		newConfigCmd(gf),
		newEstimateCmd(gf),
		newFetchCmd(gf),
		newAttachCmd(gf),
		newStatusCmd(gf),
		newExportCmd(gf),
		versionCmd,
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
