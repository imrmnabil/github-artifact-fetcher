package version

import "fmt"

// Injected via -ldflags at build time by GoReleaser.
var (
	Version = "dev"
	Commit  = "none"
	Date    = "unknown"
)

func Summary() string {
	return fmt.Sprintf("ghfetcher %s (commit %s, built %s)", Version, Commit, Date)
}
