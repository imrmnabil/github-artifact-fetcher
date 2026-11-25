package cli

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"ghfetcher/internal/config"
	"ghfetcher/internal/export"
	"ghfetcher/internal/store"
)

func newExportCmd(gf *globalFlags) *cobra.Command {
	var (
		dbPath string
		format string
		outDir string
	)

	cmd := &cobra.Command{
		Use:   "export",
		Short: "Export SQLite research tables to Apache Parquet or JSONL",
		Example: `  ghfetcher export --db study.db --format parquet --out ./dataset_parquet
  ghfetcher export --db study.db --format jsonl --out ./dataset_jsonl`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if dbPath == "" {
				cfg, _, err := config.Load(gf.configPath)
				if err == nil {
					dbPath = cfg.Defaults.DatabasePath
				} else {
					dbPath = "ghfetcher.db"
				}
			}
			fmtKind, err := export.ParseFormat(format)
			if err != nil {
				return err
			}
			if outDir == "" {
				outDir = "ghfetcher_export_" + string(fmtKind)
			}

			st, err := store.Open(dbPath)
			if err != nil {
				return err
			}
			defer st.Close()

			start := time.Now()
			stats, err := export.ExportAll(cmd.Context(), st, outDir, fmtKind)
			if err != nil {
				return err
			}

			fmt.Printf("Exported %s -> %s (%s) in %s:\n\n",
				dbPath, outDir, fmtKind, time.Since(start).Round(time.Millisecond))
			fmt.Printf("%-24s %12s   %s\n", "TABLE", "ROWS", "OUTPUT FILE")
			fmt.Println(strings.Repeat("─", 76))
			var total int64
			for _, s := range stats {
				total += s.Rows
				fmt.Printf("%-24s %12d   %s\n", s.Table, s.Rows, s.File)
			}
			fmt.Println(strings.Repeat("─", 76))
			fmt.Printf("%-24s %12d\n", "TOTAL", total)
			return nil
		},
	}

	cmd.Flags().StringVar(&dbPath, "db", "", "Source SQLite database path (default ghfetcher.db)")
	cmd.Flags().StringVarP(&format, "format", "f", "parquet", "Output format: parquet or jsonl")
	cmd.Flags().StringVarP(&outDir, "out", "o", "", "Target export directory")
	return cmd
}
