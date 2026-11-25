package cli

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"ghfetcher/internal/config"
	"ghfetcher/internal/export"
	"ghfetcher/internal/pipeline"
	"ghfetcher/internal/store"
	"ghfetcher/internal/tui"
)

func newAttachCmd(gf *globalFlags) *cobra.Command {
	var dbPath string

	cmd := &cobra.Command{
		Use:   "attach",
		Short: "Attach the interactive htop TUI to a running headless/background fetch job",
		RunE: func(cmd *cobra.Command, args []string) error {
			if dbPath == "" {
				cfg, _, err := config.Load(gf.configPath)
				if err == nil {
					dbPath = cfg.Defaults.DatabasePath
				} else {
					dbPath = "ghfetcher.db"
				}
			}
			st, err := store.Open(dbPath)
			if err != nil {
				return err
			}
			defer st.Close()

			return tui.RunDashboard(&tui.RemoteAttachController{Store: st}, nil)
		},
	}
	cmd.Flags().StringVar(&dbPath, "db", "", "SQLite database path of the running job (default ghfetcher.db)")
	return cmd
}

func newStatusCmd(gf *globalFlags) *cobra.Command {
	var dbPath string

	cmd := &cobra.Command{
		Use:   "status",
		Short: "Print dataset row counts and active/last run telemetry for a SQLite study DB",
		RunE: func(cmd *cobra.Command, args []string) error {
			if dbPath == "" {
				cfg, _, err := config.Load(gf.configPath)
				if err == nil {
					dbPath = cfg.Defaults.DatabasePath
				} else {
					dbPath = "ghfetcher.db"
				}
			}
			st, err := store.Open(dbPath)
			if err != nil {
				return err
			}
			defer st.Close()

			fmt.Printf("Database: %s (%.2f MB)\n\n", st.Path(), float64(st.FileSizeBytes())/(1024*1024))

			pid, updatedAt, payload, _ := st.ReadTelemetry(cmd.Context())
			if payload != "" {
				var snap pipeline.TelemetrySnapshot
				if json.Unmarshal([]byte(payload), &snap) == nil {
					state := "ACTIVE"
					if snap.Finished {
						state = "FINISHED"
					} else if time.Since(updatedAt) > 10*time.Second {
						state = "STALE / STOPPED (Checkpointed)"
					} else if snap.Paused {
						state = "PAUSED"
					}
					fmt.Printf("Run Telemetry : %s (PID %d, updated %s ago, ETA %ds, Saved ~%d API calls)\n\n",
						state, pid, time.Since(updatedAt).Round(time.Second), snap.TotalETASeconds, snap.TotalAPISaved)
				}
			}

			fmt.Printf("%-24s %14s\n", "TABLE", "ROWS")
			fmt.Println(strings.Repeat("─", 40))
			for _, tbl := range export.ExportableTables {
				var cnt int64
				_ = st.DB().QueryRowContext(cmd.Context(), "SELECT COUNT(*) FROM "+tbl).Scan(&cnt)
				fmt.Printf("%-24s %14d\n", tbl, cnt)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&dbPath, "db", "", "SQLite database path to inspect (default ghfetcher.db)")
	return cmd
}
