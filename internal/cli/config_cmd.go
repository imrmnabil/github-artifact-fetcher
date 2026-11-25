package cli

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/charmbracelet/huh"
	"github.com/spf13/cobra"

	"ghfetcher/internal/config"
	"ghfetcher/internal/github"
	"ghfetcher/internal/tokenpool"
)

func newConfigCmd(gf *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Configure GitHub API tokens, safety reserves, and worker defaults",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, path, err := config.Load(gf.configPath)
			if err != nil {
				return err
			}
			if !isTerminal(os.Stdin) || !isTerminal(os.Stdout) {
				printConfigSummary(cfg, path)
				return nil
			}
			return runInteractiveConfigMenu(cmd.Context(), cfg, path)
		},
	}

	// ghfetcher config add-token [token]
	var label string
	addCmd := &cobra.Command{
		Use:   "add-token [token]",
		Short: "Add a GitHub Personal Access Token to the multi-token pool",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, path, err := config.Load(gf.configPath)
			if err != nil {
				return err
			}
			var tok string
			if len(args) == 1 {
				tok = strings.TrimSpace(args[0])
			} else if isTerminal(os.Stdin) {
				err := huh.NewInput().
					Title("Enter GitHub Personal Access Token (ghp_... / github_pat_...)").
					EchoMode(huh.EchoModePassword).
					Value(&tok).
					Run()
				if err != nil {
					return err
				}
			} else {
				sc := bufio.NewScanner(os.Stdin)
				if sc.Scan() {
					tok = strings.TrimSpace(sc.Text())
				}
			}
			if err := cfg.AddToken(label, tok, "manual"); err != nil {
				return err
			}
			if err := cfg.Save(path); err != nil {
				return err
			}
			fmt.Printf("Saved token %s to %s (mode 0600, %d total configured)\n", config.MaskToken(tok), path, len(cfg.Tokens))
			return nil
		},
	}
	addCmd.Flags().StringVar(&label, "label", "", "Friendly label for this token")

	// ghfetcher config import-gh
	var ghLabel string
	importCmd := &cobra.Command{
		Use:   "import-gh",
		Short: "Import active token from GitHub CLI (`gh auth token`)",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, path, err := config.Load(gf.configPath)
			if err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), 10*time.Second)
			defer cancel()
			tok, err := config.ImportFromGHCLI(ctx)
			if err != nil {
				return err
			}
			if ghLabel == "" {
				ghLabel = "gh-cli"
			}
			if err := cfg.AddToken(ghLabel, tok, "gh-cli"); err != nil {
				return err
			}
			if err := cfg.Save(path); err != nil {
				return err
			}
			fmt.Printf("Imported GitHub CLI token (%s) into %s\n", config.MaskToken(tok), path)
			return nil
		},
	}
	importCmd.Flags().StringVar(&ghLabel, "label", "gh-cli", "Label for imported token")

	// ghfetcher config list
	var checkLive bool
	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List configured & environment tokens and defaults",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, path, err := config.Load(gf.configPath)
			if err != nil {
				return err
			}
			printConfigSummary(cfg, path)
			if checkLive {
				probeTokensLive(cmd.Context(), cfg)
			}
			return nil
		},
	}
	listCmd.Flags().BoolVar(&checkLive, "check", false, "Query live REST & GraphQL rate-limit balances for each token")

	// ghfetcher config remove-token <id-or-label>
	rmCmd := &cobra.Command{
		Use:   "remove-token <label-or-index>",
		Short: "Remove a token from config.toml by label or 1-based index",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, path, err := config.Load(gf.configPath)
			if err != nil {
				return err
			}
			if !cfg.RemoveToken(args[0]) {
				return fmt.Errorf("no token matching %q found in %s", args[0], path)
			}
			if err := cfg.Save(path); err != nil {
				return err
			}
			fmt.Printf("Removed token %q from %s\n", args[0], path)
			return nil
		},
	}

	// ghfetcher config set <key> <value>
	setCmd := &cobra.Command{
		Use:   "set <key> <value>",
		Short: "Update default settings (reserve, concurrency, git-concurrency, api-concurrency, db, keep-git)",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, path, err := config.Load(gf.configPath)
			if err != nil {
				return err
			}
			key, val := strings.ToLower(strings.TrimSpace(args[0])), strings.TrimSpace(args[1])
			switch key {
			case "reserve", "safety_reserve", "safety-reserve":
				n, err := strconv.Atoi(val)
				if err != nil || n < 0 {
					return errors.New("reserve must be a non-negative integer")
				}
				cfg.Defaults.SafetyReserve = n
			case "concurrency", "repo_concurrency", "repo-concurrency":
				n, err := strconv.Atoi(val)
				if err != nil || n < 1 {
					return errors.New("concurrency must be >= 1")
				}
				cfg.Defaults.RepoConcurrency = n
			case "git-concurrency", "git_concurrency":
				n, err := strconv.Atoi(val)
				if err != nil || n < 1 {
					return errors.New("git-concurrency must be >= 1")
				}
				cfg.Defaults.GitConcurrency = n
			case "api-concurrency", "api_concurrency":
				n, err := strconv.Atoi(val)
				if err != nil || n < 1 {
					return errors.New("api-concurrency must be >= 1")
				}
				cfg.Defaults.APIConcurrency = n
			case "db", "database", "database_path":
				cfg.Defaults.DatabasePath = val
			case "keep-git", "keep_git":
				b, err := strconv.ParseBool(val)
				if err != nil {
					return errors.New("keep-git must be true or false")
				}
				cfg.Defaults.KeepGit = b
			default:
				return fmt.Errorf("unknown config key %q (valid: reserve, concurrency, git-concurrency, api-concurrency, db, keep-git)", key)
			}
			if err := cfg.Save(path); err != nil {
				return err
			}
			fmt.Printf("Updated %s = %s in %s\n", key, val, path)
			return nil
		},
	}

	cmd.AddCommand(addCmd, importCmd, listCmd, rmCmd, setCmd)
	return cmd
}

func printConfigSummary(cfg *config.Config, path string) {
	fmt.Printf("Configuration File : %s\n", path)
	fmt.Printf("Default DB Path    : %s\n", cfg.Defaults.DatabasePath)
	fmt.Printf("Safety Reserve     : %d quota/token\n", cfg.Defaults.SafetyReserve)
	fmt.Printf("Worker Defaults    : repos=%d, git=%d, api=%d (keep-git=%v)\n\n",
		cfg.Defaults.RepoConcurrency, cfg.Defaults.GitConcurrency, cfg.Defaults.APIConcurrency, cfg.Defaults.KeepGit)

	eff := cfg.EffectiveTokens()
	if len(eff) == 0 {
		fmt.Println("Active Token Pool  : (none configured — add with `ghfetcher config add-token` or `ghfetcher config import-gh`)")
		return
	}
	fmt.Printf("Active Token Pool (%d token(s) => %d GQL pts/hr + %d REST req/hr capacity):\n",
		len(eff), len(eff)*(5000-cfg.Defaults.SafetyReserve), len(eff)*(5000-cfg.Defaults.SafetyReserve))
	for i, t := range eff {
		fmt.Printf("  #%d  %-16s  %-16s  (source: %s)\n", i+1, t.Label, config.MaskToken(t.Token), t.Source)
	}
}

func probeTokensLive(ctx context.Context, cfg *config.Config) {
	eff := cfg.EffectiveTokens()
	if len(eff) == 0 {
		return
	}
	fmt.Println("\nLive GitHub Quota Check:")
	for _, entry := range eff {
		p := tokenpool.New([]config.TokenEntry{entry}, tokenpool.Options{SafetyReserve: 0})
		client := github.NewClient(p, nil)
		cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		var rl struct {
			Viewer struct {
				Login string `json:"login"`
			} `json:"viewer"`
		}
		_, gqlErr := client.DoGraphQL(cctx, `query { viewer { login } rateLimit { limit remaining cost resetAt } }`, nil, &rl)
		var restRL map[string]any
		_, restErr := client.DoRESTGet(cctx, "/rate_limit", false, &restRL)
		cancel()

		snap := p.Snapshot()
		if gqlErr != nil && restErr != nil {
			fmt.Printf("  %-16s  ERROR: %v\n", entry.Label, gqlErr)
			continue
		}
		if len(snap) > 0 {
			tk := snap[0]
			fmt.Printf("  %-16s  user=@%-16s  GQL: %4d/%d  REST: %4d/%d\n",
				entry.Label, rl.Viewer.Login,
				tk.GraphQL.Remaining, tk.GraphQL.Limit,
				tk.REST.Remaining, tk.REST.Limit,
			)
		}
	}
}

func runInteractiveConfigMenu(ctx context.Context, cfg *config.Config, path string) error {
	for {
		var action string
		effCount := len(cfg.EffectiveTokens())
		err := huh.NewSelect[string]().
			Title(fmt.Sprintf("ghfetcher Configuration (%d active token(s))", effCount)).
			Description("Config file: " + path).
			Options(
				huh.NewOption("Add Personal Access Token", "add"),
				huh.NewOption("Import Token from GitHub CLI (`gh auth token`)", "import-gh"),
				huh.NewOption("View Active Tokens & Check Live Quotas", "list"),
				huh.NewOption("Tune Safety Reserve & Worker Concurrency", "tune"),
				huh.NewOption("Remove a Token", "remove"),
				huh.NewOption("Save & Exit", "exit"),
			).
			Value(&action).
			Run()
		if err != nil {
			if errors.Is(err, huh.ErrUserAborted) {
				return nil
			}
			return err
		}

		switch action {
		case "add":
			var lbl, tok string
			lbl = fmt.Sprintf("pat-%d", len(cfg.Tokens)+1)
			ferr := huh.NewForm(
				huh.NewGroup(
					huh.NewInput().Title("Token Label").Value(&lbl),
					huh.NewInput().Title("GitHub Token").EchoMode(huh.EchoModePassword).Value(&tok),
				),
			).Run()
			if ferr == nil && strings.TrimSpace(tok) != "" {
				_ = cfg.AddToken(lbl, tok, "manual")
				if serr := cfg.Save(path); serr != nil {
					return serr
				}
				fmt.Printf("✓ Added token %s\n\n", config.MaskToken(tok))
			}

		case "import-gh":
			cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			tok, ierr := config.ImportFromGHCLI(cctx)
			cancel()
			if ierr != nil {
				fmt.Printf("✗ Could not import from gh CLI: %v\n\n", ierr)
			} else {
				_ = cfg.AddToken("gh-cli", tok, "gh-cli")
				_ = cfg.Save(path)
				fmt.Printf("✓ Imported token %s from `gh auth token`\n\n", config.MaskToken(tok))
			}

		case "list":
			fmt.Println()
			printConfigSummary(cfg, path)
			probeTokensLive(ctx, cfg)
			fmt.Println()

		case "tune":
			reserveStr := strconv.Itoa(cfg.Defaults.SafetyReserve)
			concStr := strconv.Itoa(cfg.Defaults.RepoConcurrency)
			dbPath := cfg.Defaults.DatabasePath
			keepGit := cfg.Defaults.KeepGit
			ferr := huh.NewForm(
				huh.NewGroup(
					huh.NewInput().Title("Per-Token Quota Safety Reserve").Value(&reserveStr),
					huh.NewInput().Title("Parallel Repository Workers").Value(&concStr),
					huh.NewInput().Title("Default SQLite Output Path").Value(&dbPath),
					huh.NewConfirm().Title("Keep Bare Git Clones Cached on Disk?").Value(&keepGit),
				),
			).Run()
			if ferr == nil {
				if n, nerr := strconv.Atoi(reserveStr); nerr == nil && n >= 0 {
					cfg.Defaults.SafetyReserve = n
				}
				if n, nerr := strconv.Atoi(concStr); nerr == nil && n >= 1 {
					cfg.Defaults.RepoConcurrency = n
				}
				if strings.TrimSpace(dbPath) != "" {
					cfg.Defaults.DatabasePath = strings.TrimSpace(dbPath)
				}
				cfg.Defaults.KeepGit = keepGit
				if serr := cfg.Save(path); serr != nil {
					return serr
				}
				fmt.Println("✓ Saved defaults")
			}

		case "remove":
			if len(cfg.Tokens) == 0 {
				fmt.Println("No persisted tokens in config.toml to remove.")
				continue
			}
			opts := make([]huh.Option[string], 0, len(cfg.Tokens))
			for _, t := range cfg.Tokens {
				opts = append(opts, huh.NewOption(fmt.Sprintf("%s (%s)", t.Label, config.MaskToken(t.Token)), t.Label))
			}
			var target string
			if rerr := huh.NewSelect[string]().Title("Select token to remove").Options(opts...).Value(&target).Run(); rerr == nil {
				cfg.RemoveToken(target)
				_ = cfg.Save(path)
				fmt.Printf("✓ Removed %s\n\n", target)
			}

		case "exit":
			return cfg.Save(path)
		}
	}
}
