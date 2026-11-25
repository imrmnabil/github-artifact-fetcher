package config

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/pelletier/go-toml/v2"
)

const (
	EnvConfigPath  = "GHFETCHER_CONFIG"
	EnvTokens      = "GHFETCHER_TOKENS"
	EnvGitHubToken = "GITHUB_TOKEN"
)

// TokenEntry represents a configured GitHub Personal Access Token.
type TokenEntry struct {
	Label   string    `toml:"label" json:"label"`
	Token   string    `toml:"token" json:"-"`
	Source  string    `toml:"source" json:"source"`
	AddedAt time.Time `toml:"added_at" json:"added_at"`
}

// DefaultsConfig stores default runtime parameters for ghfetcher.
type DefaultsConfig struct {
	DatabasePath    string `toml:"database_path" json:"database_path"`
	SafetyReserve   int    `toml:"safety_reserve" json:"safety_reserve"`
	RepoConcurrency int    `toml:"repo_concurrency" json:"repo_concurrency"`
	GitConcurrency  int    `toml:"git_concurrency" json:"git_concurrency"`
	APIConcurrency  int    `toml:"api_concurrency" json:"api_concurrency"`
	MaxGQLPerToken  int    `toml:"max_gql_per_token" json:"max_gql_per_token"`
	MaxRESTPerToken int    `toml:"max_rest_per_token" json:"max_rest_per_token"`
	KeepGit         bool   `toml:"keep_git" json:"keep_git"`
}

// Config is the root configuration persisted to config.toml.
type Config struct {
	Defaults DefaultsConfig `toml:"defaults" json:"defaults"`
	Tokens   []TokenEntry   `toml:"tokens" json:"tokens"`
}

// Default returns a Config initialized with sensible research defaults.
func Default() *Config {
	return &Config{
		Defaults: DefaultsConfig{
			DatabasePath:    "ghfetcher.db",
			SafetyReserve:   100,
			RepoConcurrency: 3,
			GitConcurrency:  2,
			APIConcurrency:  6,
			MaxGQLPerToken:  3,
			MaxRESTPerToken: 8,
			KeepGit:         false,
		},
		Tokens: []TokenEntry{},
	}
}

// ResolvePath determines the configuration file location.
func ResolvePath(explicitPath string) (string, error) {
	if strings.TrimSpace(explicitPath) != "" {
		return explicitPath, nil
	}
	if envPath := strings.TrimSpace(os.Getenv(EnvConfigPath)); envPath != "" {
		return envPath, nil
	}
	cfgDir, err := os.UserConfigDir()
	if err != nil || cfgDir == "" {
		home, herr := os.UserHomeDir()
		if herr != nil {
			return filepath.Join(".ghfetcher", "config.toml"), nil
		}
		cfgDir = filepath.Join(home, ".config")
	}
	return filepath.Join(cfgDir, "ghfetcher", "config.toml"), nil
}

// Load reads config from disk (or returns Default if file does not yet exist)
// and normalizes any missing default fields.
func Load(explicitPath string) (*Config, string, error) {
	path, err := ResolvePath(explicitPath)
	if err != nil {
		return nil, "", err
	}

	cfg := Default()
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return cfg, path, nil
		}
		return nil, path, fmt.Errorf("read config %s: %w", path, err)
	}

	if len(bytes.TrimSpace(data)) > 0 {
		if err := toml.Unmarshal(data, cfg); err != nil {
			return nil, path, fmt.Errorf("parse config %s: %w", path, err)
		}
	}
	cfg.applyDefaults()
	return cfg, path, nil
}

func (c *Config) applyDefaults() {
	def := Default().Defaults
	if c.Defaults.DatabasePath == "" {
		c.Defaults.DatabasePath = def.DatabasePath
	}
	if c.Defaults.SafetyReserve < 0 {
		c.Defaults.SafetyReserve = def.SafetyReserve
	}
	if c.Defaults.RepoConcurrency <= 0 {
		c.Defaults.RepoConcurrency = def.RepoConcurrency
	}
	if c.Defaults.GitConcurrency <= 0 {
		c.Defaults.GitConcurrency = def.GitConcurrency
	}
	if c.Defaults.APIConcurrency <= 0 {
		c.Defaults.APIConcurrency = def.APIConcurrency
	}
	if c.Defaults.MaxGQLPerToken <= 0 {
		c.Defaults.MaxGQLPerToken = def.MaxGQLPerToken
	}
	if c.Defaults.MaxRESTPerToken <= 0 {
		c.Defaults.MaxRESTPerToken = def.MaxRESTPerToken
	}
}

// Save writes Config atomically with strict 0600 permissions.
func (c *Config) Save(path string) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create config directory %s: %w", dir, err)
	}

	data, err := toml.Marshal(c)
	if err != nil {
		return fmt.Errorf("encode config toml: %w", err)
	}

	tmp := fmt.Sprintf("%s.tmp.%d", path, os.Getpid())
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("create temp config %s: %w", tmp, err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("write temp config: %w", err)
	}
	if err := f.Chmod(0o600); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("chmod temp config: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("close temp config: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("atomic rename config: %w", err)
	}
	return nil
}

// AddToken adds or updates a token in Config, deduplicating by raw token value.
func (c *Config) AddToken(label, token, source string) error {
	tok := strings.TrimSpace(token)
	if tok == "" {
		return errors.New("token cannot be empty")
	}
	lbl := strings.TrimSpace(label)
	if lbl == "" {
		lbl = fmt.Sprintf("token-%d", len(c.Tokens)+1)
	}
	if source == "" {
		source = "manual"
	}

	for i := range c.Tokens {
		if c.Tokens[i].Token == tok {
			c.Tokens[i].Label = lbl
			c.Tokens[i].Source = source
			return nil
		}
	}
	c.Tokens = append(c.Tokens, TokenEntry{
		Label:   lbl,
		Token:   tok,
		Source:  source,
		AddedAt: time.Now().UTC(),
	})
	return nil
}

// RemoveToken removes a token by label, 1-based index string, or masked/raw prefix.
func (c *Config) RemoveToken(identifier string) bool {
	id := strings.TrimSpace(identifier)
	if id == "" {
		return false
	}
	kept := make([]TokenEntry, 0, len(c.Tokens))
	removed := false
	for idx, entry := range c.Tokens {
		indexStr := fmt.Sprintf("%d", idx+1)
		if !removed && (entry.Label == id || indexStr == id || entry.Token == id || MaskToken(entry.Token) == id) {
			removed = true
			continue
		}
		kept = append(kept, entry)
	}
	c.Tokens = kept
	return removed
}

// EffectiveTokens merges environment overrides (GHFETCHER_TOKENS, GITHUB_TOKEN)
// with persisted config tokens without duplicates.
func (c *Config) EffectiveTokens() []TokenEntry {
	seen := make(map[string]struct{})
	var out []TokenEntry

	add := func(entry TokenEntry) {
		tok := strings.TrimSpace(entry.Token)
		if tok == "" {
			return
		}
		if _, exists := seen[tok]; exists {
			return
		}
		seen[tok] = struct{}{}
		entry.Token = tok
		out = append(out, entry)
	}

	if rawEnv := strings.TrimSpace(os.Getenv(EnvTokens)); rawEnv != "" {
		parts := strings.FieldsFunc(rawEnv, func(r rune) bool {
			return r == ',' || r == ';' || r == '\n' || r == ' '
		})
		for i, p := range parts {
			add(TokenEntry{
				Label:   fmt.Sprintf("env-%d", i+1),
				Token:   p,
				Source:  "env:" + EnvTokens,
				AddedAt: time.Now().UTC(),
			})
		}
	}

	for _, entry := range c.Tokens {
		add(entry)
	}

	// Fallback to GITHUB_TOKEN if no explicit tokens were configured or alongside pool
	if ghEnv := strings.TrimSpace(os.Getenv(EnvGitHubToken)); ghEnv != "" {
		add(TokenEntry{
			Label:   "env-github",
			Token:   ghEnv,
			Source:  "env:" + EnvGitHubToken,
			AddedAt: time.Now().UTC(),
		})
	}

	return out
}

// ImportFromGHCLI queries `gh auth token` to import an existing GitHub CLI token.
func ImportFromGHCLI(ctx context.Context) (string, error) {
	cmd := exec.CommandContext(ctx, "gh", "auth", "token")
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("run `gh auth token`: %w", err)
	}
	tok := strings.TrimSpace(string(out))
	if tok == "" {
		return "", errors.New("`gh auth token` returned an empty token")
	}
	return tok, nil
}

// MaskToken masks a token safely for terminal and log display.
func MaskToken(token string) string {
	t := strings.TrimSpace(token)
	if len(t) <= 8 {
		if len(t) <= 2 {
			return "**"
		}
		return t[:2] + "****"
	}
	prefixLen := 4
	if idx := strings.IndexByte(t, '_'); idx > 0 && idx <= 6 {
		prefixLen = idx + 1
	}
	return fmt.Sprintf("%s..%s", t[:prefixLen], t[len(t)-4:])
}
