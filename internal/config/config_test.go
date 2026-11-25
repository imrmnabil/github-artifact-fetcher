package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestConfigSaveLoadAndPermissions(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "ghfetcher", "config.toml")

	cfg := Default()
	if err := cfg.AddToken("primary", "ghp_1234567890abcdef", "manual"); err != nil {
		t.Fatalf("AddToken failed: %v", err)
	}
	if err := cfg.AddToken("lab-pat", "github_pat_abcdef1234567890", "manual"); err != nil {
		t.Fatalf("AddToken second failed: %v", err)
	}
	cfg.Defaults.SafetyReserve = 150

	if err := cfg.Save(cfgPath); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	info, err := os.Stat(cfgPath)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("expected 0600 permissions, got %o", perm)
	}

	loaded, gotPath, err := Load(cfgPath)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if gotPath != cfgPath {
		t.Fatalf("expected path %s, got %s", cfgPath, gotPath)
	}
	if len(loaded.Tokens) != 2 {
		t.Fatalf("expected 2 tokens, got %d", len(loaded.Tokens))
	}
	if loaded.Defaults.SafetyReserve != 150 {
		t.Fatalf("expected SafetyReserve 150, got %d", loaded.Defaults.SafetyReserve)
	}

	// Deduplication test
	_ = loaded.AddToken("renamed-primary", "ghp_1234567890abcdef", "manual")
	if len(loaded.Tokens) != 2 || loaded.Tokens[0].Label != "renamed-primary" {
		t.Fatalf("expected token update in place, got %+v", loaded.Tokens)
	}

	// Masking test
	if got := MaskToken("ghp_1234567890abcdef"); got != "ghp_..cdef" {
		t.Fatalf("unexpected mask: %s", got)
	}

	// Remove test
	if !loaded.RemoveToken("renamed-primary") {
		t.Fatalf("expected RemoveToken to return true")
	}
	if len(loaded.Tokens) != 1 {
		t.Fatalf("expected 1 token left, got %d", len(loaded.Tokens))
	}
}

func TestEffectiveTokensEnvMerge(t *testing.T) {
	t.Setenv(EnvTokens, "ghp_envone123456,ghp_shared123456")
	t.Setenv(EnvGitHubToken, "ghp_ghcli1234567")

	cfg := Default()
	_ = cfg.AddToken("cfg-shared", "ghp_shared123456", "manual")
	_ = cfg.AddToken("cfg-unique", "ghp_unique123456", "manual")

	eff := cfg.EffectiveTokens()
	if len(eff) != 4 {
		t.Fatalf("expected 4 deduplicated effective tokens, got %d", len(eff))
	}
}
