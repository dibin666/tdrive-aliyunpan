package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDataDirUsesHostProvidedPath(t *testing.T) {
	configured := filepath.Join(t.TempDir(), "persistent", "aliyunpan")
	t.Setenv(pluginDataDirEnv, configured)

	if got := dataDir(); got != configured {
		t.Fatalf("dataDir = %q, want %q", got, configured)
	}
}

func TestMigrateLegacyDataDirMovesLegacyCredentials(t *testing.T) {
	root := t.TempDir()
	legacy := filepath.Join(root, "plugins", "aliyunpan-data")
	destination := filepath.Join(root, "plugin-data", "aliyunpan")
	legacyConfig := filepath.Join(legacy, "config", "aliyunpan_config.json")
	if err := os.MkdirAll(filepath.Dir(legacyConfig), 0o750); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	const content = `{"activeUID":"user-1"}`
	if err := os.WriteFile(legacyConfig, []byte(content), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if err := migrateLegacyDataDir(destination, legacy); err != nil {
		t.Fatalf("migrateLegacyDataDir: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(destination, "config", "aliyunpan_config.json"))
	if err != nil {
		t.Fatalf("ReadFile migrated credentials: %v", err)
	}
	if string(got) != content {
		t.Fatalf("migrated credentials = %q, want %q", got, content)
	}
	if _, err := os.Stat(legacy); !os.IsNotExist(err) {
		t.Fatalf("legacy directory still exists: %v", err)
	}
}

func TestMigrateLegacyDataDirDoesNotOverwriteDestination(t *testing.T) {
	root := t.TempDir()
	legacy := filepath.Join(root, "legacy")
	destination := filepath.Join(root, "destination")
	legacyConfig := filepath.Join(legacy, "config", "aliyunpan_config.json")
	destinationConfig := filepath.Join(destination, "config", "aliyunpan_config.json")
	if err := os.MkdirAll(filepath.Dir(legacyConfig), 0o750); err != nil {
		t.Fatalf("MkdirAll legacy: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(destinationConfig), 0o750); err != nil {
		t.Fatalf("MkdirAll destination: %v", err)
	}
	if err := os.WriteFile(legacyConfig, []byte("old"), 0o600); err != nil {
		t.Fatalf("WriteFile legacy: %v", err)
	}
	if err := os.WriteFile(destinationConfig, []byte("new"), 0o600); err != nil {
		t.Fatalf("WriteFile destination: %v", err)
	}

	if err := migrateLegacyDataDir(destination, legacy); err != nil {
		t.Fatalf("migrateLegacyDataDir: %v", err)
	}
	got, err := os.ReadFile(destinationConfig)
	if err != nil {
		t.Fatalf("ReadFile destination credentials: %v", err)
	}
	if string(got) != "new" {
		t.Fatalf("destination credentials = %q, want new", got)
	}
}
