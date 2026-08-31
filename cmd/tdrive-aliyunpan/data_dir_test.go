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

func TestMigrateLegacyDataDirMovesDownloadedBinary(t *testing.T) {
	root := t.TempDir()
	legacy := filepath.Join(root, "plugins", "aliyunpan-data")
	destination := filepath.Join(root, "plugin-data", "aliyunpan")
	legacyBinary := filepath.Join(legacy, "bin", "aliyunpan")
	if err := os.MkdirAll(filepath.Dir(legacyBinary), 0o750); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	const content = "old aliyunpan binary"
	if err := os.WriteFile(legacyBinary, []byte(content), 0o750); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if err := migrateLegacyDataDir(destination, legacy); err != nil {
		t.Fatalf("migrateLegacyDataDir: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(destination, "bin", "aliyunpan"))
	if err != nil {
		t.Fatalf("ReadFile migrated binary: %v", err)
	}
	if string(got) != content {
		t.Fatalf("migrated binary = %q, want %q", got, content)
	}
	if _, err := os.Stat(legacy); !os.IsNotExist(err) {
		t.Fatalf("legacy directory still exists: %v", err)
	}
}

func TestMigrateLegacyDataDirDoesNotOverwriteDestination(t *testing.T) {
	root := t.TempDir()
	legacy := filepath.Join(root, "legacy")
	destination := filepath.Join(root, "destination")
	legacyBinary := filepath.Join(legacy, "bin", "aliyunpan")
	destinationBinary := filepath.Join(destination, "bin", "aliyunpan")
	if err := os.MkdirAll(filepath.Dir(legacyBinary), 0o750); err != nil {
		t.Fatalf("MkdirAll legacy: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(destinationBinary), 0o750); err != nil {
		t.Fatalf("MkdirAll destination: %v", err)
	}
	if err := os.WriteFile(legacyBinary, []byte("old"), 0o750); err != nil {
		t.Fatalf("WriteFile legacy: %v", err)
	}
	if err := os.WriteFile(destinationBinary, []byte("new"), 0o750); err != nil {
		t.Fatalf("WriteFile destination: %v", err)
	}

	if err := migrateLegacyDataDir(destination, legacy); err != nil {
		t.Fatalf("migrateLegacyDataDir: %v", err)
	}
	got, err := os.ReadFile(destinationBinary)
	if err != nil {
		t.Fatalf("ReadFile destination binary: %v", err)
	}
	if string(got) != "new" {
		t.Fatalf("destination binary = %q, want new", got)
	}
}
