package aliyunpan

import (
	"os"
	"path/filepath"
	"testing"
)

// The plugin is published for every platform tdrive itself runs on, so the CLI
// has to be fetchable on all of them. The names below are the assets that
// actually exist on the pinned release, and upstream does not spell them the
// way Go does — "windows-x64" rather than "windows-amd64", "darwin-macos-".
func TestAssetNameMatchesTheUpstreamRelease(t *testing.T) {
	for _, test := range []struct {
		goos, goarch, want string
	}{
		{"linux", "amd64", "aliyunpan-v0.4.0-linux-amd64.zip"},
		{"linux", "arm64", "aliyunpan-v0.4.0-linux-arm64.zip"},
		{"windows", "amd64", "aliyunpan-v0.4.0-windows-x64.zip"},
		{"windows", "arm64", "aliyunpan-v0.4.0-windows-arm64.zip"},
	} {
		got, err := assetName(test.goos, test.goarch)
		if err != nil {
			t.Errorf("assetName(%s, %s): %v", test.goos, test.goarch, err)
			continue
		}
		if got != test.want {
			t.Errorf("assetName(%s, %s) = %s, want %s", test.goos, test.goarch, got, test.want)
		}
	}
}

// An unpublished platform has to say so rather than build a URL that 404s.
func TestAssetNameRejectsUnpublishedPlatforms(t *testing.T) {
	if _, err := assetName("plan9", "amd64"); err == nil {
		t.Error("assetName accepted plan9/amd64")
	}
}

func TestReplaceBinaryReplacesDestination(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, ".new")
	destination := filepath.Join(dir, "aliyunpan")
	if err := os.WriteFile(source, []byte("new"), 0o750); err != nil {
		t.Fatalf("WriteFile source: %v", err)
	}
	if err := os.WriteFile(destination, []byte("old"), 0o750); err != nil {
		t.Fatalf("WriteFile destination: %v", err)
	}

	if err := replaceBinary(source, destination); err != nil {
		t.Fatalf("replaceBinary: %v", err)
	}
	got, err := os.ReadFile(destination)
	if err != nil {
		t.Fatalf("ReadFile destination: %v", err)
	}
	if string(got) != "new" {
		t.Fatalf("destination = %q, want new", got)
	}
}
