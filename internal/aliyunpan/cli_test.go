//go:build legacycli

package aliyunpan

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	cliHelperEnv         = "ALIYUNPAN_CLI_TEST_HELPER"
	cliDownloadMarkerEnv = "ALIYUNPAN_CLI_TEST_DOWNLOAD_MARKER"
	cliReleaseMarkerEnv  = "ALIYUNPAN_CLI_TEST_RELEASE_MARKER"
	cliConfigMarkerEnv   = "ALIYUNPAN_CLI_TEST_CONFIG_MARKER"
)

func TestMain(testMain *testing.M) {
	if os.Getenv(cliHelperEnv) == "1" {
		runCLIHelperProcess()
		return
	}
	os.Exit(testMain.Run())
}

// runCLIHelperProcess makes the test binary act as a tiny cross-platform fake
// aliyunpan executable. The download branch stays alive until the parent
// releases it, which lets the test prove that a short command is not queued
// behind a long transfer.
func runCLIHelperProcess() {
	hasDownloadArgument := false
	for _, argument := range os.Args[1:] {
		if argument == "download" {
			hasDownloadArgument = true
			break
		}
	}
	if !hasDownloadArgument {
		_, _ = fmt.Fprintln(os.Stdout, "short command")
		os.Exit(0)
	}

	if marker := os.Getenv(cliDownloadMarkerEnv); marker != "" {
		_ = os.WriteFile(marker, []byte("started"), 0o600)
	}
	if marker := os.Getenv(cliConfigMarkerEnv); marker != "" {
		_ = os.WriteFile(marker, []byte(os.Getenv("ALIYUNPAN_CONFIG_DIR")), 0o600)
	}

	releaseMarker := os.Getenv(cliReleaseMarkerEnv)
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		if _, err := os.Stat(releaseMarker); err == nil {
			os.Exit(0)
		}
		select {
		case <-deadline.C:
			os.Exit(1)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestShortCommandRunsWhileDownloadIsActive(t *testing.T) {
	dataDir := t.TempDir()
	canonicalConfigDir := filepath.Join(dataDir, "config")
	if err := os.MkdirAll(canonicalConfigDir, 0o750); err != nil {
		t.Fatalf("MkdirAll config: %v", err)
	}
	configContents := []byte(`{"activeUID":"test-user"}`)
	if err := os.WriteFile(filepath.Join(canonicalConfigDir, "aliyunpan_config.json"), configContents, 0o600); err != nil {
		t.Fatalf("WriteFile config: %v", err)
	}

	downloadMarker := filepath.Join(dataDir, "download.started")
	releaseMarker := filepath.Join(dataDir, "download.release")
	configMarker := filepath.Join(dataDir, "download.config-dir")
	t.Setenv(cliHelperEnv, "1")
	t.Setenv(cliDownloadMarkerEnv, downloadMarker)
	t.Setenv(cliReleaseMarkerEnv, releaseMarker)
	t.Setenv(cliConfigMarkerEnv, configMarker)

	cli := New(dataDir, os.Args[0])
	transferContext, cancelTransfer := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancelTransfer()
	downloadDone := make(chan error, 1)
	go func() {
		_, err := cli.runDownload(transferContext, runOptions{timeout: 7 * time.Second}, "download")
		downloadDone <- err
	}()

	waitForTestFile(t, downloadMarker)
	privateConfig, err := os.ReadFile(configMarker)
	if err != nil {
		t.Fatalf("ReadFile private config marker: %v", err)
	}
	if strings.TrimSpace(string(privateConfig)) == canonicalConfigDir {
		t.Fatalf("download used the canonical config directory %q", canonicalConfigDir)
	}
	privateConfigPath := filepath.Join(string(privateConfig), "aliyunpan_config.json")
	if copiedContents, err := os.ReadFile(privateConfigPath); err != nil {
		t.Fatalf("ReadFile copied config: %v", err)
	} else if string(copiedContents) != string(configContents) {
		t.Fatalf("copied config = %q, want %q", copiedContents, configContents)
	}

	shortDone := make(chan error, 1)
	go func() {
		_, err := cli.runCommand(context.Background(), runOptions{timeout: 5 * time.Second}, "who")
		shortDone <- err
	}()
	select {
	case err := <-shortDone:
		if err != nil {
			t.Fatalf("short command failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("short command waited for the active download")
	}

	if err := os.WriteFile(releaseMarker, []byte("release"), 0o600); err != nil {
		t.Fatalf("WriteFile release marker: %v", err)
	}
	select {
	case err := <-downloadDone:
		if err != nil {
			t.Fatalf("download command failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("download did not finish after release")
	}
}

func waitForTestFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
}
