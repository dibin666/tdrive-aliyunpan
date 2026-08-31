package aliyunpan

import (
	"archive/zip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/dibin/tdrive-aliyunpan/internal/debuglog"
)

// ReleaseVersion is the aliyunpan release this plugin installs and was tested
// against. Pinning it keeps the CLI's output format — which this package
// parses — from changing under a running deployment.
const ReleaseVersion = "v0.4.0"

// downloadTimeout bounds the whole fetch-and-extract. The archive is ~9 MiB.
const downloadTimeout = 10 * time.Minute

// maxArchiveBytes caps both the download and the extracted binary, so a
// redirect to something unexpected cannot fill the data directory.
const maxArchiveBytes = 128 << 20

// assetName maps Go's platform names onto the release's own naming, which
// differs enough (macos, x64) that a format string will not do.
func assetName(goos, goarch string) (string, error) {
	var suffix string
	switch goos {
	case "linux":
		switch goarch {
		case "amd64":
			suffix = "linux-amd64"
		case "386":
			suffix = "linux-386"
		case "arm64":
			suffix = "linux-arm64"
		case "arm":
			suffix = "linux-armv7"
		case "loong64":
			suffix = "linux-loong64"
		}
	case "darwin":
		switch goarch {
		case "amd64":
			suffix = "darwin-macos-amd64"
		case "arm64":
			suffix = "darwin-macos-arm64"
		}
	case "freebsd":
		switch goarch {
		case "amd64":
			suffix = "freebsd-amd64"
		case "386":
			suffix = "freebsd-386"
		}
	case "windows":
		switch goarch {
		case "amd64":
			suffix = "windows-x64"
		case "386":
			suffix = "windows-x86"
		case "arm64":
			suffix = "windows-arm64"
		}
	}
	if suffix == "" {
		return "", fmt.Errorf("aliyunpan 没有发布 %s/%s 的二进制", goos, goarch)
	}
	return fmt.Sprintf("aliyunpan-%s-%s.zip", ReleaseVersion, suffix), nil
}

// DownloadURL is the release asset for the running platform.
func DownloadURL() (string, error) {
	asset, err := assetName(runtime.GOOS, runtime.GOARCH)
	if err != nil {
		return "", err
	}
	return "https://github.com/tickstep/aliyunpan/releases/download/" + ReleaseVersion + "/" + asset, nil
}

// executableName is what the archive calls the binary on this platform.
func executableName() string {
	if runtime.GOOS == "windows" {
		return "aliyunpan.exe"
	}
	return "aliyunpan"
}

// Install fetches the pinned release and extracts just the executable into
// destination, replacing whatever was there.
//
// The tdrive image is distroless — no shell, no package manager — but
// aliyunpan is a statically linked Go binary, so dropping it into the plugin's
// own data directory and exec'ing it is enough.
func Install(ctx context.Context, destination string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	installationSucceeded := false
	archiveBytes := int64(0)
	defer func() {
		// #region DEBUG H2 binary installation outcome
		debuglog.Write("H2", "internal/aliyunpan/binary.go:103", "binary installation outcome", map[string]any{
			"destination":          destination,
			"success":              installationSucceeded,
			"archiveBytes":         archiveBytes,
			"destinationPresent":   filePresent(destination),
		})
		// #endregion
	}()
	url, err := DownloadURL()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o750); err != nil {
		return fmt.Errorf("创建二进制目录: %w", err)
	}

	fetchCtx, cancel := context.WithTimeout(ctx, downloadTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(fetchCtx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return fmt.Errorf("下载 aliyunpan: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("下载 aliyunpan: %s", response.Status)
	}

	// zip.NewReader needs random access, so the archive is spooled to a
	// temporary file next to the destination rather than held in memory.
	archive, err := os.CreateTemp(filepath.Dir(destination), ".aliyunpan-*.zip")
	if err != nil {
		return err
	}
	defer func() {
		archive.Close()
		_ = os.Remove(archive.Name())
	}()
	size, err := io.Copy(archive, io.LimitReader(response.Body, maxArchiveBytes+1))
	archiveBytes = size
	if err != nil {
		return fmt.Errorf("下载 aliyunpan: %w", err)
	}
	if size > maxArchiveBytes {
		return errors.New("下载 aliyunpan: 压缩包超出大小上限")
	}

	reader, err := zip.NewReader(archive, size)
	if err != nil {
		return fmt.Errorf("解压 aliyunpan: %w", err)
	}
	wanted := executableName()
	for _, entry := range reader.File {
		if path.Base(entry.Name) != wanted || entry.FileInfo().IsDir() {
			continue
		}
		err := extractTo(entry, destination)
		if err == nil {
			installationSucceeded = true
		}
		return err
	}
	return fmt.Errorf("压缩包里没有找到 %s", wanted)
}

func extractTo(entry *zip.File, destination string) error {
	source, err := entry.Open()
	if err != nil {
		return fmt.Errorf("解压 aliyunpan: %w", err)
	}
	defer source.Close()

	// Written beside the destination and renamed, so a failed extraction never
	// leaves a half-written binary that the next run would try to exec.
	temporary, err := os.CreateTemp(filepath.Dir(destination), ".aliyunpan-bin-*")
	if err != nil {
		return err
	}
	defer os.Remove(temporary.Name())
	written, err := io.Copy(temporary, io.LimitReader(source, maxArchiveBytes+1))
	if err != nil {
		temporary.Close()
		return fmt.Errorf("解压 aliyunpan: %w", err)
	}
	if written > maxArchiveBytes {
		temporary.Close()
		return errors.New("解压 aliyunpan: 可执行文件超出大小上限")
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Chmod(temporary.Name(), 0o750); err != nil {
		return err
	}
	if err := replaceBinary(temporary.Name(), destination); err != nil {
		return fmt.Errorf("安装 aliyunpan: %w", err)
	}
	return nil
}

// replaceBinary is an atomic rename on Unix and a recoverable two-rename
// replacement on Windows, where os.Rename refuses to overwrite an existing
// file. The old executable is kept until the new one is in place; if the
// second rename fails it is restored.
func replaceBinary(source, destination string) error {
	if err := os.Rename(source, destination); err == nil {
		return nil
	}

	old, err := os.CreateTemp(filepath.Dir(destination), ".aliyunpan-old-*")
	if err != nil {
		return err
	}
	backup := old.Name()
	if err := old.Close(); err != nil {
		_ = os.Remove(backup)
		return err
	}
	if err := os.Remove(backup); err != nil {
		return err
	}
	if err := os.Rename(destination, backup); err != nil {
		if os.IsNotExist(err) {
			return os.Rename(source, destination)
		}
		return err
	}
	if err := os.Rename(source, destination); err != nil {
		_ = os.Rename(backup, destination)
		return err
	}
	_ = os.Remove(backup)
	return nil
}

// BinaryState is what the 账号 tab shows about the CLI itself.
type BinaryState struct {
	Path      string `json:"path"`
	Managed   bool   `json:"managed"`
	Installed bool   `json:"installed"`
	Version   string `json:"version,omitempty"`
	Error     string `json:"error,omitempty"`
}

// Probe reports whether the configured binary exists and what it says its
// version is. A binary that exists but will not run is more useful reported
// than hidden, so the error is carried rather than returned.
func (c *CLI) Probe(ctx context.Context) BinaryState {
	state := BinaryState{Path: c.binary, Managed: c.managed}
	info, err := os.Stat(c.binary)
	// #region DEBUG H2 binary filesystem probe
	debuglog.Write("H2", "internal/aliyunpan/binary.go:207", "binary filesystem probe completed", map[string]any{
		"binaryPath":  c.binary,
		"managed":     c.managed,
		"statOK":      err == nil,
		"notFound":    os.IsNotExist(err),
		"isDirectory": err == nil && info.IsDir(),
		"size":        binarySize(info),
		"mode":        binaryMode(info),
	})
	// #endregion
	if err != nil || info.IsDir() {
		return state
	}
	state.Installed = true
	output, err := c.runCommand(ctx, runOptions{timeout: 30 * time.Second}, "--version")
	if err != nil {
		state.Error = err.Error()
		// #region DEBUG H2 binary execution probe
		debuglog.Write("H2", "internal/aliyunpan/binary.go:222", "binary exists but version probe failed", map[string]any{
			"binaryPath":  c.binary,
			"managed":     c.managed,
			"installed":   state.Installed,
			"versionRead": false,
		})
		// #endregion
		return state
	}
	state.Version = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(output), "aliyunpan version"))
	// #region DEBUG H2 binary execution probe succeeded
	debuglog.Write("H2", "internal/aliyunpan/binary.go:233", "binary version probe succeeded", map[string]any{
		"binaryPath":  c.binary,
		"managed":     c.managed,
		"installed":   state.Installed,
		"versionRead": state.Version != "",
	})
	// #endregion
	return state
}

func binarySize(info os.FileInfo) int64 {
	if info == nil {
		return -1
	}
	return info.Size()
}

func binaryMode(info os.FileInfo) string {
	if info == nil {
		return ""
	}
	return info.Mode().String()
}

func filePresent(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular()
}
