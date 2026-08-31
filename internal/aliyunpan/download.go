package aliyunpan

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// DownloadSuffix is what aliyunpan appends to a file it is still writing.
// Watching that file is how this package reports progress: the CLI's own
// progress bar is a terminal animation, not something a parser can consume.
const DownloadSuffix = ".aliyunpan-downloading"

// progressInterval is how often the partial file is stat'ed. It matches the
// cadence the sync page refreshes at, so a faster poll would only produce
// numbers nobody reads.
const progressInterval = 700 * time.Millisecond

// DownloadRequest is one file to stage.
type DownloadRequest struct {
	CloudPath string
	StageDir  string
	// DriveID is the account-specific backup/resource drive ID. Empty keeps
	// aliyunpan's active-drive behavior for callers that do not select a drive.
	DriveID string
	// SliceParallel is aliyunpan's -sp, the number of connections used for a
	// single file. Aliyun rejects more than 3.
	SliceParallel int
	Retry         int
	// Timeout bounds the whole transfer.
	Timeout time.Duration
}

// StagedPath is where aliyunpan will put a cloud file. The CLI joins the save
// directory with the file's full cloud path rather than just its name, so the
// staging tree mirrors the cloud tree.
func StagedPath(stageDir, cloudPath string) string {
	clean := path.Clean("/" + strings.TrimSpace(cloudPath))
	return filepath.Join(stageDir, filepath.FromSlash(strings.TrimPrefix(clean, "/")))
}

// Download stages one cloud file on local disk.
//
// progress is called with the number of bytes on disk so far, sampled from the
// partial file. It is advisory: the authoritative result is the finished
// file's size, which is checked against expectedSize before returning.
func (c *CLI) Download(
	ctx context.Context,
	request DownloadRequest,
	expectedSize int64,
	progress func(done int64),
) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if request.SliceParallel < 1 {
		request.SliceParallel = 1
	}
	if request.SliceParallel > 3 {
		// Aliyun throttles accounts that open more; the CLI prints its own
		// warning and then gets rate limited.
		request.SliceParallel = 3
	}
	if request.Retry < 1 {
		request.Retry = 3
	}
	if request.Timeout <= 0 {
		request.Timeout = 12 * time.Hour
	}
	if err := os.MkdirAll(request.StageDir, 0o750); err != nil {
		return "", fmt.Errorf("创建暂存目录: %w", err)
	}
	cleanPath, err := cleanDownloadPath(request.CloudPath)
	if err != nil {
		return "", err
	}
	request.CloudPath = cleanPath

	staged := StagedPath(request.StageDir, request.CloudPath)
	partial := staged + DownloadSuffix
	// A partial left by a killed process would otherwise be resumed against a
	// download-state database this plugin does not manage.
	for _, oldPath := range []string{partial, staged} {
		if err := os.Remove(oldPath); err != nil && !os.IsNotExist(err) {
			return "", fmt.Errorf("清理上次下载文件 %s: %w", oldPath, err)
		}
	}
	success := false
	defer func() {
		if !success {
			_ = os.Remove(partial)
			_ = os.Remove(staged)
		}
	}()

	watchCtx, stopWatching := context.WithCancel(ctx)
	defer stopWatching()
	if progress != nil {
		watchDone := make(chan struct{})
		go func() {
			defer close(watchDone)
			watchProgress(watchCtx, partial, staged, progress)
		}()
		defer func() {
			stopWatching()
			<-watchDone
		}()
	}

	// Downloads share the same command gate as the short commands. aliyunpan
	// rewrites its config file after every command, so allowing a multi-hour
	// download beside `ll` or `who` could overwrite a refreshed token or drive
	// selection. The UI keeps the browser responsive because each request is
	// still independent and the gate is acquired with its request context.
	args := []string{
		"download", request.CloudPath,
		"-saveto", request.StageDir,
		"-np",
		"-ow",
		"-p", "1",
		"-sp", strconv.Itoa(request.SliceParallel),
		"-retry", strconv.Itoa(request.Retry),
	}
	if request.DriveID != "" {
		args = append(args, "-driveId", request.DriveID)
	}
	output, err := c.runCommand(ctx, runOptions{timeout: request.Timeout}, args...)
	stopWatching()

	if strings.Contains(output, notLoggedInMarker) {
		return "", ErrNotLoggedIn
	}
	if err != nil {
		return "", err
	}
	if strings.Contains(output, "以下文件下载失败") {
		return "", fmt.Errorf("aliyunpan 下载失败: %s", lastLines(output, 6))
	}

	// The CLI reports most failures on stdout with a zero exit status, so the
	// file on disk is what decides.
	info, statErr := os.Stat(staged)
	if statErr != nil {
		return "", fmt.Errorf("下载后找不到 %s: %s", staged, lastLines(output, 6))
	}
	if expectedSize >= 0 && info.Size() != expectedSize {
		_ = os.Remove(staged)
		return "", fmt.Errorf("下载的文件大小是 %d，云端记录是 %d", info.Size(), expectedSize)
	}
	if progress != nil {
		progress(info.Size())
	}
	success = true
	return staged, nil
}

func cleanDownloadPath(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("云盘下载路径不能为空")
	}
	if strings.ContainsRune(raw, 0) {
		return "", fmt.Errorf("云盘下载路径含有 NUL 字符")
	}
	clean := path.Clean("/" + raw)
	if clean == "/" {
		return "", fmt.Errorf("不能把云盘根目录当作文件下载")
	}
	for _, part := range strings.Split(strings.TrimPrefix(raw, "/"), "/") {
		if part == ".." {
			return "", fmt.Errorf("云盘下载路径不能含有 .. 路径段")
		}
	}
	for _, part := range strings.Split(strings.TrimPrefix(clean, "/"), "/") {
		if part == "" || part == "." || part == ".." || unsafeWindowsName(part) {
			return "", fmt.Errorf("云盘下载路径含有不安全的文件名: %q", part)
		}
	}
	return clean, nil
}

func unsafeWindowsName(name string) bool {
	if strings.ContainsAny(name, `<>:"/\\|?*`) || strings.TrimRight(name, " .") != name {
		return true
	}
	base := strings.ToUpper(name)
	if dot := strings.IndexByte(base, '.'); dot >= 0 {
		base = base[:dot]
	}
	switch base {
	case "CON", "PRN", "AUX", "NUL",
		"COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
		"LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9":
		return true
	default:
		return false
	}
}

// watchProgress samples the partial file until the download ends. It also
// looks at the finished name, because the last thing aliyunpan does is rename
// the partial into place and the final sample should reflect that.
func watchProgress(ctx context.Context, partial, staged string, progress func(int64)) {
	ticker := time.NewTicker(progressInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if info, err := os.Stat(partial); err == nil {
				progress(info.Size())
				continue
			}
			if info, err := os.Stat(staged); err == nil {
				progress(info.Size())
			}
		}
	}
}

// lastLines condenses CLI output into something that fits in an error message
// and, from there, into a table cell on the sync page.
func lastLines(output string, count int) string {
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) > count {
		lines = lines[len(lines)-count:]
	}
	joined := strings.TrimSpace(strings.Join(lines, " | "))
	const limit = 500
	if len(joined) > limit {
		// Cut on a rune boundary so the message stays valid UTF-8.
		for limit := limit; limit > 0; limit-- {
			if utf8.RuneStart(joined[limit]) {
				return joined[:limit] + "…"
			}
		}
	}
	return joined
}
