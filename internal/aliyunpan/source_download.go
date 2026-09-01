package aliyunpan

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	aliyunpanapi "github.com/tickstep/aliyunpan-api/aliyunpan"
	"github.com/tickstep/aliyunpan-api/aliyunpan_open/openapi"
)

const (
	parallelDownloadLimit = 3
	downloadBufferSize    = 256 << 10
	downloadPartSuffix    = ".part"
	legacyDownloadSuffix  = ".aliyunpan-downloading"
)

// DownloadRequest describes one source file to stage.
type DownloadRequest struct {
	CloudPath string
	StageDir  string
	// FileID is filled by the API scanner. It is optional so queue records from
	// older plugin versions can resolve the file by CloudPath.
	FileID string
	// DriveID is account-specific and must be passed for every API operation.
	DriveID       string
	SliceParallel int
	Retry         int
	Timeout       time.Duration
}

// StagedPath preserves the old staging layout: the complete cloud path is
// rooted below the private item directory.
func StagedPath(stageDir, cloudPath string) string {
	clean := path.Clean("/" + strings.TrimSpace(cloudPath))
	return filepath.Join(stageDir, filepath.FromSlash(strings.TrimPrefix(clean, "/")))
}

// Download stages one cloud file with independent HTTP Range workers.
// Progress is reported directly from the workers, so no CLI checkpoint file or
// polling loop is involved.
func (c *CLI) Download(
	ctx context.Context,
	request DownloadRequest,
	expectedSize int64,
	progress func(done int64),
) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if c.httpClient == nil {
		c.httpClient = &http.Client{}
	}
	if c.limiter == nil {
		c.limiter = &byteRateLimiter{}
	}
	if request.StageDir == "" {
		return "", errors.New("下载暂存目录不能为空")
	}
	if request.SliceParallel < 1 {
		request.SliceParallel = 1
	}
	if request.SliceParallel > parallelDownloadLimit {
		request.SliceParallel = parallelDownloadLimit
	}
	if request.Retry < 1 {
		request.Retry = 3
	}
	if request.Timeout <= 0 {
		request.Timeout = 12 * time.Hour
	}
	cleanPath, err := cleanDownloadPath(request.CloudPath)
	if err != nil {
		return "", err
	}
	request.CloudPath = cleanPath
	if request.DriveID == "" {
		drive, resolveErr := c.ResolveDrive(ctx, DriveBackup)
		if resolveErr != nil {
			return "", resolveErr
		}
		request.DriveID = drive.ID
	}

	downloadContext, cancel := context.WithTimeout(ctx, request.Timeout)
	defer cancel()
	var file *aliyunpanapi.FileEntity
	if request.FileID != "" {
		file, err = c.fileByID(downloadContext, request.DriveID, request.FileID)
		if err != nil && errors.Is(err, ErrPathNotFound) {
			// A queue can survive a cloud-side move or restore. Prefer the
			// persisted ID, but let the old path recover the transfer when that
			// ID has become invalid.
			file, err = c.fileByPath(downloadContext, request.DriveID, request.CloudPath)
		}
	} else {
		file, err = c.fileByPath(downloadContext, request.DriveID, request.CloudPath)
	}
	if err != nil {
		return "", err
	}
	if file == nil || file.IsFolder() {
		return "", fmt.Errorf("云盘路径不是文件: %s", request.CloudPath)
	}
	fileSize := file.FileSize
	if expectedSize >= 0 && fileSize != expectedSize {
		return "", fmt.Errorf("云端文件大小是 %d，队列记录是 %d", fileSize, expectedSize)
	}

	staged := StagedPath(request.StageDir, request.CloudPath)
	partial := staged + downloadPartSuffix
	for _, oldPath := range []string{staged, partial, staged + legacyDownloadSuffix} {
		if removeErr := os.Remove(oldPath); removeErr != nil && !os.IsNotExist(removeErr) {
			return "", fmt.Errorf("清理上次下载文件 %s: %w", oldPath, removeErr)
		}
	}
	if err := os.MkdirAll(filepath.Dir(partial), 0o750); err != nil {
		return "", fmt.Errorf("创建下载目录: %w", err)
	}
	partFile, err := os.OpenFile(partial, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		return "", fmt.Errorf("创建下载文件: %w", err)
	}
	cleanup := func() {
		_ = partFile.Close()
		_ = os.Remove(partial)
		_ = os.Remove(staged)
	}
	downloadCompleted := false
	defer func() {
		if !downloadCompleted {
			cleanup()
		}
	}()
	if err := partFile.Truncate(fileSize); err != nil {
		cleanup()
		return "", fmt.Errorf("预分配下载文件: %w", err)
	}

	if fileSize > 0 {
		downloadURL, err := c.getDownloadURL(downloadContext, request.DriveID, file.FileId)
		if err != nil {
			cleanup()
			return "", err
		}
		urlState := &downloadURLState{url: downloadURL}
		if err := c.downloadRanges(downloadContext, partFile, request.DriveID, file.FileId, fileSize, request.SliceParallel, request.Retry, urlState, progress); err != nil {
			cleanup()
			return "", err
		}
	}
	if err := partFile.Close(); err != nil {
		_ = os.Remove(partial)
		return "", fmt.Errorf("关闭下载文件: %w", err)
	}
	if err := os.Rename(partial, staged); err != nil {
		_ = os.Remove(partial)
		return "", fmt.Errorf("完成下载文件: %w", err)
	}
	downloadCompleted = true
	if progress != nil {
		progress(fileSize)
	}
	return staged, nil
}

type downloadURLState struct {
	mu  sync.Mutex
	url string
}

func (s *downloadURLState) current() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.url
}

func (c *CLI) getDownloadURL(ctx context.Context, driveID, fileID string) (string, error) {
	var result openapi.FileDownloadUrlResult
	if err := c.requestJSON(ctx, http.MethodPost, "/adrive/v1.0/openFile/getDownloadUrl", &openapi.FileDownloadUrlParam{
		DriveId:   driveID,
		FileId:    fileID,
		ExpireSec: 14400,
	}, &result); err != nil {
		return "", fmt.Errorf("获取云端下载链接: %w", err)
	}
	if result.Url == "" {
		return "", errors.New("阿里云盘没有返回下载链接")
	}
	return result.Url, nil
}

func (c *CLI) downloadRanges(
	ctx context.Context,
	file *os.File,
	driveID string,
	fileID string,
	fileSize int64,
	parallel int,
	retry int,
	urlState *downloadURLState,
	progress func(done int64),
) error {
	if fileSize < int64(parallel) {
		parallel = int(fileSize)
	}
	if parallel < 1 {
		parallel = 1
	}
	rangeSize := (fileSize + int64(parallel) - 1) / int64(parallel)
	type byteRange struct {
		begin int64
		end   int64
	}
	ranges := make([]byteRange, 0, parallel)
	for begin := int64(0); begin < fileSize; begin += rangeSize {
		end := begin + rangeSize
		if end > fileSize {
			end = fileSize
		}
		ranges = append(ranges, byteRange{begin: begin, end: end})
	}

	downloadContext, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan error, len(ranges))
	var progressMu sync.Mutex
	var downloadedBytes int64
	reportProgress := func(delta int64) {
		if progress == nil || delta <= 0 {
			return
		}
		progressMu.Lock()
		downloadedBytes += delta
		if downloadedBytes > fileSize {
			downloadedBytes = fileSize
		}
		currentProgress := downloadedBytes
		progressMu.Unlock()
		progress(currentProgress)
	}
	var workers sync.WaitGroup
	for _, currentRange := range ranges {
		currentRange := currentRange
		workers.Add(1)
		go func() {
			defer workers.Done()
			err := c.downloadRangeWithRetry(downloadContext, file, driveID, fileID, currentRange.begin, currentRange.end, retry, urlState, reportProgress)
			results <- err
			if err != nil {
				cancel()
			}
		}()
	}
	workers.Wait()
	close(results)

	var firstError error
	for result := range results {
		if result == nil {
			continue
		}
		if firstError == nil || errors.Is(firstError, context.Canceled) {
			firstError = result
		}
	}
	if firstError != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return firstError
	}
	return nil
}

var errDownloadURLExpired = errors.New("云端下载链接已失效")

func (c *CLI) downloadRangeWithRetry(
	ctx context.Context,
	file *os.File,
	driveID string,
	fileID string,
	begin int64,
	end int64,
	retry int,
	urlState *downloadURLState,
	progress func(done int64),
) error {
	var lastError error
	var committedBytes int64
	for attempt := 0; attempt < retry; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		currentURL := urlState.current()
		attemptBytes := int64(0)
		err := c.downloadRange(ctx, file, currentURL, begin, end, func(delta int64) {
			attemptBytes += delta
			newBytes := attemptBytes - committedBytes
			if newBytes <= 0 {
				return
			}
			committedBytes += newBytes
			if progress != nil {
				progress(newBytes)
			}
		})
		if err == nil {
			return nil
		}
		lastError = err
		if errors.Is(err, errDownloadURLExpired) {
			urlState.mu.Lock()
			if urlState.url == currentURL {
				refreshedURL, refreshErr := c.getDownloadURL(ctx, driveID, fileID)
				if refreshErr != nil {
					urlState.mu.Unlock()
					lastError = refreshErr
				} else {
					urlState.url = refreshedURL
					urlState.mu.Unlock()
				}
			} else {
				urlState.mu.Unlock()
			}
		}
		if attempt+1 < retry {
			if waitErr := waitForRetry(ctx, attempt); waitErr != nil {
				return waitErr
			}
		}
	}
	return lastError
}

func (c *CLI) downloadRange(ctx context.Context, file *os.File, downloadURL string, begin, end int64, progress func(done int64)) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadURL, nil)
	if err != nil {
		return fmt.Errorf("创建分片下载请求: %w", err)
	}
	request.Header.Set("Range", "bytes="+strconv.FormatInt(begin, 10)+"-"+strconv.FormatInt(end-1, 10))
	request.Header.Set("Referer", "https://www.aliyundrive.com/")
	response, err := c.httpClient.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("请求下载分片: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden || response.StatusCode == http.StatusGone {
		return fmt.Errorf("%w: HTTP %d", errDownloadURLExpired, response.StatusCode)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		errorBody, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		lowerBody := strings.ToLower(string(errorBody))
		if strings.Contains(lowerBody, "expired") ||
			strings.Contains(lowerBody, "accessdenied") ||
			strings.Contains(lowerBody, "signaturedoesnotmatch") {
			return fmt.Errorf("%w: HTTP %d", errDownloadURLExpired, response.StatusCode)
		}
		return fmt.Errorf("下载分片返回 HTTP %d", response.StatusCode)
	}
	if response.StatusCode == http.StatusOK && begin != 0 {
		return errors.New("下载服务器忽略了 Range 请求")
	}
	if response.StatusCode == http.StatusPartialContent {
		if err := validateContentRange(response.Header.Get("Content-Range"), begin, end); err != nil {
			return err
		}
	}

	remaining := end - begin
	currentOffset := begin
	buffer := make([]byte, downloadBufferSize)
	for remaining > 0 {
		want := int64(len(buffer))
		if remaining < want {
			want = remaining
		}
		readCount, readErr := io.ReadFull(response.Body, buffer[:want])
		if readCount > 0 {
			if waitErr := c.limiter.Wait(ctx, int64(readCount)); waitErr != nil {
				return waitErr
			}
			written, writeErr := file.WriteAt(buffer[:readCount], currentOffset)
			if writeErr != nil {
				return fmt.Errorf("写入下载分片: %w", writeErr)
			}
			if written != readCount {
				return io.ErrShortWrite
			}
			currentOffset += int64(readCount)
			remaining -= int64(readCount)
			if progress != nil {
				progress(int64(readCount))
			}
		}
		if readErr != nil {
			return fmt.Errorf("下载分片数据不完整: %w", readErr)
		}
	}
	return nil
}

func validateContentRange(value string, expectedBegin, expectedEnd int64) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	var begin, end int64
	var total string
	if _, err := fmt.Sscanf(value, "bytes %d-%d/%s", &begin, &end, &total); err != nil {
		return fmt.Errorf("下载分片的 Content-Range 不合法: %q", value)
	}
	if begin != expectedBegin || end != expectedEnd-1 {
		return fmt.Errorf("下载分片范围是 bytes %d-%d，期望 bytes %d-%d", begin, end, expectedBegin, expectedEnd-1)
	}
	return nil
}

func cleanDownloadPath(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", errors.New("云盘下载路径不能为空")
	}
	if strings.ContainsRune(raw, 0) {
		return "", errors.New("云盘下载路径含有 NUL 字符")
	}
	clean := path.Clean("/" + raw)
	if clean == "/" {
		return "", errors.New("不能把云盘根目录当作文件下载")
	}
	for _, part := range strings.Split(strings.TrimPrefix(raw, "/"), "/") {
		if part == ".." {
			return "", errors.New("云盘下载路径不能含有 .. 路径段")
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
