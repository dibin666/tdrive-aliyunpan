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
	// downloadProgressSuffix names the resume sidecar written beside the .part
	// file. It is what lets a retry, a cancellation or a plugin restart pick a
	// transfer up where it stopped instead of starting the file again.
	downloadProgressSuffix = ".progress"
	legacyDownloadSuffix   = ".aliyunpan-downloading"
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
	// ChunkSize is the work unit the file is cut into, and the granularity a
	// resumed download restarts at. Zero uses downloadChunkSize, which is what
	// every caller outside a test wants.
	ChunkSize int64
}

// StagedPath preserves the old staging layout: the complete cloud path is
// rooted below the private item directory.
func StagedPath(stageDir, cloudPath string) string {
	clean := path.Clean("/" + strings.TrimSpace(cloudPath))
	return filepath.Join(stageDir, filepath.FromSlash(strings.TrimPrefix(clean, "/")))
}

// Download stages one cloud file, resuming whatever a previous attempt left
// behind.
//
// The file is cut into fixed-size chunks fetched by a small pool of workers,
// and every completed chunk is recorded in a sidecar next to the .part file.
// Nothing here deletes a partial download on the way out: a transfer that was
// cancelled, timed out, or died with the process is expected to be retried, and
// the whole point of the sidecar is that the retry only fetches what is
// missing. Discarding partial work is left to the caller, which is the only
// layer that knows whether the queue item is being retried or abandoned.
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
	if request.ChunkSize <= 0 {
		request.ChunkSize = downloadChunkSize
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

	// A finished staged file from an earlier attempt is the cheapest possible
	// outcome: the download already succeeded and only the upload after it
	// failed, so re-fetching gigabytes to reproduce a file that is sitting
	// right there would be the most expensive way to do nothing.
	if info, statErr := os.Stat(staged); statErr == nil && !info.IsDir() && info.Size() == fileSize {
		_ = os.Remove(partial)
		_ = os.Remove(checkpointPath(partial))
		if progress != nil {
			progress(fileSize)
		}
		return staged, nil
	}
	// Anything else left under the final name is the wrong size and cannot be
	// resumed into, so it goes. The legacy suffix is from the CLI-based
	// releases and never carried usable state.
	for _, oldPath := range []string{staged, staged + legacyDownloadSuffix} {
		if removeErr := os.Remove(oldPath); removeErr != nil && !os.IsNotExist(removeErr) {
			return "", fmt.Errorf("清理上次下载文件 %s: %w", oldPath, removeErr)
		}
	}
	if err := os.MkdirAll(filepath.Dir(partial), 0o750); err != nil {
		return "", fmt.Errorf("创建下载目录: %w", err)
	}

	identity := chunkCheckpoint{
		DriveID:   request.DriveID,
		FileID:    file.FileId,
		Size:      fileSize,
		Hash:      file.ContentHash,
		ChunkSize: request.ChunkSize,
	}
	checkpoint, resumed := loadCheckpoint(partial, identity)
	if resumed {
		// The bitmap only means anything if the file it indexes is still the
		// right shape. A .part of the wrong size belongs to another attempt.
		if info, statErr := os.Stat(partial); statErr != nil || info.IsDir() || info.Size() != fileSize {
			resumed = false
		}
	}
	if !resumed {
		checkpoint = newCheckpoint(partial, identity)
		if removeErr := os.Remove(partial); removeErr != nil && !os.IsNotExist(removeErr) {
			return "", fmt.Errorf("清理上次下载文件 %s: %w", partial, removeErr)
		}
	}

	// No O_TRUNC: an existing .part is the resumable copy, not leftover rubbish.
	partFile, err := os.OpenFile(partial, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return "", fmt.Errorf("创建下载文件: %w", err)
	}
	partFileOpen := true
	closePartFile := func() error {
		if !partFileOpen {
			return nil
		}
		partFileOpen = false
		return partFile.Close()
	}
	defer func() { _ = closePartFile() }()
	// The file is pre-allocated so chunk workers can write straight to their
	// own offsets. That also means its size is the final size from the first
	// moment, which is why the sidecar rather than the file is what says how
	// much has actually landed.
	if err := partFile.Truncate(fileSize); err != nil {
		return "", fmt.Errorf("预分配下载文件: %w", err)
	}

	_, resumedBytes := checkpoint.completed()
	if progress != nil {
		// Report the resumed baseline before fetching anything, so a queue item
		// that was requeued shows what is really on disk instead of starting its
		// bar at zero and appearing to download the file a second time.
		progress(resumedBytes)
	}
	// Same rule as markDone: bookkeeping that cannot be written costs the
	// ability to resume, not the download.
	_ = checkpoint.save()

	if fileSize > 0 {
		if pending := checkpoint.pending(); len(pending) > 0 {
			downloadURL, err := c.getDownloadURL(downloadContext, request.DriveID, file.FileId)
			if err != nil {
				return "", err
			}
			urlState := &downloadURLState{url: downloadURL}
			if err := c.downloadChunks(downloadContext, partFile, request, file.FileId, checkpoint, pending, resumedBytes, urlState, progress); err != nil {
				return "", err
			}
		}
	}
	if err := closePartFile(); err != nil {
		return "", fmt.Errorf("关闭下载文件: %w", err)
	}
	if err := os.Rename(partial, staged); err != nil {
		return "", fmt.Errorf("完成下载文件: %w", err)
	}
	checkpoint.remove()
	if progress != nil {
		progress(fileSize)
	}
	return staged, nil
}

// DiscardPartialDownload removes the resumable state for one cloud file.
//
// The downloader itself never does this: it cannot tell a transfer that will be
// retried from one that has been abandoned. The queue can, so the decision to
// throw away partial work lives there.
func DiscardPartialDownload(stageDir, cloudPath string) {
	cleanPath, err := cleanDownloadPath(cloudPath)
	if err != nil {
		return
	}
	partial := StagedPath(stageDir, cleanPath) + downloadPartSuffix
	_ = os.Remove(checkpointPath(partial))
	_ = os.Remove(partial)
}

// StagedDownloadBytes reports how much of one cloud file is already on local
// disk, counting a finished staged copy as complete and a partial one by its
// checkpoint. It lets the queue show real progress for an item that has not
// started running yet.
func StagedDownloadBytes(stageDir, cloudPath string, size int64) int64 {
	cleanPath, err := cleanDownloadPath(cloudPath)
	if err != nil {
		return 0
	}
	staged := StagedPath(stageDir, cleanPath)
	if info, statErr := os.Stat(staged); statErr == nil && !info.IsDir() && info.Size() == size {
		return size
	}
	partial := staged + downloadPartSuffix
	if info, statErr := os.Stat(partial); statErr != nil || info.IsDir() || info.Size() != size {
		return 0
	}
	return checkpointBytesForSize(partial, size)
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

// downloadProgress turns per-read deltas from several concurrent chunks into
// one absolute number for the caller.
//
// It reports absolute rather than incremental progress because a chunk that
// has to be retried un-does its own partial contribution: the bytes it read
// before the connection broke are not on disk in any usable sense, and a
// counter that only ever went up would claim they were. The queue is told the
// truth and shows the bar dip by one chunk instead of silently over-reporting.
type downloadProgress struct {
	mu      sync.Mutex
	base    int64
	partial map[int]int64
	total   int64
	report  func(int64)
}

func newDownloadProgress(base, total int64, report func(int64)) *downloadProgress {
	return &downloadProgress{base: base, partial: map[int]int64{}, total: total, report: report}
}

func (p *downloadProgress) advance(index int, delta int64) {
	if p == nil || delta <= 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.partial[index] += delta
	p.emitLocked()
}

// restart drops a chunk's in-flight bytes because its attempt failed.
func (p *downloadProgress) restart(index int) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.partial, index)
	p.emitLocked()
}

// complete folds a finished chunk into the committed total.
func (p *downloadProgress) complete(index int, size int64) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.partial, index)
	p.base += size
	p.emitLocked()
}

func (p *downloadProgress) currentLocked() int64 {
	value := p.base
	for _, bytes := range p.partial {
		value += bytes
	}
	if value > p.total {
		value = p.total
	}
	if value < 0 {
		value = 0
	}
	return value
}

// emitLocked reports the current total while still holding the lock.
//
// The callback runs under the mutex on purpose. Several chunks report
// concurrently, and because the figure is absolute rather than incremental, two
// updates delivered out of order would make the reported progress jump
// backwards for no reason. Serializing them costs a mutex hold across one
// counter update in the queue, which is far cheaper than the alternative.
func (p *downloadProgress) emitLocked() {
	if p.report == nil {
		return
	}
	p.report(p.currentLocked())
}

// downloadChunks fetches the listed chunks with a fixed pool of workers,
// recording each one in the checkpoint as it lands.
func (c *CLI) downloadChunks(
	ctx context.Context,
	file *os.File,
	request DownloadRequest,
	fileID string,
	checkpoint *checkpointFile,
	pending []int,
	resumedBytes int64,
	urlState *downloadURLState,
	progress func(done int64),
) error {
	fileSize := checkpoint.meta.Size
	chunkSize := checkpoint.meta.ChunkSize
	parallel := request.SliceParallel
	if parallel > len(pending) {
		parallel = len(pending)
	}
	if parallel < 1 {
		parallel = 1
	}

	downloadContext, cancel := context.WithCancel(ctx)
	defer cancel()
	tracker := newDownloadProgress(resumedBytes, fileSize, progress)

	work := make(chan int)
	go func() {
		defer close(work)
		for _, index := range pending {
			select {
			case work <- index:
			case <-downloadContext.Done():
				return
			}
		}
	}()

	results := make(chan error, parallel)
	var workers sync.WaitGroup
	for worker := 0; worker < parallel; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range work {
				begin, end := chunkBounds(index, fileSize, chunkSize)
				if end <= begin {
					results <- errChunkOutOfRange
					cancel()
					return
				}
				err := c.downloadChunkWithRetry(
					downloadContext, file, request.DriveID, fileID,
					index, begin, end, request.Retry, urlState, tracker,
				)
				if err != nil {
					results <- err
					cancel()
					return
				}
				tracker.complete(index, end-begin)
				// A checkpoint that cannot be written costs the ability to
				// resume, not the transfer: the bytes are already on disk and
				// the remaining chunks are still fetched. Failing here would
				// turn a full staging disk into a lost download.
				_ = checkpoint.markDone(index)
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

// downloadChunkWithRetry fetches one chunk, refreshing the signed URL when the
// drive expires it. A failed attempt restarts only this chunk, which is the
// whole reason the file is cut into chunks in the first place.
func (c *CLI) downloadChunkWithRetry(
	ctx context.Context,
	file *os.File,
	driveID string,
	fileID string,
	index int,
	begin int64,
	end int64,
	retry int,
	urlState *downloadURLState,
	tracker *downloadProgress,
) error {
	var lastError error
	for attempt := 0; attempt < retry; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		currentURL := urlState.current()
		err := c.downloadRange(ctx, file, currentURL, begin, end, func(delta int64) {
			tracker.advance(index, delta)
		})
		if err == nil {
			return nil
		}
		// Whatever this attempt read is not accounted for by the checkpoint, so
		// it must not stay in the reported total either.
		tracker.restart(index)
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
