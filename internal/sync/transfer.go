package sync

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/dibin/tdrive-aliyunpan/internal/aliyunpan"
	"github.com/dibin/tdrive-aliyunpan/internal/hostapi"
	tdriveplugin "github.com/dibin/tdrive/pkg/plugin"
)

const (
	// completeAttempts is how many times a commit is re-checked before the
	// missing segments are re-sent. The host closes the plugin's end of a
	// segment stream before it waits for that segment's database commit, so a
	// segment can legitimately still be landing when the upload is completed.
	completeAttempts = 6
	// resendRounds bounds the "re-send what is missing" loop.
	resendRounds = 3
	// uploadSource labels these transfers on tdrive's own 传输 page.
	uploadSource = "aliyunpan"
)

// missingSegmentsPattern reads drive.Complete's refusal, which formats the
// pending indices with %v: "upload is still missing segments [3 7]".
var missingSegmentsPattern = regexp.MustCompile(`missing segments \[([0-9 ]*)\]`)

// transfer runs one item end to end: stage it on disk, push it to Telegram,
// commit, then clean up.
func (e *Engine) transfer(ctx context.Context, item *Item, limits hostapi.RuntimeSettings) {
	defer e.releaseReservation(item.Size)

	staged, err := e.stage(ctx, item, limits)
	if err != nil {
		e.fail(ctx, item, err)
		return
	}
	// The staged copy is disk the drive is not managing, so it is removed on
	// every exit path, successful or not.
	defer func() { _ = os.Remove(staged) }()

	if err := e.upload(ctx, item, staged, limits); err != nil {
		e.fail(ctx, item, err)
		return
	}
	if item.DeleteAfter {
		// Only reached after the drive has committed the file, so the cloud
		// copy is never the last one standing.
		if cli := e.CLI(); cli != nil {
			if err := cli.Remove(ctx, item.RemotePath); err != nil {
				e.logger.Printf("删除云端 %s 失败: %v", item.RemotePath, err)
			}
		}
	}
	e.complete(ctx, item)
}

// stage downloads the cloud file onto local disk.
func (e *Engine) stage(ctx context.Context, item *Item, limits hostapi.RuntimeSettings) (string, error) {
	cli := e.CLI()
	if cli == nil {
		return "", errors.New("aliyunpan 尚未配置")
	}
	stageDir := e.StageDir()
	if err := e.checkStageRoom(item, stageDir, limits); err != nil {
		return "", err
	}

	e.setStage(item, StageDownloading)
	current := e.Settings()
	if err := cli.SetDownloadRate(ctx, current.DownloadRate); err != nil {
		e.logger.Printf("设置下载限速失败: %v", err)
	}

	// The per-file connection count follows tdrive's own download setting so a
	// deployment that was told to be gentle stays gentle on both sides.
	slices := limits.MaxDownloadConns
	if slices < 1 {
		slices = 1
	}
	staged, err := cli.Download(ctx, aliyunpan.DownloadRequest{
		CloudPath:     item.RemotePath,
		StageDir:      stageDir,
		SliceParallel: slices,
	}, item.Size, func(done int64) { e.noteDownload(item, done) })
	if err != nil {
		return "", err
	}
	return staged, nil
}

// checkStageRoom refuses to start a download that would blow the staging
// budget, unless the staging area is empty — in which case refusing would mean
// the file can never be synced at all.
func (e *Engine) checkStageRoom(item *Item, stageDir string, limits hostapi.RuntimeSettings) error {
	limit := e.stageLimit(limits)
	if limit <= 0 {
		return nil
	}
	used := e.stagedBytes()
	if used+item.Size <= limit {
		return nil
	}
	if used == 0 {
		return nil
	}
	return fmt.Errorf("暂存空间不足：已占用 %d 字节，上限 %d 字节，本文件 %d 字节；等待其它文件完成后重试",
		used, limit, item.Size)
}

// upload pushes the staged file into the drive, one segment at a time.
func (e *Engine) upload(ctx context.Context, item *Item, staged string, limits hostapi.RuntimeSettings) error {
	owner, err := e.uploadOwner(ctx)
	if err != nil {
		return err
	}
	file, err := os.Open(staged)
	if err != nil {
		return fmt.Errorf("打开暂存文件: %w", err)
	}
	defer file.Close()

	e.setStage(item, StageUploading)
	job, _, err := e.host.BeginUpload(ctx, tdriveplugin.UploadRequest{
		DirPath: item.TargetDir,
		Name:    item.Name,
		Size:    item.Size,
		UserID:  owner,
		// Labelling the source is what makes these transfers legible on
		// tdrive's own 传输 page rather than looking like WebUI uploads.
		Source:    uploadSource,
		SourceURL: "aliyunpan:" + item.RemotePath,
		Overwrite: item.Overwrite,
	})
	if err != nil {
		return fmt.Errorf("开始上传: %w", err)
	}
	e.mu.Lock()
	item.UploadJobID = job.ID
	e.mu.Unlock()

	pending := make([]int, 0, job.SegmentCount)
	for index := 1; index <= job.SegmentCount; index++ {
		pending = append(pending, index)
	}

	var lastErr error
	for round := 0; round < resendRounds; round++ {
		if err := e.sendSegments(ctx, item, file, job, pending); err != nil {
			e.abort(ctx, item, job.ID, err.Error())
			return err
		}
		missing, err := e.commit(ctx, job.ID)
		if err == nil {
			return nil
		}
		lastErr = err
		if len(missing) == 0 {
			e.abort(ctx, item, job.ID, err.Error())
			return err
		}
		e.logger.Printf("%s 缺少分片 %v，重发第 %d 轮", item.Name, missing, round+1)
		pending = missing
	}
	e.abort(ctx, item, job.ID, "重发分片后仍未完成")
	return fmt.Errorf("上传未能完成: %w", lastErr)
}

// sendSegments streams the listed segments.
//
// They go one at a time on purpose. tdrive already parallelises inside a
// segment with UploadThreads and paces the whole thing with RateLimit; adding
// concurrency here would push past limits the deployment deliberately set.
func (e *Engine) sendSegments(
	ctx context.Context,
	item *Item,
	file *os.File,
	job tdriveplugin.UploadJob,
	indices []int,
) error {
	for _, index := range indices {
		if err := ctx.Err(); err != nil {
			return err
		}
		offset := int64(index-1) * job.SegmentSize
		size := job.SegmentSize
		if remaining := item.Size - offset; remaining < size {
			size = remaining
		}
		if size <= 0 {
			continue
		}
		if _, err := file.Seek(offset, 0); err != nil {
			return fmt.Errorf("定位分片 %d: %w", index, err)
		}
		// The host rejects a segment whose declared length does not match its
		// own geometry, so size is computed exactly as the drive computes it.
		if err := e.host.PutSegment(ctx, job.ID, index, size, file, func(delta int64) {
			e.chargeQuotaDelta(ctx, item, delta)
		}); err != nil {
			return err
		}
	}
	return nil
}

// chargeQuotaDelta folds streamed bytes into the item's progress and the daily
// counter. It is called per chunk, and only persists the counter periodically;
// the authoritative write happens when the segment finishes.
func (e *Engine) chargeQuotaDelta(ctx context.Context, item *Item, delta int64) {
	e.mu.Lock()
	item.Uploaded += delta
	item.observe(delta, time.Now())
	e.quota.UsedBytes += delta
	due := time.Since(e.lastPersist) >= persistInterval
	quota := e.quota
	e.dirty = true
	if due {
		e.lastPersist = time.Now()
	}
	e.mu.Unlock()
	if due {
		if err := e.host.SetData(ctx, quotaKey, quota); err != nil {
			e.logger.Printf("保存配额计数失败: %v", err)
		}
		e.persistNow(ctx)
	}
}

// commit finalizes the upload, tolerating segments that are still landing.
//
// It returns the still-missing indices when the drive says the upload is
// incomplete, which is the only channel the host has for per-segment failures:
// the brokered stream is closed before the segment's commit is awaited, so a
// segment that failed to store looks, from here, exactly like one that
// succeeded.
func (e *Engine) commit(ctx context.Context, jobID string) ([]int, error) {
	var lastErr error
	backoff := 250 * time.Millisecond
	for attempt := 0; attempt < completeAttempts; attempt++ {
		_, err := e.host.CompleteUpload(ctx, jobID)
		if err == nil {
			return nil, nil
		}
		lastErr = err
		missing := parseMissingSegments(err)
		if len(missing) == 0 {
			return nil, err
		}
		if attempt == completeAttempts-1 {
			return missing, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
	}
	return nil, lastErr
}

// parseMissingSegments extracts the indices from the drive's refusal.
func parseMissingSegments(err error) []int {
	if err == nil {
		return nil
	}
	match := missingSegmentsPattern.FindStringSubmatch(err.Error())
	if match == nil {
		return nil
	}
	indices := make([]int, 0, 8)
	for _, field := range strings.Fields(match[1]) {
		value, convErr := strconv.Atoi(field)
		if convErr != nil {
			continue
		}
		indices = append(indices, value)
	}
	return indices
}

// abort tears down a failed upload so the storage channel is not left holding
// documents nothing points at.
func (e *Engine) abort(ctx context.Context, item *Item, jobID, reason string) {
	if err := e.host.AbortUpload(ctx, jobID, reason, "failed"); err != nil {
		e.logger.Printf("放弃上传 %s 失败: %v", item.Name, err)
	}
	e.mu.Lock()
	item.UploadJobID = ""
	e.mu.Unlock()
}

// uploadOwner resolves the account uploads are attributed to. The owner
// decides whose quota is consumed and who sees the file, so falling back to
// the first administrator keeps a fresh installation working without asking a
// question the operator has no context for yet.
func (e *Engine) uploadOwner(ctx context.Context) (string, error) {
	configured := e.Settings().OwnerUserID
	users, err := e.host.Users(ctx)
	if err != nil {
		return "", fmt.Errorf("读取用户列表: %w", err)
	}
	if configured != "" {
		for _, user := range users {
			if user.ID == configured {
				if !user.Enabled {
					return "", fmt.Errorf("上传归属账号 %s 已被停用", user.Username)
				}
				return user.ID, nil
			}
		}
		return "", errors.New("配置里的上传归属账号不存在")
	}
	for _, user := range users {
		if user.Role == "admin" && user.Enabled {
			return user.ID, nil
		}
	}
	return "", errors.New("找不到可用的管理员账号")
}
