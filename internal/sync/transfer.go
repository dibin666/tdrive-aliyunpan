package sync

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
	// stageItemsDir separates simultaneous jobs. Two jobs can legitimately read
	// the same cloud path from different drives or write it to different target
	// directories; sharing a path under one staging root would let one download
	// delete or overwrite the other one's source while it is uploading.
	stageItemsDir = ".tdrive-aliyunpan-items"
)

var ErrStageRoom = errors.New("暂存空间不足")

// missingSegmentsPattern reads drive.Complete's refusal, which formats the
// pending indices with %v: "upload is still missing segments [3 7]".
var missingSegmentsPattern = regexp.MustCompile(`missing segments \[([0-9 ]*)\]`)

// transfer runs one item end to end: stage it on disk, push it to Telegram,
// commit, then apply the configured local-copy policy.
func (e *Engine) transfer(ctx context.Context, item *Item, limits hostapi.RuntimeSettings) bool {
	defer e.releaseReservation(item.Size)
	deleteLocalAfterUpload := e.Settings().DeleteLocalAfterUpload

	// Cancel interrupts this context. Every error branch below has to check for
	// that first: a cancelled transfer fails with a context error like any other
	// and would otherwise be read as a transient blip and queued straight back
	// up, which is the opposite of what was asked for.
	ctx, release := e.watchItem(ctx, item.ID)
	defer release()

	// Partial work is kept for anything that will be tried again, and thrown
	// away for anything that will not. A transfer that is requeued — because a
	// token expired, a context was cancelled, or the staging area was briefly
	// full — resumes from the chunks already on disk, which is the difference
	// between retrying a file and downloading it a second time. A transfer that
	// failed for good, or that somebody cancelled, leaves nothing behind.
	discardStaged := true
	defer func() {
		if discardStaged {
			e.discardStagedWork(item)
		}
	}()

	staged, err := e.stage(ctx, item, limits)
	if err != nil {
		if e.stoppedByCancel(item.ID) {
			return true
		}
		if errors.Is(err, aliyunpan.ErrNotLoggedIn) {
			discardStaged = false
			e.invalidateProbe()
			e.requestProbeRefresh()
			e.deferUntilLogin(ctx, item)
			return false
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			discardStaged = false
			e.deferUntilRetry(ctx, item, err)
			return false
		}
		if errors.Is(err, ErrStageRoom) {
			// Nothing was downloaded, but an earlier attempt's chunks may still
			// be there and are exactly what makes the retry cheap enough to fit.
			discardStaged = false
			e.deferUntilRetry(ctx, item, err)
			return false
		}
		e.fail(ctx, item, err)
		return true
	}

	e.setStage(item, StageWaitingUpload)
	releaseUpload, err := e.acquireUploadSlot(ctx, limits.UploadConcurrency)
	if err != nil {
		if e.stoppedByCancel(item.ID) {
			return true
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			discardStaged = false
			e.deferUntilRetry(ctx, item, err)
			return false
		}
		e.fail(ctx, item, err)
		return true
	}
	uploadErr := e.upload(ctx, item, staged, limits)
	releaseUpload()
	if uploadErr != nil {
		if e.stoppedByCancel(item.ID) {
			return true
		}
		if errors.Is(uploadErr, context.Canceled) || errors.Is(uploadErr, context.DeadlineExceeded) {
			// The file is completely downloaded and verified. Deleting it here
			// meant a blip on the Telegram side cost the whole download again,
			// which is the single most expensive mistake this pipeline can make.
			discardStaged = false
			e.deferUntilRetry(ctx, item, uploadErr)
			return false
		}
		e.fail(ctx, item, uploadErr)
		return true
	}
	if e.stoppedByCancel(item.ID) {
		// The upload finished in the gap between the cancellation and this
		// check. Deleting the cloud original and reporting success would both
		// contradict a decision the operator has already made.
		return true
	}
	// upload has returned only after tdrive committed every segment, and its
	// file handle has already been closed. Remove the large local copy before
	// any potentially slow cloud-side deletion or queue persistence.
	discardStaged = !retainStagedAfterSuccessfulUpload(
		staged, item.ID, deleteLocalAfterUpload,
	)
	if item.DeleteAfter {
		// Only reached after the drive has committed the file, so the cloud
		// copy is never the last one standing.
		if cli := e.CLI(); cli != nil {
			drive, resolveErr := cli.ResolveDrive(ctx, item.DriveName)
			if resolveErr != nil {
				e.logger.Printf("解析删除目标网盘失败: %v", resolveErr)
			} else {
				var removeErr error
				if item.FileID != "" {
					removeErr = cli.RemoveByID(ctx, drive.ID, item.FileID)
					if errors.Is(removeErr, aliyunpan.ErrPathNotFound) {
						removeErr = cli.Remove(ctx, item.RemotePath, drive.ID)
					}
				} else {
					removeErr = cli.Remove(ctx, item.RemotePath, drive.ID)
				}
				if removeErr != nil {
					e.logger.Printf("删除云端 %s 失败: %v", item.RemotePath, removeErr)
				}
			}
		}
	}
	e.complete(ctx, item)
	return true
}

func (e *Engine) deferUntilLogin(ctx context.Context, item *Item) {
	e.requeue(ctx, item, "等待重新登录阿里云盘")
}

func (e *Engine) deferUntilRetry(ctx context.Context, item *Item, err error) {
	e.requeue(ctx, item, err.Error())
}

// requeue puts a transfer back in the queue to be attempted again.
//
// Downloaded is deliberately left alone. The staged chunks it counts are still
// on disk and the next attempt resumes from them, so zeroing it would report a
// file as un-downloaded while its bytes sat in the staging area — which is what
// made an interrupted transfer look like it had restarted from nothing. Uploaded
// does go back to zero: the segments are re-sent from the beginning, and the old
// upload job is abandoned rather than resumed.
func (e *Engine) requeue(ctx context.Context, item *Item, reason string) {
	e.mu.Lock()
	item.State = StatePending
	item.Stage = StageIdle
	item.Uploaded = 0
	item.UploadJobID = ""
	item.Error = reason
	item.FinishedAt = 0
	item.resetSpeed()
	e.markQueueDirtyLocked()
	e.mu.Unlock()
	e.persistNow(ctx)
}

// discardStagedWork removes everything one queue item holds in the staging
// area, including the resume checkpoint. It is used for transfers that will not
// be attempted again, so the disk is not held by work nobody will finish.
func (e *Engine) discardStagedWork(item *Item) {
	if item == nil {
		return
	}
	directory := itemStageDir(e.StageDir(), item.ID)
	aliyunpan.DiscardPartialDownload(directory, item.RemotePath)
	_ = os.RemoveAll(directory)
}

// stage downloads the cloud file onto local disk.
func (e *Engine) stage(ctx context.Context, item *Item, limits hostapi.RuntimeSettings) (string, error) {
	cli := e.CLI()
	if cli == nil {
		return "", errors.New("aliyunpan 尚未配置")
	}
	releaseDownload, err := e.acquireDownloadSlot(ctx)
	if err != nil {
		return "", err
	}
	defer releaseDownload()
	stageDir := e.StageDir()
	releaseStage, err := e.reserveStageRoom(item.ID, item.Size, stageDir, limits)
	if err != nil {
		return "", err
	}
	defer releaseStage()
	downloadDir := itemStageDir(stageDir, item.ID)
	drive, err := cli.ResolveDrive(ctx, item.DriveName)
	if err != nil {
		return "", err
	}

	e.setStage(item, StageDownloading)
	current := e.Settings()
	if err := cli.SetDownloadRate(ctx, current.DownloadRate); err != nil {
		return "", fmt.Errorf("设置下载限速: %w", err)
	}

	// The per-file connection count follows tdrive's own download setting so a
	// deployment that was told to be gentle stays gentle on both sides.
	slices := limits.MaxDownloadConns
	if slices < 1 {
		slices = 1
	}
	staged, err := cli.Download(ctx, aliyunpan.DownloadRequest{
		CloudPath:     item.RemotePath,
		StageDir:      downloadDir,
		FileID:        item.FileID,
		DriveID:       drive.ID,
		SliceParallel: slices,
	}, item.Size, func(done int64) { e.noteDownload(item, done) })
	if err != nil {
		// The partial download and its checkpoint stay where they are. Whether
		// they are worth keeping depends on whether this item will be retried,
		// which only the caller knows; deleting them here is what used to make
		// every interrupted download start again from byte zero.
		return "", err
	}
	// The completed file is now visible to stagedBytesAt, so the reservation
	// must be released before the next worker evaluates the disk limit.
	releaseStage()
	return staged, nil
}

func itemStageDir(stageDir, itemID string) string {
	if itemID == "" {
		itemID = "anonymous"
	}
	return filepath.Join(stageDir, stageItemsDir, itemID)
}

// cleanupStaged removes only the private directory allocated to one queue
// item. It intentionally does not remove the administrator's stage root or
// another item's files.
func cleanupStaged(staged, itemID string) {
	_ = os.Remove(staged)
	dir := filepath.Dir(staged)
	if itemID == "" {
		itemID = "anonymous"
	}
	for dir != filepath.Dir(dir) {
		if filepath.Base(dir) == itemID && filepath.Base(filepath.Dir(dir)) == stageItemsDir {
			_ = os.RemoveAll(dir)
			return
		}
		dir = filepath.Dir(dir)
	}
}

// retainStagedAfterSuccessfulUpload applies the global local-copy policy. It
// returns whether the transfer's deferred cleanup should leave the staged file
// alone; when deletion is requested, the deferred cleanup remains enabled as a
// second attempt if the immediate removal encounters a transient filesystem
// problem.
func retainStagedAfterSuccessfulUpload(staged, itemID string, deleteLocalAfterUpload bool) bool {
	if !deleteLocalAfterUpload {
		return true
	}
	cleanupStaged(staged, itemID)
	return false
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

	// A transfer stopped on purpose is recorded as cancelled rather than failed,
	// on this page and in tdrive's history alike.
	abortState := func() string {
		if e.stoppedByCancel(item.ID) {
			return "cancelled"
		}
		return "failed"
	}

	var lastErr error
	for round := 0; round < resendRounds; round++ {
		if err := e.sendSegments(ctx, item, file, job, pending); err != nil {
			e.abort(ctx, item, job.ID, err.Error(), abortState())
			return err
		}
		missing, err := e.commit(ctx, job.ID)
		if err == nil {
			return nil
		}
		lastErr = err
		if len(missing) == 0 {
			e.abort(ctx, item, job.ID, err.Error(), abortState())
			return err
		}
		e.logger.Printf("%s 缺少分片 %v，重发第 %d 轮", item.Name, missing, round+1)
		pending = missing
	}
	e.abort(ctx, item, job.ID, "重发分片后仍未完成", abortState())
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
	if item.Size == 0 {
		// tdrive represents an empty file with a caption-only record. It still
		// needs a zero-byte PutSegment call to create that record; skipping the
		// segment would leave CompleteUpload permanently reporting segment 1 as
		// missing.
		for _, index := range indices {
			if index != 1 {
				continue
			}
			return e.host.PutSegment(ctx, job.ID, index, 0, strings.NewReader(""), nil)
		}
		return nil
	}
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
	if item.Size >= 0 && item.Uploaded > item.Size {
		// A missing segment may be resent. Progress is the committed file
		// geometry, not the sum of every attempted byte written to a stream.
		item.Uploaded = item.Size
	}
	item.observe(delta, time.Now())
	due := time.Since(e.lastPersist) >= persistInterval
	e.markQueueDirtyLocked()
	e.mu.Unlock()
	if due {
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

// abort tears down an upload that will not complete, so the storage channel is
// not left holding documents nothing points at.
//
// state is what tdrive records: "failed" for a transfer that broke, "cancelled"
// for one somebody stopped. Recording every abort as a failure made a
// cancellation from this page show up in tdrive's history as an error.
func (e *Engine) abort(ctx context.Context, item *Item, jobID, reason, state string) {
	// The cleanup context outlives the cancellation that usually causes it,
	// which is the whole reason a cancelled upload can still be tidied up.
	cleanupCtx, cancel := cleanupContext(ctx)
	defer cancel()
	if err := e.host.AbortUpload(cleanupCtx, jobID, reason, state); err != nil {
		e.logger.Printf("放弃上传 %s 失败: %v", item.Name, err)
	}
	e.mu.Lock()
	item.UploadJobID = ""
	e.mu.Unlock()
}

func cleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return withTimeout(context.WithoutCancel(ctx), 30*time.Second)
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
