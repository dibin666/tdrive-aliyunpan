package sync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/dibin/tdrive-aliyunpan/internal/aliyunpan"
	"github.com/dibin/tdrive-aliyunpan/internal/hostapi"
	"github.com/dibin/tdrive-aliyunpan/internal/settings"
)

// Plugin-data keys. "settings" is deliberately the key tdrive's own plugin
// configuration modal reads and writes, so the two editors never disagree.
const (
	SettingsKey = "settings"
	queueKey    = "queue"
	quotaKey    = "quotaState"
)

const (
	// tickInterval is how often the scheduler re-evaluates. It is short enough
	// that a window opening is noticed promptly and long enough that an idle
	// deployment costs nothing.
	tickInterval = 30 * time.Second
	// persistInterval throttles queue writes while bytes are moving. Every
	// state transition is written immediately; progress is not.
	persistInterval = 2 * time.Second
	// historyLimit is how many finished items are kept for the sync page.
	historyLimit = 500
)

// Engine runs the queue.
type Engine struct {
	host   *hostapi.Client
	cli    *aliyunpan.CLI
	logger *log.Logger
	// dataDir is the plugin's own directory; the default staging area lives
	// under it.
	dataDir string

	mu             sync.Mutex
	settings       settings.Settings
	queue          []*Item
	quota          QuotaState
	active         int
	downloadActive int
	uploadActive   int
	reserved       int64
	stageReserved  int64
	// stageReservations is the same total broken down per queue item, so a
	// directory walk can skip the files a reservation already accounts for.
	stageReservations map[string]int64
	lastScan          time.Time
	scanning          bool
	scanError         string
	paused            bool
	stopping          bool
	// runCtx is the plugin-lifetime context. HTTP-triggered background work
	// uses it instead of the short-lived request context, but still stops when
	// the host shuts the plugin down.
	runCtx context.Context
	// lastPersist throttles progress writes; dirty records that a write is
	// owed.
	lastPersist   time.Time
	dirty         bool
	queueRevision uint64
	quotaRevision uint64
	quotaDirty    bool
	persistMu     sync.Mutex

	// cancels holds the context of every transfer currently moving bytes, so a
	// cancellation from the queue view can interrupt one instead of waiting for
	// it. cancelling remembers which of those stopped on purpose: the error a
	// cancelled transfer reports is a context error like any other, and without
	// this the deferred-retry path would read it as a blip and queue the file
	// straight back up. Both are guarded by mu.
	cancels    map[string]context.CancelFunc
	cancelling map[string]bool
	// deleting tracks in-flight staged file deletions (by refcount) across
	// DeleteStaged and Cancel, so takeNext or Retry cannot claim items
	// mid-deletion and concurrent operations cannot clear each other's marks.
	deleting map[string]int

	wake chan struct{}
	// running counts in-flight transfers so Close can wait for them.
	workers sync.WaitGroup
	runDone chan struct{}
	// slotChanged wakes transfers waiting for a source-download or tdrive-upload
	// slot when a running transfer finishes or the configured limit changes.
	slotChanged chan struct{}

	// probeMu guards the cached aliyunpan account. It is cached because the
	// source client makes a network call, which must not happen on every page
	// poll.
	probeMu sync.Mutex
	probes  probeCache
}

// New builds an engine. It does not touch the host; call Load first.
func New(host *hostapi.Client, dataDir string, logger *log.Logger) *Engine {
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	return &Engine{
		host:        host,
		dataDir:     dataDir,
		logger:      logger,
		settings:    settings.Default(),
		queue:       []*Item{},
		cancels:     make(map[string]context.CancelFunc),
		cancelling:  make(map[string]bool),
		deleting:    make(map[string]int),
		wake:        make(chan struct{}, 1),
		runDone:     make(chan struct{}),
		slotChanged: make(chan struct{}),
	}
}

func (e *Engine) isDeletingLocked(id string) bool {
	return e.deleting != nil && e.deleting[id] > 0
}

func (e *Engine) markDeletingLocked(id string) {
	if e.deleting == nil {
		e.deleting = make(map[string]int)
	}
	e.deleting[id]++
}

func (e *Engine) unmarkDeletingLocked(id string) {
	if e.deleting == nil {
		return
	}
	e.deleting[id]--
	if e.deleting[id] <= 0 {
		delete(e.deleting, id)
	}
}

// CLI exposes the source client to the HTTP layer, which needs it for the
// account tab and the cloud directory browser.
func (e *Engine) CLI() *aliyunpan.CLI {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.cli
}

// Load restores settings and queue state from plugin data.
//
// Nothing here is fatal. The plugin's only purpose is to serve a page an
// administrator uses to fix exactly these things, so refusing to start because
// the stored state is unreadable would remove the one tool for repairing it.
func (e *Engine) Load(ctx context.Context) error {
	stored := settings.Default()
	if _, err := e.host.GetData(ctx, SettingsKey, &stored); err != nil {
		e.logger.Printf("读取插件配置失败，暂时使用默认值: %v", err)
		stored = settings.Default()
	}
	if err := stored.Normalize(); err != nil {
		// A configuration edited by hand into an unusable state must not stop
		// the plugin from starting, or the UI that would fix it never loads.
		e.logger.Printf("插件配置无效，暂时使用默认值: %v", err)
		stored = settings.Default()
	}

	var queue []*Item
	queueLoaded := true
	if _, err := e.host.GetData(ctx, queueKey, &queue); err != nil {
		e.logger.Printf("读取同步队列失败，从空队列开始: %v", err)
		queue = nil
		queueLoaded = false
	}
	var quota QuotaState
	if _, err := e.host.GetData(ctx, quotaKey, &quota); err != nil {
		e.logger.Printf("读取配额计数失败，从零开始: %v", err)
	}

	// Resolved before the lock is taken because StageDir would take it again.
	stageDir := stageDirFor(stored, e.aliyunpanDir())

	e.mu.Lock()
	e.settings = stored
	e.quota = quota
	e.queue = queue[:0:0]
	var finishing []*Item
	for _, item := range queue {
		if item == nil {
			continue
		}
		if item.DriveName == "" {
			item.DriveName = settings.DefaultDriveName
		}
		// Anything that was running when the process stopped is put back in the
		// queue. If it reached StageFinishing, the upload was committed before
		// the crash: complete the state record now and finish the remaining
		// cleanups once the lock is released.
		if item.State == StateRunning {
			if item.Stage == StageFinishing {
				item.State = StateComplete
				item.Stage = StageIdle
				item.FinishedAt = nowMillis()
				item.Uploaded = item.Size
				e.quota.UsedBytes += item.Size
				finishing = append(finishing, item)
			} else {
				item.State = StatePending
				item.Stage = StageIdle
				item.Uploaded = 0
				item.UploadJobID = ""
				item.NextAttemptAt = 0
				if item.Attempts > 0 {
					item.Attempts--
				}
			}
		}
		if item.Active() {
			// The download half is not restarted. Its bytes are on disk behind a
			// checkpoint, so the staging area — not the counter written before
			// the process died — decides how much of this file is really there.
			// Zeroing it here is what made every plugin restart look as though
			// nothing had ever been downloaded.
			item.Downloaded = aliyunpan.StagedDownloadBytes(
				itemStageDir(stageDir, item.ID), item.RemotePath, item.Size,
			)
		}
		e.queue = append(e.queue, item)
	}

	// Self-heal daily quota on restart: if a crash occurred before a completed
	// item's quota write was flushed to disk, recalculate from today's completed items.
	todayStart := stored.Quota.PeriodStart(time.Now()).UnixMilli()
	var todayCompletedBytes int64
	for _, item := range e.queue {
		if item != nil && item.State == StateComplete && item.FinishedAt >= todayStart {
			todayCompletedBytes += item.Size
		}
	}
	healedQuota := false
	if todayCompletedBytes > e.quota.UsedBytes {
		e.quota.UsedBytes = todayCompletedBytes
		healedQuota = true
	}

	e.cli = aliyunpan.New(e.aliyunpanDir(), "")
	e.dirty = false
	e.quotaDirty = false
	e.queueRevision = 0
	e.quotaRevision = 0
	live := make(map[string]bool, len(e.queue))
	for _, item := range e.queue {
		live[item.ID] = true
	}
	e.mu.Unlock()

	// Cleanups make network calls to delete cloud originals and touch the
	// filesystem, so they run outside the lock and with the client initialized.
	if len(finishing) > 0 || healedQuota {
		_ = e.persistNow(ctx)
		_ = e.persistQuotaNow(ctx)
	}

	// Completed items that crashed before local cleanup or cloud DeleteAfter
	// ran are tidied up idempotently on restart.
	for _, item := range e.queue {
		if item.State == StateComplete {
			if stored.DeleteLocalAfterUpload {
				directory := itemStageDir(stageDir, item.ID)
				aliyunpan.DiscardPartialDownload(directory, item.RemotePath)
				_ = os.RemoveAll(directory)
			}
			if item.DeleteAfter {
				e.removeCloudOriginal(ctx, item)
			}
		}
	}

	// Only sweep orphans if the queue was read without errors. A transient
	// database or RPC glitch must not treat every existing download as an
	// orphan and wipe the staging area.
	if queueLoaded {
		e.pruneOrphanStageDirs(stageDir, live)
	}
	return nil
}

// pruneOrphanStageDirs deletes staging directories no queue item claims.
//
// Partial downloads are kept now so an interrupted transfer can resume, which
// means the staging area can accumulate work for items that were deleted,
// cleared, or belonged to a job that no longer exists. Left alone those files
// count against the staging limit forever and eventually stop every new
// transfer from starting — trading one bug for a slower one.
func (e *Engine) pruneOrphanStageDirs(stageDir string, live map[string]bool) {
	itemsRoot := filepath.Join(stageDir, stageItemsDir)
	entries, err := os.ReadDir(itemsRoot)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if !entry.IsDir() || live[entry.Name()] {
			continue
		}
		if err := os.RemoveAll(filepath.Join(itemsRoot, entry.Name())); err != nil {
			e.logf("清理无主暂存目录 %s 失败: %v", entry.Name(), err)
		}
	}
}

// liveItemIDs is the set of items the queue still refers to. Everything else in
// the staging area is an orphan.
func (e *Engine) liveItemIDs() map[string]bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	live := make(map[string]bool, len(e.queue))
	for _, item := range e.queue {
		live[item.ID] = true
	}
	return live
}

// aliyunpanDir is where the source client's compatible credential config and
// staging data live. Older host versions passed the plugin-specific directory
// as dataDir, while newer ones pass its parent; accepting both layouts avoids
// moving an existing aliyunpan_config.json a second time during upgrade.
func (e *Engine) aliyunpanDir() string {
	canonical := filepath.Join(e.dataDir, "aliyunpan")
	if hasCredentialFile(e.dataDir) && !hasCredentialFile(canonical) {
		return e.dataDir
	}
	return canonical
}

func hasCredentialFile(dataDir string) bool {
	info, err := os.Stat(filepath.Join(dataDir, "config", "aliyunpan_config.json"))
	return err == nil && !info.IsDir()
}

// StageDir is where cloud files are staged before being pushed to Telegram.
func (e *Engine) StageDir() string {
	e.mu.Lock()
	configured := e.settings.StageDir
	e.mu.Unlock()
	return stageDirFor(settings.Settings{StageDir: configured}, e.aliyunpanDir())
}

// stageDirFor applies the same rule without touching the engine's lock, so
// startup can resolve the staging area while it is already holding it.
func stageDirFor(document settings.Settings, aliyunpanDir string) string {
	if document.StageDir != "" {
		return document.StageDir
	}
	return filepath.Join(aliyunpanDir, "stage")
}

// Settings returns the scheduler's copy of the configuration document.
func (e *Engine) Settings() settings.Settings {
	e.mu.Lock()
	defer e.mu.Unlock()
	copy := e.settings
	copy.Jobs = append([]settings.Job(nil), e.settings.Jobs...)
	for index := range copy.Jobs {
		job := &copy.Jobs[index]
		job.ExcludeNames = cloneStrings(job.ExcludeNames)
		job.IncludeFiles = cloneStrings(job.IncludeFiles)
		job.IncludeDirs = cloneStrings(job.IncludeDirs)
	}
	return copy
}

// cloneStrings keeps a caller from writing through a returned job into the
// scheduler's own configuration. A nil stays nil so a cloned job still compares
// equal to the one it was cloned from.
func cloneStrings(values []string) []string {
	if values == nil {
		return nil
	}
	return append([]string(nil), values...)
}

// ReloadSettings re-reads the stored document and returns it.
//
// tdrive's own plugin configuration modal writes the same key, so anything the
// page displays has to read through rather than trust the scheduler's cached
// copy — otherwise an edit made in the core modal appears to have been lost
// until the next tick.
func (e *Engine) ReloadSettings(ctx context.Context) settings.Settings {
	e.reloadSettings(ctx)
	return e.Settings()
}

// SaveSettings validates and stores a new configuration document.
func (e *Engine) SaveSettings(ctx context.Context, next settings.Settings) error {
	if err := next.Normalize(); err != nil {
		return err
	}
	if err := e.host.SetData(ctx, SettingsKey, next); err != nil {
		return err
	}
	e.applySettings(next)
	e.Wake()
	return nil
}

// applySettings swaps the document in. BinaryPath is intentionally ignored by
// the source client, but remains in the document for old configuration files.
func (e *Engine) applySettings(next settings.Settings) {
	e.mu.Lock()
	oldJobs := e.settings.Jobs
	changedJobs := !reflect.DeepEqual(oldJobs, next.Jobs)
	downloadConcurrencyChanged := e.settings.DownloadConcurrency != next.DownloadConcurrency
	e.settings = next
	if changedJobs {
		stale := changedJobIDs(oldJobs, next.Jobs)
		if len(stale) > 0 {
			remaining := e.queue[:0]
			for _, item := range e.queue {
				if stale[item.JobID] && item.State != StateRunning && item.State != StateComplete {
					continue
				}
				remaining = append(remaining, item)
			}
			if len(remaining) != len(e.queue) {
				e.queue = remaining
				e.markQueueDirtyLocked()
			}
		}
	}
	if downloadConcurrencyChanged {
		e.signalTransferSlotChangeLocked()
	}
	e.mu.Unlock()
}

func changedJobIDs(oldJobs, newJobs []settings.Job) map[string]bool {
	oldByID := make(map[string]settings.Job, len(oldJobs))
	for _, job := range oldJobs {
		oldByID[job.ID] = job
	}
	changed := make(map[string]bool)
	for _, job := range newJobs {
		old, ok := oldByID[job.ID]
		if !ok || !reflect.DeepEqual(old, job) {
			changed[job.ID] = true
		}
		delete(oldByID, job.ID)
	}
	for id := range oldByID {
		changed[id] = true
	}
	return changed
}

// Run drives the scheduler until ctx is cancelled.
func (e *Engine) Run(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	e.mu.Lock()
	e.runCtx = ctx
	e.mu.Unlock()
	defer func() {
		if e.runDone != nil {
			close(e.runDone)
		}
	}()
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	// Warm the account probe before anyone opens the page, so the first render
	// already knows whether the source credential is linked.
	e.probeMu.Lock()
	probeNeeded := !e.probes.refreshing
	if probeNeeded {
		e.probes.refreshing = true
	}
	e.probeMu.Unlock()
	if probeNeeded {
		if !e.launchProbe(ctx) {
			e.probeMu.Lock()
			e.probes.refreshing = false
			e.probeMu.Unlock()
		}
	}
	e.tick(ctx)
	for {
		select {
		case <-ctx.Done():
			e.mu.Lock()
			e.stopping = true
			e.mu.Unlock()
			if cli := e.CLI(); cli != nil {
				cli.CancelLogin()
			}
			e.workers.Wait()
			e.persistNow(context.WithoutCancel(ctx))
			e.persistQuotaNow(context.WithoutCancel(ctx))
			return
		case <-ticker.C:
			e.tick(ctx)
		case <-e.wake:
			e.tick(ctx)
		}
	}
}

// Wait lets the plugin host finish background work before it kills the child
// process during a restart or shutdown.
func (e *Engine) Wait(ctx context.Context) {
	if e.runDone == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-e.runDone:
	case <-ctx.Done():
	}
}

// backgroundContext is cancelled with the plugin scheduler. Requests that
// start scans or probes are only acknowledgements; their contexts must not
// cancel the work as soon as the HTTP response ends.
func (e *Engine) backgroundContext() context.Context {
	e.mu.Lock()
	ctx := e.runCtx
	e.mu.Unlock()
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

// Wake asks the scheduler to re-evaluate now rather than at the next tick.
func (e *Engine) Wake() {
	select {
	case e.wake <- struct{}{}:
	default:
	}
}

// Pause and Resume stop and restart dispatch without touching the schedule, so
// an administrator can hold everything without losing their window settings.
func (e *Engine) Pause() {
	e.mu.Lock()
	e.paused = true
	e.mu.Unlock()
}

func (e *Engine) Resume() {
	e.mu.Lock()
	e.paused = false
	e.mu.Unlock()
	e.Wake()
}

func (e *Engine) tick(ctx context.Context) {
	if ctx != nil && ctx.Err() != nil {
		return
	}
	e.reloadSettings(ctx)
	e.rollQuota(ctx)
	e.flushIfDue(ctx)

	e.mu.Lock()
	current := e.settings
	paused := e.paused
	sinceScan := time.Since(e.lastScan)
	scanning := e.scanning
	e.mu.Unlock()

	if paused {
		return
	}
	if !e.accountReady() {
		return
	}
	open := current.Schedule.Open(time.Now())
	if open && !scanning && sinceScan >= time.Duration(current.Schedule.IntervalMinutes)*time.Minute {
		e.startScan(ctx)
	}
	if open {
		e.dispatch(ctx)
	}
	e.retryPendingCleanups(ctx)
}

// retryPendingCleanups handles completed items whose cloud DeleteAfter has not
// yet succeeded.
func (e *Engine) retryPendingCleanups(ctx context.Context) {
	if ctx != nil && ctx.Err() != nil {
		return
	}
	e.mu.Lock()
	var pending []*Item
	for _, item := range e.queue {
		if item.State == StateComplete && item.DeleteAfter && !e.isDeletingLocked(item.ID) {
			pending = append(pending, item)
			e.markDeletingLocked(item.ID)
		}
	}
	e.mu.Unlock()

	defer func() {
		e.mu.Lock()
		for _, item := range pending {
			e.unmarkDeletingLocked(item.ID)
		}
		e.mu.Unlock()
	}()

	persistedAny := false
	for _, item := range pending {
		if err := e.removeCloudOriginal(ctx, item); err == nil {
			e.mu.Lock()
			item.DeleteAfter = false
			e.markQueueDirtyLocked()
			e.mu.Unlock()
			persistedAny = true
		}
	}
	if persistedAny {
		_ = e.persistNow(ctx)
	}
}

// reloadSettings re-reads the document every tick because tdrive's own plugin
// configuration modal writes the same key; polling is what keeps the two views
// from diverging without adding a change notification the host does not have.
func (e *Engine) reloadSettings(ctx context.Context) {
	stored := settings.Default()
	if _, err := e.host.GetData(ctx, SettingsKey, &stored); err != nil {
		return
	}
	if err := stored.Normalize(); err != nil {
		return
	}
	e.applySettings(stored)
}

// rollQuota resets the counter when the period named by ResetAt has turned
// over.
func (e *Engine) rollQuota(ctx context.Context) {
	e.mu.Lock()
	day := e.settings.Quota.Day(time.Now())
	changed := e.quota.Day != day
	if changed {
		e.quota = QuotaState{Day: day}
		e.markQuotaDirtyLocked()
	}
	e.mu.Unlock()
	if changed {
		e.persistQuotaNow(ctx)
	}
}

// dispatch starts as many transfers as the larger of the source-download and
// tdrive-upload limits allows. The individual stages have their own gates, so
// a faster source does not make Telegram uploads exceed the host setting.
func (e *Engine) dispatch(ctx context.Context) {
	limits, err := e.host.Settings(ctx)
	if err != nil {
		e.logger.Printf("读取 tdrive 运行参数失败: %v", err)
		return
	}
	uploadWorkers := limits.UploadConcurrency
	if uploadWorkers < 1 {
		uploadWorkers = 1
	}
	e.mu.Lock()
	downloadWorkers := e.settings.DownloadConcurrency
	e.mu.Unlock()
	if downloadWorkers < 1 {
		downloadWorkers = settings.DefaultDownloadConcurrency
	}
	workers := uploadWorkers
	if downloadWorkers > workers {
		workers = downloadWorkers
	}

	for {
		item := e.takeNext(workers)
		if item == nil {
			return
		}
		e.workers.Add(1)
		go func(item *Item) {
			defer e.workers.Done()
			wake := e.transfer(ctx, item, limits)
			e.finishActive()
			if wake {
				e.Wake()
			}
		}(item)
	}
}

// takeNext claims the next item that fits in the remaining quota, or nil.
func (e *Engine) takeNext(workers int) *Item {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.paused || e.active >= workers {
		return nil
	}
	now := nowMillis()
	for _, item := range e.queue {
		if item.State != StatePending {
			continue
		}
		// An item inside its retry backoff is skipped rather than allowed to
		// block the ones behind it. That is the opposite of the quota rule
		// below, and deliberately so: the quota is a shared budget every item
		// is waiting for, whereas one file's backoff says nothing about whether
		// the next file can move.
		if item.NextAttemptAt > now {
			continue
		}
		if e.cancels[item.ID] != nil || e.cancelling[item.ID] || e.isDeletingLocked(item.ID) {
			continue
		}
		job, ok := e.settings.JobByID(item.JobID)
		if !ok || !job.Enabled {
			continue
		}
		if !e.quotaAllowsLocked(item.Size) {
			// Items are taken in order, so a file that does not fit blocks the
			// ones behind it. That is deliberate: reordering around the quota
			// would starve large files indefinitely.
			return nil
		}
		item.State = StateRunning
		item.Stage = StageIdle
		item.StartedAt = nowMillis()
		item.Error = ""
		item.Attempts++
		item.NextAttemptAt = 0
		item.resetSpeed()
		e.active++
		e.reserved += item.Size
		e.markQueueDirtyLocked()
		return item
	}
	return nil
}

// quotaAllowsLocked decides whether one more file may start.
//
// In-flight files are counted as already spent so that several workers cannot
// collectively overshoot a cap each of them individually respected. The
// exception at the end is what keeps the queue from deadlocking on a file that
// is larger than an entire day's allowance.
func (e *Engine) quotaAllowsLocked(size int64) bool {
	limit := e.settings.Quota.DailyBytes
	if limit <= 0 {
		return true
	}
	if e.quota.UsedBytes+e.reserved+size <= limit {
		return true
	}
	return size > limit && e.quota.UsedBytes == 0 && e.reserved == 0
}

func (e *Engine) finishActive() {
	e.mu.Lock()
	e.active--
	if e.active < 0 {
		e.active = 0
	}
	e.mu.Unlock()
}

// acquireDownloadSlot waits until this transfer may download a file from
// Aliyun Drive. Waiting is interruptible so cancelling a queued transfer does
// not leave a goroutine behind until another download finishes.
func (e *Engine) acquireDownloadSlot(ctx context.Context) (func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		e.mu.Lock()
		limit := e.settings.DownloadConcurrency
		if limit < 1 {
			limit = settings.DefaultDownloadConcurrency
		}
		if e.downloadActive < limit {
			e.downloadActive++
			e.mu.Unlock()
			var releaseOnce sync.Once
			return func() {
				releaseOnce.Do(func() {
					e.mu.Lock()
					if e.downloadActive > 0 {
						e.downloadActive--
					}
					e.signalTransferSlotChangeLocked()
					e.mu.Unlock()
				})
			}, nil
		}
		changed := e.transferSlotChangeChannelLocked()
		e.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		}
	}
}

// acquireUploadSlot limits the tdrive upload portion independently from source
// downloads. The host setting is captured for this transfer by dispatch and
// refreshed for newly dispatched transfers on the next scheduler pass.
func (e *Engine) acquireUploadSlot(ctx context.Context, limit int) (func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if limit < 1 {
		limit = 1
	}
	for {
		e.mu.Lock()
		if e.uploadActive < limit {
			e.uploadActive++
			e.mu.Unlock()
			var releaseOnce sync.Once
			return func() {
				releaseOnce.Do(func() {
					e.mu.Lock()
					if e.uploadActive > 0 {
						e.uploadActive--
					}
					e.signalTransferSlotChangeLocked()
					e.mu.Unlock()
				})
			}, nil
		}
		changed := e.transferSlotChangeChannelLocked()
		e.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		}
	}
}

// transferSlotChangeChannelLocked returns the current notification channel.
// It is replaced whenever a slot becomes available so every waiter can safely
// observe the same change without holding the engine mutex while blocking.
func (e *Engine) transferSlotChangeChannelLocked() <-chan struct{} {
	if e.slotChanged == nil {
		e.slotChanged = make(chan struct{})
	}
	return e.slotChanged
}

func (e *Engine) signalTransferSlotChangeLocked() {
	if e.slotChanged == nil {
		e.slotChanged = make(chan struct{})
	}
	close(e.slotChanged)
	e.slotChanged = make(chan struct{})
}

// releaseReservation returns a finished item's unspent allowance.
func (e *Engine) releaseReservation(size int64) {
	e.mu.Lock()
	e.reserved -= size
	if e.reserved < 0 {
		e.reserved = 0
	}
	e.mu.Unlock()
}

func (e *Engine) markQueueDirtyLocked() {
	e.queueRevision++
	e.dirty = true
}

func (e *Engine) markQuotaDirtyLocked() {
	e.quotaRevision++
	e.quotaDirty = true
}

func (e *Engine) setStage(item *Item, stage Stage) {
	e.mu.Lock()
	item.Stage = stage
	e.markQueueDirtyLocked()
	e.mu.Unlock()
}

// noteDownload records the downloader's absolute view of how much of a file is
// on disk.
//
// It is absolute, and it is allowed to go down. The downloader retries one
// chunk at a time, and a chunk whose connection broke has to give back the
// bytes it had read but never committed; a counter that could only rise would
// keep claiming them. It also lets a resumed transfer correct a stale figure
// left by an earlier attempt in either direction on its very first report.
// Only forward movement feeds the speed estimate, since a rollback is not
// throughput.
func (e *Engine) noteDownload(item *Item, done int64) {
	if done < 0 {
		done = 0
	}
	e.mu.Lock()
	if item.Size >= 0 && done > item.Size {
		done = item.Size
	}
	if done == item.Downloaded {
		e.mu.Unlock()
		return
	}
	if done > item.Downloaded {
		item.observe(done-item.Downloaded, time.Now())
	}
	item.Downloaded = done
	e.markQueueDirtyLocked()
	e.mu.Unlock()
}

func (e *Engine) complete(ctx context.Context, item *Item) bool {
	e.mu.Lock()
	if e.cancelling[item.ID] || item.State == StateCancelled {
		e.mu.Unlock()
		return false
	}
	// Guard the item from background cleanup and retry until completion is persisted.
	e.markDeletingLocked(item.ID)
	defer func() {
		e.mu.Lock()
		e.unmarkDeletingLocked(item.ID)
		e.mu.Unlock()
	}()

	prevState := item.State
	prevStage := item.Stage
	prevFinishedAt := item.FinishedAt
	prevError := item.Error
	size := item.Size

	item.State = StateComplete
	item.Stage = StageIdle
	item.FinishedAt = nowMillis()
	item.Error = ""
	item.resetSpeed()
	e.quota.UsedBytes += size
	e.markQueueDirtyLocked()
	e.markQuotaDirtyLocked()
	e.mu.Unlock()

	// Persist the queue first. If this fails, no on-disk change has occurred,
	// so the in-memory state and quota are rolled back cleanly.
	if err := e.persistNow(ctx); err != nil {
		e.logf("保存同步队列失败: %v", err)
		e.mu.Lock()
		item.State = prevState
		item.Stage = prevStage
		item.FinishedAt = prevFinishedAt
		item.Error = prevError
		e.quota.UsedBytes -= size
		e.markQueueDirtyLocked()
		e.markQuotaDirtyLocked()
		e.mu.Unlock()
		return false
	}
	_ = e.persistQuotaNow(ctx)

	e.mu.Lock()
	dropped := e.trimHistoryLocked()
	if len(dropped) > 0 {
		e.markQueueDirtyLocked()
	}
	e.mu.Unlock()

	if len(dropped) > 0 {
		if err := e.persistNow(ctx); err == nil {
			e.discardStagedWorkFor(dropped)
		} else {
			e.logf("保存历史裁剪队列失败: %v", err)
		}
	}
	return true
}

// fail records a transfer that will not be attempted again.
//
// It deliberately leaves the staged file alone. Everything downloaded so far
// stays on disk and on the 下载文件 page, so an operator who fixes whatever
// broke can retry the row without paying for the download a second time.
func (e *Engine) fail(ctx context.Context, item *Item, err error) {
	e.logf("同步 %s 失败: %v", item.RemotePath, err)
	e.mu.Lock()
	if e.cancelling[item.ID] || item.State == StateCancelled {
		e.mu.Unlock()
		return
	}
	item.State = StateFailed
	item.Stage = StageIdle
	item.FinishedAt = nowMillis()
	item.Error = err.Error()
	item.NextAttemptAt = 0
	item.resetSpeed()
	e.markQueueDirtyLocked()
	dropped := e.trimHistoryLocked()
	e.mu.Unlock()
	e.discardStagedWorkFor(dropped)
	e.persistNow(ctx)
}

// trimHistoryLocked keeps finished items from growing without bound. The
// active part of the queue is never trimmed.
//
// It returns the items it dropped so the caller can free their staging
// directories outside the lock. A failed transfer now keeps its local file, so
// forgetting the row without the file would leave bytes on disk that nothing
// refers to and nobody can see until the next restart sweeps them.
func (e *Engine) trimHistoryLocked() []*Item {
	finished := make([]*Item, 0, len(e.queue))
	for _, item := range e.queue {
		if item.Finished() && !item.DeleteAfter && e.cancels[item.ID] == nil && !e.cancelling[item.ID] && !e.isDeletingLocked(item.ID) {
			finished = append(finished, item)
		}
	}
	if len(finished) <= historyLimit {
		return nil
	}
	sort.Slice(finished, func(a, b int) bool { return finished[a].FinishedAt < finished[b].FinishedAt })
	dropped := finished[:len(finished)-historyLimit]
	drop := make(map[*Item]bool, len(dropped))
	for _, item := range dropped {
		drop[item] = true
	}
	kept := e.queue[:0]
	for _, item := range e.queue {
		if !drop[item] {
			kept = append(kept, item)
		}
	}
	e.queue = kept
	return dropped
}

// logf tolerates an engine that was built without a logger, which is how the
// unit tests construct one.
func (e *Engine) logf(format string, args ...any) {
	if e.logger == nil {
		return
	}
	e.logger.Printf(format, args...)
}

// discardStagedWorkFor frees the staging directories of items that have left
// the queue.
func (e *Engine) discardStagedWorkFor(items []*Item) {
	for _, item := range items {
		e.discardStagedWork(item)
	}
}

// persistNow writes the queue unconditionally.
func (e *Engine) persistNow(ctx context.Context) error {
	e.persistMu.Lock()
	defer e.persistMu.Unlock()
	e.mu.Lock()
	snapshot := cloneQueue(e.queue)
	revision := e.queueRevision
	e.mu.Unlock()
	writeCtx, cancel := persistContext(ctx)
	defer cancel()
	if err := e.host.SetData(writeCtx, queueKey, snapshot); err != nil {
		e.logf("保存同步队列失败: %v", err)
		return err
	}
	e.mu.Lock()
	if e.queueRevision == revision {
		e.lastPersist = time.Now()
		e.dirty = false
	}
	e.mu.Unlock()
	return nil
}

func (e *Engine) persistQuotaNow(ctx context.Context) error {
	e.persistMu.Lock()
	defer e.persistMu.Unlock()
	e.mu.Lock()
	snapshot := e.quota
	revision := e.quotaRevision
	e.mu.Unlock()
	writeCtx, cancel := persistContext(ctx)
	defer cancel()
	if err := e.host.SetData(writeCtx, quotaKey, snapshot); err != nil {
		e.logf("保存配额计数失败: %v", err)
		return err
	}
	e.mu.Lock()
	if e.quotaRevision == revision {
		e.quotaDirty = false
	}
	e.mu.Unlock()
	return nil
}

func cloneQueue(queue []*Item) []*Item {
	cloned := make([]*Item, 0, len(queue))
	for _, item := range queue {
		if item == nil {
			continue
		}
		copy := *item
		cloned = append(cloned, &copy)
	}
	return cloned
}

func persistContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	// A scheduler shutdown passes an already detached context, so its final
	// snapshot can outlive cancellation. An HTTP action, however, must retain
	// its request deadline: otherwise the plugin RPC appears hung to the host
	// while it continues persisting after the host has timed out the request.
	if _, ok := ctx.Deadline(); ok {
		return withTimeout(ctx, 30*time.Second)
	}
	return withTimeout(context.WithoutCancel(ctx), 30*time.Second)
}

func withTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= timeout {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}

// flushIfDue writes the queue if progress has moved since the last write.
func (e *Engine) flushIfDue(ctx context.Context) {
	e.mu.Lock()
	due := e.dirty && time.Since(e.lastPersist) >= persistInterval
	quotaDue := e.quotaDirty && time.Since(e.lastPersist) >= persistInterval
	e.mu.Unlock()
	if due {
		e.persistNow(ctx)
	}
	if quotaDue {
		e.persistQuotaNow(ctx)
	}
}

// Retry puts a failed or cancelled item back in the queue.
//
// It also gives the item a fresh attempt budget and clears any backoff. Asking
// for a retry by hand is a statement that whatever was wrong has been dealt
// with, and leaving the spent counter in place would make the item fail again
// on its very next error.
func (e *Engine) Retry(ctx context.Context, ids ...string) error {
	if len(ids) == 0 {
		return errors.New("没有指定队列项")
	}
	wanted := make(map[string]bool, len(ids))
	for _, id := range ids {
		wanted[id] = true
	}

	e.mu.Lock()
	matched, requeued := 0, 0
	for _, item := range e.queue {
		if !wanted[item.ID] {
			continue
		}
		matched++
		// A running item is already doing what a retry would ask for. A
		// complete item has already moved its file and must not be retried
		// into an upload loop that recharges the daily quota. An item with an
		// active transfer or staged-file deletion in flight is also skipped.
		if item.State == StateRunning || item.State == StateComplete ||
			e.cancels[item.ID] != nil || e.cancelling[item.ID] || e.isDeletingLocked(item.ID) {
			continue
		}
		item.State = StatePending
		item.Stage = StageIdle
		item.Error = ""
		item.Downloaded = 0
		item.Uploaded = 0
		item.UploadJobID = ""
		item.FinishedAt = 0
		item.Attempts = 0
		item.NextAttemptAt = 0
		requeued++
	}
	if requeued > 0 {
		e.markQueueDirtyLocked()
	}
	e.mu.Unlock()

	if matched == 0 {
		return errors.New("找不到这些队列项")
	}
	if requeued == 0 {
		return errors.New("选中的项目都在进行中，无法重试")
	}
	e.persistNow(ctx)
	e.Wake()
	return nil
}

// Cancel stops the named items.
//
// A running transfer is interrupted rather than left to finish. Refusing to
// cancel it meant that pressing 取消 on the file actually moving bytes did
// nothing at all, so the queue kept uploading something nobody wanted while the
// page offered no way to stop it. Its context is cancelled, and the upload job
// it opened in tdrive is aborted so the drive's own record agrees with this
// one; the transfer goroutine then finds the item already marked cancelled and
// leaves it alone.
func (e *Engine) Cancel(ctx context.Context, ids ...string) error {
	if len(ids) == 0 {
		return errors.New("没有指定队列项")
	}
	wanted := make(map[string]bool, len(ids))
	for _, id := range ids {
		wanted[id] = true
	}

	type stopping struct {
		item   *Item
		jobID  string
		cancel context.CancelFunc
	}

	e.mu.Lock()
	if e.cancels == nil {
		e.cancels = make(map[string]context.CancelFunc)
	}
	if e.cancelling == nil {
		e.cancelling = make(map[string]bool)
	}
	if e.deleting == nil {
		e.deleting = make(map[string]int)
	}
	matched, cancelled := 0, 0
	var interrupted []stopping
	var nonRunning []*Item
	for _, item := range e.queue {
		if !wanted[item.ID] || item.State == StateComplete || item.State == StateCancelled {
			if wanted[item.ID] {
				matched++
			}
			continue
		}
		matched++
		cancelled++
		if item.State == StateRunning || e.cancels[item.ID] != nil || e.cancelling[item.ID] {
			e.cancelling[item.ID] = true
			interrupted = append(interrupted, stopping{
				item: item, jobID: item.UploadJobID, cancel: e.cancels[item.ID],
			})
		} else {
			nonRunning = append(nonRunning, item)
			e.markDeletingLocked(item.ID)
		}
		item.State = StateCancelled
		item.Stage = StageIdle
		item.Error = ""
		item.NextAttemptAt = 0
		item.FinishedAt = nowMillis()
	}
	if cancelled > 0 {
		e.markQueueDirtyLocked()
	}
	e.mu.Unlock()

	if matched == 0 {
		return errors.New("找不到这些队列项")
	}
	if cancelled == 0 {
		return errors.New("选中的项目都已经结束了")
	}

	// Non-running items have no active transfer goroutine to discard their
	// partial work on exit, so they are discarded here explicitly.
	e.discardStagedWorkFor(nonRunning)
	if len(nonRunning) > 0 {
		e.mu.Lock()
		for _, item := range nonRunning {
			e.unmarkDeletingLocked(item.ID)
		}
		e.mu.Unlock()
	}

	for _, stopped := range interrupted {
		if stopped.cancel != nil {
			stopped.cancel()
		}
		// Aborting the drive's job is what stops tdrive from showing a transfer
		// that is still running behind a queue entry that says cancelled.
		if stopped.jobID != "" {
			e.abort(ctx, stopped.item, stopped.jobID, "cancelled", "cancelled")
		}
	}
	e.persistNow(ctx)
	return nil
}

// ClearFinished empties the history part of the queue. With ids given it
// removes only those, which is the per-row and multi-select delete.
//
// Removing a row also removes whatever it left in the staging area. A failed
// transfer keeps its download so it can be retried cheaply, so the row is the
// only handle anyone has on those bytes — dropping it silently would strand
// them against the staging limit until the next restart.
func (e *Engine) ClearFinished(ctx context.Context, ids ...string) {
	var wanted map[string]bool
	if len(ids) > 0 {
		wanted = make(map[string]bool, len(ids))
		for _, id := range ids {
			wanted[id] = true
		}
	}

	e.mu.Lock()
	kept := e.queue[:0]
	var dropped []*Item
	for _, item := range e.queue {
		// Only finished rows with no active goroutine in flight are history.
		// Dropping a running or still-aborting one would leave its goroutine
		// writing to an item nothing can show any more.
		if !item.Finished() || e.cancels[item.ID] != nil || e.cancelling[item.ID] || (wanted != nil && !wanted[item.ID]) {
			kept = append(kept, item)
			continue
		}
		dropped = append(dropped, item)
	}
	e.queue = kept
	e.markQueueDirtyLocked()
	e.mu.Unlock()
	e.discardStagedWorkFor(dropped)
	e.persistNow(ctx)
}

// watchItem derives the context one transfer runs on and registers it so Cancel
// can interrupt it.
func (e *Engine) watchItem(ctx context.Context, id string) (context.Context, func()) {
	ctx, cancel := context.WithCancel(ctx)
	e.mu.Lock()
	if e.cancels == nil {
		e.cancels = make(map[string]context.CancelFunc)
	}
	if e.cancelling == nil {
		e.cancelling = make(map[string]bool)
	}
	if e.cancelling[id] {
		// Cancel arrived in the gap between takeNext and watchItem. Cancel
		// the context immediately so the transfer aborts before starting any
		// network work.
		cancel()
	}
	e.cancels[id] = cancel
	e.mu.Unlock()
	return ctx, func() {
		cancel()
		e.mu.Lock()
		delete(e.cancels, id)
		delete(e.cancelling, id)
		e.mu.Unlock()
	}
}

// stoppedByCancel reports whether this transfer ended because somebody
// cancelled it, in which case the item's state is already recorded and the
// error it returned is only the shape the cancellation took.
func (e *Engine) stoppedByCancel(id string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.cancelling[id]
}

// stagedBytesExcluding measures the staging tree while ignoring the private
// directories of items that already hold a reservation.
//
// Those two numbers describe the same bytes. A download pre-allocates its
// .part file at the file's final size so its chunk workers can write straight
// to their own offsets, which means the whole file shows up in a directory walk
// from the first moment — while its reservation is still outstanding. Adding
// the walk to the reservations therefore counted every in-flight download
// twice, and the second concurrent transfer was refused for lack of room long
// before the limit was really reached. Skipping the reserved items' own
// directories makes the walk mean "everything nobody has already accounted
// for".
func (e *Engine) stagedBytesExcluding(stageDir string, skipItems map[string]bool) int64 {
	itemsRoot := filepath.Join(stageDir, stageItemsDir)
	var total int64
	_ = filepath.WalkDir(stageDir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return nil //nolint:nilerr // an unreadable staging tree is reported by the transfer itself
		}
		if entry.IsDir() {
			if len(skipItems) > 0 &&
				filepath.Dir(path) == itemsRoot &&
				skipItems[entry.Name()] {
				return filepath.SkipDir
			}
			return nil
		}
		if info, statErr := entry.Info(); statErr == nil {
			total += info.Size()
		}
		return nil
	})
	return total
}

// stageLimit is the cap on the staging area, following tdrive's own staged
// download budget unless the plugin was given its own.
func (e *Engine) stageLimit(limits hostapi.RuntimeSettings) int64 {
	e.mu.Lock()
	configured := e.settings.StageLimitBytes
	e.mu.Unlock()
	if configured > 0 {
		return configured
	}
	return limits.CacheLimit
}

func (e *Engine) reserveStageRoom(itemID string, size int64, stageDir string, limits hostapi.RuntimeSettings) (func(), error) {
	if size < 0 {
		return nil, fmt.Errorf("文件大小不能为负数: %d", size)
	}
	limit := e.stageLimit(limits)
	if limit <= 0 {
		return func() {}, nil
	}

	e.mu.Lock()
	reservedItems := make(map[string]bool, len(e.stageReservations)+1)
	for id := range e.stageReservations {
		reservedItems[id] = true
	}
	// This item's own directory may already hold a partially downloaded file
	// from an earlier attempt. Those bytes are covered by the reservation about
	// to be taken, so counting them as well would make a resumed transfer look
	// twice as expensive as a fresh one.
	if itemID != "" {
		reservedItems[itemID] = true
	}
	e.mu.Unlock()

	used := e.stagedBytesExcluding(stageDir, reservedItems)

	e.mu.Lock()
	reserved := e.stageReserved
	if used+reserved+size > limit && used+reserved > 0 {
		e.mu.Unlock()
		return nil, fmt.Errorf("%w：已占用 %d 字节，上限 %d 字节，本文件 %d 字节；等待其它文件完成后重试",
			ErrStageRoom, used+reserved, limit, size)
	}
	e.stageReserved += size
	if itemID != "" {
		if e.stageReservations == nil {
			e.stageReservations = make(map[string]int64)
		}
		e.stageReservations[itemID] += size
	}
	e.mu.Unlock()

	var releaseOnce sync.Once
	return func() {
		releaseOnce.Do(func() {
			e.mu.Lock()
			e.stageReserved -= size
			if e.stageReserved < 0 {
				e.stageReserved = 0
			}
			if itemID != "" && e.stageReservations != nil {
				e.stageReservations[itemID] -= size
				if e.stageReservations[itemID] <= 0 {
					delete(e.stageReservations, itemID)
				}
			}
			e.mu.Unlock()
		})
	}, nil
}
