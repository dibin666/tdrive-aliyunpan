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

	mu            sync.Mutex
	settings      settings.Settings
	queue         []*Item
	quota         QuotaState
	active        int
	reserved      int64
	stageReserved int64
	lastScan      time.Time
	scanning      bool
	scanError     string
	paused        bool
	stopping      bool
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

	wake chan struct{}
	// running counts in-flight transfers so Close can wait for them.
	workers sync.WaitGroup
	runDone chan struct{}

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
		host:       host,
		dataDir:    dataDir,
		logger:     logger,
		settings:   settings.Default(),
		queue:      []*Item{},
		cancels:    make(map[string]context.CancelFunc),
		cancelling: make(map[string]bool),
		wake:       make(chan struct{}, 1),
		runDone:    make(chan struct{}),
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
	if _, err := e.host.GetData(ctx, queueKey, &queue); err != nil {
		e.logger.Printf("读取同步队列失败，从空队列开始: %v", err)
		queue = nil
	}
	var quota QuotaState
	if _, err := e.host.GetData(ctx, quotaKey, &quota); err != nil {
		e.logger.Printf("读取配额计数失败，从零开始: %v", err)
	}

	e.mu.Lock()
	e.settings = stored
	e.quota = quota
	e.queue = queue[:0:0]
	for _, item := range queue {
		if item == nil {
			continue
		}
		if item.DriveName == "" {
			item.DriveName = settings.DefaultDriveName
		}
		// Anything that was running when the process stopped is put back in
		// the queue: segments already in Telegram are re-sent, which is
		// wasteful but always correct, whereas trusting a counter written
		// before a crash is not.
		if item.State == StateRunning {
			item.State = StatePending
			item.Stage = StageIdle
			item.Downloaded = 0
			item.Uploaded = 0
			item.UploadJobID = ""
		}
		e.queue = append(e.queue, item)
	}
	e.cli = aliyunpan.New(e.aliyunpanDir(), stored.BinaryPath)
	e.dirty = false
	e.quotaDirty = false
	e.queueRevision = 0
	e.quotaRevision = 0
	e.mu.Unlock()
	return nil
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
	if configured != "" {
		return configured
	}
	return filepath.Join(e.aliyunpanDir(), "stage")
}

// Settings returns the scheduler's copy of the configuration document.
func (e *Engine) Settings() settings.Settings {
	e.mu.Lock()
	defer e.mu.Unlock()
	copy := e.settings
	copy.Jobs = append([]settings.Job(nil), e.settings.Jobs...)
	for index := range copy.Jobs {
		copy.Jobs[index].ExcludeNames = append([]string(nil), copy.Jobs[index].ExcludeNames...)
	}
	return copy
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

// dispatch starts as many transfers as the drive's own upload concurrency and
// the daily quota allow.
func (e *Engine) dispatch(ctx context.Context) {
	limits, err := e.host.Settings(ctx)
	if err != nil {
		e.logger.Printf("读取 tdrive 运行参数失败: %v", err)
		return
	}
	// Whole-file parallelism follows tdrive's own setting. Inside a file the
	// segments stay sequential: UploadThreads, UploadPartSize and RateLimit
	// already parallelise and pace the Telegram side, and a second layer of
	// concurrency here would only fight them.
	workers := limits.UploadConcurrency
	if workers < 1 {
		workers = 1
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
	for _, item := range e.queue {
		if item.State != StatePending {
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

func (e *Engine) noteDownload(item *Item, done int64) {
	e.mu.Lock()
	if done > item.Downloaded {
		if item.Size >= 0 && done > item.Size {
			done = item.Size
		}
		if done <= item.Downloaded {
			e.mu.Unlock()
			return
		}
		item.observe(done-item.Downloaded, time.Now())
		item.Downloaded = done
		e.markQueueDirtyLocked()
	}
	e.mu.Unlock()
}

func (e *Engine) complete(ctx context.Context, item *Item) {
	e.mu.Lock()
	item.State = StateComplete
	item.Stage = StageIdle
	item.FinishedAt = nowMillis()
	item.Error = ""
	item.resetSpeed()
	e.quota.UsedBytes += item.Size
	e.markQueueDirtyLocked()
	e.markQuotaDirtyLocked()
	e.trimHistoryLocked()
	e.mu.Unlock()
	e.persistQuotaNow(ctx)
	e.persistNow(ctx)
}

func (e *Engine) fail(ctx context.Context, item *Item, err error) {
	e.logger.Printf("同步 %s 失败: %v", item.RemotePath, err)
	e.mu.Lock()
	item.State = StateFailed
	item.Stage = StageIdle
	item.FinishedAt = nowMillis()
	item.Error = err.Error()
	item.resetSpeed()
	e.markQueueDirtyLocked()
	e.trimHistoryLocked()
	e.mu.Unlock()
	e.persistNow(ctx)
}

// trimHistoryLocked keeps finished items from growing without bound. The
// active part of the queue is never trimmed.
func (e *Engine) trimHistoryLocked() {
	finished := make([]*Item, 0, len(e.queue))
	for _, item := range e.queue {
		if item.Finished() {
			finished = append(finished, item)
		}
	}
	if len(finished) <= historyLimit {
		return
	}
	sort.Slice(finished, func(a, b int) bool { return finished[a].FinishedAt < finished[b].FinishedAt })
	drop := make(map[*Item]bool, len(finished)-historyLimit)
	for _, item := range finished[:len(finished)-historyLimit] {
		drop[item] = true
	}
	kept := e.queue[:0]
	for _, item := range e.queue {
		if !drop[item] {
			kept = append(kept, item)
		}
	}
	e.queue = kept
}

// persistNow writes the queue unconditionally.
func (e *Engine) persistNow(ctx context.Context) {
	e.persistMu.Lock()
	defer e.persistMu.Unlock()
	e.mu.Lock()
	snapshot := cloneQueue(e.queue)
	revision := e.queueRevision
	e.mu.Unlock()
	writeCtx, cancel := persistContext(ctx)
	defer cancel()
	if err := e.host.SetData(writeCtx, queueKey, snapshot); err != nil {
		e.logger.Printf("保存同步队列失败: %v", err)
		return
	}
	e.mu.Lock()
	if e.queueRevision == revision {
		e.lastPersist = time.Now()
		e.dirty = false
	}
	e.mu.Unlock()
}

func (e *Engine) persistQuotaNow(ctx context.Context) {
	e.persistMu.Lock()
	defer e.persistMu.Unlock()
	e.mu.Lock()
	snapshot := e.quota
	revision := e.quotaRevision
	e.mu.Unlock()
	writeCtx, cancel := persistContext(ctx)
	defer cancel()
	if err := e.host.SetData(writeCtx, quotaKey, snapshot); err != nil {
		e.logger.Printf("保存配额计数失败: %v", err)
		return
	}
	e.mu.Lock()
	if e.quotaRevision == revision {
		e.quotaDirty = false
	}
	e.mu.Unlock()
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
		// A running item is already doing what a retry would ask for. Skipping
		// it rather than refusing the whole request is what makes selecting a
		// screenful of rows and pressing retry behave sensibly.
		if item.State == StateRunning {
			continue
		}
		item.State = StatePending
		item.Stage = StageIdle
		item.Error = ""
		item.Downloaded = 0
		item.Uploaded = 0
		item.UploadJobID = ""
		item.FinishedAt = 0
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
	matched, cancelled := 0, 0
	var interrupted []stopping
	for _, item := range e.queue {
		if !wanted[item.ID] || item.Finished() {
			if wanted[item.ID] {
				matched++
			}
			continue
		}
		matched++
		cancelled++
		if item.State == StateRunning {
			e.cancelling[item.ID] = true
			interrupted = append(interrupted, stopping{
				item: item, jobID: item.UploadJobID, cancel: e.cancels[item.ID],
			})
		}
		item.State = StateCancelled
		item.Stage = StageIdle
		item.Error = ""
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
	for _, item := range e.queue {
		// Only finished rows are history. Dropping a running one would leave its
		// goroutine writing to an item nothing can show any more.
		if !item.Finished() || (wanted != nil && !wanted[item.ID]) {
			kept = append(kept, item)
		}
	}
	e.queue = kept
	e.markQueueDirtyLocked()
	e.mu.Unlock()
	e.persistNow(ctx)
}

// watchItem derives the context one transfer runs on and registers it so Cancel
// can interrupt it.
func (e *Engine) watchItem(ctx context.Context, id string) (context.Context, func()) {
	ctx, cancel := context.WithCancel(ctx)
	e.mu.Lock()
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

// stagedBytes is how much disk the staging area currently holds.
func (e *Engine) stagedBytes() int64 {
	return e.stagedBytesAt(e.StageDir())
}

func (e *Engine) stagedBytesAt(stageDir string) int64 {
	var total int64
	_ = filepath.WalkDir(stageDir, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return nil //nolint:nilerr // an unreadable staging tree is reported by the transfer itself
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

func (e *Engine) reserveStageRoom(size int64, stageDir string, limits hostapi.RuntimeSettings) (func(), error) {
	if size < 0 {
		return nil, fmt.Errorf("文件大小不能为负数: %d", size)
	}
	limit := e.stageLimit(limits)
	if limit <= 0 {
		return func() {}, nil
	}
	used := e.stagedBytesAt(stageDir)
	e.mu.Lock()
	reserved := e.stageReserved
	if used+reserved+size > limit && used+reserved > 0 {
		e.mu.Unlock()
		return nil, fmt.Errorf("%w：已占用 %d 字节，上限 %d 字节，本文件 %d 字节；等待其它文件完成后重试",
			ErrStageRoom, used+reserved, limit, size)
	}
	e.stageReserved += size
	e.mu.Unlock()

	released := false
	return func() {
		if released {
			return
		}
		released = true
		e.mu.Lock()
		e.stageReserved -= size
		if e.stageReserved < 0 {
			e.stageReserved = 0
		}
		e.mu.Unlock()
	}, nil
}

// checkStageRoom keeps the old inspection-only helper for callers and tests;
// transfers use reserveStageRoom so concurrent downloads also reserve space.
func (e *Engine) checkStageRoom(item *Item, stageDir string, limits hostapi.RuntimeSettings) error {
	release, err := e.reserveStageRoom(item.Size, stageDir, limits)
	if release != nil {
		release()
	}
	return err
}
