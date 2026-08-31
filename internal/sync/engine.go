package sync

import (
	"context"
	"errors"
	"log"
	"os"
	"path/filepath"
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

	mu        sync.Mutex
	settings  settings.Settings
	queue     []*Item
	quota     QuotaState
	active    int
	reserved  int64
	lastScan  time.Time
	scanning  bool
	scanError string
	paused    bool
	// lastPersist throttles progress writes; dirty records that a write is
	// owed.
	lastPersist time.Time
	dirty       bool

	wake chan struct{}
	// running counts in-flight transfers so Close can wait for them.
	workers sync.WaitGroup

	// probeMu guards the cached aliyunpan account and binary state. They are
	// cached because both shell out and one of them makes a network call,
	// which must not happen on the sync page's poll interval.
	probeMu sync.Mutex
	probes  probeCache
	// installing and installError report the background binary download.
	installing   bool
	installError string
}

// New builds an engine. It does not touch the host; call Load first.
func New(host *hostapi.Client, dataDir string, logger *log.Logger) *Engine {
	return &Engine{
		host:     host,
		dataDir:  dataDir,
		logger:   logger,
		settings: settings.Default(),
		queue:    []*Item{},
		wake:     make(chan struct{}, 1),
	}
}

// CLI exposes the aliyunpan wrapper to the HTTP layer, which needs it for the
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
	e.mu.Unlock()
	return nil
}

// aliyunpanDir is where the CLI's binary and its isolated config live.
func (e *Engine) aliyunpanDir() string { return filepath.Join(e.dataDir, "aliyunpan") }

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
	return e.settings
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

// applySettings swaps the document in, rebuilding the CLI when the binary path
// changed so an operator's correction takes effect without a restart.
func (e *Engine) applySettings(next settings.Settings) {
	e.mu.Lock()
	rebuild := e.cli == nil || e.settings.BinaryPath != next.BinaryPath
	e.settings = next
	if rebuild {
		e.cli = aliyunpan.New(e.aliyunpanDir(), next.BinaryPath)
	}
	e.mu.Unlock()
}

// Run drives the scheduler until ctx is cancelled.
func (e *Engine) Run(ctx context.Context) {
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	// Warm the account and binary probe before anyone opens the page, so the
	// first render already knows whether the CLI is installed and linked
	// rather than reporting "not installed" until its own refresh lands.
	e.workers.Add(1)
	go func() {
		defer e.workers.Done()
		e.refreshProbe(ctx)
	}()
	e.tick(ctx)
	for {
		select {
		case <-ctx.Done():
			e.workers.Wait()
			e.persistNow(context.WithoutCancel(ctx))
			return
		case <-ticker.C:
			e.tick(ctx)
		case <-e.wake:
			e.tick(ctx)
		}
	}
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
	}
	quota := e.quota
	e.mu.Unlock()
	if changed {
		if err := e.host.SetData(ctx, quotaKey, quota); err != nil {
			e.logger.Printf("保存配额计数失败: %v", err)
		}
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
			e.transfer(ctx, item, limits)
			e.finishActive()
			e.Wake()
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
		e.dirty = true
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

func (e *Engine) setStage(item *Item, stage Stage) {
	e.mu.Lock()
	item.Stage = stage
	e.dirty = true
	e.mu.Unlock()
}

func (e *Engine) noteDownload(item *Item, done int64) {
	e.mu.Lock()
	if done > item.Downloaded {
		item.observe(done-item.Downloaded, time.Now())
		item.Downloaded = done
	}
	e.dirty = true
	e.mu.Unlock()
}

func (e *Engine) complete(ctx context.Context, item *Item) {
	e.mu.Lock()
	item.State = StateComplete
	item.Stage = StageIdle
	item.FinishedAt = nowMillis()
	item.Error = ""
	item.resetSpeed()
	e.trimHistoryLocked()
	e.mu.Unlock()
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
	e.mu.Lock()
	snapshot := append([]*Item(nil), e.queue...)
	e.lastPersist = time.Now()
	e.dirty = false
	e.mu.Unlock()
	if err := e.host.SetData(ctx, queueKey, snapshot); err != nil {
		e.logger.Printf("保存同步队列失败: %v", err)
	}
}

// flushIfDue writes the queue if progress has moved since the last write.
func (e *Engine) flushIfDue(ctx context.Context) {
	e.mu.Lock()
	due := e.dirty && time.Since(e.lastPersist) >= persistInterval
	e.mu.Unlock()
	if due {
		e.persistNow(ctx)
	}
}

// Retry puts a failed or cancelled item back in the queue.
func (e *Engine) Retry(ctx context.Context, id string) error {
	e.mu.Lock()
	var target *Item
	for _, item := range e.queue {
		if item.ID == id {
			target = item
			break
		}
	}
	if target == nil {
		e.mu.Unlock()
		return errors.New("找不到这个队列项")
	}
	if target.State == StateRunning {
		e.mu.Unlock()
		return errors.New("正在进行中的项目不能重试")
	}
	target.State = StatePending
	target.Stage = StageIdle
	target.Error = ""
	target.Downloaded = 0
	target.Uploaded = 0
	target.UploadJobID = ""
	target.FinishedAt = 0
	e.mu.Unlock()
	e.persistNow(ctx)
	e.Wake()
	return nil
}

// Cancel drops a queued item. A running one is left alone: interrupting it
// would strand a partial upload in the storage channel, and it will finish
// within one file's worth of time anyway.
func (e *Engine) Cancel(ctx context.Context, id string) error {
	e.mu.Lock()
	var target *Item
	for _, item := range e.queue {
		if item.ID == id {
			target = item
			break
		}
	}
	if target == nil {
		e.mu.Unlock()
		return errors.New("找不到这个队列项")
	}
	if target.State == StateRunning {
		e.mu.Unlock()
		return errors.New("正在进行中的项目无法取消，等它结束后再操作")
	}
	target.State = StateCancelled
	target.Stage = StageIdle
	target.FinishedAt = nowMillis()
	e.mu.Unlock()
	e.persistNow(ctx)
	return nil
}

// ClearFinished empties the history part of the queue.
func (e *Engine) ClearFinished(ctx context.Context) {
	e.mu.Lock()
	kept := e.queue[:0]
	for _, item := range e.queue {
		if !item.Finished() {
			kept = append(kept, item)
		}
	}
	e.queue = kept
	e.mu.Unlock()
	e.persistNow(ctx)
}

// stagedBytes is how much disk the staging area currently holds.
func (e *Engine) stagedBytes() int64 {
	var total int64
	_ = filepath.WalkDir(e.StageDir(), func(path string, entry os.DirEntry, err error) error {
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
