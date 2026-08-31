package sync

import (
	"context"
	"errors"
	"time"

	"github.com/dibin/tdrive-aliyunpan/internal/aliyunpan"
	"github.com/dibin/tdrive-aliyunpan/internal/settings"
)

// Snapshot is everything the sync page renders. It is assembled in one call
// because the page polls: tdrive's plugin routes buffer a whole response
// before returning it, so a plugin cannot stream events the way the core's
// 传输 page does, and several small endpoints would just multiply the polls.
type Snapshot struct {
	Rows     []Row                 `json:"rows"`
	Summary  Summary               `json:"summary"`
	Quota    QuotaView             `json:"quota"`
	Schedule ScheduleView          `json:"schedule"`
	Scan     ScanView              `json:"scan"`
	Limits   LimitsView            `json:"limits"`
	Account  AccountView           `json:"account"`
	Binary   aliyunpan.BinaryState `json:"binary"`
	Login    aliyunpan.LoginState  `json:"login"`
	Jobs     []settings.Job        `json:"jobs"`
	Status   string                `json:"status"`
	Paused   bool                  `json:"paused"`
	Now      int64                 `json:"now"`
	StageDir string                `json:"stageDir"`
	// Installing and InstallError track the background binary download. It
	// cannot be awaited inside a request: the host gives a plugin route thirty
	// seconds, and a slow link needs more.
	Installing   bool   `json:"installing"`
	InstallError string `json:"installError,omitempty"`
}

// Row mirrors one line of tdrive's 传输 page.
type Row struct {
	ID         string `json:"id"`
	JobID      string `json:"jobId"`
	JobName    string `json:"jobName"`
	DriveName  string `json:"driveName,omitempty"`
	Name       string `json:"name"`
	RemotePath string `json:"remotePath"`
	TargetPath string `json:"targetPath"`
	State      State  `json:"state"`
	Stage      Stage  `json:"stage,omitempty"`
	Total      int64  `json:"total"`
	Done       int64  `json:"done"`
	Downloaded int64  `json:"downloaded"`
	Error      string `json:"error,omitempty"`
	Attempts   int    `json:"attempts"`
	// Speed is bytes per second, and SpeedAt is when it was measured. The page
	// applies its own freshness window to decide whether to show it, the same
	// way the core page treats its live snapshots.
	Speed      float64 `json:"speed,omitempty"`
	SpeedAt    int64   `json:"speedAt,omitempty"`
	CreatedAt  int64   `json:"createdAt"`
	StartedAt  int64   `json:"startedAt,omitempty"`
	FinishedAt int64   `json:"finishedAt,omitempty"`
}

// Summary feeds the bar above the list.
type Summary struct {
	Active        int     `json:"active"`
	Pending       int     `json:"pending"`
	Failed        int     `json:"failed"`
	UploadSpeed   float64 `json:"uploadSpeed"`
	DownloadSpeed float64 `json:"downloadSpeed"`
	TodayCount    int     `json:"todayCount"`
	TodayBytes    int64   `json:"todayBytes"`
}

// QuotaView is the daily Telegram allowance.
type QuotaView struct {
	Day        string `json:"day"`
	UsedBytes  int64  `json:"usedBytes"`
	DailyBytes int64  `json:"dailyBytes"`
	Remaining  int64  `json:"remaining"`
	ResetAt    int64  `json:"resetAt"`
	// Oversize warns that a queued file is larger than a whole day's
	// allowance. Such a file is allowed to run on an otherwise untouched day,
	// because refusing it would leave the queue stuck on it forever.
	Oversize bool `json:"oversize"`
}

// ScheduleView is the window state.
type ScheduleView struct {
	Enabled         bool   `json:"enabled"`
	Open            bool   `json:"open"`
	WindowStart     string `json:"windowStart"`
	WindowEnd       string `json:"windowEnd"`
	IntervalMinutes int    `json:"intervalMinutes"`
	NextOpenAt      int64  `json:"nextOpenAt,omitempty"`
}

// ScanView is the state of cloud enumeration.
type ScanView struct {
	Scanning   bool   `json:"scanning"`
	LastScanAt int64  `json:"lastScanAt,omitempty"`
	Error      string `json:"error,omitempty"`
}

// LimitsView echoes tdrive's own transfer settings so the 计划与限额 tab can
// show what the sync is actually bound by without the operator having to open
// another page.
type LimitsView struct {
	SegmentSize         int64 `json:"segmentSize"`
	UploadPartSize      int64 `json:"uploadPartSize"`
	UploadThreads       int   `json:"uploadThreads"`
	UploadConcurrency   int   `json:"uploadConcurrency"`
	DownloadConcurrency int   `json:"downloadConcurrency"`
	MaxDownloadConns    int   `json:"maxDownloadConns"`
	RateLimitMillis     int64 `json:"rateLimitMillis"`
	CacheLimit          int64 `json:"cacheLimit"`
	StageLimit          int64 `json:"stageLimit"`
}

// AccountView is the linked Aliyun Drive account.
type AccountView struct {
	LoggedIn       bool              `json:"loggedIn"`
	Nickname       string            `json:"nickname,omitempty"`
	UserID         string            `json:"userId,omitempty"`
	DriveName      string            `json:"driveName,omitempty"`
	CurrentDriveID string            `json:"currentDriveId,omitempty"`
	Drives         []aliyunpan.Drive `json:"drives,omitempty"`
	Error          string            `json:"error,omitempty"`
	CheckedAt      int64             `json:"checkedAt,omitempty"`
}

// probeTTL is how long a cached account or binary probe is reused. Both shell
// out — `who` even makes a network call — so they must not run on the page's
// poll interval.
const probeTTL = 60 * time.Second

type probeCache struct {
	account    AccountView
	binary     aliyunpan.BinaryState
	checkedAt  time.Time
	refreshing bool
	// revision prevents a probe that started before logout/login from writing
	// its stale account back into the cache after that action completed.
	revision uint64
	rerun    bool
}

// State assembles the snapshot.
func (e *Engine) State(ctx context.Context) Snapshot {
	// Read the configuration through rather than from the scheduler's cache:
	// the core's plugin configuration modal writes the same key, and the page
	// should not show a document that has already been replaced.
	e.reloadSettings(ctx)
	e.rollQuota(ctx)
	limits, limitsErr := e.host.Settings(ctx)
	if limitsErr != nil {
		e.logger.Printf("读取 tdrive 运行参数失败: %v", limitsErr)
	}
	now := time.Now()

	e.mu.Lock()
	current := e.settings
	paused := e.paused
	scanning := e.scanning
	scanError := e.scanError
	lastScan := e.lastScan
	quota := e.quota
	quotaDay := current.Quota.Day(now)

	rows := make([]Row, 0, len(e.queue))
	summary := Summary{}
	oversize := false
	todayStart := current.Quota.PeriodStart(now).UnixMilli()
	for _, item := range e.queue {
		rows = append(rows, item.row())
		switch item.State {
		case StateRunning:
			summary.Active++
			if fresh(item.speedAt, now) {
				if item.Stage == StageDownloading {
					summary.DownloadSpeed += item.speed
				} else {
					summary.UploadSpeed += item.speed
				}
			}
		case StatePending:
			summary.Pending++
			if current.Quota.DailyBytes > 0 && item.Size > current.Quota.DailyBytes {
				oversize = true
			}
		case StateFailed:
			summary.Failed++
		case StateComplete:
			if item.FinishedAt >= todayStart {
				summary.TodayCount++
				summary.TodayBytes += item.Size
			}
		}
	}
	e.mu.Unlock()

	remaining := int64(0)
	if current.Quota.DailyBytes > 0 {
		remaining = current.Quota.DailyBytes - quota.UsedBytes
		if remaining < 0 {
			remaining = 0
		}
	}
	open := current.Schedule.Open(now)
	schedule := ScheduleView{
		Enabled:         current.Schedule.Enabled,
		Open:            open,
		WindowStart:     current.Schedule.WindowStart,
		WindowEnd:       current.Schedule.WindowEnd,
		IntervalMinutes: current.Schedule.IntervalMinutes,
	}
	if next := current.Schedule.NextOpen(now); !next.IsZero() {
		schedule.NextOpenAt = next.UnixMilli()
	}

	account, binary := e.probe(ctx)
	e.probeMu.Lock()
	installing, installError := e.installing, e.installError
	e.probeMu.Unlock()
	snapshot := Snapshot{
		Rows:    rows,
		Summary: summary,
		Quota: QuotaView{
			Day:        quotaDay,
			UsedBytes:  quota.UsedBytes,
			DailyBytes: current.Quota.DailyBytes,
			Remaining:  remaining,
			ResetAt:    current.Quota.NextReset(now).UnixMilli(),
			Oversize:   oversize,
		},
		Schedule: schedule,
		Scan:     ScanView{Scanning: scanning, Error: scanError},
		Limits: LimitsView{
			SegmentSize:         limits.SegmentSize,
			UploadPartSize:      limits.UploadPartSize,
			UploadThreads:       limits.UploadThreads,
			UploadConcurrency:   limits.UploadConcurrency,
			DownloadConcurrency: limits.DownloadConcurrency,
			MaxDownloadConns:    limits.MaxDownloadConns,
			RateLimitMillis:     limits.RateLimit.Milliseconds(),
			CacheLimit:          limits.CacheLimit,
			StageLimit:          e.stageLimit(limits),
		},
		Account:  account,
		Binary:   binary,
		Login:    e.loginState(),
		Jobs:     current.Jobs,
		Paused:   paused,
		Now:      now.UnixMilli(),
		StageDir: e.StageDir(),

		Installing:   installing,
		InstallError: installError,
	}
	if !lastScan.IsZero() {
		snapshot.Scan.LastScanAt = lastScan.UnixMilli()
	}
	snapshot.Status = describeStatus(snapshot, current, now)
	return snapshot
}

func (i *Item) row() Row {
	row := Row{
		ID:         i.ID,
		JobID:      i.JobID,
		JobName:    i.JobName,
		DriveName:  i.DriveName,
		Name:       i.Name,
		RemotePath: i.RemotePath,
		TargetPath: i.TargetPath(),
		State:      i.State,
		Stage:      i.Stage,
		Total:      i.Size,
		Done:       i.Uploaded,
		Downloaded: i.Downloaded,
		Error:      i.Error,
		Attempts:   i.Attempts,
		Speed:      i.speed,
		CreatedAt:  i.CreatedAt,
		StartedAt:  i.StartedAt,
		FinishedAt: i.FinishedAt,
	}
	if !i.speedAt.IsZero() {
		row.SpeedAt = i.speedAt.UnixMilli()
	}
	// A completed item shows a full bar. Uploaded is the byte counter, and it
	// can end a hair short when the last chunk's accounting races the commit.
	if i.State == StateComplete {
		row.Done = i.Size
	}
	return row
}

func fresh(at, now time.Time) bool {
	return !at.IsZero() && now.Sub(at) < 5*time.Second
}

// describeStatus is the single line at the top of the sync page. It answers
// "why is nothing moving", which is the question an operator actually has.
func describeStatus(snapshot Snapshot, current settings.Settings, now time.Time) string {
	switch {
	case snapshot.Paused:
		return "已暂停"
	case snapshot.Installing:
		return "正在下载 aliyunpan"
	case !snapshot.Binary.Installed:
		return "尚未安装 aliyunpan，请到「账号」页安装"
	case !snapshot.Account.LoggedIn:
		return "阿里云盘未登录，请到「账号」页扫码登录"
	case len(current.Jobs) == 0:
		return "还没有同步任务，请到「任务」页新建"
	case !current.Schedule.Enabled:
		return "计划任务已关闭"
	case snapshot.Summary.Active > 0:
		return "同步中"
	case !snapshot.Schedule.Open:
		if snapshot.Schedule.NextOpenAt > 0 {
			next := time.UnixMilli(snapshot.Schedule.NextOpenAt)
			return "不在同步窗口内，" + next.Format("01-02 15:04") + " 继续"
		}
		return "不在同步窗口内"
	case snapshot.Summary.Pending > 0 && snapshot.Quota.DailyBytes > 0 && snapshot.Quota.Remaining == 0:
		return "今日配额已用尽，" + time.UnixMilli(snapshot.Quota.ResetAt).Format("01-02 15:04") + " 后继续"
	case snapshot.Summary.Pending > 0:
		return "排队中"
	case snapshot.Scan.Scanning:
		return "正在扫描云盘"
	default:
		_ = now
		return "空闲"
	}
}

// probe returns the cached account and binary state, refreshing it in the
// background when it has gone stale.
func (e *Engine) probe(ctx context.Context) (AccountView, aliyunpan.BinaryState) {
	e.probeMu.Lock()
	cache := e.probes
	cold := cache.checkedAt.IsZero()
	stale := time.Since(cache.checkedAt) > probeTTL
	shouldRefresh := stale && !cache.refreshing
	if shouldRefresh {
		e.probes.refreshing = true
	}
	e.probeMu.Unlock()

	if shouldRefresh {
		// Detached from the request: the poll that noticed the staleness must
		// not wait for a network round trip to Aliyun.
		if !e.launchProbe(e.backgroundContext()) {
			e.probeMu.Lock()
			e.probes.refreshing = false
			e.probeMu.Unlock()
		}
	}
	if cold {
		// Before the first probe lands, the configured path and whether the
		// plugin owns it are still known without any I/O. Reporting them right
		// away keeps the 账号 tab from showing an empty path and a disabled
		// install button for the first second after a restart.
		if cli := e.CLI(); cli != nil {
			cache.binary.Path = cli.Binary()
			cache.binary.Managed = cli.Managed()
		}
	}
	return cache.account, cache.binary
}

// RefreshProbe re-reads the binary and account state now.
func (e *Engine) RefreshProbe(_ context.Context) (AccountView, aliyunpan.BinaryState) {
	e.invalidateProbe()
	e.requestProbeRefresh()
	e.probeMu.Lock()
	defer e.probeMu.Unlock()
	return e.probes.account, e.probes.binary
}

func (e *Engine) requestProbeRefresh() {
	e.probeMu.Lock()
	if e.probes.refreshing {
		e.probes.rerun = true
		e.probeMu.Unlock()
		return
	}
	e.probes.refreshing = true
	e.probeMu.Unlock()
	if !e.launchProbe(e.backgroundContext()) {
		e.probeMu.Lock()
		e.probes.refreshing = false
		e.probeMu.Unlock()
	}
}

func (e *Engine) launchProbe(ctx context.Context) bool {
	if ctx == nil {
		ctx = context.Background()
	}
	e.mu.Lock()
	if e.stopping {
		e.mu.Unlock()
		return false
	}
	e.workers.Add(1)
	e.mu.Unlock()
	go func() {
		defer e.workers.Done()
		e.refreshProbe(ctx)
	}()
	return true
}

func (e *Engine) accountReady() bool {
	e.probeMu.Lock()
	defer e.probeMu.Unlock()
	return !e.probes.checkedAt.IsZero() && e.probes.account.LoggedIn
}

func (e *Engine) refreshProbe(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	e.probeMu.Lock()
	revision := e.probes.revision
	e.probeMu.Unlock()
	probeCtx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	cli := e.CLI()
	var (
		account AccountView
		binary  aliyunpan.BinaryState
	)
	if cli != nil {
		binary = cli.Probe(probeCtx)
		if binary.Installed {
			resolved, err := cli.Who(probeCtx)
			switch {
			case err == nil:
				account = AccountView{
					LoggedIn: true, Nickname: resolved.Nickname,
					UserID: resolved.UserID, DriveName: resolved.DriveName,
				}
				currentKind, _ := aliyunpan.NormalizeDriveName(resolved.DriveName)
				drives, driveErr := cli.DrivesForKnownAccount(probeCtx)
				if driveErr == nil {
					account.Drives = drives
					for _, drive := range drives {
						if drive.Kind == currentKind || drive.Name == resolved.DriveName {
							account.CurrentDriveID = drive.ID
							break
						}
					}
				} else {
					account.Error = "读取网盘列表失败: " + driveErr.Error()
				}
			case errors.Is(err, aliyunpan.ErrNotLoggedIn):
				account = AccountView{}
			default:
				account = AccountView{Error: err.Error()}
			}
		}
	}
	account.CheckedAt = time.Now().UnixMilli()

	e.probeMu.Lock()
	stale := e.probes.revision != revision || e.probes.rerun
	if !stale {
		e.probes.account = account
		e.probes.binary = binary
		e.probes.checkedAt = time.Now()
	}
	e.probes.refreshing = false
	e.probes.rerun = false
	e.probeMu.Unlock()
	if stale {
		// A logout/login or an explicit refresh happened while this command was
		// in flight. The old result is discarded and exactly one fresh probe is
		// queued after it releases the CLI gate.
		e.requestProbeRefresh()
		return
	}
	e.Wake()
}

func (e *Engine) loginState() aliyunpan.LoginState {
	cli := e.CLI()
	if cli == nil {
		return aliyunpan.LoginState{}
	}
	return cli.LoginState()
}
