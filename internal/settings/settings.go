// Package settings holds the plugin's configuration document.
//
// It lives in the plugin-data key "settings", which is the same key the tdrive
// core reads and writes from Settings → 插件 → 配置. Sharing the key means the
// plugin's own forms and the core's raw-JSON editor always describe the same
// document instead of drifting apart.
package settings

import (
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Settings is the whole configuration document.
type Settings struct {
	// BinaryPath is retained for compatibility with settings written by the
	// binary-based release. The source client ignores it.
	//
	// Deprecated: the plugin no longer launches an aliyunpan executable.
	BinaryPath string `json:"binaryPath"`
	// StageDir is where a cloud file lands before it is pushed to Telegram.
	// Empty means <plugin data>/aliyunpan/stage.
	StageDir string `json:"stageDir"`
	// StageLimitBytes bounds that directory. 0 follows tdrive's CacheLimit.
	StageLimitBytes int64 `json:"stageLimitBytes"`
	// OwnerUserID is the account uploads are attributed to, which decides
	// whose quota they consume and who sees them in 文件. Empty means the
	// first enabled administrator.
	OwnerUserID string `json:"ownerUserId"`
	// DownloadRate configures the source client's aggregate download limiter,
	// e.g. "2MB". Empty means unlimited.
	DownloadRate string `json:"downloadRate"`
	// DownloadConcurrency limits how many files may be downloaded from Aliyun
	// Drive into the local staging area at once. It is independent of tdrive's
	// Telegram download concurrency.
	DownloadConcurrency int `json:"downloadConcurrency"`
	// DeleteLocalAfterUpload removes the staged copy once tdrive has committed
	// the upload. It defaults to true so older configurations do not gradually
	// fill the staging disk after an upgrade.
	DeleteLocalAfterUpload bool     `json:"deleteLocalAfterUpload"`
	Schedule               Schedule `json:"schedule"`
	Quota                  Quota    `json:"quota"`
	Jobs                   []Job    `json:"jobs"`
}

// Schedule decides when the engine is allowed to start new files.
type Schedule struct {
	Enabled bool `json:"enabled"`
	// WindowStart and WindowEnd are "HH:MM" in the server's local time. Equal
	// or empty values mean the window is always open. A start later than the
	// end wraps past midnight, which is the common "overnight" case.
	WindowStart string `json:"windowStart"`
	WindowEnd   string `json:"windowEnd"`
	// IntervalMinutes is how often the cloud is re-scanned while the window is
	// open.
	IntervalMinutes int `json:"intervalMinutes"`
}

// Quota is the daily ceiling on bytes pushed to Telegram. It exists because
// the Telegram API, not the drive, is the scarce resource.
type Quota struct {
	DailyBytes int64 `json:"dailyBytes"`
	// ResetAt is the "HH:MM" at which the counter returns to zero.
	ResetAt string `json:"resetAt"`
}

// Job is one cloud directory mirrored into one drive directory.
type Job struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Enabled bool   `json:"enabled"`
	// DriveName selects the Aliyun Drive namespace independently for each job.
	// The source client always passes the selected drive ID explicitly, so jobs
	// never rely on a process-global active drive.
	DriveName  string `json:"driveName"`
	RemotePath string `json:"remotePath"`
	TargetPath string `json:"targetPath"`
	// IncludeFiles turns the job from "mirror this directory" into "sync
	// exactly these files". They are absolute cloud paths under RemotePath,
	// picked in the browser, and when the list is non-empty the scan does not
	// walk the tree at all — it looks only at the directories holding them.
	//
	// The size and exclude filters are deliberately not applied to them.
	// Choosing a file by hand is a more specific instruction than a rule that
	// was written to describe a directory, and silently dropping a file
	// somebody ticked would be the worst of both.
	IncludeFiles []string `json:"includeFiles,omitempty"`
	// ExcludeNames are Go regular expressions matched against each entry's
	// name; a matching directory is not descended into.
	ExcludeNames []string `json:"excludeNames"`
	MinSizeBytes int64    `json:"minSizeBytes"`
	MaxSizeBytes int64    `json:"maxSizeBytes"`
	Overwrite    bool     `json:"overwrite"`
	// DeleteAfterUpload removes the cloud original once the drive has
	// committed the file. It defaults to off: a sync that silently destroys
	// its source is not something to opt out of after the fact.
	DeleteAfterUpload bool `json:"deleteAfterUpload"`
}

const (
	// DefaultDailyBytes is 20 GiB, comfortably under what a single Telegram
	// account will tolerate in a day while still moving a useful amount.
	DefaultDailyBytes          = 20 << 30
	DefaultInterval            = 15
	DefaultSegmentChunk        = 256 << 10
	DefaultDownloadConcurrency = 2
	minIntervalMinutes         = 1
	maxIntervalMinutes         = 24 * 60
	maxExcludePatternLen       = 512
	maxDownloadConcurrency     = 32
	DefaultDriveName           = "backup"
)

// Default is the document a fresh installation starts from. The schedule is
// enabled but the job list is empty, so nothing moves until an administrator
// says what to move.
func Default() Settings {
	return Settings{
		DownloadConcurrency:    DefaultDownloadConcurrency,
		DeleteLocalAfterUpload: true,
		Schedule:               Schedule{Enabled: true, IntervalMinutes: DefaultInterval},
		Quota:                  Quota{DailyBytes: DefaultDailyBytes, ResetAt: "00:00"},
		Jobs:                   []Job{},
	}
}

var (
	clockPattern = regexp.MustCompile(`^([01]?\d|2[0-3]):([0-5]\d)$`)
	idPattern    = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
)

// Normalize fills in defaults and rejects a document that cannot be executed.
// It is called on every load and on every save, so a hand-edited JSON blob
// from the core's textarea gets the same treatment as the plugin's own forms.
func (s *Settings) Normalize() error {
	s.BinaryPath = strings.TrimSpace(s.BinaryPath)
	if err := checkPathText("binaryPath", s.BinaryPath); err != nil {
		return err
	}
	s.StageDir = strings.TrimSpace(s.StageDir)
	if s.StageDir != "" && !filepath.IsAbs(s.StageDir) {
		return errors.New("stageDir 必须是绝对路径")
	}
	if err := checkPathText("stageDir", s.StageDir); err != nil {
		return err
	}
	if s.StageLimitBytes < 0 {
		return errors.New("stageLimitBytes 不能为负数")
	}
	s.OwnerUserID = strings.TrimSpace(s.OwnerUserID)
	s.DownloadRate = strings.TrimSpace(s.DownloadRate)
	if s.DownloadConcurrency == 0 {
		s.DownloadConcurrency = DefaultDownloadConcurrency
	}
	if s.DownloadConcurrency < 1 || s.DownloadConcurrency > maxDownloadConcurrency {
		return fmt.Errorf("阿里云盘同时下载数必须在 1~%d 之间", maxDownloadConcurrency)
	}

	if s.Schedule.IntervalMinutes == 0 {
		s.Schedule.IntervalMinutes = DefaultInterval
	}
	if s.Schedule.IntervalMinutes < minIntervalMinutes || s.Schedule.IntervalMinutes > maxIntervalMinutes {
		return fmt.Errorf("扫描间隔必须在 %d~%d 分钟之间", minIntervalMinutes, maxIntervalMinutes)
	}
	if err := checkClock("窗口开始", s.Schedule.WindowStart); err != nil {
		return err
	}
	if err := checkClock("窗口结束", s.Schedule.WindowEnd); err != nil {
		return err
	}
	if s.Quota.ResetAt == "" {
		s.Quota.ResetAt = "00:00"
	}
	if err := checkClock("配额重置时刻", s.Quota.ResetAt); err != nil {
		return err
	}
	if s.Quota.DailyBytes < 0 {
		return errors.New("每日配额不能为负数")
	}

	if s.Jobs == nil {
		s.Jobs = []Job{}
	}
	seen := make(map[string]bool, len(s.Jobs))
	for i := range s.Jobs {
		job := &s.Jobs[i]
		if err := job.normalize(); err != nil {
			return err
		}
		if seen[job.ID] {
			return fmt.Errorf("任务 ID 重复: %s", job.ID)
		}
		seen[job.ID] = true
	}
	return nil
}

func (j *Job) normalize() error {
	j.ID = strings.TrimSpace(j.ID)
	if !idPattern.MatchString(j.ID) {
		return fmt.Errorf("任务 ID 不合法: %q", j.ID)
	}
	j.Name = strings.TrimSpace(j.Name)
	if j.Name == "" {
		j.Name = j.ID
	}
	driveName := strings.TrimSpace(j.DriveName)
	if driveName == "" {
		j.DriveName = DefaultDriveName
	} else {
		switch strings.ToLower(driveName) {
		case "backup", "file", "备份盘", "文件":
			j.DriveName = "backup"
		case "resource", "资源盘", "资源库":
			j.DriveName = "resource"
		default:
			return fmt.Errorf("任务 %s 的网盘不支持 %q，只能是 backup（备份盘）或 resource（资源库）", j.Name, j.DriveName)
		}
	}
	j.RemotePath = CleanCloudPath(j.RemotePath)
	if j.RemotePath == "" {
		return fmt.Errorf("任务 %s 缺少云盘路径", j.Name)
	}
	if err := checkPathText("云盘路径", j.RemotePath); err != nil {
		return fmt.Errorf("任务 %s: %w", j.Name, err)
	}
	j.TargetPath = CleanCloudPath(j.TargetPath)
	if j.TargetPath == "" {
		return fmt.Errorf("任务 %s 缺少 tdrive 目标路径", j.Name)
	}
	if err := checkPathText("tdrive 目标路径", j.TargetPath); err != nil {
		return fmt.Errorf("任务 %s: %w", j.Name, err)
	}
	// Every picked file has to live under the job's cloud directory, because
	// that directory is what the target path is rebased from. One outside it has
	// no defined destination, and guessing one would put the file somewhere the
	// operator never named.
	includes := make([]string, 0, len(j.IncludeFiles))
	seenInclude := make(map[string]bool, len(j.IncludeFiles))
	for _, path := range j.IncludeFiles {
		cleaned := CleanCloudPath(path)
		if cleaned == "" || cleaned == "/" {
			continue
		}
		if err := checkPathText("选定文件", cleaned); err != nil {
			return fmt.Errorf("任务 %s: %w", j.Name, err)
		}
		if j.RemotePath != "/" && !strings.HasPrefix(cleaned, j.RemotePath+"/") {
			return fmt.Errorf("任务 %s 选定的文件 %s 不在云盘目录 %s 里", j.Name, cleaned, j.RemotePath)
		}
		if seenInclude[cleaned] {
			continue
		}
		seenInclude[cleaned] = true
		includes = append(includes, cleaned)
	}
	j.IncludeFiles = includes

	if j.MinSizeBytes < 0 || j.MaxSizeBytes < 0 {
		return fmt.Errorf("任务 %s 的大小过滤不能为负数", j.Name)
	}
	if j.MaxSizeBytes > 0 && j.MinSizeBytes > j.MaxSizeBytes {
		return fmt.Errorf("任务 %s 的最小值大于最大值", j.Name)
	}
	patterns := make([]string, 0, len(j.ExcludeNames))
	for _, pattern := range j.ExcludeNames {
		pattern = strings.TrimSpace(pattern)
		if pattern == "" {
			continue
		}
		if len(pattern) > maxExcludePatternLen {
			return fmt.Errorf("任务 %s 的排除规则过长", j.Name)
		}
		if _, err := regexp.Compile(pattern); err != nil {
			return fmt.Errorf("任务 %s 的排除规则 %q 不是合法的正则: %w", j.Name, pattern, err)
		}
		patterns = append(patterns, pattern)
	}
	j.ExcludeNames = patterns
	return nil
}

// CleanCloudPath normalizes a slash path to a rooted, unslashed-suffix form.
// Both the cloud side and the drive side use the same shape, so one helper
// serves both.
func CleanCloudPath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}
	parts := strings.Split(path, "/")
	cleaned := make([]string, 0, len(parts))
	for _, part := range parts {
		if part == "" || part == "." {
			continue
		}
		if part == ".." {
			if len(cleaned) > 0 {
				cleaned = cleaned[:len(cleaned)-1]
			}
			continue
		}
		cleaned = append(cleaned, part)
	}
	return "/" + strings.Join(cleaned, "/")
}

func checkPathText(label, value string) error {
	if strings.ContainsAny(value, "\x00\r\n") {
		return fmt.Errorf("%s 不能含有 NUL 或换行字符", label)
	}
	return nil
}

func checkClock(label, value string) error {
	if value == "" {
		return nil
	}
	if !clockPattern.MatchString(value) {
		return fmt.Errorf("%s 必须形如 HH:MM，收到 %q", label, value)
	}
	return nil
}

// minutesOfDay converts "HH:MM" to minutes past midnight. An empty or invalid
// value yields -1, which callers read as "not set".
func minutesOfDay(value string) int {
	match := clockPattern.FindStringSubmatch(value)
	if match == nil {
		return -1
	}
	hour, _ := strconv.Atoi(match[1])
	minute, _ := strconv.Atoi(match[2])
	return hour*60 + minute
}

// Open reports whether the schedule permits starting new work at now.
//
// A window whose start equals its end, or either of whose bounds is unset, is
// treated as always open rather than never open: an operator who clears the
// field means "no restriction", and the opposite reading would silently stop
// every sync.
func (s Schedule) Open(now time.Time) bool {
	if !s.Enabled {
		return false
	}
	start, end := minutesOfDay(s.WindowStart), minutesOfDay(s.WindowEnd)
	if start < 0 || end < 0 || start == end {
		return true
	}
	current := now.Hour()*60 + now.Minute()
	if start < end {
		return current >= start && current < end
	}
	// The window wraps past midnight, so it is the union of two ranges.
	return current >= start || current < end
}

// NextOpen returns when the window opens again, for the "等待下一个窗口"
// caption. It returns the zero time when the window is already open or when
// the schedule is off entirely.
func (s Schedule) NextOpen(now time.Time) time.Time {
	if !s.Enabled || s.Open(now) {
		return time.Time{}
	}
	start := minutesOfDay(s.WindowStart)
	if start < 0 {
		return time.Time{}
	}
	candidate := time.Date(now.Year(), now.Month(), now.Day(), start/60, start%60, 0, 0, now.Location())
	if !candidate.After(now) {
		candidate = candidate.AddDate(0, 0, 1)
	}
	return candidate
}

// Day names the quota period now falls in. The counter resets at ResetAt, so
// the period starting at 03:00 on the 5th is still "the 5th" at 02:00 on the
// 6th; naming it by its start date is what makes the stored counter comparable
// across restarts.
func (q Quota) Day(now time.Time) string {
	return q.PeriodStart(now).Format("2006-01-02")
}

// PeriodStart returns the beginning of the current quota period. It is also
// used for the UI's "today" totals, which must follow a custom reset time
// rather than a rolling 24-hour window.
func (q Quota) PeriodStart(now time.Time) time.Time {
	reset := minutesOfDay(q.ResetAt)
	if reset < 0 {
		reset = 0
	}
	start := time.Date(now.Year(), now.Month(), now.Day(), reset/60, reset%60, 0, 0, now.Location())
	if now.Before(start) {
		start = start.AddDate(0, 0, -1)
	}
	return start
}

// NextReset is when the current quota period ends.
func (q Quota) NextReset(now time.Time) time.Time {
	reset := minutesOfDay(q.ResetAt)
	if reset < 0 {
		reset = 0
	}
	candidate := time.Date(now.Year(), now.Month(), now.Day(), reset/60, reset%60, 0, 0, now.Location())
	if !candidate.After(now) {
		candidate = candidate.AddDate(0, 0, 1)
	}
	return candidate
}

// JobByID finds a configured job.
func (s Settings) JobByID(id string) (Job, bool) {
	for _, job := range s.Jobs {
		if job.ID == id {
			return job, true
		}
	}
	return Job{}, false
}
