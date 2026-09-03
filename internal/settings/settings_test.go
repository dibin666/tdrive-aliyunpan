package settings

import (
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func at(hour, minute int) time.Time {
	return time.Date(2026, 8, 31, hour, minute, 0, 0, time.Local)
}

func TestScheduleOvernightWindow(t *testing.T) {
	// 01:00–07:00 wraps past midnight, which is the common "run it while
	// nobody is watching" case and the one an interval check gets wrong.
	schedule := Schedule{Enabled: true, WindowStart: "01:00", WindowEnd: "07:00", IntervalMinutes: 15}
	cases := []struct {
		hour, minute int
		want         bool
	}{
		{0, 59, false},
		{1, 0, true}, // the start is inclusive
		{3, 30, true},
		{6, 59, true},
		{7, 0, false}, // the end is exclusive
		{23, 0, false},
	}
	for _, tc := range cases {
		if got := schedule.Open(at(tc.hour, tc.minute)); got != tc.want {
			t.Errorf("Open(%02d:%02d) = %v, want %v", tc.hour, tc.minute, got, tc.want)
		}
	}
}

func TestScheduleWrappingWindow(t *testing.T) {
	schedule := Schedule{Enabled: true, WindowStart: "22:00", WindowEnd: "04:00", IntervalMinutes: 15}
	for _, tc := range []struct {
		hour int
		want bool
	}{{21, false}, {22, true}, {23, true}, {0, true}, {3, true}, {4, false}, {12, false}} {
		if got := schedule.Open(at(tc.hour, 0)); got != tc.want {
			t.Errorf("Open(%02d:00) = %v, want %v", tc.hour, got, tc.want)
		}
	}
}

func TestScheduleEmptyWindowIsAlwaysOpen(t *testing.T) {
	// An operator who clears the fields means "no restriction". Reading it the
	// other way would silently stop every sync.
	for _, schedule := range []Schedule{
		{Enabled: true, IntervalMinutes: 15},
		{Enabled: true, WindowStart: "03:00", WindowEnd: "03:00", IntervalMinutes: 15},
		{Enabled: true, WindowStart: "03:00", IntervalMinutes: 15},
	} {
		if !schedule.Open(at(13, 0)) {
			t.Errorf("%+v should be always open", schedule)
		}
	}
}

func TestScheduleDisabled(t *testing.T) {
	schedule := Schedule{Enabled: false, WindowStart: "00:00", WindowEnd: "23:59"}
	if schedule.Open(at(12, 0)) {
		t.Error("a disabled schedule is never open")
	}
}

func TestScheduleNextOpen(t *testing.T) {
	schedule := Schedule{Enabled: true, WindowStart: "01:00", WindowEnd: "07:00", IntervalMinutes: 15}
	next := schedule.NextOpen(at(9, 0))
	if next.Day() != 1 || next.Hour() != 1 {
		t.Errorf("NextOpen after the window = %v, want tomorrow 01:00", next)
	}
	if got := schedule.NextOpen(at(3, 0)); !got.IsZero() {
		t.Errorf("NextOpen while open = %v, want zero", got)
	}
}

func TestQuotaDay(t *testing.T) {
	// The period is named by the date it started on, so a counter written at
	// 02:00 is still comparable with one written at 23:00 the evening before.
	quota := Quota{DailyBytes: 1 << 30, ResetAt: "03:00"}
	if day := quota.Day(at(2, 59)); day != "2026-08-30" {
		t.Errorf("before the reset the period is still yesterday's, got %q", day)
	}
	if day := quota.Day(at(3, 0)); day != "2026-08-31" {
		t.Errorf("at the reset a new period starts, got %q", day)
	}
	if day := (Quota{ResetAt: "00:00"}).Day(at(0, 0)); day != "2026-08-31" {
		t.Errorf("midnight reset, got %q", day)
	}
}

func TestQuotaNextReset(t *testing.T) {
	quota := Quota{ResetAt: "03:00"}
	if next := quota.NextReset(at(2, 0)); next.Day() != 31 || next.Hour() != 3 {
		t.Errorf("NextReset = %v, want today 03:00", next)
	}
	if next := quota.NextReset(at(4, 0)); next.Day() != 1 {
		t.Errorf("NextReset = %v, want tomorrow", next)
	}
}

func TestNormalizeDefaults(t *testing.T) {
	document := Settings{}
	if err := document.Normalize(); err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if document.DownloadConcurrency != DefaultDownloadConcurrency {
		t.Errorf("download concurrency = %d, want %d", document.DownloadConcurrency, DefaultDownloadConcurrency)
	}
	if document.Schedule.IntervalMinutes != DefaultInterval {
		t.Errorf("interval = %d", document.Schedule.IntervalMinutes)
	}
	if document.Quota.ResetAt != "00:00" {
		t.Errorf("resetAt = %q", document.Quota.ResetAt)
	}
	if document.Jobs == nil {
		t.Error("jobs should be an empty slice, not nil, so the UI renders a list")
	}

	defaults := Default()
	if !defaults.DeleteLocalAfterUpload {
		t.Error("new installations should delete successful local staging files by default")
	}
}

func TestNormalizeKeepsExplicitLocalRetentionChoice(t *testing.T) {
	document := Default()
	document.DeleteLocalAfterUpload = false
	if err := document.Normalize(); err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if document.DeleteLocalAfterUpload {
		t.Error("Normalize should keep an explicit request to retain local staging files")
	}
}

func TestNormalizeJobDefaultsToBackupDrive(t *testing.T) {
	document := Settings{Jobs: []Job{{ID: "j1", RemotePath: "/a", TargetPath: "/b"}}}
	if err := document.Normalize(); err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if document.Jobs[0].DriveName != DefaultDriveName {
		t.Fatalf("driveName = %q, want %q", document.Jobs[0].DriveName, DefaultDriveName)
	}
}

func TestNormalizeRejections(t *testing.T) {
	cases := map[string]Settings{
		"相对暂存路径":   {StageDir: "stage"},
		"负配额":      {Quota: Quota{DailyBytes: -1}},
		"负阿里云盘并发":  {DownloadConcurrency: -1},
		"超出阿里云盘并发": {DownloadConcurrency: maxDownloadConcurrency + 1},
		"畸形时刻":     {Schedule: Schedule{WindowStart: "25:00"}},
		"畸形重置时刻":   {Quota: Quota{ResetAt: "7:5"}},
		"越界间隔":     {Schedule: Schedule{IntervalMinutes: 99999}},
		"任务缺少云盘路径": {Jobs: []Job{{ID: "j1", TargetPath: "/a"}}},
		"任务缺少目标路径": {Jobs: []Job{{ID: "j1", RemotePath: "/a"}}},
		"任务 ID 非法": {Jobs: []Job{{ID: "bad id!", RemotePath: "/a", TargetPath: "/b"}}},
		"排除规则非法":   {Jobs: []Job{{ID: "j1", RemotePath: "/a", TargetPath: "/b", ExcludeNames: []string{"("}}}},
		"大小区间颠倒":   {Jobs: []Job{{ID: "j1", RemotePath: "/a", TargetPath: "/b", MinSizeBytes: 10, MaxSizeBytes: 5}}},
		"路径含 NUL":  {Jobs: []Job{{ID: "j1", RemotePath: "/a\x00b", TargetPath: "/b"}}},
	}
	for name, document := range cases {
		if err := document.Normalize(); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

func TestNormalizeAcceptsLegacyBinaryPath(t *testing.T) {
	document := Settings{BinaryPath: "bin/aliyunpan"}
	if err := document.Normalize(); err != nil {
		t.Fatalf("Normalize rejected a legacy binaryPath: %v", err)
	}
}

func TestNormalizeAcceptsPlatformAbsolutePaths(t *testing.T) {
	root := t.TempDir()
	document := Settings{
		BinaryPath: filepath.Join(root, "bin", "aliyunpan"),
		StageDir:   filepath.Join(root, "stage"),
	}
	if err := document.Normalize(); err != nil {
		t.Fatalf("Normalize rejected absolute paths: %v", err)
	}
}

func TestNormalizeRejectsDuplicateJobIDs(t *testing.T) {
	document := Settings{Jobs: []Job{
		{ID: "j1", RemotePath: "/a", TargetPath: "/x"},
		{ID: "j1", RemotePath: "/b", TargetPath: "/y"},
	}}
	if err := document.Normalize(); err == nil {
		t.Fatal("expected an error for duplicate job IDs")
	}
}

func TestNormalizeRejectsUnsupportedDrive(t *testing.T) {
	document := Settings{Jobs: []Job{{ID: "j1", DriveName: "album", RemotePath: "/a", TargetPath: "/b"}}}
	if err := document.Normalize(); err == nil {
		t.Fatal("expected an unsupported drive to be rejected")
	}
}

func TestCleanCloudPath(t *testing.T) {
	cases := map[string]string{
		"":             "",
		"/":            "/",
		"a/b":          "/a/b",
		"/a//b/":       "/a/b",
		"/a/./b":       "/a/b",
		"/a/../b":      "/b",
		"/../..":       "/",
		"  /我的资源/影视  ": "/我的资源/影视",
	}
	for input, want := range cases {
		if got := CleanCloudPath(input); got != want {
			t.Errorf("CleanCloudPath(%q) = %q, want %q", input, got, want)
		}
	}
}

// A picked file's destination is worked out by rebasing it from the job's cloud
// directory. One outside that directory has no destination at all, and guessing
// one would put the file somewhere nobody named.
func TestNormalizeRejectsAPickedFileOutsideTheJobDirectory(t *testing.T) {
	document := Settings{Jobs: []Job{{
		ID: "j1", DriveName: "backup", RemotePath: "/动画", TargetPath: "/动画",
		IncludeFiles: []string{"/电影/a.mkv"},
	}}}
	if err := document.Normalize(); err == nil {
		t.Fatal("a picked file outside the job's cloud directory was accepted")
	}
}

func TestNormalizeCleansAndDedupesPickedFiles(t *testing.T) {
	document := Settings{Jobs: []Job{{
		ID: "j1", DriveName: "backup", RemotePath: "/动画", TargetPath: "/动画",
		IncludeFiles: []string{
			"/动画//01.ts", " /动画/01.ts ", "/动画/第一季/./ep1.mkv", "", "/",
		},
	}}}
	if err := document.Normalize(); err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	got := document.Jobs[0].IncludeFiles
	want := []string{"/动画/01.ts", "/动画/第一季/ep1.mkv"}
	if len(got) != len(want) {
		t.Fatalf("IncludeFiles = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("IncludeFiles = %v, want %v", got, want)
		}
	}
}

// A ticked directory is held to the same rule as a ticked file: it is rebased
// from the job's cloud directory, so it has to be under it.
func TestNormalizeCleansPickedDirectories(t *testing.T) {
	document := Settings{Jobs: []Job{{
		ID: "j1", DriveName: "backup", RemotePath: "/动画", TargetPath: "/动画",
		IncludeDirs: []string{"/动画//第一季/", " /动画/第一季 ", "/动画/剧场版", ""},
	}}}
	if err := document.Normalize(); err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	got := document.Jobs[0].IncludeDirs
	want := []string{"/动画/第一季", "/动画/剧场版"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("IncludeDirs = %v, want %v", got, want)
	}
	if !document.Jobs[0].HasSelection() {
		t.Fatal("a job with only ticked directories does not report a selection")
	}
}

func TestNormalizeRejectsAPickedDirectoryOutsideTheJobDirectory(t *testing.T) {
	document := Settings{Jobs: []Job{{
		ID: "j1", DriveName: "backup", RemotePath: "/动画", TargetPath: "/动画",
		IncludeDirs: []string{"/电影/第一季"},
	}}}
	if err := document.Normalize(); err == nil {
		t.Fatal("a picked directory outside the job's cloud directory was accepted")
	}
}

// A job rooted at "/" holds everything, so no file can be outside it.
func TestNormalizeAcceptsPickedFilesUnderTheRoot(t *testing.T) {
	document := Settings{Jobs: []Job{{
		ID: "j1", DriveName: "backup", RemotePath: "/", TargetPath: "/云盘",
		IncludeFiles: []string{"/a.mkv"},
	}}}
	if err := document.Normalize(); err != nil {
		t.Fatalf("Normalize: %v", err)
	}
}

// A configuration written before the retry section existed has no retry object
// at all. Normalize has to fill it in rather than leave a policy of zero
// attempts, which would fail every transfer on its first error.
func TestNormalizeFillsInARetryPolicy(t *testing.T) {
	document := Default()
	document.Retry = Retry{}
	if err := document.Normalize(); err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if document.Retry.MaxAttempts != DefaultRetryAttempts {
		t.Errorf("maxAttempts = %d, want %d", document.Retry.MaxAttempts, DefaultRetryAttempts)
	}
	if document.Retry.InitialSeconds != DefaultRetryInitialSeconds {
		t.Errorf("initialSeconds = %d, want %d", document.Retry.InitialSeconds, DefaultRetryInitialSeconds)
	}
	if document.Retry.MaxSeconds != DefaultRetryMaxSeconds {
		t.Errorf("maxSeconds = %d, want %d", document.Retry.MaxSeconds, DefaultRetryMaxSeconds)
	}
}

func TestNormalizeRejectsAnUnusableRetryPolicy(t *testing.T) {
	for name, retry := range map[string]Retry{
		"negative attempts":   {MaxAttempts: -1, InitialSeconds: 30, MaxSeconds: 1800},
		"too many attempts":   {MaxAttempts: 1000, InitialSeconds: 30, MaxSeconds: 1800},
		"negative interval":   {MaxAttempts: 5, InitialSeconds: -1, MaxSeconds: 1800},
		"absurd interval":     {MaxAttempts: 5, InitialSeconds: 30, MaxSeconds: 90000},
		"ceiling below floor": {MaxAttempts: 5, InitialSeconds: 600, MaxSeconds: 60},
	} {
		document := Default()
		document.Retry = retry
		if err := document.Normalize(); err == nil {
			t.Errorf("%s: expected the policy to be rejected", name)
		}
	}
}

// 1 attempt is how an operator turns automatic retries off, so it has to be
// accepted rather than corrected up to the default.
func TestRetryAllowsBeingTurnedOff(t *testing.T) {
	document := Default()
	document.Retry = Retry{MaxAttempts: 1, InitialSeconds: 30, MaxSeconds: 1800}
	if err := document.Normalize(); err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if document.Retry.MaxAttempts != 1 {
		t.Fatalf("maxAttempts = %d, want the operator's 1 to be kept", document.Retry.MaxAttempts)
	}
	if document.Retry.Allows(1) {
		t.Error("a single-attempt policy must not schedule a second attempt")
	}
}

func TestRetryBackoffDoublesAndIsCapped(t *testing.T) {
	retry := Retry{MaxAttempts: 10, InitialSeconds: 30, MaxSeconds: 300}
	for attempts, want := range map[int]time.Duration{
		1: 30 * time.Second,
		2: 60 * time.Second,
		3: 120 * time.Second,
		4: 240 * time.Second,
		5: 300 * time.Second,
		9: 300 * time.Second,
	} {
		if got := retry.Backoff(attempts); got != want {
			t.Errorf("Backoff(%d) = %s, want %s", attempts, got, want)
		}
	}
	// A large attempt count must not overflow its way back to a short wait.
	if got := retry.Backoff(1000); got != 300*time.Second {
		t.Errorf("Backoff(1000) = %s, want the ceiling", got)
	}
}
