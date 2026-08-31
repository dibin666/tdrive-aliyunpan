package settings

import (
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
	if document.Schedule.IntervalMinutes != DefaultInterval {
		t.Errorf("interval = %d", document.Schedule.IntervalMinutes)
	}
	if document.Quota.ResetAt != "00:00" {
		t.Errorf("resetAt = %q", document.Quota.ResetAt)
	}
	if document.Jobs == nil {
		t.Error("jobs should be an empty slice, not nil, so the UI renders a list")
	}
}

func TestNormalizeRejections(t *testing.T) {
	cases := map[string]Settings{
		"相对二进制路径":  {BinaryPath: "bin/aliyunpan"},
		"相对暂存路径":   {StageDir: "stage"},
		"负配额":      {Quota: Quota{DailyBytes: -1}},
		"畸形时刻":     {Schedule: Schedule{WindowStart: "25:00"}},
		"畸形重置时刻":   {Quota: Quota{ResetAt: "7:5"}},
		"越界间隔":     {Schedule: Schedule{IntervalMinutes: 99999}},
		"任务缺少云盘路径": {Jobs: []Job{{ID: "j1", TargetPath: "/a"}}},
		"任务缺少目标路径": {Jobs: []Job{{ID: "j1", RemotePath: "/a"}}},
		"任务 ID 非法": {Jobs: []Job{{ID: "bad id!", RemotePath: "/a", TargetPath: "/b"}}},
		"排除规则非法":   {Jobs: []Job{{ID: "j1", RemotePath: "/a", TargetPath: "/b", ExcludeNames: []string{"("}}}},
		"大小区间颠倒":   {Jobs: []Job{{ID: "j1", RemotePath: "/a", TargetPath: "/b", MinSizeBytes: 10, MaxSizeBytes: 5}}},
	}
	for name, document := range cases {
		if err := document.Normalize(); err == nil {
			t.Errorf("%s: expected an error", name)
		}
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
