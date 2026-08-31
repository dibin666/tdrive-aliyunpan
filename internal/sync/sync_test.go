package sync

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/dibin/tdrive-aliyunpan/internal/settings"
)

// The drive refuses an incomplete upload by formatting the pending indices
// with %v. That message is the only channel a plugin has for per-segment
// failures: the host closes the brokered stream before it waits for a
// segment's commit, so a segment that failed to store looks, from the plugin's
// side, exactly like one that succeeded.
func TestParseMissingSegments(t *testing.T) {
	cases := []struct {
		err  error
		want []int
	}{
		{fmt.Errorf("upload is still missing segments [3 7]"), []int{3, 7}},
		{fmt.Errorf("upload is still missing segments [1]"), []int{1}},
		{errors.New("rpc error: upload is still missing segments [2 4 6 8]"), []int{2, 4, 6, 8}},
		{fmt.Errorf("upload is still missing segments []"), []int{}},
		{errors.New("telegram rejected the document"), nil},
		{nil, nil},
	}
	for _, tc := range cases {
		got := parseMissingSegments(tc.err)
		if len(got) == 0 && len(tc.want) == 0 {
			continue
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("parseMissingSegments(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
}

func TestMapPath(t *testing.T) {
	cases := []struct {
		remoteRoot, targetRoot, cloudPath, want string
	}{
		{"/我的资源", "/阿里云盘", "/我的资源", "/阿里云盘"},
		{"/我的资源", "/阿里云盘", "/我的资源/影视", "/阿里云盘/影视"},
		{"/我的资源", "/阿里云盘", "/我的资源/影视/2024", "/阿里云盘/影视/2024"},
		{"/", "/阿里云盘", "/影视", "/阿里云盘/影视"},
		{"/我的资源", "/", "/我的资源/影视", "/影视"},
	}
	for _, tc := range cases {
		if got := mapPath(tc.remoteRoot, tc.targetRoot, tc.cloudPath); got != tc.want {
			t.Errorf("mapPath(%q, %q, %q) = %q, want %q",
				tc.remoteRoot, tc.targetRoot, tc.cloudPath, got, tc.want)
		}
	}
}

func TestUnstorableName(t *testing.T) {
	if reason := unstorableName("普通文件.mkv"); reason != "" {
		t.Errorf("a normal name was rejected: %s", reason)
	}
	long := make([]byte, maxNameBytes+1)
	for i := range long {
		long[i] = 'x'
	}
	if unstorableName(string(long)) == "" {
		t.Error("a name past tdrive's byte limit should be rejected during the scan")
	}
	if unstorableName("bad\x01name") == "" {
		t.Error("a control character should be rejected")
	}
}

// The quota has to hold across several workers: each of them individually
// respecting the cap is not the same as the cap being respected.
func TestQuotaAllows(t *testing.T) {
	engine := &Engine{}
	engine.settings = settings.Settings{Quota: settings.Quota{DailyBytes: 100}}

	if !engine.quotaAllowsLocked(60) {
		t.Error("60 of 100 should fit")
	}
	engine.reserved = 60
	if engine.quotaAllowsLocked(60) {
		t.Error("a second 60 must not be allowed while the first is in flight")
	}
	if !engine.quotaAllowsLocked(40) {
		t.Error("40 should still fit alongside the reserved 60")
	}

	engine.reserved = 0
	engine.quota = QuotaState{UsedBytes: 100}
	if engine.quotaAllowsLocked(1) {
		t.Error("nothing fits once the day is spent")
	}
}

// A file bigger than a whole day's allowance would otherwise sit at the head
// of the queue forever, blocking everything behind it.
func TestQuotaAllowsOversizeFileOnAnUntouchedDay(t *testing.T) {
	engine := &Engine{}
	engine.settings = settings.Settings{Quota: settings.Quota{DailyBytes: 100}}
	if !engine.quotaAllowsLocked(500) {
		t.Error("an oversize file should be allowed to run on an otherwise unused day")
	}
	engine.quota = QuotaState{UsedBytes: 1}
	if engine.quotaAllowsLocked(500) {
		t.Error("it should not be allowed once the day has been touched")
	}
}

func TestQuotaUnlimited(t *testing.T) {
	engine := &Engine{}
	engine.settings = settings.Settings{Quota: settings.Quota{DailyBytes: 0}}
	engine.quota = QuotaState{UsedBytes: 1 << 50}
	if !engine.quotaAllowsLocked(1 << 40) {
		t.Error("a zero daily cap means unlimited")
	}
}

func TestItemTargetPath(t *testing.T) {
	if got := (&Item{TargetDir: "/", Name: "a.mkv"}).TargetPath(); got != "/a.mkv" {
		t.Errorf("root target = %q", got)
	}
	if got := (&Item{TargetDir: "/影视", Name: "a.mkv"}).TargetPath(); got != "/影视/a.mkv" {
		t.Errorf("nested target = %q", got)
	}
}

func TestItemSpeedSampling(t *testing.T) {
	item := &Item{}
	start := time.Now()
	item.observe(1000, start)
	if item.speed != 0 {
		t.Error("the first sample only starts the window")
	}
	item.observe(1000, start.Add(speedSampleWindow/2))
	if item.speed != 0 {
		t.Error("a sample shorter than the window should not publish a rate")
	}
	item.observe(1000, start.Add(2*time.Second))
	if item.speed <= 0 {
		t.Fatalf("speed = %v, want a positive rate", item.speed)
	}
	// 2000 bytes accumulated after the first sample, over two seconds.
	if item.speed < 900 || item.speed > 1100 {
		t.Errorf("speed = %v, want roughly 1000 B/s", item.speed)
	}
	item.resetSpeed()
	if item.speed != 0 || !item.speedAt.IsZero() {
		t.Error("resetSpeed should clear both the rate and its timestamp")
	}
}

func TestTrimHistoryKeepsActiveItems(t *testing.T) {
	engine := &Engine{}
	for i := 0; i < historyLimit+50; i++ {
		engine.queue = append(engine.queue, &Item{
			ID: fmt.Sprint(i), State: StateComplete, FinishedAt: int64(i),
		})
	}
	pending := &Item{ID: "pending", State: StatePending}
	running := &Item{ID: "running", State: StateRunning}
	engine.queue = append(engine.queue, pending, running)

	engine.trimHistoryLocked()

	if len(engine.queue) != historyLimit+2 {
		t.Fatalf("queue length = %d, want %d", len(engine.queue), historyLimit+2)
	}
	found := map[string]bool{}
	for _, item := range engine.queue {
		found[item.ID] = true
	}
	if !found["pending"] || !found["running"] {
		t.Error("active items must never be trimmed")
	}
	if found["0"] {
		t.Error("the oldest finished item should have been dropped first")
	}
}
