package sync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dibin/tdrive-aliyunpan/internal/aliyunpan"
	"github.com/dibin/tdrive-aliyunpan/internal/hostapi"
	"github.com/dibin/tdrive-aliyunpan/internal/settings"
	tdriveplugin "github.com/dibin/tdrive/pkg/plugin"
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

func TestMapPathDoesNotTrimACommonPrefix(t *testing.T) {
	got := mapPath("/a", "/target", "/ab/file.bin")
	if got != "/target/ab/file.bin" {
		t.Errorf("mapPath = %q, want %q", got, "/target/ab/file.bin")
	}
}

func TestItemKeyIncludesSizeWhenHashIsMissing(t *testing.T) {
	first := &Item{JobID: "j1", RemotePath: "/a/file.bin", Size: 10}
	second := &Item{JobID: "j1", RemotePath: "/a/file.bin", Size: 11}
	if first.key() == second.key() {
		t.Fatal("items with changed sizes and no SHA1 must have different keys")
	}
}

func TestConsiderIgnoresResultsFromAnEditedJob(t *testing.T) {
	engine := &Engine{
		settings: settings.Settings{Jobs: []settings.Job{{
			ID: "j1", Name: "new", DriveName: settings.DefaultDriveName,
			RemotePath: "/new", TargetPath: "/target",
		}}},
	}
	oldJob := settings.Job{
		ID: "j1", Name: "old", DriveName: settings.DefaultDriveName,
		RemotePath: "/old", TargetPath: "/target",
	}
	engine.consider(context.Background(), oldJob, aliyunpan.Entry{
		Name: "file.bin", Path: "/old/file.bin", Size: 1,
	}, "/target", map[string]tdriveplugin.Entry{}, newDriveCache(engine))
	if len(engine.queue) != 0 {
		t.Fatal("a scan using an old job snapshot queued stale work")
	}
}

// listingHost answers files.list from a fixed drive tree. The drive is the
// scan's index of what has already been synced, so this is the only thing it
// consults to decide whether a file still needs downloading.
type listingHost struct {
	dirs map[string][]tdriveplugin.Entry
	read []string
}

func (host *listingHost) Call(_ context.Context, method string, request, response any) error {
	if method != "files.list" {
		return nil
	}
	query, _ := request.(map[string]string)
	host.read = append(host.read, query["path"])
	entries, ok := host.dirs[query["path"]]
	if !ok {
		return errors.New("drive: no such file or directory")
	}
	target, ok := response.(*[]tdriveplugin.Entry)
	if !ok {
		return fmt.Errorf("unexpected files.list response %T", response)
	}
	*target = entries
	return nil
}

func (host *listingHost) OpenStream(context.Context, string, any) (io.ReadWriteCloser, error) {
	return nil, errors.New("not supported")
}

// deliveredEngine is a job that has already synced one file, set up so that the
// destination the scan now derives for it is not the one it was delivered to.
// Ticking a second entry in another folder is enough to move the job's cloud
// root a level up and do exactly this.
func deliveredEngine(host *listingHost) (*Engine, settings.Job, aliyunpan.Entry) {
	job := settings.Job{
		ID: "j1", Name: "动漫", DriveName: settings.DefaultDriveName,
		RemotePath: "/粤语", TargetPath: "/动画",
	}
	engine := &Engine{
		host:     hostapi.New(host),
		settings: settings.Settings{Jobs: []settings.Job{job}},
		queue: []*Item{{
			ID: "already-done", JobID: "j1", JobName: "动漫",
			RemotePath: "/粤语/庞波小姐/01.mkv", Name: "01.mkv",
			SHA1: "6be2daf3b6b1", Size: 2720257321,
			TargetDir: "/动画", State: StateComplete,
		}},
	}
	entry := aliyunpan.Entry{
		Name: "01.mkv", Path: "/粤语/庞波小姐/01.mkv",
		SHA1: "6be2daf3b6b1", Size: 2720257321,
	}
	return engine, job, entry
}

// A file whose destination moved is still in the drive, just not where the scan
// now looks for it. Queueing it again downloads and re-uploads every byte a
// second time and spends the daily quota on a duplicate.
func TestConsiderSkipsAFileAlreadyDeliveredToItsOldTarget(t *testing.T) {
	host := &listingHost{dirs: map[string][]tdriveplugin.Entry{
		"/动画": {{Name: "01.mkv", Size: 2720257321}},
	}}
	engine, job, entry := deliveredEngine(host)

	engine.consider(context.Background(), job, entry,
		"/动画/庞波小姐", map[string]tdriveplugin.Entry{}, newDriveCache(engine))

	if len(engine.queue) != 1 {
		t.Fatalf("queue = %d items, want only the completed one", len(engine.queue))
	}
}

// The drive is the index rather than a ledger of what was once synced, so
// deleting a file from 文件 still has to bring it back.
func TestConsiderQueuesAFileRemovedFromItsOldTarget(t *testing.T) {
	host := &listingHost{dirs: map[string][]tdriveplugin.Entry{"/动画": {}}}
	engine, job, entry := deliveredEngine(host)

	engine.consider(context.Background(), job, entry,
		"/动画/庞波小姐", map[string]tdriveplugin.Entry{}, newDriveCache(engine))

	if len(engine.queue) != 2 {
		t.Fatalf("queue = %d items, want the completed one and a fresh candidate", len(engine.queue))
	}
	if got := engine.queue[1].TargetDir; got != "/动画/庞波小姐" {
		t.Fatalf("re-queued target = %q, want the destination it maps onto now", got)
	}
}

// A different file that happens to share a name is not the delivered one.
func TestConsiderQueuesADifferentFileWithTheSameName(t *testing.T) {
	host := &listingHost{dirs: map[string][]tdriveplugin.Entry{
		"/动画": {{Name: "01.mkv", Size: 2720257321}},
	}}
	engine, job, entry := deliveredEngine(host)
	entry.Size = 1_000_000
	entry.SHA1 = "0000deadbeef"

	engine.consider(context.Background(), job, entry,
		"/动画/庞波小姐", map[string]tdriveplugin.Entry{}, newDriveCache(engine))

	if len(engine.queue) != 2 {
		t.Fatalf("queue = %d items, want the replacement queued", len(engine.queue))
	}
}

// Ticking a directory and a file inside it is one click away — "全选本目录" does
// it — so the same file reaches consider twice in one scan, once from the walk
// of the directory and once from the file's own listing.
func TestConsiderQueuesAFileCoveredByTwoTicksOnlyOnce(t *testing.T) {
	host := &listingHost{dirs: map[string][]tdriveplugin.Entry{"/动画": {}}}
	engine, job, entry := deliveredEngine(host)
	engine.queue = nil
	cache := newDriveCache(engine)

	for round := 0; round < 2; round++ {
		engine.consider(context.Background(), job, entry,
			"/动画/庞波小姐", map[string]tdriveplugin.Entry{}, cache)
	}
	if len(engine.queue) != 1 {
		t.Fatalf("queue = %d items, want one candidate", len(engine.queue))
	}
}

// The check costs one host call per destination, not one per file, or a scan of
// a full season would pay for the same listing dozens of times.
func TestDriveCacheReadsEachDirectoryOnce(t *testing.T) {
	host := &listingHost{dirs: map[string][]tdriveplugin.Entry{
		"/动画": {{Name: "01.mkv", Size: 2720257321}},
	}}
	engine, job, entry := deliveredEngine(host)
	cache := newDriveCache(engine)

	for round := 0; round < 3; round++ {
		engine.consider(context.Background(), job, entry,
			"/动画/庞波小姐", map[string]tdriveplugin.Entry{}, cache)
	}
	if len(host.read) != 1 {
		t.Fatalf("files.list calls = %v, want the old destination read once", host.read)
	}
}

func TestSettingsReturnsAnIndependentJobSlice(t *testing.T) {
	engine := &Engine{settings: settings.Settings{Jobs: []settings.Job{{
		ID: "j1", RemotePath: "/a", TargetPath: "/b", ExcludeNames: []string{"old"},
		IncludeFiles: []string{"/a/one.mkv"}, IncludeDirs: []string{"/a/season"},
	}}}}
	copy := engine.Settings()
	copy.Jobs[0].Name = "changed"
	copy.Jobs[0].ExcludeNames[0] = "changed"
	copy.Jobs[0].IncludeFiles[0] = "changed"
	copy.Jobs[0].IncludeDirs[0] = "changed"
	if got := engine.settings.Jobs[0].Name; got == "changed" {
		t.Fatal("Settings exposed the engine's job slice")
	}
	if got := engine.settings.Jobs[0].ExcludeNames[0]; got == "changed" {
		t.Fatal("Settings exposed the engine's exclude slice")
	}
	if got := engine.settings.Jobs[0].IncludeFiles[0]; got == "changed" {
		t.Fatal("Settings exposed the engine's picked-file slice")
	}
	if got := engine.settings.Jobs[0].IncludeDirs[0]; got == "changed" {
		t.Fatal("Settings exposed the engine's picked-directory slice")
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
	for _, name := range []string{"CON.txt", "bad?name", "trailing."} {
		if unstorableName(name) == "" {
			t.Errorf("unsafe Windows name %q should be rejected", name)
		}
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

func TestDownloadSlotRespectsConfiguredConcurrency(t *testing.T) {
	engine := &Engine{settings: settings.Settings{DownloadConcurrency: 1}}
	releaseFirst, acquireError := engine.acquireDownloadSlot(context.Background())
	if acquireError != nil {
		t.Fatalf("acquire first download slot: %v", acquireError)
	}

	blockedContext, cancelBlocked := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelBlocked()
	_, acquireError = engine.acquireDownloadSlot(blockedContext)
	if !errors.Is(acquireError, context.DeadlineExceeded) {
		t.Fatalf("second download slot error = %v, want deadline exceeded", acquireError)
	}

	releaseFirst()
	releaseSecond, acquireError := engine.acquireDownloadSlot(context.Background())
	if acquireError != nil {
		t.Fatalf("acquire released download slot: %v", acquireError)
	}
	releaseSecond()
}

func TestDownloadSlotWakesWhenConfiguredLimitIncreases(t *testing.T) {
	engine := &Engine{settings: settings.Settings{DownloadConcurrency: 1}}
	releaseFirst, acquireError := engine.acquireDownloadSlot(context.Background())
	if acquireError != nil {
		t.Fatalf("acquire first download slot: %v", acquireError)
	}

	acquired := make(chan func(), 1)
	acquireDone := make(chan error, 1)
	go func() {
		releaseSecond, waitError := engine.acquireDownloadSlot(context.Background())
		if waitError == nil {
			acquired <- releaseSecond
		}
		acquireDone <- waitError
	}()

	engine.applySettings(settings.Settings{DownloadConcurrency: 2})
	select {
	case waitError := <-acquireDone:
		if waitError != nil {
			t.Fatalf("acquire after increasing limit: %v", waitError)
		}
	case <-time.After(time.Second):
		t.Fatal("download slot was not woken after increasing the configured limit")
	}

	select {
	case releaseSecond := <-acquired:
		releaseSecond()
	case <-time.After(time.Second):
		t.Fatal("download slot acquisition did not return its release function")
	}
	releaseFirst()
}

func TestUploadSlotRespectsHostConcurrency(t *testing.T) {
	engine := &Engine{}
	releaseFirst, acquireError := engine.acquireUploadSlot(context.Background(), 1)
	if acquireError != nil {
		t.Fatalf("acquire first upload slot: %v", acquireError)
	}

	blockedContext, cancelBlocked := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelBlocked()
	_, acquireError = engine.acquireUploadSlot(blockedContext, 1)
	if !errors.Is(acquireError, context.DeadlineExceeded) {
		t.Fatalf("second upload slot error = %v, want deadline exceeded", acquireError)
	}
	releaseFirst()
}

func TestItemTargetPath(t *testing.T) {
	if got := (&Item{TargetDir: "/", Name: "a.mkv"}).TargetPath(); got != "/a.mkv" {
		t.Errorf("root target = %q", got)
	}
	if got := (&Item{TargetDir: "/影视", Name: "a.mkv"}).TargetPath(); got != "/影视/a.mkv" {
		t.Errorf("nested target = %q", got)
	}
}

func TestItemStageDirIsUniquePerQueueItem(t *testing.T) {
	first := itemStageDir("/stage", "item-a")
	second := itemStageDir("/stage", "item-b")
	if first == second {
		t.Fatal("different queue items must not share a staging directory")
	}
	if first != "/stage/"+stageItemsDir+"/item-a" {
		t.Fatalf("item stage directory = %q", first)
	}
}

func TestSuccessfulUploadLocalCleanupPolicy(t *testing.T) {
	cases := []struct {
		name                   string
		deleteLocalAfterUpload bool
		wantRetained           bool
	}{
		{name: "delete immediately", deleteLocalAfterUpload: true, wantRetained: false},
		{name: "retain when disabled", deleteLocalAfterUpload: false, wantRetained: true},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			stageRoot := t.TempDir()
			itemDirectory := itemStageDir(stageRoot, "item-a")
			stagedPath := filepath.Join(itemDirectory, "file.bin")
			if err := os.MkdirAll(itemDirectory, 0o750); err != nil {
				t.Fatalf("MkdirAll: %v", err)
			}
			if err := os.WriteFile(stagedPath, []byte("staged"), 0o600); err != nil {
				t.Fatalf("WriteFile: %v", err)
			}

			gotRetained := retainStagedAfterSuccessfulUpload(
				stagedPath, "item-a", testCase.deleteLocalAfterUpload,
			)
			if gotRetained != testCase.wantRetained {
				t.Fatalf("retained = %v, want %v", gotRetained, testCase.wantRetained)
			}
			_, statError := os.Stat(stagedPath)
			if testCase.wantRetained && statError != nil {
				t.Fatalf("retained staged file stat: %v", statError)
			}
			if !testCase.wantRetained && !os.IsNotExist(statError) {
				t.Fatalf("staged file error = %v, want not-exist", statError)
			}
		})
	}
}

func TestFailedUploadIsRequeuedWithBackoff(t *testing.T) {
	engine := &Engine{
		host:     hostapi.New(&zeroSegmentHost{}),
		settings: settings.Settings{Retry: settings.Retry{MaxAttempts: 5, InitialSeconds: 30, MaxSeconds: 1800}},
	}
	item := &Item{
		State:       StateRunning,
		Stage:       StageUploading,
		Downloaded:  20,
		Uploaded:    10,
		UploadJobID: "upload-1",
		Attempts:    1,
		Error:       "old error",
		FinishedAt:  123,
	}

	before := nowMillis()
	if wake := engine.retryLater(context.Background(), item, errors.New("telegram 抽风")); wake {
		t.Fatal("a transfer parked until a deadline has nothing for the scheduler to take yet")
	}

	if item.State != StatePending || item.Stage != StageIdle {
		t.Fatalf("requeued item state = %s/%s, want pending/idle", item.State, item.Stage)
	}
	if item.Uploaded != 0 || item.UploadJobID != "" || item.FinishedAt != 0 {
		t.Fatalf("requeued item retained upload state: %+v", item)
	}
	// The staged bytes survive the requeue, because the retry resumes from them
	// instead of downloading the file a second time.
	if item.Downloaded != 20 {
		t.Fatalf("requeued item downloaded = %d, want the staged bytes to be kept", item.Downloaded)
	}
	// One attempt has been made, so the wait is the initial interval give or
	// take the jitter that keeps a batch of transfers from returning together.
	earliest := before + int64((30*time.Second - 6*time.Second).Milliseconds())
	latest := nowMillis() + int64((30*time.Second + 6*time.Second).Milliseconds())
	if item.NextAttemptAt < earliest || item.NextAttemptAt > latest {
		t.Fatalf("next attempt at %d, want within [%d, %d]", item.NextAttemptAt, earliest, latest)
	}
	if !strings.Contains(item.Error, "telegram 抽风") {
		t.Fatalf("requeued item error = %q, want it to name the failure", item.Error)
	}
}

// The budget is what stops a genuinely broken transfer from cycling forever.
// Spending it has to produce a plain failure, and it must not take the download
// with it.
func TestRetryBudgetExhaustionFailsTheTransfer(t *testing.T) {
	engine := &Engine{
		host:     hostapi.New(&zeroSegmentHost{}),
		settings: settings.Settings{Retry: settings.Retry{MaxAttempts: 3, InitialSeconds: 30, MaxSeconds: 1800}},
	}
	item := &Item{State: StateRunning, Stage: StageUploading, Downloaded: 20, Attempts: 3}

	if wake := engine.retryLater(context.Background(), item, errors.New("上传失败")); !wake {
		t.Fatal("a transfer that has finally failed should wake the scheduler")
	}
	if item.State != StateFailed {
		t.Fatalf("state = %s, want failed once the budget is spent", item.State)
	}
	if item.NextAttemptAt != 0 {
		t.Fatalf("next attempt = %d, want no pending backoff on a failed item", item.NextAttemptAt)
	}
	if item.Downloaded != 20 {
		t.Fatalf("downloaded = %d, want the local file to be kept for a manual retry", item.Downloaded)
	}
	if !strings.Contains(item.Error, "上传失败") {
		t.Fatalf("error = %q, want it to name the underlying failure", item.Error)
	}
}

// MaxAttempts of 1 is how an operator turns automatic retries off. It has to
// mean "one attempt", not "one retry".
func TestRetryCanBeTurnedOff(t *testing.T) {
	engine := &Engine{
		host:     hostapi.New(&zeroSegmentHost{}),
		settings: settings.Settings{Retry: settings.Retry{MaxAttempts: 1, InitialSeconds: 30, MaxSeconds: 1800}},
	}
	item := &Item{State: StateRunning, Attempts: 1}

	engine.retryLater(context.Background(), item, errors.New("下载失败"))
	if item.State != StateFailed {
		t.Fatalf("state = %s, want failed when retries are disabled", item.State)
	}
}

func TestNoteDownloadFollowsTheDownloaderInBothDirections(t *testing.T) {
	engine := &Engine{}
	item := &Item{Size: 100}

	engine.noteDownload(item, 60)
	if item.Downloaded != 60 {
		t.Fatalf("downloaded = %d, want 60", item.Downloaded)
	}
	// A chunk whose connection broke gives back the bytes it never committed.
	engine.noteDownload(item, 32)
	if item.Downloaded != 32 {
		t.Fatalf("downloaded = %d, want the downloader's rollback to be reflected", item.Downloaded)
	}
	engine.noteDownload(item, 500)
	if item.Downloaded != 100 {
		t.Fatalf("downloaded = %d, want it clamped to the file size", item.Downloaded)
	}
}

func TestStageRoomDoesNotDoubleCountAnInFlightDownload(t *testing.T) {
	stageRoot := t.TempDir()
	engine := &Engine{settings: settings.Settings{StageLimitBytes: 30}}
	limits := hostapi.RuntimeSettings{}

	release, err := engine.reserveStageRoom("item-a", 20, stageRoot, limits)
	if err != nil {
		t.Fatalf("reserve first: %v", err)
	}
	// A download pre-allocates its .part file at the final size, so the same
	// bytes are visible to a directory walk while the reservation is still held.
	itemDirectory := itemStageDir(stageRoot, "item-a")
	if err := os.MkdirAll(itemDirectory, 0o750); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join(itemDirectory, "file.bin.part"), make([]byte, 20), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	secondRelease, err := engine.reserveStageRoom("item-b", 10, stageRoot, limits)
	if err != nil {
		t.Fatalf("reserve second: %v, want the pre-allocated file counted once", err)
	}
	secondRelease()

	if _, err := engine.reserveStageRoom("item-c", 15, stageRoot, limits); !errors.Is(err, ErrStageRoom) {
		t.Fatalf("third reservation error = %v, want ErrStageRoom once the limit is really reached", err)
	}
	release()
}

// storedDataHost serves plugin-data reads from an in-memory map, which is what
// Load needs in order to restore a queue.
type storedDataHost struct {
	values map[string]string
}

func (host *storedDataHost) Call(_ context.Context, method string, request, response any) error {
	if method != "data.get" {
		return nil
	}
	key, _ := request.(map[string]string)
	stored, ok := host.values[key["key"]]
	if !ok {
		return errors.New("database: not found")
	}
	raw, ok := response.(*json.RawMessage)
	if !ok {
		return fmt.Errorf("unexpected data.get response %T", response)
	}
	*raw = json.RawMessage(stored)
	return nil
}

func (host *storedDataHost) OpenStream(context.Context, string, any) (io.ReadWriteCloser, error) {
	return nil, errors.New("not supported")
}

// A plugin restart used to publish every in-flight transfer as un-downloaded,
// which is what made a file at 50% jump back to 0 and start again. The staging
// area is the authority now, so a restart reports whatever is actually there.
func TestLoadReportsDownloadProgressFromTheStagingArea(t *testing.T) {
	dataDir := t.TempDir()
	stageDir := filepath.Join(dataDir, "stage")
	item := &Item{
		ID: "item-a", JobID: "job-1", RemotePath: "/movies/a.mkv", Name: "a.mkv",
		TargetDir: "/in", Size: 12, State: StateRunning, Stage: StageDownloading,
		Downloaded: 12, Uploaded: 5, UploadJobID: "upload-1",
	}
	queueJSON, err := json.Marshal([]*Item{item})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	settingsJSON, err := json.Marshal(settings.Settings{StageDir: stageDir})
	if err != nil {
		t.Fatalf("Marshal settings: %v", err)
	}
	host := &storedDataHost{values: map[string]string{
		queueKey:    string(queueJSON),
		SettingsKey: string(settingsJSON),
	}}

	// Half the file is on disk behind a checkpoint written by the downloader.
	downloadDir := itemStageDir(stageDir, item.ID)
	if err := writePartialDownload(downloadDir, item.RemotePath, item.Size); err != nil {
		t.Fatalf("stage a partial download: %v", err)
	}

	engine := New(hostapi.New(host), dataDir, nil)
	if err := engine.Load(context.Background()); err != nil {
		t.Fatalf("Load: %v", err)
	}

	engine.mu.Lock()
	defer engine.mu.Unlock()
	if len(engine.queue) != 1 {
		t.Fatalf("queue = %d items, want 1", len(engine.queue))
	}
	restored := engine.queue[0]
	if restored.State != StatePending || restored.Stage != StageIdle {
		t.Fatalf("restored state = %s/%s, want pending/idle", restored.State, restored.Stage)
	}
	if restored.Uploaded != 0 || restored.UploadJobID != "" {
		t.Fatalf("restored item kept upload state: %+v", restored)
	}
	if restored.Downloaded != 6 {
		t.Fatalf("restored downloaded = %d, want the 6 bytes actually staged", restored.Downloaded)
	}
}

// Keeping partial downloads means the staging area can hold work for items that
// no longer exist. Left alone it counts against the staging limit forever and
// eventually stops everything from starting.
func TestLoadRemovesStagingDirectoriesWithNoQueueItem(t *testing.T) {
	dataDir := t.TempDir()
	stageDir := filepath.Join(dataDir, "stage")
	item := &Item{
		ID: "item-a", JobID: "job-1", RemotePath: "/movies/a.mkv", Name: "a.mkv",
		TargetDir: "/in", Size: 12, State: StatePending,
	}
	queueJSON, _ := json.Marshal([]*Item{item})
	settingsJSON, _ := json.Marshal(settings.Settings{StageDir: stageDir})
	host := &storedDataHost{values: map[string]string{
		queueKey: string(queueJSON), SettingsKey: string(settingsJSON),
	}}

	if err := writePartialDownload(itemStageDir(stageDir, "item-a"), item.RemotePath, item.Size); err != nil {
		t.Fatalf("stage the live item: %v", err)
	}
	orphan := itemStageDir(stageDir, "item-gone")
	if err := writePartialDownload(orphan, "/movies/b.mkv", 12); err != nil {
		t.Fatalf("stage the orphan: %v", err)
	}

	engine := New(hostapi.New(host), dataDir, nil)
	if err := engine.Load(context.Background()); err != nil {
		t.Fatalf("Load: %v", err)
	}

	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatalf("orphaned staging directory survived: %v", err)
	}
	if _, err := os.Stat(itemStageDir(stageDir, "item-a")); err != nil {
		t.Fatalf("a queued item's staging directory was removed: %v", err)
	}
}

// writePartialDownload reproduces what the downloader leaves behind mid-file: a
// .part pre-allocated at the final size, and a sidecar naming the chunks that
// landed. The document is written literally because it is the on-disk contract
// between the two packages, and this test exists to prove the engine reads it;
// the downloader's own tests cover producing and consuming it.
//
// A 12-byte file in 6-byte chunks is two chunks, of which only the first is
// done — bit 0 set, which is the single byte 0x01.
func writePartialDownload(downloadDir, cloudPath string, size int64) error {
	partial := aliyunpan.StagedPath(downloadDir, cloudPath) + ".part"
	if err := os.MkdirAll(filepath.Dir(partial), 0o750); err != nil {
		return err
	}
	if err := os.WriteFile(partial, make([]byte, size), 0o600); err != nil {
		return err
	}
	sidecar := fmt.Sprintf(
		`{"version":1,"driveId":"drive-1","fileId":"file-1","size":%d,"chunkSize":6,"done":"AQ=="}`, size,
	)
	return os.WriteFile(partial+".progress", []byte(sidecar), 0o600)
}

type zeroSegmentHost struct {
	size int64
	data map[string]json.RawMessage
}

func (host *zeroSegmentHost) Call(_ context.Context, method string, request any, response any) error {
	if method == "data.get" && host.data != nil {
		payload, _ := json.Marshal(request)
		var input struct {
			Key string `json:"key"`
		}
		_ = json.Unmarshal(payload, &input)
		val, ok := host.data[input.Key]
		if ok && response != nil {
			if target, isRaw := response.(*json.RawMessage); isRaw {
				*target = val
				return nil
			}
			return json.Unmarshal(val, response)
		}
	}
	return nil
}

func (host *zeroSegmentHost) OpenStream(_ context.Context, method string, request any) (io.ReadWriteCloser, error) {
	if method != "files.putSegment" {
		return nil, fmt.Errorf("unexpected stream method %q", method)
	}
	payload, ok := request.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("unexpected stream request %T", request)
	}
	size, ok := payload["size"].(int64)
	if !ok {
		return nil, fmt.Errorf("unexpected stream size %T", payload["size"])
	}
	host.size = size
	return nopReadWriteCloser{Buffer: bytes.NewBuffer(nil)}, nil
}

type nopReadWriteCloser struct{ *bytes.Buffer }

func (nopReadWriteCloser) Close() error { return nil }

func TestSendSegmentsCreatesZeroByteFileRecord(t *testing.T) {
	host := &zeroSegmentHost{}
	file, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer file.Close()
	engine := &Engine{host: hostapi.New(host)}
	item := &Item{Size: 0}
	job := tdriveplugin.UploadJob{ID: "job", SegmentSize: 1, SegmentCount: 1}

	if err := engine.sendSegments(context.Background(), item, file, job, []int{1}); err != nil {
		t.Fatalf("sendSegments: %v", err)
	}
	if host.size != 0 {
		t.Fatalf("zero-byte segment size = %d, want 0", host.size)
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

// A file inside its retry backoff must not hold up the queue behind it. That
// is the opposite of the quota rule, which deliberately does block, and getting
// them the same way round would let one broken file stall everything.
func TestTakeNextSkipsBackoffWithoutBlockingTheQueue(t *testing.T) {
	job := settings.Job{ID: "j1", Name: "job", Enabled: true, RemotePath: "/a", TargetPath: "/b"}
	engine := &Engine{settings: settings.Settings{Jobs: []settings.Job{job}}}
	waiting := &Item{ID: "waiting", JobID: "j1", State: StatePending, NextAttemptAt: nowMillis() + 60_000}
	ready := &Item{ID: "ready", JobID: "j1", State: StatePending}
	engine.queue = []*Item{waiting, ready}

	taken := engine.takeNext(4)
	if taken == nil || taken.ID != "ready" {
		t.Fatalf("takeNext returned %v, want the item behind the backing-off one", taken)
	}
	if waiting.State != StatePending {
		t.Fatalf("backing-off item state = %s, want it left alone", waiting.State)
	}

	// Once the deadline passes it is claimed like anything else, and claiming it
	// clears the deadline so a crash mid-transfer does not leave it parked.
	engine.active = 0
	waiting.NextAttemptAt = nowMillis() - 1
	taken = engine.takeNext(4)
	if taken == nil || taken.ID != "waiting" {
		t.Fatalf("takeNext returned %v, want the item whose backoff has elapsed", taken)
	}
	if taken.NextAttemptAt != 0 {
		t.Fatalf("claimed item still carries a deadline: %d", taken.NextAttemptAt)
	}
	if taken.Attempts != 1 {
		t.Fatalf("attempts = %d, want the claim to count as one", taken.Attempts)
	}
}

// Asking for a retry by hand says the operator has dealt with whatever broke,
// so the spent budget has to come back with it.
func TestManualRetryRestoresTheAttemptBudget(t *testing.T) {
	engine := &Engine{host: hostapi.New(&zeroSegmentHost{})}
	item := &Item{
		ID: "item-1", State: StateFailed, Attempts: 5,
		NextAttemptAt: nowMillis() + 60_000, Error: "上传失败", FinishedAt: 123,
	}
	engine.queue = []*Item{item}

	if err := engine.Retry(context.Background(), "item-1"); err != nil {
		t.Fatalf("Retry: %v", err)
	}
	if item.State != StatePending {
		t.Fatalf("state = %s, want pending", item.State)
	}
	if item.Attempts != 0 || item.NextAttemptAt != 0 {
		t.Fatalf("attempts = %d, nextAttemptAt = %d; want both cleared", item.Attempts, item.NextAttemptAt)
	}
}

// The whole point of keeping a download after a failed upload is that the retry
// does not pay for it again. Removing the row is the one thing that has to take
// the bytes with it, or they sit against the staging limit unseen.
func TestRemovingAQueueRowFreesItsStagedFile(t *testing.T) {
	dataDir := t.TempDir()
	engine := &Engine{host: hostapi.New(&zeroSegmentHost{}), dataDir: dataDir}
	item := &Item{ID: "item-1", State: StateFailed, RemotePath: "/a/file.bin", Size: 6}
	engine.queue = []*Item{item}

	directory := itemStageDir(engine.StageDir(), item.ID)
	staged := aliyunpan.StagedPath(directory, item.RemotePath)
	if err := os.MkdirAll(filepath.Dir(staged), 0o750); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(staged, []byte("staged"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	engine.ClearFinished(context.Background(), "item-1")

	if _, err := os.Stat(directory); !os.IsNotExist(err) {
		t.Fatalf("staging directory survived the row that referred to it: %v", err)
	}
}

// The 下载文件 page has to describe both numbers: what a file occupies and what
// of it has actually arrived. They differ because a .part is pre-allocated at
// the final size from the first moment.
func TestDownloadsReportsDiskUseAndOrphans(t *testing.T) {
	dataDir := t.TempDir()
	engine := &Engine{host: hostapi.New(&zeroSegmentHost{}), dataDir: dataDir}
	known := &Item{
		ID: "item-1", State: StateFailed, JobName: "job", Name: "file.bin",
		RemotePath: "/a/file.bin", TargetDir: "/target", Size: 6, Error: "上传失败",
	}
	engine.queue = []*Item{known}

	stageDir := engine.StageDir()
	knownDir := itemStageDir(stageDir, "item-1")
	staged := aliyunpan.StagedPath(knownDir, known.RemotePath)
	if err := os.MkdirAll(filepath.Dir(staged), 0o750); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(staged, []byte("staged"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	orphanDir := itemStageDir(stageDir, "item-gone")
	if err := os.MkdirAll(orphanDir, 0o750); err != nil {
		t.Fatalf("MkdirAll orphan: %v", err)
	}
	if err := os.WriteFile(filepath.Join(orphanDir, "left.bin"), []byte("abcd"), 0o600); err != nil {
		t.Fatalf("WriteFile orphan: %v", err)
	}

	view := engine.Downloads(context.Background())
	if len(view.Files) != 2 {
		t.Fatalf("files = %d, want the known item and the orphan", len(view.Files))
	}
	if view.UsedBytes != 10 {
		t.Fatalf("usedBytes = %d, want 10", view.UsedBytes)
	}
	if view.OrphanCount != 1 || view.OrphanBytes != 4 {
		t.Fatalf("orphans = %d / %d bytes, want 1 / 4", view.OrphanCount, view.OrphanBytes)
	}

	byID := map[string]StagedFile{}
	for _, file := range view.Files {
		byID[file.ItemID] = file
	}
	if got := byID["item-1"]; !got.Complete || got.Downloaded != 6 || got.DiskBytes != 6 {
		t.Fatalf("known file = %+v, want a complete 6-byte download", got)
	}
	if got := byID["item-1"]; got.Orphan || got.JobName != "job" || got.Error == "" {
		t.Fatalf("known file lost its queue context: %+v", got)
	}
	if got := byID["item-gone"]; !got.Orphan {
		t.Fatal("a directory with no queue row should be reported as an orphan")
	}

	// Pruning takes the orphan and leaves the file somebody can still retry.
	removed, freed := engine.PruneStaged()
	if removed != 1 || freed != 4 {
		t.Fatalf("prune removed %d / %d bytes, want 1 / 4", removed, freed)
	}
	if _, err := os.Stat(staged); err != nil {
		t.Fatalf("pruning took a file the queue still refers to: %v", err)
	}
}

// Deleting a running transfer's file out from under it would corrupt the upload
// in flight, so it is refused rather than obeyed.
func TestDeleteStagedRefusesARunningTransfer(t *testing.T) {
	engine := &Engine{host: hostapi.New(&zeroSegmentHost{}), dataDir: t.TempDir()}
	engine.queue = []*Item{{ID: "item-1", State: StateRunning, RemotePath: "/a/file.bin"}}

	if _, err := engine.DeleteStaged(context.Background(), "item-1"); err == nil {
		t.Fatal("deleting a running transfer's staged file should be refused")
	}
}

func TestDeleteStagedKeepsTheQueueRow(t *testing.T) {
	engine := &Engine{host: hostapi.New(&zeroSegmentHost{}), dataDir: t.TempDir()}
	item := &Item{ID: "item-1", State: StateFailed, RemotePath: "/a/file.bin", Size: 6, Downloaded: 6}
	engine.queue = []*Item{item}

	directory := itemStageDir(engine.StageDir(), item.ID)
	staged := aliyunpan.StagedPath(directory, item.RemotePath)
	if err := os.MkdirAll(filepath.Dir(staged), 0o750); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(staged, []byte("staged"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	deleted, err := engine.DeleteStaged(context.Background(), "item-1")
	if err != nil || deleted != 1 {
		t.Fatalf("DeleteStaged = %d, %v; want 1, nil", deleted, err)
	}
	if _, err := os.Stat(directory); !os.IsNotExist(err) {
		t.Fatalf("staged file survived an explicit delete: %v", err)
	}
	if len(engine.queue) != 1 {
		t.Fatal("deleting the local file must not delete the queue record it belongs to")
	}
	if item.Downloaded != 0 {
		t.Fatalf("downloaded = %d, want the progress reset once the bytes are gone", item.Downloaded)
	}
}

// stageOneItem plants a fully downloaded file where a transfer would have left
// it, and returns the item's private staging directory.
func stageOneItem(t *testing.T, engine *Engine, item *Item) string {
	t.Helper()
	directory := itemStageDir(engine.StageDir(), item.ID)
	staged := aliyunpan.StagedPath(directory, item.RemotePath)
	if err := os.MkdirAll(filepath.Dir(staged), 0o750); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(staged, []byte("staged"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	return directory
}

func retryingEngine(t *testing.T, attempts int) *Engine {
	t.Helper()
	return &Engine{
		host:       hostapi.New(&zeroSegmentHost{}),
		dataDir:    t.TempDir(),
		cancels:    map[string]context.CancelFunc{},
		cancelling: map[string]bool{},
		settings: settings.Settings{
			Retry: settings.Retry{MaxAttempts: attempts, InitialSeconds: 30, MaxSeconds: 1800},
		},
	}
}

// The single most expensive mistake this pipeline can make is deleting a
// finished download because the upload after it broke. transfer's deferred
// cleanup is where that decision lives, so it is driven here end to end rather
// than asserted on the helpers underneath it.
func TestFailedTransferKeepsWhatItAlreadyDownloaded(t *testing.T) {
	engine := retryingEngine(t, 5)
	// No CLI is configured, so staging fails with an ordinary retryable error —
	// which is exactly the shape of the network failures this has to survive.
	item := &Item{ID: "item-1", State: StateRunning, RemotePath: "/a/file.bin", Size: 6, Attempts: 1}
	engine.queue = []*Item{item}
	directory := stageOneItem(t, engine, item)

	if wake := engine.transfer(context.Background(), item, hostapi.RuntimeSettings{UploadConcurrency: 1}); wake {
		t.Fatal("a transfer parked until its backoff has nothing for the scheduler yet")
	}
	if _, err := os.Stat(directory); err != nil {
		t.Fatalf("a failed transfer deleted what it had already downloaded: %v", err)
	}
	if item.State != StatePending || item.NextAttemptAt == 0 {
		t.Fatalf("item = %s with next attempt %d, want it queued behind a backoff", item.State, item.NextAttemptAt)
	}
}

// Once the budget is spent the transfer is a failure, but the bytes are still
// the expensive half of the job and the operator may yet fix whatever broke.
func TestExhaustedTransferStillKeepsItsLocalFile(t *testing.T) {
	engine := retryingEngine(t, 1)
	item := &Item{ID: "item-1", State: StateRunning, RemotePath: "/a/file.bin", Size: 6, Attempts: 1}
	engine.queue = []*Item{item}
	directory := stageOneItem(t, engine, item)

	if wake := engine.transfer(context.Background(), item, hostapi.RuntimeSettings{UploadConcurrency: 1}); !wake {
		t.Fatal("a transfer that has finally failed should wake the scheduler")
	}
	if item.State != StateFailed {
		t.Fatalf("state = %s, want failed once the budget is spent", item.State)
	}
	if _, err := os.Stat(directory); err != nil {
		t.Fatalf("a permanently failed transfer deleted its local file: %v", err)
	}
}

// Cancelling is the one failure that is a decision rather than an accident, and
// it is still what throws the partial work away.
func TestCancelledTransferDiscardsItsStagedWork(t *testing.T) {
	engine := retryingEngine(t, 5)
	item := &Item{ID: "item-1", State: StateRunning, RemotePath: "/a/file.bin", Size: 6}
	engine.queue = []*Item{item}
	directory := stageOneItem(t, engine, item)
	engine.cancelling[item.ID] = true

	engine.transfer(context.Background(), item, hostapi.RuntimeSettings{UploadConcurrency: 1})
	if _, err := os.Stat(directory); !os.IsNotExist(err) {
		t.Fatalf("a cancelled transfer left its partial download behind: %v", err)
	}
}

// A source that is no longer the file this row describes cannot be fixed by
// waiting, so it fails immediately instead of spending an hour of backoff first.
func TestPermanentTransferErrorsAreNotRetried(t *testing.T) {
	if !permanentTransferError(fmt.Errorf("staging: %w", aliyunpan.ErrSourceChanged)) {
		t.Error("a changed source should not be retried")
	}
	if !permanentTransferError(fmt.Errorf("staging: %w", aliyunpan.ErrPathNotFound)) {
		t.Error("a file that is no longer there should not be retried")
	}
	for _, err := range []error{
		errors.New("下载分片返回 HTTP 502"),
		context.Canceled,
		context.DeadlineExceeded,
		errors.New("开始上传: rpc error"),
	} {
		if permanentTransferError(err) {
			t.Errorf("%v should be retried rather than failed outright", err)
		}
	}
}

// takeNext charges an attempt the moment it claims an item, so every requeue
// that is not the transfer's own fault has to give that attempt back. Without
// this a handful of plugin restarts, or a staging disk that stayed full for an
// hour, would exhaust the budget of a file that had never actually failed.
func TestRequeueOnlyChargesRealFailures(t *testing.T) {
	engine := retryingEngine(t, 5)
	item := &Item{ID: "item-1", State: StateRunning, RemotePath: "/a/file.bin"}
	engine.queue = []*Item{item}

	for name, requeue := range map[string]func(){
		"no login":       func() { engine.deferUntilLogin(context.Background(), item) },
		"no stage room":  func() { engine.deferUntilResource(context.Background(), item, ErrStageRoom) },
		"plugin restart": func() { engine.deferUntilRestart(context.Background(), item) },
	} {
		item.Attempts = 3
		requeue()
		if item.Attempts != 2 {
			t.Errorf("%s: attempts = %d, want the claim's charge refunded to 2", name, item.Attempts)
		}
	}

	// A real failure keeps the charge.
	item.Attempts = 3
	engine.retryLater(context.Background(), item, errors.New("下载分片返回 HTTP 502"))
	if item.Attempts != 3 {
		t.Errorf("attempts = %d, want a genuine failure to stay charged", item.Attempts)
	}
}

// Shutting the plugin down interrupts every running transfer at once. None of
// them failed, so none of them should be charged or made to wait.
func TestShutdownRequeuesWithoutChargingOrWaiting(t *testing.T) {
	engine := retryingEngine(t, 5)
	item := &Item{ID: "item-1", State: StateRunning, RemotePath: "/a/file.bin", Attempts: 1}
	engine.queue = []*Item{item}
	engine.stopping = true
	directory := stageOneItem(t, engine, item)

	engine.transfer(context.Background(), item, hostapi.RuntimeSettings{UploadConcurrency: 1})

	if item.State != StatePending {
		t.Fatalf("state = %s, want pending", item.State)
	}
	if item.Attempts != 0 {
		t.Fatalf("attempts = %d, want the shutdown not to count against the budget", item.Attempts)
	}
	if item.NextAttemptAt != 0 {
		t.Fatalf("nextAttemptAt = %d, want no backoff across a restart", item.NextAttemptAt)
	}
	if _, err := os.Stat(directory); err != nil {
		t.Fatalf("a shutdown discarded the partial download: %v", err)
	}
}

// Cancelling a pending or failed item must discard its staging directory,
// because non-running items have no transfer goroutine to run deferred cleanup.
func TestCancelNonRunningItemDiscardsStagedWork(t *testing.T) {
	dataDir := t.TempDir()
	engine := &Engine{host: hostapi.New(&zeroSegmentHost{}), dataDir: dataDir}
	item := &Item{ID: "item-1", State: StatePending, RemotePath: "/a/file.bin", Size: 6}
	engine.queue = []*Item{item}

	directory := stageOneItem(t, engine, item)

	if err := engine.Cancel(context.Background(), "item-1"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if item.State != StateCancelled {
		t.Fatalf("state = %s, want cancelled", item.State)
	}
	if _, err := os.Stat(directory); !os.IsNotExist(err) {
		t.Fatalf("cancelled pending item left staged directory behind: %v", err)
	}
}

// Completed items have already moved their file and must not be retried.
func TestRetryRejectsCompletedItems(t *testing.T) {
	engine := &Engine{host: hostapi.New(&zeroSegmentHost{})}
	item := &Item{ID: "item-1", State: StateComplete, RemotePath: "/a/file.bin"}
	engine.queue = []*Item{item}

	if err := engine.Retry(context.Background(), "item-1"); err == nil {
		t.Fatal("retrying a completed item should be refused")
	}
	if item.State != StateComplete {
		t.Fatalf("state = %s, want complete", item.State)
	}
}

// A crash while running should refund the attempt when the process recovers.
func TestLoadRefundsAttemptForRunningItems(t *testing.T) {
	dataDir := t.TempDir()
	host := &zeroSegmentHost{
		data: map[string]json.RawMessage{
			queueKey: json.RawMessage(`[{"id":"item-1","jobId":"j1","state":"running","remotePath":"/a","attempts":3}]`),
		},
	}
	engine := New(hostapi.New(host), dataDir, nil)
	if err := engine.Load(context.Background()); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(engine.queue) != 1 {
		t.Fatalf("queue len = %d, want 1", len(engine.queue))
	}
	recovered := engine.queue[0]
	if recovered.State != StatePending {
		t.Fatalf("recovered state = %s, want pending", recovered.State)
	}
	if recovered.Attempts != 2 {
		t.Fatalf("recovered attempts = %d, want 2 (refunded)", recovered.Attempts)
	}
}

// DeleteStaged must sanitize IDs and reject path traversal.
func TestDeleteStagedRejectsTraversalIDs(t *testing.T) {
	dataDir := t.TempDir()
	engine := &Engine{host: hostapi.New(&zeroSegmentHost{}), dataDir: dataDir}

	for _, badID := range []string{"..", "../..", "a/b", "/etc/passwd", "foo\x00bar"} {
		deleted, err := engine.DeleteStaged(context.Background(), badID)
		if err == nil || deleted != 0 {
			t.Errorf("DeleteStaged(%q) = %d, %v; want error and 0 deleted", badID, deleted, err)
		}
	}
}

// Cancelling a failed item must also discard its staging directory.
func TestCancelFailedItemDiscardsStagedWork(t *testing.T) {
	dataDir := t.TempDir()
	engine := &Engine{host: hostapi.New(&zeroSegmentHost{}), dataDir: dataDir}
	item := &Item{ID: "item-1", State: StateFailed, RemotePath: "/a/file.bin", Size: 6}
	engine.queue = []*Item{item}

	directory := stageOneItem(t, engine, item)

	if err := engine.Cancel(context.Background(), "item-1"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if item.State != StateCancelled {
		t.Fatalf("state = %s, want cancelled", item.State)
	}
	if _, err := os.Stat(directory); !os.IsNotExist(err) {
		t.Fatalf("cancelled failed item left staged directory behind: %v", err)
	}
}

// A crash during StageFinishing should complete the item and clean up on restart.
func TestLoadFinishesInterruptedCleanups(t *testing.T) {
	dataDir := t.TempDir()
	host := &zeroSegmentHost{
		data: map[string]json.RawMessage{
			queueKey: json.RawMessage(`[{"id":"item-1","jobId":"j1","state":"running","stage":"finishing","remotePath":"/a","size":100}]`),
		},
	}
	engine := New(hostapi.New(host), dataDir, nil)
	directory := itemStageDir(engine.StageDir(), "item-1")
	staged := aliyunpan.StagedPath(directory, "/a")
	_ = os.MkdirAll(filepath.Dir(staged), 0o750)
	_ = os.WriteFile(staged, []byte("staged"), 0o600)

	if err := engine.Load(context.Background()); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(engine.queue) != 1 {
		t.Fatalf("queue len = %d, want 1", len(engine.queue))
	}
	recovered := engine.queue[0]
	if recovered.State != StateComplete {
		t.Fatalf("state = %s, want complete", recovered.State)
	}
	if recovered.Uploaded != 100 {
		t.Fatalf("uploaded = %d, want 100", recovered.Uploaded)
	}
	// Default DeleteLocalAfterUpload is true, so staged file should be cleaned up.
	if _, err := os.Stat(directory); !os.IsNotExist(err) {
		t.Fatalf("staged directory survived recovery of finishing item: %v", err)
	}
}
