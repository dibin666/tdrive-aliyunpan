package sync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/dibin/tdrive-aliyunpan/internal/hostapi"
	"github.com/dibin/tdrive-aliyunpan/internal/settings"
	tdriveplugin "github.com/dibin/tdrive/pkg/plugin"
)

// uploadScenarioHost models the host calls that matter to a cancellation. The
// first segment blocks until its plugin context is cancelled; subsequent jobs
// complete normally so the test can prove that retry starts a fresh upload.
type uploadScenarioHost struct {
	mu sync.Mutex

	nextJob       int
	jobIDs        []string
	aborted       []string
	abortStates   []string
	persistCancel bool
	putStarted    chan struct{}
	abortStarted  chan struct{}
	abortRelease  chan struct{}
	blockAbort    bool
	putOnce       sync.Once
	abortOnce     sync.Once
}

func (host *uploadScenarioHost) Call(ctx context.Context, method string, request, response any) error {
	switch method {
	case "users.list":
		return assignUploadTestResponse(response, []tdriveplugin.User{{
			ID: "user-1", Username: "tester", Enabled: true,
		}})
	case "files.beginUpload":
		host.mu.Lock()
		host.nextJob++
		jobID := "job-" + string(rune('0'+host.nextJob))
		host.jobIDs = append(host.jobIDs, jobID)
		host.mu.Unlock()
		job := tdriveplugin.UploadJob{
			ID: jobID, SegmentSize: 1, SegmentCount: 1, TotalSize: 1,
		}
		return assignUploadTestResponse(response, struct {
			Job  tdriveplugin.UploadJob `json:"job"`
			File tdriveplugin.File      `json:"file"`
		}{Job: job})
	case "files.completeUpload":
		return assignUploadTestResponse(response, tdriveplugin.File{ID: "file-1", Size: 1})
	case "files.abortUpload":
		var input struct {
			JobID string `json:"jobId"`
			State string `json:"state"`
		}
		if err := decodeUploadTestRequest(request, &input); err != nil {
			return err
		}
		host.mu.Lock()
		host.aborted = append(host.aborted, input.JobID)
		host.abortStates = append(host.abortStates, input.State)
		block := host.blockAbort
		host.mu.Unlock()
		if block {
			host.abortOnce.Do(func() { close(host.abortStarted) })
			<-host.abortRelease
		}
		return nil
	case "data.set":
		host.mu.Lock()
		host.persistCancel = ctx.Err() != nil
		host.mu.Unlock()
		return nil
	default:
		return nil
	}
}

func (host *uploadScenarioHost) OpenStream(ctx context.Context, method string, request any) (io.ReadWriteCloser, error) {
	if method != "files.putSegment" {
		return nil, errors.New("unexpected stream method")
	}
	var input struct {
		JobID string `json:"jobId"`
	}
	if err := decodeUploadTestRequest(request, &input); err != nil {
		return nil, err
	}
	if input.JobID == "job-1" {
		host.putOnce.Do(func() { close(host.putStarted) })
		return blockingUploadStream{ctx: ctx}, nil
	}
	return uploadBufferStream{Buffer: bytes.NewBuffer(nil)}, nil
}

func assignUploadTestResponse(target, value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return json.Unmarshal(encoded, target)
}

func decodeUploadTestRequest(request, target any) error {
	encoded, err := json.Marshal(request)
	if err != nil {
		return err
	}
	return json.Unmarshal(encoded, target)
}

type blockingUploadStream struct {
	ctx context.Context
}

func (stream blockingUploadStream) Write(p []byte) (int, error) {
	select {
	case <-stream.ctx.Done():
		return 0, stream.ctx.Err()
	}
}

func (blockingUploadStream) Read([]byte) (int, error) { return 0, io.EOF }

func (blockingUploadStream) Close() error { return nil }

type uploadBufferStream struct{ *bytes.Buffer }

func (uploadBufferStream) Close() error { return nil }

func newUploadScenarioEngine(host *uploadScenarioHost) *Engine {
	return &Engine{
		host:       hostapi.New(host),
		dataDir:    ".",
		settings:   settings.Settings{},
		runs:       make(map[string]*transferRun),
		cancels:    make(map[string]context.CancelFunc),
		cancelling: make(map[string]bool),
		deleting:   make(map[string]int),
	}
}

// A running upload must be fully unwound before Cancel returns. The old code
// returned as soon as it called cancel, allowing an immediate retry to race
// the old writer and its host cleanup.
func TestCancelWaitsForUploadAndRetryUsesANewHostJob(t *testing.T) {
	host := &uploadScenarioHost{putStarted: make(chan struct{})}
	engine := newUploadScenarioEngine(host)
	item := &Item{
		ID: "item-1", State: StateRunning, RemotePath: "/a/file.bin",
		TargetDir: "/target", Name: "file.bin", Size: 1,
	}
	engine.queue = []*Item{item}
	staged := t.TempDir() + "/file.bin"
	if err := os.WriteFile(staged, []byte("x"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	ctx, release := engine.watchItem(context.Background(), item.ID)
	workerDone := make(chan error, 1)
	go func() {
		workerDone <- engine.upload(ctx, item, staged, hostapi.RuntimeSettings{})
		release()
	}()
	select {
	case <-host.putStarted:
	case <-time.After(time.Second):
		t.Fatal("upload did not reach the blocking segment")
	}

	cancelDone := make(chan error, 1)
	go func() { cancelDone <- engine.Cancel(context.Background(), item.ID) }()
	if err := <-cancelDone; err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if err := <-workerDone; err == nil {
		t.Fatal("cancelled upload unexpectedly completed")
	}

	host.mu.Lock()
	jobIDs := append([]string(nil), host.jobIDs...)
	aborted := append([]string(nil), host.aborted...)
	abortStates := append([]string(nil), host.abortStates...)
	persistCancel := host.persistCancel
	host.mu.Unlock()
	if len(jobIDs) != 1 || jobIDs[0] != "job-1" {
		t.Fatalf("host jobs = %v, want [job-1]", jobIDs)
	}
	if len(aborted) != 1 || aborted[0] != "job-1" {
		t.Fatalf("host aborts = %v, want one cleanup of job-1", aborted)
	}
	if len(abortStates) != 1 || abortStates[0] != "cancelled" {
		t.Fatalf("abort states = %v, want [cancelled]", abortStates)
	}
	if persistCancel {
		t.Fatal("Cancel persistence used its already-cancelled request context")
	}
	if item.State != StateCancelled || item.UploadJobID != "" {
		t.Fatalf("cancelled item = %+v, want no old host job", item)
	}
	engine.mu.Lock()
	if len(engine.runs) != 0 || len(engine.cancels) != 0 || len(engine.cancelling) != 0 {
		t.Fatalf("run state survived Cancel: runs=%d cancels=%d cancelling=%d", len(engine.runs), len(engine.cancels), len(engine.cancelling))
	}
	engine.mu.Unlock()

	requestCtx, cancelRequest := context.WithCancel(context.Background())
	cancelRequest()
	if err := engine.Retry(requestCtx, item.ID); err != nil {
		t.Fatalf("Retry: %v", err)
	}
	if item.State != StatePending || item.Error != "" || item.NextAttemptAt != 0 {
		t.Fatalf("retried item = %+v, want clean pending state", item)
	}

	engine.mu.Lock()
	item.State = StateRunning
	engine.mu.Unlock()
	ctx, release = engine.watchItem(context.Background(), item.ID)
	workerDone = make(chan error, 1)
	go func() {
		workerDone <- engine.upload(ctx, item, staged, hostapi.RuntimeSettings{})
		release()
	}()
	if err := <-workerDone; err != nil {
		t.Fatalf("retry upload: %v", err)
	}

	host.mu.Lock()
	jobIDs = append([]string(nil), host.jobIDs...)
	aborted = append([]string(nil), host.aborted...)
	host.mu.Unlock()
	if len(jobIDs) != 2 || jobIDs[0] == jobIDs[1] || jobIDs[1] != "job-2" {
		t.Fatalf("host jobs after retry = %v, want distinct job-1 and job-2", jobIDs)
	}
	if len(aborted) != 1 {
		t.Fatalf("retry cleaned a new host job unexpectedly: %v", aborted)
	}
	if item.UploadJobID != "" {
		t.Fatalf("successful retry retained host job ID %q", item.UploadJobID)
	}
}

// StateRunning can survive a process stop without a live worker. Cancelling
// such a row must not manufacture a cancelling marker that blocks Retry.
func TestCancelStaleRunningItemCanRetryImmediately(t *testing.T) {
	engine := newUploadScenarioEngine(&uploadScenarioHost{putStarted: make(chan struct{})})
	item := &Item{
		ID: "item-1", State: StateRunning, RemotePath: "/a/file.bin",
		UploadJobID: "stale-job", Error: "old error", Attempts: 3,
	}
	engine.queue = []*Item{item}

	if err := engine.Cancel(context.Background(), item.ID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if item.State != StateCancelled {
		t.Fatalf("state = %s, want cancelled", item.State)
	}
	engine.mu.Lock()
	if len(engine.runs) != 0 || len(engine.cancels) != 0 || len(engine.cancelling) != 0 {
		t.Fatalf("stale cancellation state survived: runs=%d cancels=%d cancelling=%d", len(engine.runs), len(engine.cancels), len(engine.cancelling))
	}
	engine.mu.Unlock()

	if err := engine.Retry(context.Background(), item.ID); err != nil {
		t.Fatalf("Retry after stale running cancellation: %v", err)
	}
	if item.State != StatePending || item.Error != "" || item.UploadJobID != "" || item.Attempts != 0 {
		t.Fatalf("retried stale item = %+v", item)
	}
	if err := engine.Cancel(context.Background(), item.ID); err != nil {
		t.Fatalf("repeated Cancel: %v", err)
	}
}

func TestRetryWaitsForStaleUploadCleanup(t *testing.T) {
	host := &uploadScenarioHost{
		putStarted:   make(chan struct{}),
		abortStarted: make(chan struct{}),
		abortRelease: make(chan struct{}),
		blockAbort:   true,
	}
	engine := newUploadScenarioEngine(host)
	item := &Item{
		ID: "item-1", State: StateFailed, RemotePath: "/a/file.bin",
		UploadJobID: "stale-job", Error: "old error", Attempts: 2,
	}
	engine.queue = []*Item{item}

	firstDone := make(chan error, 1)
	go func() { firstDone <- engine.Retry(context.Background(), item.ID) }()
	select {
	case <-host.abortStarted:
	case <-time.After(time.Second):
		t.Fatal("retry did not clean the stale upload job")
	}
	if err := engine.Retry(context.Background(), item.ID); err == nil {
		t.Fatal("a second retry was admitted while old-job cleanup was running")
	}
	if item.State != StateFailed {
		t.Fatalf("item state during cleanup = %s, want failed", item.State)
	}
	close(host.abortRelease)
	if err := <-firstDone; err != nil {
		t.Fatalf("first Retry: %v", err)
	}
	if item.State != StatePending || item.UploadJobID != "" {
		t.Fatalf("item after cleanup = %+v, want clean pending state", item)
	}
}

// The dispatch path records the run while it still holds the queue lock. A
// cancellation between that claim and the worker's watchItem call therefore
// has a done channel to wait on and the worker enters already cancelled.
func TestDispatchRegistersRunBeforeWorkerStarts(t *testing.T) {
	job := settings.Job{ID: "job-1", Enabled: true, RemotePath: "/a", TargetPath: "/b"}
	item := &Item{ID: "item-1", JobID: job.ID, State: StatePending, Size: 1}
	engine := newUploadScenarioEngine(&uploadScenarioHost{putStarted: make(chan struct{})})
	engine.settings = settings.Settings{Jobs: []settings.Job{job}}
	engine.queue = []*Item{item}

	taken := engine.takeNextForDispatch(context.Background(), 1)
	if taken != item {
		t.Fatalf("takeNextForDispatch returned %v, want item", taken)
	}
	engine.mu.Lock()
	run := engine.runs[item.ID]
	engine.mu.Unlock()
	if run == nil {
		t.Fatal("dispatch claim did not install a run record")
	}

	cancelDone := make(chan error, 1)
	go func() { cancelDone <- engine.Cancel(context.Background(), item.ID) }()
	select {
	case <-run.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("pre-start run was not cancelled")
	}

	workerCtx, release := engine.watchItem(context.Background(), item.ID)
	if workerCtx.Err() == nil {
		t.Fatal("worker registered after cancellation with a live context")
	}
	release()
	engine.finishActive()
	if err := <-cancelDone; err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if item.State != StateCancelled {
		t.Fatalf("state = %s, want cancelled", item.State)
	}
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if len(engine.runs) != 0 || len(engine.cancels) != 0 || len(engine.cancelling) != 0 {
		t.Fatalf("pre-start cancellation left run state: %+v/%+v/%+v", engine.runs, engine.cancels, engine.cancelling)
	}
}
