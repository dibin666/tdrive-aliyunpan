package sync

import (
	"context"
	"testing"

	"github.com/dibin/tdrive-aliyunpan/internal/hostapi"
)

func newQueueEngine(items ...*Item) *Engine {
	return &Engine{
		host:       hostapi.New(&zeroSegmentHost{}),
		queue:      items,
		cancels:    make(map[string]context.CancelFunc),
		cancelling: make(map[string]bool),
		deleting:   make(map[string]int),
	}
}

// Refusing to cancel a running transfer meant that pressing 取消 on the one
// file actually moving bytes did nothing, and the queue kept uploading
// something nobody wanted with no way to stop it.
func TestCancelInterruptsARunningTransfer(t *testing.T) {
	item := &Item{ID: "a", State: StateRunning, Stage: StageUploading}
	engine := newQueueEngine(item)

	ctx, release := engine.watchItem(context.Background(), item.ID)
	defer release()

	if err := engine.Cancel(context.Background(), item.ID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if item.State != StateCancelled {
		t.Errorf("State = %q, want %q", item.State, StateCancelled)
	}
	select {
	case <-ctx.Done():
	default:
		t.Error("cancelling left the running transfer's context alive")
	}
	// The transfer goroutine asks this before deciding what its error meant.
	if !engine.stoppedByCancel(item.ID) {
		t.Error("the transfer was not told it stopped on purpose")
	}
}

// A cancelled transfer fails with a context error like any other, and the
// deferred-retry path would read that as a blip and queue the file straight
// back up — undoing the cancellation the operator just asked for.
func TestCancelledItemIsNotRequeued(t *testing.T) {
	item := &Item{ID: "a", State: StateRunning}
	engine := newQueueEngine(item)

	_, release := engine.watchItem(context.Background(), item.ID)
	defer release()
	if err := engine.Cancel(context.Background(), item.ID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	if !engine.stoppedByCancel(item.ID) {
		t.Fatal("stoppedByCancel did not report the cancellation")
	}
	if item.State != StateCancelled {
		t.Fatalf("State = %q, want %q", item.State, StateCancelled)
	}
}

// Selecting a screenful and pressing an action should do what it can rather
// than refuse because one row does not qualify.
func TestCancelSkipsFinishedItemsInABatch(t *testing.T) {
	pending := &Item{ID: "a", State: StatePending}
	done := &Item{ID: "b", State: StateComplete}
	engine := newQueueEngine(pending, done)

	if err := engine.Cancel(context.Background(), "a", "b"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if pending.State != StateCancelled {
		t.Errorf("pending item = %q, want cancelled", pending.State)
	}
	if done.State != StateComplete {
		t.Errorf("finished item was changed to %q", done.State)
	}
}

func TestCancelReportsWhenNothingCanBeStopped(t *testing.T) {
	engine := newQueueEngine(&Item{ID: "a", State: StateComplete})
	if err := engine.Cancel(context.Background(), "a"); err == nil {
		t.Error("cancelling an already-finished item was reported as success")
	}
	if err := engine.Cancel(context.Background(), "missing"); err == nil {
		t.Error("cancelling an unknown id was reported as success")
	}
}

// A retry has nothing to add to a transfer that is already running, but it must
// not stop the rest of the batch from being requeued.
func TestRetryRequeuesEverythingButRunningItems(t *testing.T) {
	failed := &Item{ID: "a", State: StateFailed, Error: "boom", Uploaded: 10, FinishedAt: 5}
	running := &Item{ID: "b", State: StateRunning}
	engine := newQueueEngine(failed, running)

	if err := engine.Retry(context.Background(), "a", "b"); err != nil {
		t.Fatalf("Retry: %v", err)
	}
	if failed.State != StatePending || failed.Error != "" || failed.Uploaded != 0 || failed.FinishedAt != 0 {
		t.Errorf("failed item was not reset: %+v", failed)
	}
	if running.State != StateRunning {
		t.Errorf("running item = %q, want it left alone", running.State)
	}
}

// Deleting selected records must leave a running transfer's row in place, or
// its goroutine would go on writing to something nothing can show.
func TestClearFinishedRemovesOnlyTheNamedFinishedRows(t *testing.T) {
	engine := newQueueEngine(
		&Item{ID: "a", State: StateComplete},
		&Item{ID: "b", State: StateFailed},
		&Item{ID: "c", State: StateRunning},
	)

	engine.ClearFinished(context.Background(), "a", "c")

	kept := make([]string, 0, len(engine.queue))
	for _, item := range engine.queue {
		kept = append(kept, item.ID)
	}
	if len(kept) != 2 || kept[0] != "b" || kept[1] != "c" {
		t.Fatalf("queue = %v, want the unnamed failed row and the running one", kept)
	}
}
