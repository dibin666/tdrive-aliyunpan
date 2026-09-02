package aliyunpan

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// identityFor builds the metadata a checkpoint is matched against, so the tests
// below only have to say what they are actually varying.
func identityFor(size, chunkSize int64) chunkCheckpoint {
	return chunkCheckpoint{
		DriveID:   "drive-1",
		FileID:    "file-1",
		Size:      size,
		Hash:      "hash-1",
		ChunkSize: chunkSize,
	}
}

func writeRawCheckpoint(t *testing.T, partial string, document chunkCheckpoint) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(partial), 0o750); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	encoded, err := json.Marshal(document)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if err := os.WriteFile(checkpointPath(partial), encoded, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
}

// The part-way offsets are the whole reason for version 2, so they have to
// survive a write and a read by a process that knows nothing about the one that
// wrote them.
func TestCheckpointPersistsPartialOffsets(t *testing.T) {
	partial := filepath.Join(t.TempDir(), "file.bin.part")
	identity := identityFor(100, 10)

	checkpoint := newCheckpoint(partial, identity)
	if err := checkpoint.markDone(0); err != nil {
		t.Fatalf("markDone: %v", err)
	}
	if err := checkpoint.setPartials(map[int]int64{3: 4, 5: 7}); err != nil {
		t.Fatalf("setPartials: %v", err)
	}

	reloaded, ok := loadCheckpoint(partial, identity)
	if !ok {
		t.Fatal("a checkpoint this process just wrote was not accepted")
	}
	if got := reloaded.partialBytes(3); got != 4 {
		t.Errorf("partialBytes(3) = %d, want 4", got)
	}
	if got := reloaded.partialBytes(5); got != 7 {
		t.Errorf("partialBytes(5) = %d, want 7", got)
	}
	chunks, done, partialBytes := reloaded.completed()
	if chunks != 1 || done != 10 {
		t.Errorf("completed chunks = %d, bytes = %d; want 1 and 10", chunks, done)
	}
	if partialBytes != 11 {
		t.Errorf("partial bytes = %d, want 11", partialBytes)
	}
	if got := checkpointBytesForSize(partial, 100); got != 21 {
		t.Errorf("checkpointBytesForSize = %d, want the 10 committed plus 11 part-way bytes", got)
	}
}

// A chunk the bitmap has accepted owns all of its bytes. Leaving its part-way
// offset behind would count those bytes a second time and report more progress
// than the file holds.
func TestCheckpointMarkDoneClearsPartialOffset(t *testing.T) {
	partial := filepath.Join(t.TempDir(), "file.bin.part")
	identity := identityFor(100, 10)

	checkpoint := newCheckpoint(partial, identity)
	if err := checkpoint.setPartials(map[int]int64{2: 6}); err != nil {
		t.Fatalf("setPartials: %v", err)
	}
	if err := checkpoint.markDone(2); err != nil {
		t.Fatalf("markDone: %v", err)
	}
	if got := checkpoint.partialBytes(2); got != 0 {
		t.Errorf("partialBytes after markDone = %d, want 0", got)
	}
	if _, _, partialBytes := checkpoint.completed(); partialBytes != 0 {
		t.Errorf("partial bytes after markDone = %d, want 0", partialBytes)
	}
	if got := checkpointBytesForSize(partial, 100); got != 10 {
		t.Errorf("checkpointBytesForSize = %d, want exactly the one finished chunk", got)
	}
}

// Upgrading the plugin must not throw away a download that is already half
// finished, so a version 1 sidecar is read as "no chunk part-way done" rather
// than rejected.
func TestCheckpointAcceptsVersionOneSidecar(t *testing.T) {
	partial := filepath.Join(t.TempDir(), "file.bin.part")
	identity := identityFor(100, 10)

	document := identity
	document.Version = 1
	done := make([]bool, 10)
	done[0], done[1] = true, true
	document.Done = encodeChunkBitmap(done)
	writeRawCheckpoint(t, partial, document)

	reloaded, ok := loadCheckpoint(partial, identity)
	if !ok {
		t.Fatal("a version 1 sidecar was rejected, discarding a resumable download")
	}
	chunks, doneBytes, partialBytes := reloaded.completed()
	if chunks != 2 || doneBytes != 20 || partialBytes != 0 {
		t.Errorf("completed = (%d, %d, %d), want (2, 20, 0)", chunks, doneBytes, partialBytes)
	}
	// Writing it back must upgrade the document, not preserve the old shape.
	if err := reloaded.save(); err != nil {
		t.Fatalf("save: %v", err)
	}
	raw, err := os.ReadFile(checkpointPath(partial))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	var stored chunkCheckpoint
	if err := json.Unmarshal(raw, &stored); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if stored.Version != chunkCheckpointVersion {
		t.Errorf("rewritten version = %d, want %d", stored.Version, chunkCheckpointVersion)
	}
}

// An offset that cannot describe a chunk that still has to be fetched is
// dropped rather than repaired. Re-fetching one chunk is cheap; resuming at the
// wrong offset writes a hole into the file and is not detectable afterwards.
func TestCheckpointDropsUnusablePartialOffsets(t *testing.T) {
	partial := filepath.Join(t.TempDir(), "file.bin.part")
	identity := identityFor(100, 10)

	document := identity
	document.Version = chunkCheckpointVersion
	done := make([]bool, 10)
	done[4] = true
	document.Done = encodeChunkBitmap(done)
	document.Partial = map[string]int64{
		"4":     3,  // the bitmap already owns every byte of this chunk
		"9":     10, // at the chunk's end, which only the bitmap may say
		"8":     99, // past the chunk's end entirely
		"2":     0,  // says nothing
		"1":     -5, // nonsense
		"77":    2,  // no such chunk
		"three": 2,  // not an index
		"6":     6,  // the only usable entry
	}
	writeRawCheckpoint(t, partial, document)

	reloaded, ok := loadCheckpoint(partial, identity)
	if !ok {
		t.Fatal("checkpoint was rejected outright")
	}
	for _, index := range []int{1, 2, 4, 8, 9, 77} {
		if got := reloaded.partialBytes(index); got != 0 {
			t.Errorf("partialBytes(%d) = %d, want the unusable entry dropped", index, got)
		}
	}
	if got := reloaded.partialBytes(6); got != 6 {
		t.Errorf("partialBytes(6) = %d, want the one usable entry kept", got)
	}
	if _, _, partialBytes := reloaded.completed(); partialBytes != 6 {
		t.Errorf("partial bytes = %d, want only the usable entry counted", partialBytes)
	}
}

// A sidecar from a version this build does not understand cannot be
// interpreted, and guessing would write chunks into the wrong offsets.
func TestCheckpointRejectsFutureVersion(t *testing.T) {
	partial := filepath.Join(t.TempDir(), "file.bin.part")
	identity := identityFor(100, 10)

	document := identity
	document.Version = chunkCheckpointVersion + 1
	document.Done = encodeChunkBitmap(make([]bool, 10))
	writeRawCheckpoint(t, partial, document)

	if _, ok := loadCheckpoint(partial, identity); ok {
		t.Fatal("a sidecar from a future version was accepted")
	}
	if got := checkpointBytesForSize(partial, 100); got != 0 {
		t.Errorf("checkpointBytesForSize = %d, want 0 for an unreadable version", got)
	}
}

// setPartials is fed by the flusher, which does not know which chunks finished
// while its snapshot was being taken. It has to apply the same filtering the
// loader does rather than trust its caller.
func TestCheckpointSetPartialsFiltersItsInput(t *testing.T) {
	partial := filepath.Join(t.TempDir(), "file.bin.part")
	identity := identityFor(100, 10)

	checkpoint := newCheckpoint(partial, identity)
	if err := checkpoint.markDone(7); err != nil {
		t.Fatalf("markDone: %v", err)
	}
	if err := checkpoint.setPartials(map[int]int64{7: 5, 12: 3, 0: 10, 1: 4}); err != nil {
		t.Fatalf("setPartials: %v", err)
	}
	if _, _, partialBytes := checkpoint.completed(); partialBytes != 4 {
		t.Errorf("partial bytes = %d, want only the usable entry counted", partialBytes)
	}
	// A later snapshot replaces the previous one wholesale, because a chunk
	// that is no longer in flight must not keep contributing to the total.
	if err := checkpoint.setPartials(map[int]int64{}); err != nil {
		t.Fatalf("setPartials: %v", err)
	}
	if _, _, partialBytes := checkpoint.completed(); partialBytes != 0 {
		t.Errorf("partial bytes = %d, want the stale entry dropped", partialBytes)
	}
}
