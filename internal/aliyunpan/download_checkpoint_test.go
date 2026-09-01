//go:build legacycli

package aliyunpan

import (
	"encoding/base64"
	"testing"
)

// The two fixtures below were captured verbatim from a running transfer twelve
// seconds apart. They are the real on-disk contents of
// "03.mkv.aliyunpan-downloading" for a 722517896 byte file being fetched with
// three parallel slices, which is why they are kept base64 encoded rather than
// spelled out as JSON: that is exactly the shape the parser has to accept.
const (
	checkpointFirstSample  = "eyJyYW5nZUdlbk1vZGUiOjEsInRvdGFsU2l6ZSI6NzIyNTE3ODk2LCJnZW5CZWdpbiI6MTczMDE1MDQwLCJibG9ja1NpemUiOjU3NjcxNjgwLCJyYW5nZXMiOlt7ImJlZ2luIjo0NTAyMzIzMiwiZW5kIjo1NzY3MTY4MH0seyJiZWdpbiI6MTAyNjk0OTEyLCJlbmQiOjExNTM0MzM2MH0seyJiZWdpbiI6MTYwNjI4NzM2LCJlbmQiOjE3MzAxNTA0MH1dfQ=="
	checkpointSecondSample = "eyJyYW5nZUdlbk1vZGUiOjEsInRvdGFsU2l6ZSI6NzIyNTE3ODk2LCJnZW5CZWdpbiI6MTczMDE1MDQwLCJibG9ja1NpemUiOjU3NjcxNjgwLCJyYW5nZXMiOlt7ImJlZ2luIjo0NjMzMzk1MiwiZW5kIjo1NzY3MTY4MH0seyJiZWdpbiI6MTA0MDA1NjMyLCJlbmQiOjExNTM0MzM2MH0seyJiZWdpbiI6MTYxNjc3MzEyLCJlbmQiOjE3MzAxNTA0MH1dfQ=="

	capturedTotalSize = int64(722517896)
)

func TestDownloadedFromCheckpointReadsRealSamples(t *testing.T) {
	first, ok := downloadedFromCheckpoint([]byte(checkpointFirstSample))
	if !ok {
		t.Fatal("first sample was not understood")
	}
	second, ok := downloadedFromCheckpoint([]byte(checkpointSecondSample))
	if !ok {
		t.Fatal("second sample was not understood")
	}

	// Derived by hand from the fixture: total minus the three outstanding
	// ranges minus everything past genBegin that has not been handed out yet.
	const wantFirst = int64(135331840)
	const wantSecond = int64(139001856)
	if first != wantFirst {
		t.Errorf("first sample downloaded = %d, want %d", first, wantFirst)
	}
	if second != wantSecond {
		t.Errorf("second sample downloaded = %d, want %d", second, wantSecond)
	}
	if second <= first {
		t.Errorf("progress must advance between samples, got %d then %d", first, second)
	}
	if second > capturedTotalSize {
		t.Errorf("progress %d exceeded the file size %d", second, capturedTotalSize)
	}
}

// A transfer that has only just started has handed out no ranges at all, and
// must read as zero rather than as a finished file.
func TestDownloadedFromCheckpointTreatsUngeneratedTailAsOutstanding(t *testing.T) {
	document := `{"rangeGenMode":1,"totalSize":1000,"genBegin":0,"blockSize":100,"ranges":[]}`
	encoded := base64.StdEncoding.EncodeToString([]byte(document))

	done, ok := downloadedFromCheckpoint([]byte(encoded))
	if !ok {
		t.Fatal("a freshly started checkpoint was not understood")
	}
	if done != 0 {
		t.Errorf("downloaded = %d, want 0 for a transfer that has not started", done)
	}
}

// When every range has been consumed and the generator has reached the end, the
// whole file is on disk.
func TestDownloadedFromCheckpointReportsCompletion(t *testing.T) {
	document := `{"rangeGenMode":1,"totalSize":1000,"genBegin":1000,"blockSize":100,"ranges":[]}`
	encoded := base64.StdEncoding.EncodeToString([]byte(document))

	done, ok := downloadedFromCheckpoint([]byte(encoded))
	if !ok {
		t.Fatal("a completed checkpoint was not understood")
	}
	if done != 1000 {
		t.Errorf("downloaded = %d, want 1000", done)
	}
}

// Anything that is not a checkpoint has to be rejected rather than guessed at.
// Callers rely on the boolean to avoid falling back to the pre-allocated target
// file's size, which would report a finished transfer immediately.
func TestDownloadedFromCheckpointRejectsUnusableInput(t *testing.T) {
	cases := map[string]string{
		"empty":           "",
		"whitespace":      "   \n",
		"not base64":      "this is not base64 !!!",
		"base64 not json": base64.StdEncoding.EncodeToString([]byte("plain text")),
		"json without size": base64.StdEncoding.EncodeToString(
			[]byte(`{"rangeGenMode":1,"genBegin":0,"ranges":[]}`),
		),
	}
	for name, input := range cases {
		if done, ok := downloadedFromCheckpoint([]byte(input)); ok {
			t.Errorf("%s: expected rejection, got downloaded = %d", name, done)
		}
	}
}

// A checkpoint whose ranges are stale enough to overshoot must still produce a
// value inside the file, because the queue clamps against it and a negative or
// oversized reading would corrupt the progress bar.
func TestDownloadedFromCheckpointStaysWithinTheFile(t *testing.T) {
	document := `{"rangeGenMode":1,"totalSize":500,"genBegin":500,"blockSize":100,"ranges":[{"begin":0,"end":9000}]}`
	encoded := base64.StdEncoding.EncodeToString([]byte(document))

	done, ok := downloadedFromCheckpoint([]byte(encoded))
	if !ok {
		t.Fatal("checkpoint was not understood")
	}
	if done < 0 || done > 500 {
		t.Errorf("downloaded = %d, want a value within [0, 500]", done)
	}
}
