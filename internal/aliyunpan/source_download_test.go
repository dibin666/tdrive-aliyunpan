package aliyunpan

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSourceDownloadUsesParallelRangesAndReportsAbsoluteProgress(t *testing.T) {
	content := []byte("abcdefghijkl")
	var activeRequests int32
	var maximumActiveRequests int32
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/adrive/v1.0/openFile/get_by_path":
			writeJSONResponse(response, map[string]any{"drive_id": "drive-1", "file_id": "file-1", "name": "file.bin", "type": "file", "size": len(content)})
		case "/adrive/v1.0/openFile/getDownloadUrl":
			writeJSONResponse(response, map[string]any{"url": "http://" + request.Host + "/content", "size": len(content)})
		case "/content":
			current := atomic.AddInt32(&activeRequests, 1)
			for {
				previous := atomic.LoadInt32(&maximumActiveRequests)
				if current <= previous || atomic.CompareAndSwapInt32(&maximumActiveRequests, previous, current) {
					break
				}
			}
			defer atomic.AddInt32(&activeRequests, -1)
			time.Sleep(30 * time.Millisecond)
			var begin, end int64
			if _, err := fmt.Sscanf(request.Header.Get("Range"), "bytes=%d-%d", &begin, &end); err != nil {
				response.WriteHeader(http.StatusBadRequest)
				return
			}
			response.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", begin, end, len(content)))
			response.WriteHeader(http.StatusPartialContent)
			_, _ = response.Write(content[begin : end+1])
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()

	client := New(t.TempDir(), "")
	client.httpClient = server.Client()
	client.openAPIURL = server.URL
	client.setCredentials(accountCredentials{OpenAPIAccess: "access"})
	var progressMu sync.Mutex
	progressValues := make([]int64, 0, 8)
	stageDir := t.TempDir()
	staged, err := client.Download(context.Background(), DownloadRequest{
		CloudPath:     "/file.bin",
		StageDir:      stageDir,
		DriveID:       "drive-1",
		SliceParallel: 3,
		Retry:         2,
		ChunkSize:     4,
	}, int64(len(content)), func(done int64) {
		progressMu.Lock()
		progressValues = append(progressValues, done)
		progressMu.Unlock()
	})
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	got, err := os.ReadFile(staged)
	if err != nil {
		t.Fatalf("ReadFile staged: %v", err)
	}
	if string(got) != string(content) {
		t.Fatalf("staged content = %q, want %q", got, content)
	}
	if atomic.LoadInt32(&maximumActiveRequests) < 2 {
		t.Fatalf("maximum active range requests = %d, want parallel requests", maximumActiveRequests)
	}
	progressMu.Lock()
	defer progressMu.Unlock()
	if len(progressValues) == 0 || progressValues[len(progressValues)-1] != int64(len(content)) {
		t.Fatalf("progress = %v, want final absolute value %d", progressValues, len(content))
	}
}

func TestSourceDownloadRefreshesExpiredDownloadURL(t *testing.T) {
	content := []byte("abcdefgh")
	var urlRequests int32
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/adrive/v1.0/openFile/get_by_path":
			writeJSONResponse(response, map[string]any{"drive_id": "drive-1", "file_id": "file-1", "name": "file.bin", "type": "file", "size": len(content)})
		case "/adrive/v1.0/openFile/getDownloadUrl":
			version := atomic.AddInt32(&urlRequests, 1)
			writeJSONResponse(response, map[string]string{"url": "http://" + request.Host + "/content?version=" + strconv.Itoa(int(version))})
		case "/content":
			if request.URL.Query().Get("version") == "1" {
				response.WriteHeader(http.StatusForbidden)
				return
			}
			response.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(content)-1, len(content)))
			response.WriteHeader(http.StatusPartialContent)
			_, _ = response.Write(content)
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()

	client := New(t.TempDir(), "")
	client.httpClient = server.Client()
	client.openAPIURL = server.URL
	client.setCredentials(accountCredentials{OpenAPIAccess: "access"})
	staged, err := client.Download(context.Background(), DownloadRequest{
		CloudPath:     "/file.bin",
		StageDir:      t.TempDir(),
		DriveID:       "drive-1",
		SliceParallel: 1,
		Retry:         2,
	}, int64(len(content)), nil)
	if err != nil {
		t.Fatalf("Download after URL expiry: %v", err)
	}
	got, err := os.ReadFile(staged)
	if err != nil {
		t.Fatalf("ReadFile staged: %v", err)
	}
	if string(got) != string(content) {
		t.Fatalf("staged content = %q, want %q", got, content)
	}
	if atomic.LoadInt32(&urlRequests) != 2 {
		t.Fatalf("download URL requests = %d, want 2", urlRequests)
	}
}

func TestSourceDownloadCancellationKeepsResumableState(t *testing.T) {
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/adrive/v1.0/openFile/get_by_path":
			writeJSONResponse(response, map[string]any{"drive_id": "drive-1", "file_id": "file-1", "name": "file.bin", "type": "file", "size": 4})
		case "/adrive/v1.0/openFile/getDownloadUrl":
			writeJSONResponse(response, map[string]string{"url": "http://" + request.Host + "/content"})
		case "/content":
			close(started)
			<-request.Context().Done()
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()

	client := New(t.TempDir(), "")
	client.httpClient = server.Client()
	client.openAPIURL = server.URL
	client.setCredentials(accountCredentials{OpenAPIAccess: "access"})
	stageDir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	downloadDone := make(chan error, 1)
	go func() {
		_, err := client.Download(ctx, DownloadRequest{
			CloudPath:     "/file.bin",
			StageDir:      stageDir,
			DriveID:       "drive-1",
			SliceParallel: 1,
			Retry:         1,
		}, 4, nil)
		downloadDone <- err
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("download request did not start")
	}
	cancel()
	select {
	case err := <-downloadDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("download error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled download did not finish")
	}
	staged := StagedPath(stageDir, "/file.bin")
	if _, err := os.Stat(staged); !os.IsNotExist(err) {
		t.Fatalf("staged file still exists: %v", err)
	}
	// The partial file and its checkpoint are what let the retry resume. The
	// downloader does not know whether this transfer was abandoned or merely
	// interrupted, so it keeps them; the queue decides.
	partial := staged + downloadPartSuffix
	if _, err := os.Stat(partial); err != nil {
		t.Fatalf("partial file was discarded by a cancelled download: %v", err)
	}
	if _, err := os.Stat(checkpointPath(partial)); err != nil {
		t.Fatalf("resume checkpoint was discarded by a cancelled download: %v", err)
	}

	DiscardPartialDownload(stageDir, "/file.bin")
	if _, err := os.Stat(partial); !os.IsNotExist(err) {
		t.Fatalf("partial file survived an explicit discard: %v", err)
	}
	if _, err := os.Stat(checkpointPath(partial)); !os.IsNotExist(err) {
		t.Fatalf("checkpoint survived an explicit discard: %v", err)
	}
}

// The point of the checkpoint is that an interrupted transfer costs the chunks
// it lost, not the file. This drives a download that dies after one chunk and
// then re-runs it against a server that refuses to serve anything already on
// disk, so a resumed byte would show up as a failure rather than as a slow
// success.
func TestSourceDownloadResumesFromCheckpoint(t *testing.T) {
	content := []byte("0123456789abcdef")
	const chunkSize = 4
	var servedRanges struct {
		sync.Mutex
		begins []int64
	}
	failAfterFirstChunk := true
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/adrive/v1.0/openFile/get_by_path":
			writeJSONResponse(response, map[string]any{"drive_id": "drive-1", "file_id": "file-1", "name": "file.bin", "type": "file", "size": len(content)})
		case "/adrive/v1.0/openFile/getDownloadUrl":
			writeJSONResponse(response, map[string]string{"url": "http://" + request.Host + "/content"})
		case "/content":
			var begin, end int64
			if _, err := fmt.Sscanf(request.Header.Get("Range"), "bytes=%d-%d", &begin, &end); err != nil {
				response.WriteHeader(http.StatusBadRequest)
				return
			}
			servedRanges.Lock()
			alreadyServed := len(servedRanges.begins)
			servedRanges.begins = append(servedRanges.begins, begin)
			servedRanges.Unlock()
			if failAfterFirstChunk && alreadyServed >= 1 {
				response.WriteHeader(http.StatusInternalServerError)
				return
			}
			response.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", begin, end, len(content)))
			response.WriteHeader(http.StatusPartialContent)
			_, _ = response.Write(content[begin : end+1])
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()

	client := New(t.TempDir(), "")
	client.httpClient = server.Client()
	client.openAPIURL = server.URL
	client.setCredentials(accountCredentials{OpenAPIAccess: "access"})
	stageDir := t.TempDir()
	requestTemplate := DownloadRequest{
		CloudPath:     "/file.bin",
		StageDir:      stageDir,
		DriveID:       "drive-1",
		SliceParallel: 1,
		Retry:         1,
		ChunkSize:     chunkSize,
	}

	if _, err := client.Download(context.Background(), requestTemplate, int64(len(content)), nil); err == nil {
		t.Fatal("first download should have failed after the server started refusing chunks")
	}
	servedRanges.Lock()
	firstRun := append([]int64(nil), servedRanges.begins...)
	servedRanges.begins = nil
	servedRanges.Unlock()
	if len(firstRun) == 0 || firstRun[0] != 0 {
		t.Fatalf("first run served %v, want it to start at byte 0", firstRun)
	}

	failAfterFirstChunk = false
	var resumedBaseline int64 = -1
	staged, err := client.Download(context.Background(), requestTemplate, int64(len(content)), func(done int64) {
		if resumedBaseline < 0 {
			resumedBaseline = done
		}
	})
	if err != nil {
		t.Fatalf("resumed download: %v", err)
	}
	if resumedBaseline != chunkSize {
		t.Fatalf("first reported progress = %d, want the %d resumed bytes", resumedBaseline, chunkSize)
	}
	servedRanges.Lock()
	secondRun := append([]int64(nil), servedRanges.begins...)
	servedRanges.Unlock()
	for _, begin := range secondRun {
		if begin == 0 {
			t.Fatalf("resumed download re-fetched the completed first chunk: %v", secondRun)
		}
	}

	got, err := os.ReadFile(staged)
	if err != nil {
		t.Fatalf("ReadFile staged: %v", err)
	}
	if string(got) != string(content) {
		t.Fatalf("resumed content = %q, want %q", got, content)
	}
	if _, err := os.Stat(staged + downloadPartSuffix + downloadProgressSuffix); !os.IsNotExist(err) {
		t.Fatalf("checkpoint outlived a finished download: %v", err)
	}
}

// Resuming at a chunk boundary throws away up to a whole chunk on every
// connection, which for the real 32 MiB geometry is around 96 MiB per
// interruption. This drives a download whose connection dies part-way through a
// chunk and asserts that the next attempt asks for the bytes it is actually
// missing rather than the whole chunk again.
func TestSourceDownloadResumesPartwayThroughAChunk(t *testing.T) {
	// The chunk has to be larger than one read buffer, because a chunk is
	// written to disk a buffer at a time and a resume point can only ever be at
	// a boundary the file has actually reached.
	const chunkSize = 2 * downloadBufferSize
	const servedBeforeBreak = downloadBufferSize
	content := make([]byte, 2*chunkSize)
	for index := range content {
		content[index] = byte(index)
	}

	// The flusher is what makes an offset resumable, so the test must not
	// outrun it. Two seconds of real time proves nothing extra.
	previousInterval := partialFlushInterval
	partialFlushInterval = 5 * time.Millisecond
	defer func() { partialFlushInterval = previousInterval }()

	var served struct {
		sync.Mutex
		begins []int64
	}
	breakSecondChunk := true
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/adrive/v1.0/openFile/get_by_path":
			writeJSONResponse(response, map[string]any{"drive_id": "drive-1", "file_id": "file-1", "name": "file.bin", "type": "file", "size": len(content)})
		case "/adrive/v1.0/openFile/getDownloadUrl":
			writeJSONResponse(response, map[string]string{"url": "http://" + request.Host + "/content"})
		case "/content":
			var begin, end int64
			if _, err := fmt.Sscanf(request.Header.Get("Range"), "bytes=%d-%d", &begin, &end); err != nil {
				response.WriteHeader(http.StatusBadRequest)
				return
			}
			served.Lock()
			served.begins = append(served.begins, begin)
			served.Unlock()
			response.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", begin, end, len(content)))
			response.WriteHeader(http.StatusPartialContent)
			if breakSecondChunk && begin == chunkSize {
				// Hand over exactly one buffer, wait for it to be flushed into
				// the checkpoint, then drop the connection mid-chunk.
				_, _ = response.Write(content[begin : begin+servedBeforeBreak])
				http.NewResponseController(response).Flush()
				time.Sleep(20 * partialFlushInterval)
				panic(http.ErrAbortHandler)
			}
			_, _ = response.Write(content[begin : end+1])
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()

	client := New(t.TempDir(), "")
	client.httpClient = server.Client()
	client.openAPIURL = server.URL
	client.setCredentials(accountCredentials{OpenAPIAccess: "access"})
	stageDir := t.TempDir()
	// One connection and one attempt, so the run stops at the broken chunk
	// instead of quietly repairing it and hiding what is being tested.
	requestTemplate := DownloadRequest{
		CloudPath:     "/file.bin",
		StageDir:      stageDir,
		DriveID:       "drive-1",
		SliceParallel: 1,
		Retry:         1,
		ChunkSize:     chunkSize,
	}

	if _, err := client.Download(context.Background(), requestTemplate, int64(len(content)), nil); err == nil {
		t.Fatal("first download should have failed when the connection broke")
	}

	// The first chunk is complete and the second is one buffer in. Both have to
	// be on disk, or the resume below proves nothing.
	wantResumed := int64(chunkSize + servedBeforeBreak)
	if got := StagedDownloadBytes(stageDir, "/file.bin", int64(len(content))); got != wantResumed {
		t.Fatalf("bytes on disk after the break = %d, want %d", got, wantResumed)
	}

	served.Lock()
	served.begins = nil
	served.Unlock()
	breakSecondChunk = false

	var resumedBaseline int64 = -1
	staged, err := client.Download(context.Background(), requestTemplate, int64(len(content)), func(done int64) {
		if resumedBaseline < 0 {
			resumedBaseline = done
		}
	})
	if err != nil {
		t.Fatalf("resumed download: %v", err)
	}
	if resumedBaseline != wantResumed {
		t.Fatalf("first reported progress = %d, want the %d bytes already on disk", resumedBaseline, wantResumed)
	}

	served.Lock()
	secondRun := append([]int64(nil), served.begins...)
	served.Unlock()
	for _, begin := range secondRun {
		if begin == 0 {
			t.Fatalf("resumed download re-fetched the finished first chunk: %v", secondRun)
		}
		if begin == chunkSize {
			t.Fatalf("resumed download restarted the broken chunk from its start: %v", secondRun)
		}
	}
	if len(secondRun) == 0 || secondRun[0] != chunkSize+servedBeforeBreak {
		t.Fatalf("resumed download served %v, want it to start at byte %d", secondRun, chunkSize+servedBeforeBreak)
	}

	got, err := os.ReadFile(staged)
	if err != nil {
		t.Fatalf("ReadFile staged: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatal("resumed content does not match the source file")
	}
}

// A staged file that is already complete belongs to a download that succeeded
// and an upload that did not. Fetching it again is the most expensive possible
// way to produce a file that is sitting right there.
func TestSourceDownloadReusesCompletedStagedFile(t *testing.T) {
	content := []byte("abcdefgh")
	var contentRequests int32
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/adrive/v1.0/openFile/get_by_path":
			writeJSONResponse(response, map[string]any{"drive_id": "drive-1", "file_id": "file-1", "name": "file.bin", "type": "file", "size": len(content)})
		case "/adrive/v1.0/openFile/getDownloadUrl":
			writeJSONResponse(response, map[string]string{"url": "http://" + request.Host + "/content"})
		case "/content":
			atomic.AddInt32(&contentRequests, 1)
			response.WriteHeader(http.StatusPartialContent)
			_, _ = response.Write(content)
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()

	client := New(t.TempDir(), "")
	client.httpClient = server.Client()
	client.openAPIURL = server.URL
	client.setCredentials(accountCredentials{OpenAPIAccess: "access"})
	stageDir := t.TempDir()
	staged := StagedPath(stageDir, "/file.bin")
	if err := os.MkdirAll(filepath.Dir(staged), 0o750); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(staged, content, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	var lastProgress int64
	got, err := client.Download(context.Background(), DownloadRequest{
		CloudPath:     "/file.bin",
		StageDir:      stageDir,
		DriveID:       "drive-1",
		SliceParallel: 1,
	}, int64(len(content)), func(done int64) { lastProgress = done })
	if err != nil {
		t.Fatalf("Download with a complete staged file: %v", err)
	}
	if got != staged {
		t.Fatalf("staged path = %q, want %q", got, staged)
	}
	if requests := atomic.LoadInt32(&contentRequests); requests != 0 {
		t.Fatalf("content requests = %d, want the existing staged file reused", requests)
	}
	if lastProgress != int64(len(content)) {
		t.Fatalf("progress = %d, want %d", lastProgress, len(content))
	}
	if StagedDownloadBytes(stageDir, "/file.bin", int64(len(content))) != int64(len(content)) {
		t.Fatal("a complete staged file should be reported as fully downloaded")
	}
}

func TestParseByteRate(t *testing.T) {
	for _, test := range []struct {
		input string
		want  int64
	}{
		{"", 0},
		{"0", 0},
		{"2MB", 2 << 20},
		{"512 KB/s", 512 << 10},
		{"1.5GiB", int64(1.5 * (1 << 30))},
	} {
		got, err := parseByteRate(test.input)
		if err != nil || got != test.want {
			t.Errorf("parseByteRate(%q) = %d, %v; want %d", test.input, got, err, test.want)
		}
	}
	if _, err := parseByteRate("fast"); err == nil {
		t.Fatal("invalid byte rate was accepted")
	}
}
