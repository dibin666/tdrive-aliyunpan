package aliyunpan

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
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

func TestSourceDownloadCancellationCleansTemporaryFile(t *testing.T) {
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
	if _, err := os.Stat(staged + downloadPartSuffix); !os.IsNotExist(err) {
		t.Fatalf("partial file still exists: %v", err)
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
