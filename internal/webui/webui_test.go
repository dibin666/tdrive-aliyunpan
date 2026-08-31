package webui

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"
	"testing"

	"github.com/dibin/tdrive-aliyunpan/internal/hostapi"
	syncengine "github.com/dibin/tdrive-aliyunpan/internal/sync"
	tdriveplugin "github.com/dibin/tdrive/pkg/plugin"
)

// fakeHost answers the handful of host calls the server makes, so the routing
// and the authorization gate can be exercised without a running tdrive.
type fakeHost struct {
	users []tdriveplugin.User
	data  map[string]json.RawMessage
}

func (h *fakeHost) Call(_ context.Context, method string, request any, response any) error {
	encode := func(value any) error {
		if response == nil {
			return nil
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return err
		}
		return json.Unmarshal(encoded, response)
	}
	payload, _ := json.Marshal(request)

	switch method {
	case "users.list":
		return encode(h.users)
	case "settings.get":
		return encode(map[string]any{"UploadConcurrency": 2, "SegmentSize": 1 << 20})
	case "data.get":
		var input struct {
			Key string `json:"key"`
		}
		_ = json.Unmarshal(payload, &input)
		value, ok := h.data[input.Key]
		if !ok {
			return encode(nil)
		}
		if target, isRaw := response.(*json.RawMessage); isRaw {
			*target = value
			return nil
		}
		return json.Unmarshal(value, response)
	case "data.set":
		var input struct {
			Key   string          `json:"key"`
			Value json.RawMessage `json:"value"`
		}
		_ = json.Unmarshal(payload, &input)
		h.data[input.Key] = input.Value
		return nil
	case "files.list":
		return encode([]tdriveplugin.Entry{})
	}
	return nil
}

func (h *fakeHost) OpenStream(context.Context, string, any) (io.ReadWriteCloser, error) {
	return nil, nil
}

func newServer(t *testing.T) *Server {
	t.Helper()
	host := &fakeHost{
		users: []tdriveplugin.User{
			{ID: "admin-1", Username: "root", Role: "admin", Enabled: true},
			{ID: "user-1", Username: "alice", Role: "user", Enabled: true},
			{ID: "admin-2", Username: "old", Role: "admin", Enabled: false},
		},
		data: map[string]json.RawMessage{},
	}
	client := hostapi.New(host)
	engine := syncengine.New(client, t.TempDir(), log.New(io.Discard, "", 0))
	if err := engine.Load(context.Background()); err != nil {
		t.Fatalf("engine.Load: %v", err)
	}
	return New(engine, client)
}

func request(method, path, userID, body string) tdriveplugin.HTTPRequest {
	return tdriveplugin.HTTPRequest{Method: method, Path: path, UserID: userID, Body: []byte(body)}
}

// tdrive's own middleware only guarantees that the caller is signed in. Every
// route here reads or changes the drive's contents or reveals the linked cloud
// account, so the plugin has to make the administrator check itself.
func TestAPIRequiresAdministrator(t *testing.T) {
	server := newServer(t)
	cases := map[string]string{
		"anonymous":       "",
		"ordinary user":   "user-1",
		"disabled admin":  "admin-2",
		"unknown account": "ghost",
	}
	for name, userID := range cases {
		response, err := server.Handle(context.Background(), request(http.MethodGet, "/api/state", userID, ""))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if response.Status != http.StatusForbidden {
			t.Errorf("%s: status = %d, want 403", name, response.Status)
		}
	}
}

func TestStateIsServedToAdministrator(t *testing.T) {
	server := newServer(t)
	response, err := server.Handle(context.Background(), request(http.MethodGet, "/api/state", "admin-1", ""))
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if response.Status != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Status, response.Body)
	}
	var snapshot syncengine.Snapshot
	if err := json.Unmarshal(response.Body, &snapshot); err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	if snapshot.Status == "" {
		t.Error("the snapshot should always explain what the engine is doing")
	}
	if snapshot.Rows == nil {
		t.Error("rows should be an empty array so the page can render a list")
	}
}

// The page itself is the one route that does not require an administrator: it
// renders nothing until its own API answers, and gating it too would send a
// non-admin a JSON error where a browser expects a document.
func TestPageIsServedWithoutAdminCheck(t *testing.T) {
	server := newServer(t)
	response, err := server.Handle(context.Background(), request(http.MethodGet, "/", "user-1", ""))
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if response.Status != http.StatusOK {
		t.Fatalf("status = %d", response.Status)
	}
	if !strings.Contains(string(response.Body), "阿里云盘同步") {
		t.Error("the page body does not look like the app shell")
	}
}

func TestUnknownAPIRouteIs404(t *testing.T) {
	server := newServer(t)
	response, _ := server.Handle(context.Background(), request(http.MethodGet, "/api/nope", "admin-1", ""))
	if response.Status != http.StatusNotFound {
		t.Errorf("status = %d, want 404", response.Status)
	}
}

func TestSettingsRoundTrip(t *testing.T) {
	server := newServer(t)
	body := `{"schedule":{"enabled":true,"windowStart":"01:00","windowEnd":"07:00","intervalMinutes":20},
	          "quota":{"dailyBytes":1073741824,"resetAt":"03:00"},
	          "jobs":[{"id":"j1","name":"影视","enabled":true,"remotePath":"/a","targetPath":"/b"}]}`
	response, err := server.Handle(context.Background(), request(http.MethodPut, "/api/settings", "admin-1", body))
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if response.Status != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Status, response.Body)
	}

	read, _ := server.Handle(context.Background(), request(http.MethodGet, "/api/settings", "admin-1", ""))
	if !strings.Contains(string(read.Body), `"windowStart":"01:00"`) {
		t.Errorf("settings did not round-trip: %s", read.Body)
	}
}

// An unusable document must be refused at the boundary rather than stored and
// discovered later by the scheduler.
func TestSettingsRejectsInvalidDocument(t *testing.T) {
	server := newServer(t)
	response, _ := server.Handle(context.Background(),
		request(http.MethodPut, "/api/settings", "admin-1", `{"schedule":{"windowStart":"99:99"}}`))
	if response.Status != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", response.Status)
	}
}

func TestJobUpsertAssignsAnID(t *testing.T) {
	server := newServer(t)
	response, err := server.Handle(context.Background(),
		request(http.MethodPost, "/api/jobs", "admin-1", `{"name":"影视","enabled":true,"remotePath":"/a","targetPath":"/b"}`))
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if response.Status != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Status, response.Body)
	}
	var stored struct {
		Jobs []struct {
			ID string `json:"id"`
		} `json:"jobs"`
	}
	if err := json.Unmarshal(response.Body, &stored); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(stored.Jobs) != 1 || stored.Jobs[0].ID == "" {
		t.Fatalf("expected one job with a generated id, got %+v", stored.Jobs)
	}
}

func TestMethodIsChecked(t *testing.T) {
	server := newServer(t)
	response, _ := server.Handle(context.Background(), request(http.MethodGet, "/api/pause", "admin-1", ""))
	if response.Status != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", response.Status)
	}
	response, _ = server.Handle(context.Background(), request(http.MethodPost, "/api/state", "admin-1", ""))
	if response.Status != http.StatusMethodNotAllowed {
		t.Errorf("state POST status = %d, want 405", response.Status)
	}
	response, _ = server.Handle(context.Background(), request(http.MethodGet, "/api/queue/clear", "admin-1", ""))
	if response.Status != http.StatusMethodNotAllowed {
		t.Errorf("clear GET status = %d, want 405", response.Status)
	}
}

func TestPauseAndResume(t *testing.T) {
	server := newServer(t)
	if response, _ := server.Handle(context.Background(), request(http.MethodPost, "/api/pause", "admin-1", "")); response.Status != http.StatusOK {
		t.Fatalf("pause status = %d", response.Status)
	}
	state, _ := server.Handle(context.Background(), request(http.MethodGet, "/api/state", "admin-1", ""))
	if !strings.Contains(string(state.Body), `"paused":true`) {
		t.Error("pause was not reflected in the snapshot")
	}
	if response, _ := server.Handle(context.Background(), request(http.MethodPost, "/api/resume", "admin-1", "")); response.Status != http.StatusOK {
		t.Fatalf("resume status = %d", response.Status)
	}
}
