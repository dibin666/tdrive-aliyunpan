package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	goPlugin "github.com/hashicorp/go-plugin"

	tdriveplugin "github.com/dibin/tdrive/pkg/plugin"
)

// stubHost stands in for tdrive on the other end of the bridge. It answers the
// calls the plugin makes while idle, which is enough to prove the handshake,
// the manifest exchange, initialization and HTTP dispatch all work against the
// real compiled binary rather than against an in-process fake.
type stubHost struct {
	data map[string]json.RawMessage
}

func (h *stubHost) Call(_ context.Context, method string, request any, response any) error {
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
		return encode([]tdriveplugin.User{{ID: "admin-1", Username: "root", Role: "admin", Enabled: true}})
	case "settings.get":
		return encode(map[string]any{"UploadConcurrency": 1, "SegmentSize": 1 << 20, "CacheLimit": 0})
	case "files.list":
		return encode([]tdriveplugin.Entry{})
	case "data.get":
		var input struct {
			Key string `json:"key"`
		}
		_ = json.Unmarshal(payload, &input)
		return encode(json.RawMessage(h.data[input.Key]))
	case "data.set":
		var input struct {
			Key   string          `json:"key"`
			Value json.RawMessage `json:"value"`
		}
		_ = json.Unmarshal(payload, &input)
		h.data[input.Key] = input.Value
		return nil
	}
	return nil
}

func (h *stubHost) OpenStream(context.Context, string, any) (io.ReadWriteCloser, error) {
	return nil, nil
}

// TestPluginServesOverRPC launches the plugin exactly the way tdrive's manager
// does — same handshake, same plugin set, same Dispense — so a break in the
// wiring shows up here rather than only after an install.
func TestPluginServesOverRPC(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the plugin binary")
	}
	binary := buildPlugin(t)

	process := goPlugin.NewClient(&goPlugin.ClientConfig{
		HandshakeConfig: tdriveplugin.HandshakeConfig,
		Plugins:         goPlugin.PluginSet{tdriveplugin.PluginName: &tdriveplugin.RPCPlugin{}},
		Cmd:             exec.Command(binary),
		Logger:          hclog.NewNullLogger(),
	})
	defer process.Kill()

	protocol, err := process.Client()
	if err != nil {
		t.Fatalf("connect plugin process: %v", err)
	}
	dispensed, err := protocol.Dispense(tdriveplugin.PluginName)
	if err != nil {
		t.Fatalf("dispense: %v", err)
	}
	client, ok := dispensed.(*tdriveplugin.Client)
	if !ok {
		t.Fatalf("unexpected client type %T", dispensed)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	manifest, err := client.Manifest(ctx)
	if err != nil {
		t.Fatalf("manifest: %v", err)
	}
	// The host refuses to start a plugin whose reported manifest differs from
	// the installed JSON, comparing the two as marshalled JSON. The version is
	// included here — this is the only place that proves the number linked
	// into the binary is the number the published manifest declares.
	compiled := installedManifestJSON(t, manifest)
	installed := installedManifestJSON(t, readDeclaredManifest(t))
	if compiled != installed {
		t.Errorf("manifest mismatch:\nbinary: %s\nfile:   %s", compiled, installed)
	}

	if err := client.AttachHost(ctx, &stubHost{data: map[string]json.RawMessage{}}); err != nil {
		t.Fatalf("attach host: %v", err)
	}

	page, err := client.HandleHTTP(ctx, tdriveplugin.HTTPRequest{Method: http.MethodGet, Path: "/"})
	if err != nil {
		t.Fatalf("serve page: %v", err)
	}
	if page.Status != http.StatusOK || len(page.Body) == 0 {
		t.Fatalf("page status = %d, %d bytes", page.Status, len(page.Body))
	}

	state, err := client.HandleHTTP(ctx, tdriveplugin.HTTPRequest{
		Method: http.MethodGet, Path: "/api/state", UserID: "admin-1",
	})
	if err != nil {
		t.Fatalf("serve state: %v", err)
	}
	if state.Status != http.StatusOK {
		t.Fatalf("state status = %d, body = %s", state.Status, state.Body)
	}
	var snapshot struct {
		Status string `json:"status"`
		Rows   []any  `json:"rows"`
	}
	if err := json.Unmarshal(state.Body, &snapshot); err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	if snapshot.Status == "" {
		t.Error("the snapshot carries no status line")
	}

	// An ordinary account must not reach the API even though tdrive has
	// already established that it is signed in.
	denied, err := client.HandleHTTP(ctx, tdriveplugin.HTTPRequest{
		Method: http.MethodGet, Path: "/api/state", UserID: "nobody",
	})
	if err != nil {
		t.Fatalf("serve denied: %v", err)
	}
	if denied.Status != http.StatusForbidden {
		t.Errorf("unknown account got %d, want 403", denied.Status)
	}

	if err := client.Shutdown(ctx); err != nil {
		t.Errorf("shutdown: %v", err)
	}
}

func buildPlugin(t *testing.T) string {
	t.Helper()
	// The release workflow points this at the artifact it is about to publish,
	// so the handshake runs against those exact bytes instead of a rebuild of
	// them. Rebuilding would hide the one mistake that matters most here: a
	// build that lost its version stamp still produces a binary this test
	// would happily recreate correctly.
	if prebuilt := os.Getenv("TDRIVE_PLUGIN_TEST_BINARY"); prebuilt != "" {
		return prebuilt
	}

	binary := filepath.Join(t.TempDir(), "tdrive-aliyunpan")
	// Built the way the release workflow builds it: stamped with the version
	// the manifest beside it declares, so the comparison holds for any version
	// rather than only for the placeholder in a working copy.
	//
	// The symbol is main.version rather than the full import path because this
	// links a real main package; a -ldflags passed to `go test` would have to
	// name the package path instead, which is why the workflow does not use one.
	declared := readDeclaredManifest(t)
	build := exec.Command("go", "build",
		"-ldflags", "-X main.version="+declared.Version, "-o", binary, ".")
	build.Env = append(os.Environ(), "CGO_ENABLED=0")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build plugin: %v: %s", err, output)
	}
	return binary
}
