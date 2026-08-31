package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	tdriveplugin "github.com/dibin/tdrive/pkg/plugin"
)

// tdrive reads tdrive.plugin.json when it inspects and installs a plugin, but
// the running process reports Manifest() over RPC. The manager compares the
// two as marshalled JSON and refuses to start the plugin if they differ, so
// this check mirrors that comparison exactly.
func TestManifestMatchesFile(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", tdriveplugin.ManifestFile))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var declared tdriveplugin.Manifest
	if err := json.Unmarshal(data, &declared); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	installed, err := json.Marshal(declared)
	if err != nil {
		t.Fatalf("encode declared manifest: %v", err)
	}
	compiled, err := json.Marshal((&plugin{}).Manifest())
	if err != nil {
		t.Fatalf("encode compiled manifest: %v", err)
	}
	if string(installed) != string(compiled) {
		t.Errorf("tdrive.plugin.json and Manifest() disagree:\nfile:   %s\nbinary: %s", installed, compiled)
	}
}

func TestManifestIsValid(t *testing.T) {
	if err := (&plugin{}).Manifest().Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

// The plugin serves a page and a JSON API below it. Losing either route
// declaration would leave the UI reachable but inert, or the reverse.
func TestManifestDeclaresUIAndAPI(t *testing.T) {
	manifest := (&plugin{}).Manifest()
	var hasUI, hasAPI bool
	for _, route := range manifest.Routes {
		if route.UI && route.Path == "/" {
			hasUI = true
		}
		if route.Path == "/api/*" {
			hasAPI = true
		}
	}
	if !hasUI {
		t.Error("no UI route declared, so Settings → 插件 would show no way in")
	}
	if !hasAPI {
		t.Error("no /api/* route declared, so the page would have nothing to call")
	}
}
