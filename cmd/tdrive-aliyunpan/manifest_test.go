package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	tdriveplugin "github.com/dibin/tdrive/pkg/plugin"
)

// releaseDownloadPrefix is where the release workflow uploads the binaries.
// The manifest spells out one absolute URL per platform, so the two have to
// agree or tdrive downloads nothing.
const releaseDownloadPrefix = "https://github.com/dibin666/tdrive-aliyunpan/releases/download/"

// tdrive reads tdrive.plugin.json when it inspects and installs a plugin, but
// the running process reports Manifest() over RPC. The manager compares the
// two as marshalled JSON and refuses to start the plugin if they differ, so
// this check mirrors that comparison exactly.
func TestManifestMatchesFile(t *testing.T) {
	installed := installedManifestJSON(t, readDeclaredManifest(t))
	compiled := installedManifestJSON(t, (&plugin{}).Manifest())
	if installed != compiled {
		t.Errorf("tdrive.plugin.json and Manifest() disagree:\nfile:   %s\nbinary: %s", installed, compiled)
	}
}

func TestManifestIsValid(t *testing.T) {
	if err := (&plugin{}).Manifest().Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

// The published manifest is what an administrator points tdrive at, so it has
// to carry an artifact per platform. The compiled manifest must not: a binary
// cannot contain its own SHA-256, and the host clears the field before
// comparing the two.
func TestPublishedManifestDeclaresArtifacts(t *testing.T) {
	if compiled := (&plugin{}).Manifest(); compiled.Artifacts != nil {
		t.Errorf("Manifest() declares artifacts %v; they belong only in tdrive.plugin.json", compiled.Artifacts)
	}

	manifest := readDeclaredManifest(t)
	if err := manifest.ValidatePublished(); err != nil {
		t.Fatalf("ValidatePublished: %v", err)
	}
	for _, platform := range [][2]string{{"linux", "amd64"}, {"linux", "arm64"}} {
		goos, goarch := platform[0], platform[1]
		artifact, err := manifest.ArtifactFor(goos, goarch)
		if err != nil {
			t.Errorf("ArtifactFor(%s, %s): %v", goos, goarch, err)
			continue
		}
		// The release workflow fills in the digests but takes the URLs as
		// given, so a version bump that forgets them would publish a manifest
		// pointing at the previous release.
		want := fmt.Sprintf("%sv%s/tdrive-aliyunpan-%s-%s", releaseDownloadPrefix, manifest.Version, goos, goarch)
		if artifact.URL != want {
			t.Errorf("%s/%s url = %s, want %s", goos, goarch, artifact.URL, want)
		}
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

func readDeclaredManifest(t *testing.T) tdriveplugin.Manifest {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", tdriveplugin.ManifestFile))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var manifest tdriveplugin.Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	return manifest
}

// installedManifestJSON encodes a manifest the way the host's manifestsMatch
// does: artifacts are dropped first, because they say where the executable was
// downloaded from, which the running plugin has no way to report.
func installedManifestJSON(t *testing.T, manifest tdriveplugin.Manifest) string {
	t.Helper()
	manifest.Artifacts = nil
	encoded, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("encode manifest: %v", err)
	}
	return string(encoded)
}
