// Package plugin is the public SDK for tdrive plugins.
//
// Plugins are ordinary Go programs. They are compiled by their author into
// per-platform binaries, published as release artifacts, and communicate with
// tdrive through the RPC contract in this package. A plugin should never import
// tdrive/internal: those packages are intentionally private and can change
// without preserving a plugin author's build.
package plugin

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"runtime"
	"sort"
	"strings"
)

// ProtocolVersion changes when the wire contract is no longer compatible.
// API compatibility is negotiated separately through Manifest.APIVersion.
const ProtocolVersion uint = 1

// APIVersion is the current host API contract exposed to plugins.
const APIVersion = 1

// ManifestFile is the conventional name of the manifest document. A plugin
// keeps it at its source root and publishes it alongside the release binaries;
// the published copy is what an administrator points tdrive at.
const ManifestFile = "tdrive.plugin.json"

var (
	pluginIDPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
	semverPattern   = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?(\+[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$`)
	platformPattern = regexp.MustCompile(`^[a-z0-9]+/[a-z0-9]+$`)
	sha256Pattern   = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// Platform is the artifact key for one GOOS/GOARCH pair, written the way Go
// spells it: "linux/amd64".
func Platform(goos, goarch string) string { return goos + "/" + goarch }

// HostPlatform is the artifact key tdrive looks for when it installs a plugin
// on this machine.
func HostPlatform() string { return Platform(runtime.GOOS, runtime.GOARCH) }

// Artifact is one prebuilt plugin executable. tdrive downloads URL and refuses
// to install anything whose content does not hash to SHA256, so the manifest —
// and, through it, whatever pinned the manifest — fixes the exact bytes that
// will be executed.
type Artifact struct {
	URL    string `json:"url"`
	SHA256 string `json:"sha256"`
}

// Manifest describes a plugin to tdrive and to the plugin store. The
// Capabilities field is informational only. tdrive is deliberately a
// full-trust plugin host and does not turn this list into an authorization
// boundary.
type Manifest struct {
	ID               string `json:"id"`
	Name             string `json:"name"`
	Description      string `json:"description,omitempty"`
	Version          string `json:"version"`
	SDKVersion       string `json:"sdkVersion"`
	APIVersion       int    `json:"apiVersion"`
	MinTDriveVersion string `json:"minTdriveVersion,omitempty"`
	Author           string `json:"author"`
	License          string `json:"license"`
	RepositoryURL    string `json:"repositoryUrl"`
	DocumentationURL string `json:"documentationUrl,omitempty"`
	// Artifacts maps "goos/goarch" to the prebuilt executable for that
	// platform. tdrive does not compile plugins, so a platform missing here
	// cannot install this plugin at all.
	Artifacts map[string]Artifact `json:"artifacts"`
	// Entrypoint is the Go package the author builds. It is documentation:
	// tdrive never uses it, because it only ever receives finished binaries.
	Entrypoint   string      `json:"entrypoint,omitempty"`
	Capabilities []string    `json:"capabilities,omitempty"`
	Events       []string    `json:"events,omitempty"`
	Routes       []RouteSpec `json:"routes,omitempty"`
}

// RouteSpec declares one HTTP namespace owned by a plugin. Routes are mounted
// below /plugins/{id}; they are authenticated by tdrive before the plugin is
// called. A path ending in /* matches the remaining path below that prefix.
type RouteSpec struct {
	Path    string   `json:"path"`
	Methods []string `json:"methods,omitempty"`
	UI      bool     `json:"ui,omitempty"`
}

// Validate checks fields that affect loading, routing, and compatibility. It
// deliberately says nothing about Artifacts: a running plugin reports its
// manifest over RPC and cannot know the SHA-256 of its own executable, since
// embedding that digest would change the very bytes being hashed. Use
// ValidatePublished for a manifest document tdrive is asked to install from.
func (m Manifest) Validate() error {
	if !pluginIDPattern.MatchString(m.ID) {
		return fmt.Errorf("plugin id %q must match %s", m.ID, pluginIDPattern.String())
	}
	if strings.TrimSpace(m.Name) == "" {
		return errors.New("plugin name is required")
	}
	if !semverPattern.MatchString(m.Version) {
		return fmt.Errorf("plugin version %q is not SemVer", m.Version)
	}
	if m.APIVersion <= 0 {
		return errors.New("plugin apiVersion must be positive")
	}
	if strings.TrimSpace(m.SDKVersion) == "" {
		return errors.New("plugin sdkVersion is required")
	}
	if strings.TrimSpace(m.Author) == "" {
		return errors.New("plugin author is required")
	}
	if strings.TrimSpace(m.License) == "" {
		return errors.New("plugin license is required")
	}
	if err := validateHTTPSURL(m.RepositoryURL, "repositoryUrl", true); err != nil {
		return err
	}
	if err := validateHTTPSURL(m.DocumentationURL, "documentationUrl", false); err != nil {
		return err
	}
	if err := validateEntrypoint(m.Entrypoint); err != nil {
		return err
	}
	for index, route := range m.Routes {
		if err := route.Validate(); err != nil {
			return fmt.Errorf("route %d: %w", index, err)
		}
	}
	return nil
}

// ValidatePublished additionally requires the artifact table. It is what a
// manifest must satisfy to be installable from a URL.
func (m Manifest) ValidatePublished() error {
	if err := m.Validate(); err != nil {
		return err
	}
	return validateArtifacts(m.Artifacts)
}

// ArtifactFor returns the executable declared for one platform. The error
// names the platforms the plugin does publish, because "this plugin has no
// linux/arm64 build" is a fact the administrator can act on and a generic
// lookup failure is not.
func (m Manifest) ArtifactFor(goos, goarch string) (Artifact, error) {
	platform := Platform(goos, goarch)
	if artifact, ok := m.Artifacts[platform]; ok {
		return artifact, nil
	}
	published := make([]string, 0, len(m.Artifacts))
	for key := range m.Artifacts {
		published = append(published, key)
	}
	sort.Strings(published)
	if len(published) == 0 {
		return Artifact{}, fmt.Errorf("plugin %q publishes no binaries", m.ID)
	}
	return Artifact{}, fmt.Errorf("plugin %q has no %s binary; it publishes %s",
		m.ID, platform, strings.Join(published, ", "))
}

// validateEntrypoint allows an empty value: the field only records how the
// author builds the plugin, and tdrive installs finished binaries.
func validateEntrypoint(entrypoint string) error {
	clean := strings.TrimSpace(entrypoint)
	if clean == "" {
		return nil
	}
	if strings.HasPrefix(clean, "/") || clean == "." || strings.Contains(clean, "..") {
		return fmt.Errorf("plugin entrypoint %q must be a relative package path", entrypoint)
	}
	return nil
}

func validateArtifacts(artifacts map[string]Artifact) error {
	if len(artifacts) == 0 {
		return errors.New("plugin artifacts must declare at least one platform binary")
	}
	for platform, artifact := range artifacts {
		if !platformPattern.MatchString(platform) {
			return fmt.Errorf("plugin artifact platform %q must be written as goos/goarch", platform)
		}
		if err := validateHTTPSURL(artifact.URL, "artifact "+platform+" url", true); err != nil {
			return err
		}
		if !sha256Pattern.MatchString(artifact.SHA256) {
			return fmt.Errorf("plugin artifact %q sha256 must be 64 lowercase hexadecimal characters", platform)
		}
	}
	return nil
}

func validateHTTPSURL(raw, field string, required bool) error {
	if strings.TrimSpace(raw) == "" {
		if required {
			return fmt.Errorf("plugin %s is required", field)
		}
		return nil
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "https" || parsed.Hostname() == "" || parsed.User != nil {
		return fmt.Errorf("plugin %s must be an absolute HTTPS URL without credentials", field)
	}
	return nil
}

// Validate checks a route before it is registered.
func (r RouteSpec) Validate() error {
	if r.Path == "" || !strings.HasPrefix(r.Path, "/") {
		return errors.New("route path must start with /")
	}
	if strings.Contains(r.Path, "..") {
		return errors.New("route path cannot contain ..")
	}
	for _, method := range r.Methods {
		if method == "" {
			return errors.New("route method cannot be empty")
		}
	}
	return nil
}

// ParseManifest decodes and validates a published manifest document.
func ParseManifest(data []byte) (Manifest, error) {
	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return Manifest{}, fmt.Errorf("decode %s: %w", ManifestFile, err)
	}
	if err := manifest.ValidatePublished(); err != nil {
		return Manifest{}, fmt.Errorf("invalid %s: %w", ManifestFile, err)
	}
	return manifest, nil
}
