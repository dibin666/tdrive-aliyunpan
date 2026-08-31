// Package plugin is the public SDK for tdrive plugins.
//
// Plugins are ordinary Go programs. They are compiled as separate binaries
// and communicate with tdrive through the RPC contract in this package. A
// plugin should never import tdrive/internal: those packages are intentionally
// private and can change without preserving a plugin author's build.
package plugin

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// ProtocolVersion changes when the wire contract is no longer compatible.
// API compatibility is negotiated separately through Manifest.APIVersion.
const ProtocolVersion uint = 1

// APIVersion is the current host API contract exposed to plugins.
const APIVersion = 1

// ManifestFile is the required file name at the root of a plugin source tree.
const ManifestFile = "tdrive.plugin.json"

var (
	pluginIDPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
	semverPattern   = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?(\+[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$`)
)

// Manifest describes a plugin to tdrive and to the plugin store. The
// Capabilities field is informational only. tdrive is deliberately a
// full-trust plugin host and does not turn this list into an authorization
// boundary.
type Manifest struct {
	ID               string      `json:"id"`
	Name             string      `json:"name"`
	Description      string      `json:"description,omitempty"`
	Version          string      `json:"version"`
	SDKVersion       string      `json:"sdkVersion"`
	APIVersion       int         `json:"apiVersion"`
	MinTDriveVersion string      `json:"minTdriveVersion,omitempty"`
	Author           string      `json:"author"`
	License          string      `json:"license"`
	RepositoryURL    string      `json:"repositoryUrl"`
	DocumentationURL string      `json:"documentationUrl,omitempty"`
	Entrypoint       string      `json:"entrypoint"`
	Capabilities     []string    `json:"capabilities,omitempty"`
	Events           []string    `json:"events,omitempty"`
	Routes           []RouteSpec `json:"routes,omitempty"`
}

// RouteSpec declares one HTTP namespace owned by a plugin. Routes are mounted
// below /plugins/{id}; they are authenticated by tdrive before the plugin is
// called. A path ending in /* matches the remaining path below that prefix.
type RouteSpec struct {
	Path    string   `json:"path"`
	Methods []string `json:"methods,omitempty"`
	UI      bool     `json:"ui,omitempty"`
}

// Validate checks fields that affect loading, routing, and compatibility.
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

func validateEntrypoint(entrypoint string) error {
	clean := strings.TrimSpace(entrypoint)
	if clean == "" {
		return errors.New("plugin entrypoint is required")
	}
	if strings.HasPrefix(clean, "/") || clean == "." || strings.Contains(clean, "..") {
		return fmt.Errorf("plugin entrypoint %q must be a relative package path", entrypoint)
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

// ReadManifest reads and validates the manifest at a plugin source root.
func ReadManifest(root string) (Manifest, error) {
	data, err := os.ReadFile(filepath.Join(root, ManifestFile))
	if err != nil {
		return Manifest{}, fmt.Errorf("read %s: %w", ManifestFile, err)
	}
	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return Manifest{}, fmt.Errorf("decode %s: %w", ManifestFile, err)
	}
	if err := manifest.Validate(); err != nil {
		return Manifest{}, fmt.Errorf("invalid %s: %w", ManifestFile, err)
	}
	return manifest, nil
}
