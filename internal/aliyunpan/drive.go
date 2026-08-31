package aliyunpan

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"
)

const (
	DriveBackup   = "backup"
	DriveResource = "resource"
)

// Drive is one usable Aliyun Drive exposed by an account.
type Drive struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Kind string `json:"kind"`
}

var driveRowPattern = regexp.MustCompile(`^\s*\d+\s+(\S+)\s+(.+?)\s*$`)

// NormalizeDriveName accepts the stable values used in plugin settings and
// the Chinese labels printed by aliyunpan. The CLI's active drive is process
// global, so the plugin uses these names to resolve an ID and passes that ID to
// each operation instead of changing global CLI state between jobs.
func NormalizeDriveName(value string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", DriveBackup, "file", "备份盘", "文件":
		return DriveBackup, nil
	case DriveResource, "资源盘", "资源库":
		return DriveResource, nil
	default:
		return "", fmt.Errorf("不支持的网盘 %q，只能是 backup（备份盘）或 resource（资源库）", value)
	}
}

func driveKind(name string) string {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "backup", "file", "备份盘", "文件":
		return DriveBackup
	case "resource", "资源盘", "资源库":
		return DriveResource
	default:
		return ""
	}
}

// parseDrives parses the table printed by `aliyunpan drive` when it is run
// without an argument. Unknown entries such as the optional album drive are
// ignored because this plugin only supports file and resource drives.
func parseDrives(output string) ([]Drive, error) {
	drives := make([]Drive, 0, 2)
	seen := make(map[string]bool)
	for _, line := range strings.Split(output, "\n") {
		match := driveRowPattern.FindStringSubmatch(strings.TrimRight(line, "\r"))
		if match == nil {
			continue
		}
		kind := driveKind(match[2])
		if kind == "" || seen[match[1]] {
			continue
		}
		seen[match[1]] = true
		drives = append(drives, Drive{ID: match[1], Name: strings.TrimSpace(match[2]), Kind: kind})
	}
	if len(drives) == 0 {
		return nil, fmt.Errorf("无法解析 aliyunpan 的网盘列表: %q", strings.TrimSpace(output))
	}
	return drives, nil
}

// Drives returns the account's backup/resource drive IDs. `drive` without an
// argument prints the list and then receives EOF from the non-interactive
// child stdin, so it does not change the active drive.
func (c *CLI) Drives(ctx context.Context) ([]Drive, error) {
	return c.drives(ctx, true)
}

// DrivesForKnownAccount is used after Who has already succeeded, avoiding a
// second network-backed who call during the periodic account probe.
func (c *CLI) DrivesForKnownAccount(ctx context.Context) ([]Drive, error) {
	return c.drives(ctx, false)
}

func (c *CLI) drives(ctx context.Context, checkLogin bool) ([]Drive, error) {
	// Upstream's `drive` command dereferences the active user before it prints
	// anything. Check login first so opening the picker after logout returns a
	// normal sentinel error instead of crashing the child process.
	if checkLogin {
		if _, err := c.Who(ctx); err != nil {
			return nil, err
		}
	}
	output, err := c.runCommand(ctx, runOptions{timeout: time.Minute}, "drive")
	if strings.Contains(output, notLoggedInMarker) || strings.Contains(output, "未设置任何帐号") {
		return nil, ErrNotLoggedIn
	}
	if err != nil {
		return nil, err
	}
	return parseDrives(output)
}

// ResolveDrive turns a configured backup/resource choice into the current
// account's actual drive ID. IDs are account-specific and must not be stored
// permanently in plugin settings.
func (c *CLI) ResolveDrive(ctx context.Context, name string) (Drive, error) {
	kind, err := NormalizeDriveName(name)
	if err != nil {
		return Drive{}, err
	}
	drives, err := c.Drives(ctx)
	if err != nil {
		return Drive{}, err
	}
	return resolveDrive(kind, drives)
}

func resolveDrive(kind string, drives []Drive) (Drive, error) {
	for _, drive := range drives {
		if drive.Kind == kind {
			return drive, nil
		}
	}
	label := "备份盘"
	if kind == DriveResource {
		label = "资源库"
	}
	return Drive{}, fmt.Errorf("当前阿里云盘账号没有%s", label)
}
