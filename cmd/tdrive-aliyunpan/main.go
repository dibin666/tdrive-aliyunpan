// Command tdrive-aliyunpan moves files from Aliyun Drive into tdrive.
//
// It is a tdrive plugin: a standalone binary the host launches and speaks to
// over local RPC. Everything an administrator needs to do — linking the cloud
// account, defining what to sync, setting the schedule and the daily Telegram
// budget, watching transfers — happens on the page this plugin serves inside
// the tdrive web interface.
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dibin/tdrive-aliyunpan/internal/hostapi"
	syncengine "github.com/dibin/tdrive-aliyunpan/internal/sync"
	"github.com/dibin/tdrive-aliyunpan/internal/webui"
	tdriveplugin "github.com/dibin/tdrive/pkg/plugin"
)

const pluginID = "aliyunpan"

// version is the SemVer this build reports, injected with -ldflags.
//
// The release workflow tags every commit on main and passes the resulting
// number both to the compiler and into the tdrive.plugin.json it publishes, so
// the binary and the manifest installed beside it can never disagree — which
// matters because the host compares the two and refuses to start the plugin if
// they differ. The default is what a working copy reports, and the committed
// manifest carries the same placeholder so the two still match locally.
var version = "0.0.0"

type plugin struct {
	logger *log.Logger
	engine *syncengine.Engine
	web    *webui.Server
	cancel context.CancelFunc
}

func (p *plugin) Manifest() tdriveplugin.Manifest {
	return tdriveplugin.Manifest{
		ID:               pluginID,
		Name:             "阿里云盘同步",
		Description:      "使用内置 Go/OpenAPI 客户端按计划把阿里云盘文件搬进 tdrive（Telegram），支持备份盘、资源库、并行下载和每日流量配额。",
		Version:          version,
		SDKVersion:       "0.1",
		APIVersion:       tdriveplugin.APIVersion,
		Author:           "dibin",
		License:          "MIT",
		RepositoryURL:    "https://github.com/dibin666/tdrive-aliyunpan",
		DocumentationURL: "https://github.com/dibin666/tdrive-aliyunpan/blob/main/README.md",
		Entrypoint:       "./cmd/tdrive-aliyunpan",
		Capabilities:     []string{"http"},
		Routes: []tdriveplugin.RouteSpec{
			{Path: "/", Methods: []string{"GET"}, UI: true},
			{Path: "/api/*", Methods: []string{"GET", "POST", "PUT", "DELETE"}},
		},
	}
}

// Initialize is called once, after the host has attached its bridge.
//
// The scheduler runs on a context of its own rather than the one passed in:
// the host's initialization context is finished as soon as this returns, and
// the engine has to outlive it.
func (p *plugin) Initialize(ctx context.Context, host tdriveplugin.Host) error {
	if p.logger == nil {
		p.logger = log.New(io.Discard, "", 0)
	}
	client := hostapi.New(host)
	storageDir := dataDir()
	legacyDir := legacyDataDir()
	if err := migrateLegacyDataDir(storageDir, legacyDir); err != nil {
		// The host's plugin-data directory is the canonical location. Migration
		// is best effort so an old, otherwise healthy installation still gets a
		// usable config page if a filesystem policy prevents moving a leftover.
		p.logger.Printf("迁移旧 aliyunpan 数据目录失败（将继续使用新目录）: %v", err)
		if !hasLegacyCredentials(storageDir) && hasLegacyCredentials(legacyDir) {
			// A read-only or cross-device legacy tree is still better than
			// forcing a re-login. It will be retried on the next start after the
			// filesystem becomes writable.
			storageDir = legacyDir
			p.logger.Printf("改用旧 aliyunpan 数据目录以保留已有登录凭证: %s", storageDir)
		}
	}
	p.engine = syncengine.New(client, storageDir, p.logger)
	if err := p.engine.Load(ctx); err != nil {
		return err
	}
	p.web = webui.New(p.engine, client)

	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	p.cancel = cancel
	go p.engine.Run(runCtx)
	p.logger.Printf("阿里云盘同步插件已启动，数据目录 %s", storageDir)
	return nil
}

func (p *plugin) HandleHTTP(ctx context.Context, request tdriveplugin.HTTPRequest) (tdriveplugin.HTTPResponse, error) {
	return p.web.Handle(ctx, request)
}

func (p *plugin) Shutdown(context.Context) error {
	if p.cancel != nil {
		p.cancel()
	}
	if p.engine != nil {
		// The host kills the child immediately after this RPC returns. Give the
		// scheduler time to cancel transfers and persist the final queue rather
		// than returning while those goroutines are still writing state.
		waitCtx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
		defer cancel()
		p.engine.Wait(waitCtx)
	}
	return nil
}

const pluginDataDirEnv = "TDRIVE_PLUGIN_DATA_DIR"

// dataDir is the directory this plugin may write to. The host supplies an
// absolute path under its persistent data volume. The executable-relative
// path remains as a compatibility fallback for standalone launches and older
// hosts that do not know this environment variable yet.
func dataDir() string {
	if configured := strings.TrimSpace(os.Getenv(pluginDataDirEnv)); configured != "" {
		if absolute, err := filepath.Abs(configured); err == nil {
			return filepath.Clean(absolute)
		}
		return filepath.Clean(configured)
	}
	return legacyDataDir()
}

func legacyDataDir() string {
	executable, err := os.Executable()
	if err != nil {
		return filepath.Join(os.TempDir(), "tdrive-"+pluginID)
	}
	return filepath.Join(filepath.Dir(executable), pluginID+"-data")
}

func hasLegacyCredentials(dir string) bool {
	info, err := os.Stat(filepath.Join(dir, "config", "aliyunpan_config.json"))
	return err == nil && !info.IsDir()
}

// migrateLegacyDataDir moves the data created by releases that derived their
// location from the executable into the host-owned path. It never replaces an
// existing destination entry: a partially migrated or newer destination wins,
// while missing files are moved across one by one. If the two directories are
// on different filesystems, moveEntry falls back to copy-then-remove.
func migrateLegacyDataDir(destination, legacy string) error {
	destination = filepath.Clean(destination)
	legacy = filepath.Clean(legacy)
	if samePath(destination, legacy) {
		return nil
	}
	legacyInfo, err := os.Stat(legacy)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if !legacyInfo.IsDir() {
		return fmt.Errorf("旧数据路径不是目录: %s", legacy)
	}

	destinationInfo, err := os.Stat(destination)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(filepath.Dir(destination), 0o750); err != nil {
			return err
		}
		if err := os.Rename(legacy, destination); err == nil {
			return nil
		}
		if err := os.MkdirAll(destination, 0o750); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else if !destinationInfo.IsDir() {
		return fmt.Errorf("新数据路径不是目录: %s", destination)
	}

	return mergeDataDir(legacy, destination)
}

func mergeDataDir(source, destination string) error {
	entries, err := os.ReadDir(source)
	if err != nil {
		return err
	}
	var firstErr error
	for _, entry := range entries {
		from := filepath.Join(source, entry.Name())
		to := filepath.Join(destination, entry.Name())
		_, statErr := os.Lstat(to)
		if os.IsNotExist(statErr) {
			if err := moveEntry(from, to); err != nil && firstErr == nil {
				firstErr = err
			}
			continue
		}
		if statErr != nil {
			if firstErr == nil {
				firstErr = statErr
			}
			continue
		}
		if entry.IsDir() {
			info, err := os.Stat(to)
			if err != nil || !info.IsDir() {
				if firstErr == nil {
					if err != nil {
						firstErr = err
					} else {
						firstErr = fmt.Errorf("数据目标不是目录: %s", to)
					}
				}
				continue
			}
			if err := mergeDataDir(from, to); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		// If the destination already contains this entry, retain the legacy
		// copy. It is safer to leave a duplicate than to delete data we did not
		// prove was identical.
	}
	return firstErr
}

func moveEntry(source, destination string) error {
	if err := os.Rename(source, destination); err == nil {
		return nil
	}
	info, err := os.Lstat(source)
	if err != nil {
		return err
	}
	if info.IsDir() {
		if err := os.MkdirAll(destination, info.Mode().Perm()); err != nil {
			return err
		}
		if err := mergeDataDir(source, destination); err != nil {
			return err
		}
		return os.Remove(source)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("不支持迁移特殊文件: %s", source)
	}

	input, err := os.Open(source)
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(destination), ".aliyunpan-migrate-*")
	if err != nil {
		input.Close()
		return err
	}
	temporaryName := temporary.Name()
	cleanup := func() {
		input.Close()
		temporary.Close()
		_ = os.Remove(temporaryName)
	}
	if _, err := io.Copy(temporary, input); err != nil {
		cleanup()
		return err
	}
	if err := input.Close(); err != nil {
		cleanup()
		return err
	}
	if err := temporary.Chmod(info.Mode().Perm()); err != nil {
		cleanup()
		return err
	}
	if err := temporary.Close(); err != nil {
		cleanup()
		return err
	}
	if err := os.Rename(temporaryName, destination); err != nil {
		cleanup()
		return err
	}
	if err := os.Remove(source); err != nil {
		return err
	}
	return nil
}

func samePath(left, right string) bool {
	leftAbs, leftErr := filepath.Abs(left)
	rightAbs, rightErr := filepath.Abs(right)
	if leftErr == nil && rightErr == nil {
		left, right = leftAbs, rightAbs
	}
	rel, err := filepath.Rel(filepath.Clean(left), filepath.Clean(right))
	return err == nil && rel == "."
}

func main() {
	// go-plugin owns stdout: it is the handshake channel. Logging anywhere
	// else would corrupt it, and stderr is what the host relays into its own
	// log.
	logger := log.New(os.Stderr, "[aliyunpan] ", log.LstdFlags)
	tdriveplugin.Serve(&plugin{logger: logger})
}
