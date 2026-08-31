// Command tdrive-aliyunpan moves files from Aliyun Drive into tdrive.
//
// It is a tdrive plugin: a standalone binary the host launches and speaks to
// over local RPC. Everything an administrator needs to do — installing the
// aliyunpan CLI, linking the cloud account, defining what to sync, setting the
// schedule and the daily Telegram budget, watching transfers — happens on the
// page this plugin serves inside the tdrive web interface.
package main

import (
	"context"
	"log"
	"os"
	"path/filepath"

	"github.com/dibin/tdrive-aliyunpan/internal/hostapi"
	syncengine "github.com/dibin/tdrive-aliyunpan/internal/sync"
	"github.com/dibin/tdrive-aliyunpan/internal/webui"
	tdriveplugin "github.com/dibin/tdrive/pkg/plugin"
)

const pluginID = "aliyunpan"

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
		Description:      "按计划把阿里云盘的文件搬进 tdrive（Telegram）存储，遵循 tdrive 当前的传输限制并支持每日流量配额。",
		Version:          "0.1.0",
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
	client := hostapi.New(host)
	p.engine = syncengine.New(client, dataDir(), p.logger)
	if err := p.engine.Load(ctx); err != nil {
		return err
	}
	p.web = webui.New(p.engine, client)

	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	p.cancel = cancel
	go p.engine.Run(runCtx)
	p.logger.Printf("阿里云盘同步插件已启动，数据目录 %s", dataDir())
	return nil
}

func (p *plugin) HandleHTTP(ctx context.Context, request tdriveplugin.HTTPRequest) (tdriveplugin.HTTPResponse, error) {
	return p.web.Handle(ctx, request)
}

func (p *plugin) Shutdown(context.Context) error {
	if p.cancel != nil {
		p.cancel()
	}
	return nil
}

// dataDir is the directory this plugin may write to.
//
// tdrive installs a plugin binary under its plugin directory and launches it
// with the host's environment, so the executable's own location identifies a
// directory that already belongs to this deployment. Deriving it that way
// avoids inventing a configuration value for something the host has already
// decided.
func dataDir() string {
	executable, err := os.Executable()
	if err != nil {
		return filepath.Join(os.TempDir(), "tdrive-"+pluginID)
	}
	return filepath.Join(filepath.Dir(executable), pluginID+"-data")
}

func main() {
	// go-plugin owns stdout: it is the handshake channel. Logging anywhere
	// else would corrupt it, and stderr is what the host relays into its own
	// log.
	logger := log.New(os.Stderr, "[aliyunpan] ", log.LstdFlags)
	tdriveplugin.Serve(&plugin{logger: logger})
}
