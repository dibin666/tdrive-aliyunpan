// Package webui serves the plugin's configuration page and the JSON API it
// talks to.
//
// The page is a single self-contained document rather than a build artifact.
// It borrows tdrive's compiled stylesheet at runtime, so it inherits the same
// tokens and control styles as the rest of the interface without adding a
// second frontend toolchain to a Go plugin.
package webui

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"github.com/dibin/tdrive-aliyunpan/internal/aliyunpan"
	"github.com/dibin/tdrive-aliyunpan/internal/hostapi"
	"github.com/dibin/tdrive-aliyunpan/internal/settings"
	syncengine "github.com/dibin/tdrive-aliyunpan/internal/sync"
	tdriveplugin "github.com/dibin/tdrive/pkg/plugin"
)

//go:embed app.html
var page []byte

// Server routes plugin HTTP requests.
type Server struct {
	engine *syncengine.Engine
	host   *hostapi.Client
}

func New(engine *syncengine.Engine, host *hostapi.Client) *Server {
	return &Server{engine: engine, host: host}
}

// Handle dispatches one request.
//
// Every route below /api requires the account that owns this installation.
// tdrive's own middleware only guarantees that the caller is signed in — a
// plugin route is not automatically an owner-only route — and everything here
// changes what the drive stores or reveals the linked cloud account.
func (s *Server) Handle(ctx context.Context, request tdriveplugin.HTTPRequest) (tdriveplugin.HTTPResponse, error) {
	path := strings.TrimSuffix(request.Path, "/")
	if path == "" {
		if request.Method != http.MethodGet {
			return status(http.StatusMethodNotAllowed, "只支持 GET"), nil
		}
		return tdriveplugin.HTTPResponse{
			Status: http.StatusOK,
			Headers: map[string][]string{
				"Content-Type": {"text/html; charset=utf-8"},
				// The page is small and the plugin may be reinstalled at any
				// time, so it is never cached.
				"Cache-Control": {"no-store"},
			},
			Body: page,
		}, nil
	}

	if err := s.requireCaller(ctx, request.UserID); err != nil {
		return status(http.StatusForbidden, err.Error()), nil
	}
	query, _ := url.ParseQuery(request.RawQuery)

	switch path {
	case "/api/state":
		if request.Method != http.MethodGet {
			return status(http.StatusMethodNotAllowed, "只支持 GET"), nil
		}
		return okJSON(s.engine.State(ctx))

	case "/api/settings":
		switch request.Method {
		case http.MethodGet:
			return okJSON(s.engine.ReloadSettings(ctx))
		case http.MethodPut:
			// Decode into the defaults so configurations written by an older page
			// receive safe values for newly added fields. JSON still overwrites
			// those defaults when the caller explicitly sends false or another
			// zero-value that is meaningful for an existing setting.
			next := settings.Default()
			if err := json.Unmarshal(request.Body, &next); err != nil {
				return status(http.StatusBadRequest, "配置不是合法的 JSON: "+err.Error()), nil
			}
			if err := s.engine.SaveSettings(ctx, next); err != nil {
				return status(http.StatusBadRequest, err.Error()), nil
			}
			return okJSON(s.engine.Settings())
		}
		return status(http.StatusMethodNotAllowed, "只支持 GET 和 PUT"), nil

	case "/api/jobs":
		switch request.Method {
		case http.MethodPost, http.MethodPut:
			var job settings.Job
			if err := json.Unmarshal(request.Body, &job); err != nil {
				return status(http.StatusBadRequest, "任务不是合法的 JSON: "+err.Error()), nil
			}
			if err := s.engine.UpsertJob(ctx, job); err != nil {
				return status(http.StatusBadRequest, err.Error()), nil
			}
			return okJSON(s.engine.Settings())
		case http.MethodDelete:
			if err := s.engine.DeleteJob(ctx, query.Get("id")); err != nil {
				return status(http.StatusBadRequest, err.Error()), nil
			}
			return okJSON(s.engine.Settings())
		}
		return status(http.StatusMethodNotAllowed, "只支持 POST、PUT 和 DELETE"), nil

	case "/api/scan":
		if request.Method != http.MethodPost {
			return status(http.StatusMethodNotAllowed, "只支持 POST"), nil
		}
		s.engine.StartScan(ctx)
		return okJSON(map[string]bool{"ok": true})

	case "/api/pause":
		if request.Method != http.MethodPost {
			return status(http.StatusMethodNotAllowed, "只支持 POST"), nil
		}
		s.engine.Pause()
		return okJSON(map[string]bool{"paused": true})

	case "/api/resume":
		if request.Method != http.MethodPost {
			return status(http.StatusMethodNotAllowed, "只支持 POST"), nil
		}
		s.engine.Resume()
		return okJSON(map[string]bool{"paused": false})

	case "/api/queue/retry":
		if request.Method != http.MethodPost {
			return status(http.StatusMethodNotAllowed, "只支持 POST"), nil
		}
		ids, err := queueIDs(request.Body, query.Get("id"))
		if err != nil {
			return status(http.StatusBadRequest, err.Error()), nil
		}
		if err := s.engine.Retry(ctx, ids...); err != nil {
			return status(http.StatusBadRequest, err.Error()), nil
		}
		return okJSON(map[string]any{"ok": true, "count": len(ids)})

	case "/api/queue/cancel":
		if request.Method != http.MethodPost {
			return status(http.StatusMethodNotAllowed, "只支持 POST"), nil
		}
		ids, err := queueIDs(request.Body, query.Get("id"))
		if err != nil {
			return status(http.StatusBadRequest, err.Error()), nil
		}
		if err := s.engine.Cancel(ctx, ids...); err != nil {
			return status(http.StatusBadRequest, err.Error()), nil
		}
		return okJSON(map[string]any{"ok": true, "count": len(ids)})

	case "/api/queue/delete":
		if request.Method != http.MethodPost {
			return status(http.StatusMethodNotAllowed, "只支持 POST"), nil
		}
		ids, err := queueIDs(request.Body, query.Get("id"))
		if err != nil {
			return status(http.StatusBadRequest, err.Error()), nil
		}
		s.engine.ClearFinished(ctx, ids...)
		return okJSON(map[string]any{"ok": true, "count": len(ids)})

	case "/api/queue/clear":
		if request.Method != http.MethodPost {
			return status(http.StatusMethodNotAllowed, "只支持 POST"), nil
		}
		s.engine.ClearFinished(ctx)
		return okJSON(map[string]bool{"ok": true})

	case "/api/downloads":
		if request.Method != http.MethodGet {
			return status(http.StatusMethodNotAllowed, "只支持 GET"), nil
		}
		// It walks the staging tree, so the page asks for it only while the
		// 下载文件 tab is open rather than on the queue's own poll interval.
		return okJSON(s.engine.Downloads(ctx))

	case "/api/downloads/delete":
		if request.Method != http.MethodPost {
			return status(http.StatusMethodNotAllowed, "只支持 POST"), nil
		}
		ids, err := queueIDs(request.Body, query.Get("id"))
		if err != nil {
			return status(http.StatusBadRequest, err.Error()), nil
		}
		deleted, err := s.engine.DeleteStaged(ctx, ids...)
		if err != nil {
			return status(http.StatusBadRequest, err.Error()), nil
		}
		return okJSON(map[string]any{"ok": true, "count": deleted})

	case "/api/downloads/prune":
		if request.Method != http.MethodPost {
			return status(http.StatusMethodNotAllowed, "只支持 POST"), nil
		}
		removed, freed := s.engine.PruneStaged()
		return okJSON(map[string]any{"ok": true, "count": removed, "bytes": freed})

	case "/api/browse":
		if request.Method != http.MethodGet {
			return status(http.StatusMethodNotAllowed, "只支持 GET"), nil
		}
		entries, err := s.engine.Browse(ctx, query.Get("path"), query.Get("driveName"))
		if err != nil {
			return status(http.StatusBadGateway, err.Error()), nil
		}
		return okJSON(map[string]any{"path": browsePath(query.Get("path")), "entries": entries})

	case "/api/drive/browse":
		if request.Method != http.MethodGet {
			return status(http.StatusMethodNotAllowed, "只支持 GET"), nil
		}
		target := browsePath(query.Get("path"))
		entries, err := s.host.List(ctx, target)
		if err != nil {
			if hostapi.IsNotFound(err) {
				return okJSON(map[string]any{"path": target, "entries": []tdriveplugin.Entry{}})
			}
			return status(http.StatusBadGateway, err.Error()), nil
		}
		directories := make([]tdriveplugin.Entry, 0, len(entries))
		for _, entry := range entries {
			if entry.IsDir {
				directories = append(directories, entry)
			}
		}
		return okJSON(map[string]any{"path": target, "entries": directories})

	case "/api/binary/install":
		if request.Method != http.MethodPost {
			return status(http.StatusMethodNotAllowed, "只支持 POST"), nil
		}
		// Keep the route for older cached pages, but there is no external
		// executable to download after the source-client migration.
		return status(http.StatusGone, "aliyunpan 已内置源码客户端，无需安装二进制"), nil

	case "/api/account":
		if request.Method != http.MethodGet {
			return status(http.StatusMethodNotAllowed, "只支持 GET"), nil
		}
		account := s.engine.RefreshProbe(ctx)
		return okJSON(map[string]any{"account": account, "binary": aliyunpan.BuiltInBinaryState()})

	case "/api/login/start":
		if request.Method != http.MethodPost {
			return status(http.StatusMethodNotAllowed, "只支持 POST"), nil
		}
		state, err := s.engine.StartLogin(ctx)
		if err != nil {
			return status(http.StatusBadGateway, err.Error()), nil
		}
		return okJSON(state)

	case "/api/login/confirm":
		if request.Method != http.MethodPost {
			return status(http.StatusMethodNotAllowed, "只支持 POST"), nil
		}
		state, err := s.engine.ConfirmLogin(ctx)
		if err != nil {
			return status(http.StatusBadRequest, err.Error()), nil
		}
		return okJSON(state)

	case "/api/login/cancel":
		if request.Method != http.MethodPost {
			return status(http.StatusMethodNotAllowed, "只支持 POST"), nil
		}
		s.engine.CancelLogin()
		return okJSON(map[string]bool{"ok": true})

	case "/api/logout":
		if request.Method != http.MethodPost {
			return status(http.StatusMethodNotAllowed, "只支持 POST"), nil
		}
		if err := s.engine.Logout(ctx); err != nil {
			return status(http.StatusBadGateway, err.Error()), nil
		}
		return okJSON(map[string]bool{"ok": true})
	}

	return status(http.StatusNotFound, "没有这个接口"), nil
}

// requireCaller resolves the caller against what the host will say about them.
//
// Under per-account plugin ownership the installing account is the owner, and
// tdrive already resolved /plugins/{id} against the caller's own installation
// before this handler ran — so reaching here is itself proof of ownership, and
// the only thing left worth checking is that the account is still enabled. The
// administrator test that used to live here belonged to the era when a plugin
// was installed once for the whole deployment and every signed-in user shared
// it.
//
// On such an older host users.list still returns everybody, and the caller
// genuinely might be an unrelated non-administrator, so the role test is kept
// for exactly that case. Dropping it unconditionally would quietly hand this
// page to every account on an unupgraded deployment.
func (s *Server) requireCaller(ctx context.Context, userID string) error {
	if userID == "" {
		return fmt.Errorf("未登录，请先登录 tdrive")
	}
	users, err := s.host.Users(ctx)
	if err != nil {
		return fmt.Errorf("读取用户信息失败: %w", err)
	}
	scoped := hostapi.OwnedByCaller(users)
	for _, user := range users {
		if user.ID != userID {
			continue
		}
		if !user.Enabled {
			return fmt.Errorf("当前账号已被停用")
		}
		if !scoped && user.Role != "admin" {
			return fmt.Errorf("此操作仅对插件所有者或管理员开放")
		}
		return nil
	}
	return fmt.Errorf("找不到当前账号，请重新登录")
}

var safeIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// maxQueueIDs bounds one batch. The queue view can select a whole screenful at
// a time, but a request naming tens of thousands of ids is a mistake rather
// than an intention.
const maxQueueIDs = 2000

// queueIDs reads the items a queue action applies to, from either a JSON body
// of ids or the single-id query parameter the per-row buttons still use.
func queueIDs(body []byte, single string) ([]string, error) {
	if len(body) > 0 {
		var request struct {
			IDs []string `json:"ids"`
		}
		if err := json.Unmarshal(body, &request); err != nil {
			return nil, fmt.Errorf("请求格式错误，非有效 JSON: %w", err)
		}
		ids := make([]string, 0, len(request.IDs))
		seen := make(map[string]bool, len(request.IDs))
		for _, id := range request.IDs {
			id = strings.TrimSpace(id)
			if id == "" || seen[id] {
				continue
			}
			if !safeIDPattern.MatchString(id) {
				return nil, fmt.Errorf("队列项 ID %q 不合法，仅支持字母、数字、下划线和短横线", id)
			}
			seen[id] = true
			ids = append(ids, id)
		}
		if len(ids) == 0 {
			return nil, fmt.Errorf("未选中任何队列项，请勾选后再试")
		}
		if len(ids) > maxQueueIDs {
			return nil, fmt.Errorf("单次操作超出上限 %d 项，请分批处理", maxQueueIDs)
		}
		return ids, nil
	}
	single = strings.TrimSpace(single)
	if single == "" {
		return nil, fmt.Errorf("未选中任何队列项，请指定队列项后再试")
	}
	if !safeIDPattern.MatchString(single) {
		return nil, fmt.Errorf("队列项 ID %q 不合法，仅支持字母、数字、下划线和短横线", single)
	}
	return []string{single}, nil
}

func browsePath(path string) string {
	cleaned := settings.CleanCloudPath(path)
	if cleaned == "" {
		return "/"
	}
	return cleaned
}

func okJSON(value any) (tdriveplugin.HTTPResponse, error) {
	body, err := json.Marshal(value)
	if err != nil {
		return status(http.StatusInternalServerError, "无法编码响应: "+err.Error()), nil
	}
	return tdriveplugin.HTTPResponse{
		Status:  http.StatusOK,
		Headers: map[string][]string{"Content-Type": {"application/json; charset=utf-8"}},
		Body:    body,
	}, nil
}

func status(code int, message string) tdriveplugin.HTTPResponse {
	body, _ := json.Marshal(map[string]string{"error": message})
	return tdriveplugin.HTTPResponse{
		Status:  code,
		Headers: map[string][]string{"Content-Type": {"application/json; charset=utf-8"}},
		Body:    body,
	}
}
