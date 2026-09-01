package aliyunpan

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func newTestSourceClient(t *testing.T, handler http.Handler) *CLI {
	t.Helper()
	server := httptest.NewServer(handler)
	client := New(t.TempDir(), "")
	client.httpClient = server.Client()
	client.openAPIURL = server.URL
	client.tokenServiceURL = server.URL
	client.setCredentials(accountCredentials{
		UserID:        "user-1",
		TicketID:      "ticket-1",
		OpenAPIAccess: "access-old",
	})
	t.Cleanup(server.Close)
	return client
}

func writeJSONResponse(response http.ResponseWriter, value any) {
	response.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(response).Encode(value)
}

func TestSourceClientMapsDriveAndPaginatedFiles(t *testing.T) {
	client := newTestSourceClient(t, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/adrive/v1.0/user/getDriveInfo":
			writeJSONResponse(response, map[string]any{
				"user_id":           "user-1",
				"name":              "测试账号",
				"default_drive_id":  "backup-drive",
				"backup_drive_id":   "backup-drive",
				"resource_drive_id": "resource-drive",
			})
		case "/adrive/v1.0/openFile/get_by_path":
			writeJSONResponse(response, map[string]any{
				"drive_id":       "backup-drive",
				"file_id":        "docs-id",
				"name":           "docs",
				"parent_file_id": "root",
				"type":           "folder",
			})
		case "/adrive/v1.0/openFile/list":
			var payload struct {
				Marker string `json:"marker"`
			}
			_ = json.NewDecoder(request.Body).Decode(&payload)
			if payload.Marker == "" {
				writeJSONResponse(response, map[string]any{
					"items": []map[string]any{
						{"drive_id": "backup-drive", "file_id": "folder-id", "name": "第一季", "type": "folder", "parent_file_id": "docs-id"},
						{"drive_id": "backup-drive", "file_id": "file-1", "name": "带 空格.mkv", "type": "file", "size": 17, "content_hash": strings.Repeat("A", 40), "content_hash_name": "sha1", "updated_at": "2026-09-01T00:00:00Z", "parent_file_id": "docs-id"},
					},
					"next_marker": "next-page",
				})
				return
			}
			writeJSONResponse(response, map[string]any{
				"items": []map[string]any{
					{"drive_id": "backup-drive", "file_id": "file-2", "name": "second.txt", "type": "file", "size": 3, "parent_file_id": "docs-id"},
				},
			})
		default:
			http.NotFound(response, request)
		}
	}))

	drives, err := client.Drives(context.Background())
	if err != nil {
		t.Fatalf("Drives: %v", err)
	}
	if len(drives) != 2 || drives[0].Kind != DriveBackup || drives[1].Kind != DriveResource {
		t.Fatalf("drives = %+v", drives)
	}
	account, err := client.Who(context.Background())
	if err != nil {
		t.Fatalf("Who: %v", err)
	}
	if account.UserID != "user-1" || account.Nickname != "测试账号" {
		t.Fatalf("account = %+v", account)
	}

	entries, err := client.List(context.Background(), "/docs", "backup-drive")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("entries = %+v", entries)
	}
	if entries[0].Path != "/docs/第一季" || !entries[0].IsDir || entries[0].FileID != "folder-id" {
		t.Errorf("directory entry = %+v", entries[0])
	}
	if entries[1].Path != "/docs/带 空格.mkv" || entries[1].SHA1 != strings.Repeat("a", 40) || entries[1].Size != 17 {
		t.Errorf("file entry = %+v", entries[1])
	}
}

func TestSourceClientRefreshesAccessTokenOnceForConcurrentRequests(t *testing.T) {
	var refreshCount int32
	var oldRequestCount int32
	var refreshHeaderValid int32
	client := newTestSourceClient(t, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/probe":
			if request.Header.Get("Authorization") == "Bearer access-old" {
				atomic.AddInt32(&oldRequestCount, 1)
				response.WriteHeader(http.StatusUnauthorized)
				writeJSONResponse(response, map[string]string{"code": "AccessTokenInvalid", "message": "expired"})
				return
			}
			writeJSONResponse(response, map[string]bool{"ok": true})
		case "/auth/tickstep/aliyunpan/token/openapi/ticket-1/refresh":
			if request.Header.Get("old-token") == "access-old" {
				atomic.StoreInt32(&refreshHeaderValid, 1)
			}
			atomic.AddInt32(&refreshCount, 1)
			writeJSONResponse(response, map[string]any{"code": 0, "data": map[string]any{"accessToken": "access-new", "expired": 1999999999}})
		default:
			http.NotFound(response, request)
		}
	}))

	var workers sync.WaitGroup
	errorsFound := make(chan error, 10)
	for index := 0; index < 10; index++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			var result struct {
				OK bool `json:"ok"`
			}
			if err := client.requestJSON(context.Background(), http.MethodGet, "/probe", nil, &result); err != nil {
				errorsFound <- err
			}
		}()
	}
	workers.Wait()
	close(errorsFound)
	for err := range errorsFound {
		t.Fatalf("concurrent request: %v", err)
	}
	if atomic.LoadInt32(&refreshCount) != 1 {
		t.Fatalf("refresh count = %d, want 1", refreshCount)
	}
	if atomic.LoadInt32(&refreshHeaderValid) != 1 {
		t.Fatal("token refresh did not send the expired token in old-token header")
	}
	if atomic.LoadInt32(&oldRequestCount) == 0 {
		t.Fatal("requests never exercised the expired token")
	}
	if accessToken, _ := client.accessTokenSnapshot(); accessToken != "access-new" {
		t.Fatalf("access token = %q, want refreshed token", accessToken)
	}
}

func TestSourceClientLoadsAndPreservesLegacyCredentials(t *testing.T) {
	dataDir := t.TempDir()
	configDir := filepath.Join(dataDir, "config")
	if err := os.MkdirAll(configDir, 0o750); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	legacy := `{
  "configVer": "1.0",
  "activeUID": "user-1",
  "futureOption": {"keep": true},
  "userList": [{
    "userId": "user-1",
    "nickname": "旧昵称",
    "ticketId": "ticket-1",
    "customField": "keep",
    "openapiToken": {"accessToken": "access-legacy", "expired": 1700000000},
    "webapiToken": {"accessToken": "web-legacy", "expired": 1700000000}
  }]
}`
	if err := os.WriteFile(filepath.Join(configDir, legacyCredentialFileName), []byte(legacy), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	client := New(dataDir, "relative/old-binary")
	client.credentialsMu.Lock()
	credentials := client.credentials
	client.credentials.Nickname = "更新昵称"
	client.credentialsMu.Unlock()
	if credentials.UserID != "user-1" || credentials.OpenAPIAccess != "access-legacy" || credentials.WebAPIAccess != "web-legacy" {
		t.Fatalf("loaded credentials = %+v", credentials)
	}
	if err := client.persistCredentials(); err != nil {
		t.Fatalf("persistCredentials: %v", err)
	}
	contents, err := os.ReadFile(filepath.Join(configDir, legacyCredentialFileName))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !strings.Contains(string(contents), `"futureOption"`) || !strings.Contains(string(contents), `"customField"`) {
		t.Fatalf("unknown legacy fields were lost: %s", contents)
	}
	if !strings.Contains(string(contents), `"nickname": "更新昵称"`) {
		t.Fatalf("updated nickname was not persisted: %s", contents)
	}
	if err := client.Logout(context.Background()); err != nil {
		t.Fatalf("Logout: %v", err)
	}
	reloaded := New(dataDir, "")
	if accessToken, _ := reloaded.accessTokenSnapshot(); accessToken != "" {
		t.Fatalf("logout left an active access token: %q", accessToken)
	}
}

func TestSourceClientLoginUsesTokenServiceAndPersistsAccount(t *testing.T) {
	client := New(t.TempDir(), "")
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/auth/tickstep/aliyunpan/token/qrcode/create":
			writeJSONResponse(response, map[string]any{"code": 0, "data": map[string]string{
				"tokenId":  "ticket-login",
				"tokenUrl": "https://openapi.alipan.com/oauth/authorize?tokenId=ticket-login",
			}})
		case "/auth/tickstep/aliyunpan/token/common/ticket-login/login":
			writeJSONResponse(response, map[string]any{"code": 0, "data": map[string]any{
				"openapi": map[string]any{"accessToken": "access-login", "expired": 1999999999},
				"webapi":  map[string]any{"accessToken": "web-login", "expired": 1999999999},
			}})
		case "/adrive/v1.0/user/getDriveInfo":
			writeJSONResponse(response, map[string]any{
				"user_id": "user-login", "name": "登录账号", "default_drive_id": "backup-drive", "backup_drive_id": "backup-drive",
			})
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()
	client.httpClient = server.Client()
	client.openAPIURL = server.URL
	client.tokenServiceURL = server.URL

	state, err := client.StartLogin(context.Background())
	if err != nil {
		t.Fatalf("StartLogin: %v", err)
	}
	if !state.Active || state.Phase != LoginWaiting || !strings.Contains(state.URL, "ticket-login") {
		t.Fatalf("login start state = %+v", state)
	}
	state, err = client.ConfirmLogin(context.Background())
	if err != nil {
		t.Fatalf("ConfirmLogin: %v", err)
	}
	if state.Phase != LoginDone || state.Nickname != "登录账号" {
		t.Fatalf("login completion state = %+v", state)
	}
	if _, err := os.Stat(filepath.Join(client.configDir, legacyCredentialFileName)); err != nil {
		t.Fatalf("legacy credential file was not written: %v", err)
	}
}

func TestSourceClientMapsNotFoundAndCancellation(t *testing.T) {
	client := newTestSourceClient(t, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.WriteHeader(http.StatusNotFound)
		writeJSONResponse(response, map[string]string{"code": "NotFound.File", "message": "missing"})
	}))
	_, err := client.List(context.Background(), "/missing", "drive")
	if !errors.Is(err, ErrPathNotFound) {
		t.Fatalf("List error = %v, want ErrPathNotFound", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := client.requestJSON(ctx, http.MethodGet, "/missing", nil, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled request error = %v", err)
	}
}
