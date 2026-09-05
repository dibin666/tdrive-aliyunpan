package aliyunpan

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	aliyunpanapi "github.com/tickstep/aliyunpan-api/aliyunpan"
	"github.com/tickstep/aliyunpan-api/aliyunpan_open/openapi"
)

const (
	defaultOpenAPIURL       = "https://openapi.alipan.com"
	defaultTokenServiceURL  = "https://api.tickstep.com"
	defaultAliyunpanVersion = "v0.4.0"
	defaultAliyunpanClient  = "cf9f70e8fc61430f8ec5ab5cadf31375"
	apiResponseLimit        = 16 << 20
	apiRequestTimeout       = 30 * time.Second
	driveInfoCacheTTL       = 30 * time.Second
)

// CLI is retained as the package's compatibility name for the old wrapper.
// It is now a source client: no executable is installed or launched, and all
// requests use the upstream aliyunpan OpenAPI models directly.
type CLI struct {
	dataDir   string
	configDir string

	httpClient      *http.Client
	openAPIURL      string
	tokenServiceURL string
	clientID        string

	credentialsMu      sync.RWMutex
	credentials        accountCredentials
	credentialDocument *credentialDocument
	tokenRevision      uint64

	refreshMu sync.Mutex

	driveMu     sync.Mutex
	driveInfo   openapi.DriveInfoResult
	driveInfoAt time.Time

	limiter *byteRateLimiter

	loginStartMu sync.Mutex
	loginMu      sync.Mutex
	login        *LoginSession
}

// New creates a source client and attempts to load the old aliyunpan JSON
// credential file. The second argument is intentionally ignored: it was the
// old custom-binary path and remains in settings only for JSON compatibility.
func New(dataDir, _ string) *CLI {
	client := &CLI{
		dataDir:         dataDir,
		configDir:       filepath.Join(dataDir, "config"),
		httpClient:      &http.Client{},
		openAPIURL:      defaultOpenAPIURL,
		tokenServiceURL: defaultTokenServiceURL,
		clientID:        defaultAliyunpanClient,
		limiter:         &byteRateLimiter{},
	}
	client.loadLegacyCredentials()
	return client
}

type accountCredentials struct {
	UserID          string
	Nickname        string
	TicketID        string
	OpenAPIAccess   string
	OpenAPIExpired  int64
	OpenAPIRefresh  string
	WebAPIAccess    string
	WebAPIExpired   int64
	WebAPIRefresh   string
	BackupDriveID   string
	ResourceDriveID string
}

type apiResponseError struct {
	statusCode   int
	code         string
	message      string
	tokenExpired bool
	retryable    bool
	notFound     bool
}

func (e *apiResponseError) Error() string {
	if e == nil {
		return ""
	}
	if e.message == "" {
		return fmt.Sprintf("阿里云盘 API 请求失败（HTTP %d）", e.statusCode)
	}
	if e.statusCode == 0 {
		return e.message
	}
	return fmt.Sprintf("阿里云盘 API 请求失败（HTTP %d）: %s", e.statusCode, e.message)
}

// requestJSON performs a context-aware OpenAPI request. The upstream API
// package is useful for its stable models, but its methods predate context
// support; this small transport keeps cancellation and concurrent requests
// inside the plugin under our control.
func (c *CLI) requestJSON(ctx context.Context, method, endpoint string, payload, response any) error {
	if ctx == nil {
		ctx = context.Background()
	}
	requestContext, cancel := context.WithTimeout(ctx, apiRequestTimeout)
	defer cancel()
	ctx = requestContext
	if c.httpClient == nil {
		c.httpClient = &http.Client{}
	}

	var requestBody []byte
	var err error
	if payload != nil {
		requestBody, err = json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("编码阿里云盘请求: %w", err)
		}
	}

	for authenticationAttempt := 0; authenticationAttempt < 2; authenticationAttempt++ {
		_, observedRevision := c.accessTokenSnapshot()
		for transientAttempt := 0; transientAttempt < 3; transientAttempt++ {
			accessToken, _ := c.accessTokenSnapshot()
			if accessToken == "" {
				return ErrNotLoggedIn
			}
			requestURL := joinURL(c.openAPIURL, endpoint)
			request, requestErr := http.NewRequestWithContext(ctx, method, requestURL, bytes.NewReader(requestBody))
			if requestErr != nil {
				return fmt.Errorf("创建阿里云盘请求: %w", requestErr)
			}
			request.Header.Set("Authorization", "Bearer "+accessToken)
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Accept", "application/json")

			result, requestErr := c.httpClient.Do(request)
			if requestErr != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if transientAttempt < 2 {
					if waitErr := waitForRetry(ctx, transientAttempt); waitErr != nil {
						return waitErr
					}
					continue
				}
				return fmt.Errorf("请求阿里云盘: %w", requestErr)
			}
			body, readErr := io.ReadAll(io.LimitReader(result.Body, apiResponseLimit))
			_ = result.Body.Close()
			if readErr != nil {
				return fmt.Errorf("读取阿里云盘响应: %w", readErr)
			}

			apiErr := decodeAPIResponseError(result.StatusCode, body)
			if apiErr != nil {
				if apiErr.tokenExpired && authenticationAttempt == 0 {
					if refreshErr := c.refreshAccessToken(ctx, observedRevision); refreshErr != nil {
						return refreshErr
					}
					break
				}
				if apiErr.retryable && transientAttempt < 2 {
					if waitErr := waitForRetry(ctx, transientAttempt); waitErr != nil {
						return waitErr
					}
					continue
				}
				return mapAPIError(apiErr)
			}
			if response == nil || len(bytes.TrimSpace(body)) == 0 {
				return nil
			}
			if err := json.Unmarshal(body, response); err != nil {
				return fmt.Errorf("解析阿里云盘响应: %w", err)
			}
			return nil
		}
		// A token refresh restarts the request with the new access token.
		if authenticationAttempt == 0 {
			continue
		}
	}
	return ErrNotLoggedIn
}

func (c *CLI) tokenServiceData(ctx context.Context, method, endpoint string, payload any) (json.RawMessage, error) {
	return c.tokenServiceDataWithHeaders(ctx, method, endpoint, payload, nil)
}

func (c *CLI) tokenServiceDataWithHeaders(ctx context.Context, method, endpoint string, payload any, headers map[string]string) (json.RawMessage, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	requestContext, cancel := context.WithTimeout(ctx, apiRequestTimeout)
	defer cancel()
	ctx = requestContext
	if c.httpClient == nil {
		c.httpClient = &http.Client{}
	}
	var requestBody []byte
	var err error
	if payload != nil {
		requestBody, err = json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("编码令牌服务请求: %w", err)
		}
	}
	request, err := http.NewRequestWithContext(ctx, method, joinURL(c.tokenServiceURL, endpoint), bytes.NewReader(requestBody))
	if err != nil {
		return nil, fmt.Errorf("创建令牌服务请求: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Content-Type", "application/json;charset=UTF-8")
	request.Header.Set("User-Agent", "aliyunpan/"+defaultAliyunpanVersion)
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("请求令牌服务: %w", err)
	}
	body, readErr := io.ReadAll(io.LimitReader(response.Body, apiResponseLimit))
	_ = response.Body.Close()
	if readErr != nil {
		return nil, fmt.Errorf("读取令牌服务响应: %w", readErr)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("令牌服务返回 HTTP %d: %s", response.StatusCode, serviceMessage(body))
	}

	var envelope struct {
		Code json.RawMessage `json:"code"`
		Msg  string          `json:"msg"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, fmt.Errorf("解析令牌服务响应: %w", err)
	}
	if serviceCodeFailed(envelope.Code) {
		if envelope.Msg == "" {
			envelope.Msg = "令牌服务返回失败"
		}
		return nil, errors.New(envelope.Msg)
	}
	if len(envelope.Data) == 0 || string(envelope.Data) == "null" {
		return json.RawMessage(body), nil
	}
	return envelope.Data, nil
}

func decodeAPIResponseError(statusCode int, body []byte) *apiResponseError {
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(body, &fields)
	code := rawString(fields, "error_code", "errorCode", "code")
	message := rawString(fields, "message", "description", "msg", "error_description")
	if statusCode >= http.StatusOK && statusCode < http.StatusMultipleChoices && codeIsSuccess(code) {
		return nil
	}
	if statusCode >= http.StatusOK && statusCode < http.StatusMultipleChoices && code == "" {
		return nil
	}
	if message == "" {
		message = strings.TrimSpace(string(body))
	}
	if message == "" {
		message = http.StatusText(statusCode)
	}
	normalizedCode := strings.ToLower(code)
	tokenExpired := statusCode == http.StatusUnauthorized ||
		strings.Contains(normalizedCode, "accesstoken") ||
		strings.Contains(normalizedCode, "tokenexpired") ||
		strings.Contains(normalizedCode, "invalidtoken")
	notFound := statusCode == http.StatusNotFound || strings.Contains(normalizedCode, "notfound.file")
	retryable := statusCode == http.StatusTooManyRequests || statusCode >= 500
	return &apiResponseError{
		statusCode:   statusCode,
		code:         code,
		message:      message,
		tokenExpired: tokenExpired,
		retryable:    retryable,
		notFound:     notFound,
	}
}

func mapAPIError(apiErr *apiResponseError) error {
	if apiErr == nil {
		return nil
	}
	if apiErr.tokenExpired {
		return fmt.Errorf("%w: %s", ErrNotLoggedIn, apiErr.message)
	}
	if apiErr.notFound {
		return fmt.Errorf("%w: %s", ErrPathNotFound, apiErr.message)
	}
	return apiErr
}

func waitForRetry(ctx context.Context, attempt int) error {
	delay := time.Duration(1<<attempt) * 250 * time.Millisecond
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func joinURL(base, endpoint string) string {
	return strings.TrimRight(base, "/") + "/" + strings.TrimLeft(endpoint, "/")
}

func rawString(fields map[string]json.RawMessage, names ...string) string {
	for _, name := range names {
		for key, raw := range fields {
			if !sameJSONKey(key, name) {
				continue
			}
			var value string
			if json.Unmarshal(raw, &value) == nil {
				return strings.TrimSpace(value)
			}
			var number json.Number
			if json.Unmarshal(raw, &number) == nil {
				return number.String()
			}
		}
	}
	return ""
}

func sameJSONKey(left, right string) bool {
	clean := func(value string) string {
		value = strings.ToLower(value)
		value = strings.ReplaceAll(value, "_", "")
		value = strings.ReplaceAll(value, "-", "")
		return value
	}
	return clean(left) == clean(right)
}

func codeIsSuccess(code string) bool {
	return code == "" || code == "0" || strings.EqualFold(code, "ok") || strings.EqualFold(code, "success")
}

func serviceCodeFailed(raw json.RawMessage) bool {
	if len(raw) == 0 || string(raw) == "null" {
		return false
	}
	var number int64
	if json.Unmarshal(raw, &number) == nil {
		return number != 0
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return !codeIsSuccess(strings.TrimSpace(text))
	}
	return false
}

func serviceMessage(body []byte) string {
	var fields map[string]json.RawMessage
	if json.Unmarshal(body, &fields) == nil {
		if message := rawString(fields, "msg", "message", "description"); message != "" {
			return message
		}
	}
	return strings.TrimSpace(string(body))
}

func (c *CLI) accessTokenSnapshot() (string, uint64) {
	c.credentialsMu.RLock()
	defer c.credentialsMu.RUnlock()
	return c.credentials.OpenAPIAccess, c.tokenRevision
}

func (c *CLI) setCredentials(credentials accountCredentials) {
	c.refreshMu.Lock()
	defer c.refreshMu.Unlock()
	c.credentialsMu.Lock()
	c.credentials = credentials
	c.tokenRevision++
	c.credentialsMu.Unlock()
	c.invalidateDriveInfo()
}

func (c *CLI) refreshAccessToken(ctx context.Context, observedRevision uint64) error {
	c.refreshMu.Lock()
	defer c.refreshMu.Unlock()

	c.credentialsMu.RLock()
	credentials := c.credentials
	currentRevision := c.tokenRevision
	c.credentialsMu.RUnlock()
	if currentRevision != observedRevision && credentials.OpenAPIAccess != "" {
		return nil
	}
	if credentials.TicketID == "" || credentials.UserID == "" {
		return ErrNotLoggedIn
	}
	endpoint := "/auth/tickstep/aliyunpan/token/openapi/" + url.PathEscape(credentials.TicketID) +
		"/refresh?userId=" + url.QueryEscape(credentials.UserID)
	refreshContext, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	data, err := c.tokenServiceDataWithHeaders(refreshContext, http.MethodGet, endpoint, nil, map[string]string{
		"old-token": credentials.OpenAPIAccess,
	})
	if err != nil {
		return fmt.Errorf("刷新阿里云盘令牌: %w", err)
	}
	accessToken, expired, refreshToken := parseToken(data)
	if accessToken == "" {
		return errors.New("令牌服务没有返回 OpenAPI access token")
	}

	c.credentialsMu.Lock()
	c.credentials.OpenAPIAccess = accessToken
	c.credentials.OpenAPIExpired = expired
	if refreshToken != "" {
		c.credentials.OpenAPIRefresh = refreshToken
	}
	c.tokenRevision++
	c.credentialsMu.Unlock()
	c.invalidateDriveInfo()
	if err := c.persistCredentials(); err != nil {
		return fmt.Errorf("保存刷新后的阿里云盘令牌: %w", err)
	}
	return nil
}

func parseToken(raw json.RawMessage) (accessToken string, expired int64, refreshToken string) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		var token string
		if json.Unmarshal(raw, &token) == nil {
			return strings.TrimSpace(token), 0, ""
		}
		return "", 0, ""
	}
	accessToken = rawString(fields, "accessToken", "access_token", "token")
	refreshToken = rawString(fields, "refreshToken", "refresh_token")
	if value := rawString(fields, "expired", "expiresAt", "expires_at", "expiredTime"); value != "" {
		expired, _ = parseInt64(value)
	}
	return accessToken, expired, refreshToken
}

func parseInt64(value string) (int64, error) {
	return strconv.ParseInt(strings.TrimSpace(value), 10, 64)
}

func (c *CLI) invalidateDriveInfo() {
	c.driveMu.Lock()
	c.driveInfoAt = time.Time{}
	c.driveMu.Unlock()
}

func (c *CLI) fetchDriveInfo(ctx context.Context) (openapi.DriveInfoResult, error) {
	c.driveMu.Lock()
	if !c.driveInfoAt.IsZero() && time.Since(c.driveInfoAt) < driveInfoCacheTTL {
		cached := c.driveInfo
		c.driveMu.Unlock()
		return cached, nil
	}
	c.driveMu.Unlock()

	var result openapi.DriveInfoResult
	if err := c.requestJSON(ctx, http.MethodPost, "/adrive/v1.0/user/getDriveInfo", nil, &result); err != nil {
		return openapi.DriveInfoResult{}, err
	}
	c.driveMu.Lock()
	c.driveInfo = result
	c.driveInfoAt = time.Now()
	c.driveMu.Unlock()
	return result, nil
}

// Who validates the credential and returns account information from the
// OpenAPI drive-info endpoint.
func (c *CLI) Who(ctx context.Context) (Account, error) {
	info, err := c.fetchDriveInfo(ctx)
	if err != nil {
		return Account{}, err
	}
	if info.UserId == "" {
		return Account{}, errors.New("阿里云盘响应中没有用户 ID")
	}
	driveName := DriveBackup
	if info.DefaultDriveId != "" && info.DefaultDriveId == info.ResourceDriveId {
		driveName = DriveResource
	}
	c.updateAccountIdentity(info.UserId, info.Name)
	return Account{UserID: info.UserId, Nickname: info.Name, DriveName: driveName}, nil
}

func (c *CLI) updateAccountIdentity(userID, nickname string) {
	c.refreshMu.Lock()
	defer c.refreshMu.Unlock()
	c.credentialsMu.Lock()
	changed := false
	if c.credentials.UserID != userID && userID != "" {
		c.credentials.UserID = userID
		changed = true
	}
	if c.credentials.Nickname != nickname && nickname != "" {
		c.credentials.Nickname = nickname
		changed = true
	}
	c.credentialsMu.Unlock()
	if changed {
		_ = c.persistCredentials()
	}
}

// Drives returns the account's backup and resource drive IDs.
func (c *CLI) Drives(ctx context.Context) ([]Drive, error) {
	info, err := c.fetchDriveInfo(ctx)
	if err != nil {
		return nil, err
	}
	c.credentialsMu.Lock()
	if info.BackupDriveId == "" {
		info.BackupDriveId = c.credentials.BackupDriveID
	}
	if info.ResourceDriveId == "" {
		info.ResourceDriveId = c.credentials.ResourceDriveID
	}
	c.credentials.BackupDriveID = info.BackupDriveId
	c.credentials.ResourceDriveID = info.ResourceDriveId
	c.credentialsMu.Unlock()
	return drivesFromInfo(info), nil
}

// DrivesForKnownAccount keeps the old method name used by the probe. The
// source client already validates credentials through the same API request.
func (c *CLI) DrivesForKnownAccount(ctx context.Context) ([]Drive, error) {
	return c.Drives(ctx)
}

func drivesFromInfo(info openapi.DriveInfoResult) []Drive {
	drives := make([]Drive, 0, 2)
	backupID := info.BackupDriveId
	if backupID == "" && info.DefaultDriveId != "" && info.DefaultDriveId != info.ResourceDriveId {
		backupID = info.DefaultDriveId
	}
	if backupID == "" && info.ResourceDriveId == "" {
		backupID = info.DefaultDriveId
	}
	if backupID != "" {
		drives = append(drives, Drive{ID: backupID, Name: "备份盘", Kind: DriveBackup})
	}
	if info.ResourceDriveId != "" && info.ResourceDriveId != backupID {
		drives = append(drives, Drive{ID: info.ResourceDriveId, Name: "资源库", Kind: DriveResource})
	}
	return drives
}

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

// List reads a cloud directory using JSON pagination. It accepts a variadic
// drive ID to preserve the old plugin call sites and to keep callers that do
// not select a drive defaulting to the backup drive.
func (c *CLI) List(ctx context.Context, cloudPath string, driveID ...string) ([]Entry, error) {
	selectedDriveID := firstString(driveID)
	if selectedDriveID == "" {
		drive, err := c.ResolveDrive(ctx, DriveBackup)
		if err != nil {
			return nil, err
		}
		selectedDriveID = drive.ID
	}
	cleanPath, err := cleanCloudPath(cloudPath)
	if err != nil {
		return nil, err
	}
	target, err := c.fileByPath(ctx, selectedDriveID, cleanPath)
	if err != nil {
		return nil, err
	}
	if !target.IsFolder() {
		return []Entry{convertFileEntity(target, path.Dir(cleanPath))}, nil
	}

	entries := make([]Entry, 0, 32)
	request := &openapi.FileListParam{
		DriveId:        selectedDriveID,
		ParentFileId:   target.FileId,
		Limit:          100,
		OrderBy:        string(aliyunpanapi.FileOrderByUpdatedAt),
		OrderDirection: string(aliyunpanapi.FileOrderDirectionDesc),
		Type:           "all",
		Fields:         "*",
	}
	seenMarkers := map[string]bool{"": true}
	for {
		var result openapi.FileListResult
		if err := c.requestJSON(ctx, http.MethodPost, "/adrive/v1.0/openFile/list", request, &result); err != nil {
			return nil, err
		}
		for _, item := range result.Items {
			if item == nil {
				continue
			}
			fullPath := joinCloudPath(cleanPath, item.Name)
			entries = append(entries, convertFileEntity(fileItemToEntity(item, fullPath), cleanPath))
		}
		if result.NextMarker == "" {
			break
		}
		if seenMarkers[result.NextMarker] {
			return nil, errors.New("阿里云盘目录分页返回了重复 marker")
		}
		seenMarkers[result.NextMarker] = true
		request.Marker = result.NextMarker
	}
	return entries, nil
}

func (c *CLI) fileByPath(ctx context.Context, driveID, cloudPath string) (*aliyunpanapi.FileEntity, error) {
	if cloudPath == "/" {
		root := aliyunpanapi.NewFileEntityForRootDir()
		root.DriveId = driveID
		return root, nil
	}
	var item openapi.FileItem
	if err := c.requestJSON(ctx, http.MethodPost, "/adrive/v1.0/openFile/get_by_path", &openapi.FilePathPair{
		DriveId:  driveID,
		FilePath: cloudPath,
	}, &item); err != nil {
		return nil, err
	}
	return fileItemToEntity(&item, cloudPath), nil
}

func (c *CLI) fileByID(ctx context.Context, driveID, fileID string) (*aliyunpanapi.FileEntity, error) {
	var item openapi.FileItem
	if err := c.requestJSON(ctx, http.MethodPost, "/adrive/v1.0/openFile/get", &openapi.FileIdentityPair{
		DriveId: driveID,
		FileId:  fileID,
	}, &item); err != nil {
		return nil, err
	}
	return fileItemToEntity(&item, ""), nil
}

func fileItemToEntity(item *openapi.FileItem, fullPath string) *aliyunpanapi.FileEntity {
	if item == nil {
		return nil
	}
	return &aliyunpanapi.FileEntity{
		DriveId:         item.DriveId,
		DomainId:        item.DomainId,
		FileId:          item.FileId,
		FileName:        item.Name,
		FileSize:        item.Size,
		FileType:        item.Type,
		UpdatedAt:       item.UpdatedAt,
		CreatedAt:       item.CreatedAt,
		ParentFileId:    item.ParentFileId,
		ContentHash:     item.ContentHash,
		ContentHashName: item.ContentHashName,
		FileExtension:   item.FileExtension,
		Path:            fullPath,
		Category:        item.Category,
	}
}

func cleanCloudPath(raw string) (string, error) {
	if strings.ContainsRune(raw, 0) {
		return "", errors.New("云盘路径含有 NUL 字符")
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "/", nil
	}
	return path.Clean("/" + raw), nil
}

func firstString(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return strings.TrimSpace(values[0])
}

// Remove deletes a cloud file after it has been committed by tdrive.
func (c *CLI) Remove(ctx context.Context, cloudPath string, driveID ...string) error {
	cleanPath, err := cleanCloudPath(cloudPath)
	if err != nil {
		return err
	}
	if cleanPath == "/" {
		return errors.New("不能删除云盘根目录")
	}
	selectedDriveID := firstString(driveID)
	if selectedDriveID == "" {
		drive, resolveErr := c.ResolveDrive(ctx, DriveBackup)
		if resolveErr != nil {
			return resolveErr
		}
		selectedDriveID = drive.ID
	}
	file, err := c.fileByPath(ctx, selectedDriveID, cleanPath)
	if err != nil {
		return err
	}
	return c.RemoveByID(ctx, selectedDriveID, file.FileId)
}

// RemoveByID avoids a second path lookup for queue items created by the new
// API scanner, while old queue records can still use Remove with their path.
func (c *CLI) RemoveByID(ctx context.Context, driveID, fileID string) error {
	if driveID == "" || fileID == "" {
		return errors.New("删除云端文件缺少网盘 ID 或文件 ID")
	}
	var result openapi.FileAsyncTaskResult
	return c.requestJSON(ctx, http.MethodPost, "/adrive/v1.0/openFile/delete", &openapi.FileIdentityPair{
		DriveId: driveID,
		FileId:  fileID,
	}, &result)
}

// SetDownloadRate configures the shared source downloader limiter. The old
// implementation wrote a CLI config file; an empty value still explicitly
// means unlimited so clearing the setting takes effect immediately.
func (c *CLI) SetDownloadRate(_ context.Context, rate string) error {
	bytesPerSecond, err := parseByteRate(rate)
	if err != nil {
		return err
	}
	c.limiter.SetRate(bytesPerSecond)
	return nil
}

// byteRateLimiter paces all source downloads together. Reserving the next
// interval under one mutex means increasing the number of files cannot
// silently multiply the configured aggregate bandwidth.
type byteRateLimiter struct {
	mu             sync.Mutex
	bytesPerSecond int64
	nextAvailable  time.Time
}

func (l *byteRateLimiter) SetRate(bytesPerSecond int64) {
	l.mu.Lock()
	if l.bytesPerSecond == bytesPerSecond {
		l.mu.Unlock()
		return
	}
	l.bytesPerSecond = bytesPerSecond
	l.nextAvailable = time.Now()
	l.mu.Unlock()
}

func (l *byteRateLimiter) Wait(ctx context.Context, byteCount int64) error {
	if byteCount <= 0 {
		return nil
	}
	l.mu.Lock()
	if l.bytesPerSecond <= 0 {
		l.mu.Unlock()
		return nil
	}
	now := time.Now()
	if l.nextAvailable.Before(now) {
		l.nextAvailable = now
	}
	start := l.nextAvailable
	l.nextAvailable = start.Add(time.Duration(float64(byteCount) / float64(l.bytesPerSecond) * float64(time.Second)))
	l.mu.Unlock()

	delay := time.Until(start)
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func parseByteRate(raw string) (int64, error) {
	value := strings.ToUpper(strings.TrimSpace(raw))
	if value == "" || value == "0" {
		return 0, nil
	}
	value = strings.TrimSpace(strings.TrimSuffix(value, "/S"))
	value = strings.TrimSpace(strings.TrimSuffix(value, "PS"))
	index := 0
	for index < len(value) && ((value[index] >= '0' && value[index] <= '9') || value[index] == '.') {
		index++
	}
	if index == 0 {
		return 0, fmt.Errorf("下载限速格式错误: %q，示例: 2MB、512KB", raw)
	}
	number, err := strconv.ParseFloat(value[:index], 64)
	if err != nil || number < 0 {
		return 0, fmt.Errorf("下载限速格式错误: %q，示例: 2MB、512KB", raw)
	}
	unit := strings.TrimSpace(value[index:])
	multiplier := float64(1)
	switch unit {
	case "", "B":
	case "K", "KB", "KIB":
		multiplier = 1 << 10
	case "M", "MB", "MIB":
		multiplier = 1 << 20
	case "G", "GB", "GIB":
		multiplier = 1 << 30
	case "T", "TB", "TIB":
		multiplier = 1 << 40
	default:
		return 0, fmt.Errorf("下载限速单位 %q 不支持，支持 KB、MB、GB", raw)
	}
	calculated := number * multiplier
	if calculated > float64(^uint64(0)>>1) {
		return 0, fmt.Errorf("下载限速数值 %q 超出支持上限", raw)
	}
	return int64(calculated), nil
}
