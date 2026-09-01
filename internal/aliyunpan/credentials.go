package aliyunpan

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const legacyCredentialFileName = "aliyunpan_config.json"

// credentialDocument keeps the original JSON object around so upgrading the
// plugin does not discard CLI options or future fields it does not understand.
type credentialDocument struct {
	path         string
	data         map[string]json.RawMessage
	activeUIDKey string
	userListKey  string
}

func (c *CLI) loadLegacyCredentials() {
	credentialPath := filepath.Join(c.configDir, legacyCredentialFileName)
	contents, err := os.ReadFile(credentialPath)
	if err != nil {
		return
	}
	var documentData map[string]json.RawMessage
	if err := json.Unmarshal(contents, &documentData); err != nil || documentData == nil {
		return
	}

	document := &credentialDocument{
		path:         credentialPath,
		data:         documentData,
		activeUIDKey: findJSONKey(documentData, "activeUID", "activeUid", "active_uid"),
		userListKey:  findJSONKey(documentData, "userList", "user_list"),
	}
	if document.activeUIDKey == "" {
		document.activeUIDKey = "activeUID"
	}
	if document.userListKey == "" {
		document.userListKey = "userList"
	}

	activeUserID := rawString(documentData, "activeUID", "activeUid", "active_uid")
	_, activeUserConfigured := findJSONRaw(documentData, "activeUID", "activeUid", "active_uid")
	users := rawUserList(documentData, document.userListKey)
	var selected accountCredentials
	selectedUser := false
	for _, userData := range users {
		candidate := parseLegacyUser(userData)
		if candidate.UserID == "" && candidate.OpenAPIAccess == "" {
			continue
		}
		if activeUserID != "" && candidate.UserID == activeUserID {
			selected = candidate
			selectedUser = true
			break
		}
		if !activeUserConfigured && !selectedUser && selected.OpenAPIAccess == "" && candidate.OpenAPIAccess != "" {
			selected = candidate
		}
	}

	c.credentialsMu.Lock()
	c.credentialDocument = document
	if selected.OpenAPIAccess != "" {
		c.credentials = selected
		c.tokenRevision = 1
	}
	c.credentialsMu.Unlock()
}

func rawUserList(documentData map[string]json.RawMessage, key string) []map[string]json.RawMessage {
	raw, ok := documentData[key]
	if !ok {
		for candidateKey, candidateRaw := range documentData {
			if sameJSONKey(candidateKey, key) {
				raw = candidateRaw
				ok = true
				break
			}
		}
	}
	if !ok {
		return nil
	}
	var entries []json.RawMessage
	if json.Unmarshal(raw, &entries) != nil {
		return nil
	}
	users := make([]map[string]json.RawMessage, 0, len(entries))
	for _, entry := range entries {
		var user map[string]json.RawMessage
		if json.Unmarshal(entry, &user) == nil && user != nil {
			users = append(users, user)
		}
	}
	return users
}

func parseLegacyUser(userData map[string]json.RawMessage) accountCredentials {
	credentials := accountCredentials{
		UserID:   rawString(userData, "userId", "user_id", "uid"),
		Nickname: rawString(userData, "nickname", "nickName", "name"),
		TicketID: rawString(userData, "ticketId", "ticket_id"),
	}
	credentials.OpenAPIAccess, credentials.OpenAPIExpired, credentials.OpenAPIRefresh = legacyToken(userData,
		"openapiToken", "openApiToken", "open_token", "openToken", "open_api_token")
	credentials.WebAPIAccess, credentials.WebAPIExpired, credentials.WebAPIRefresh = legacyToken(userData,
		"webapiToken", "webApiToken", "web_token", "webToken", "web_api_token")
	if credentials.OpenAPIAccess == "" {
		credentials.OpenAPIAccess, credentials.OpenAPIExpired, credentials.OpenAPIRefresh = legacyToken(userData, "token")
	}
	credentials.BackupDriveID = rawString(userData, "backupDriveId", "backup_drive_id", "fileDriveId", "file_drive_id")
	credentials.ResourceDriveID = rawString(userData, "resourceDriveId", "resource_drive_id")
	if drivesRaw, ok := findJSONRaw(userData, "driveList", "drive_list"); ok {
		var drives []map[string]json.RawMessage
		if json.Unmarshal(drivesRaw, &drives) == nil {
			for _, drive := range drives {
				driveID := rawString(drive, "driveId", "drive_id", "id")
				driveTag := strings.ToLower(rawString(drive, "driveTag", "drive_tag", "kind", "name"))
				switch driveTag {
				case "file", "backup", "备份盘", "文件":
					if credentials.BackupDriveID == "" {
						credentials.BackupDriveID = driveID
					}
				case "resource", "资源盘", "资源库":
					if credentials.ResourceDriveID == "" {
						credentials.ResourceDriveID = driveID
					}
				}
			}
		}
	}
	return credentials
}

func legacyToken(userData map[string]json.RawMessage, names ...string) (string, int64, string) {
	if raw, ok := findJSONRaw(userData, names...); ok {
		return parseToken(raw)
	}
	return "", 0, ""
}

func findJSONRaw(fields map[string]json.RawMessage, names ...string) (json.RawMessage, bool) {
	for _, name := range names {
		for key, raw := range fields {
			if sameJSONKey(key, name) {
				return raw, true
			}
		}
	}
	return nil, false
}

func findJSONKey(fields map[string]json.RawMessage, names ...string) string {
	for _, name := range names {
		for key := range fields {
			if sameJSONKey(key, name) {
				return key
			}
		}
	}
	return ""
}

func (c *CLI) persistCredentials() error {
	c.credentialsMu.Lock()
	defer c.credentialsMu.Unlock()
	return c.persistCredentialsLocked()
}

func (c *CLI) clearCredentials() error {
	c.refreshMu.Lock()
	defer c.refreshMu.Unlock()
	c.credentialsMu.Lock()
	defer c.credentialsMu.Unlock()
	oldUserID := c.credentials.UserID
	if c.credentialDocument != nil {
		users := rawUserList(c.credentialDocument.data, c.credentialDocument.userListKey)
		kept := make([]map[string]json.RawMessage, 0, len(users))
		for _, userData := range users {
			if rawString(userData, "userId", "user_id", "uid") != oldUserID {
				kept = append(kept, userData)
			}
		}
		encodedUsers, err := json.Marshal(kept)
		if err != nil {
			return fmt.Errorf("编码阿里云盘账号: %w", err)
		}
		c.credentialDocument.data[c.credentialDocument.userListKey] = encodedUsers
	}
	c.credentials = accountCredentials{}
	c.tokenRevision++
	err := c.persistCredentialsLocked()
	c.invalidateDriveInfo()
	return err
}

func (c *CLI) persistCredentialsLocked() error {
	document := c.credentialDocument
	if document == nil {
		document = &credentialDocument{
			path:         filepath.Join(c.configDir, legacyCredentialFileName),
			data:         make(map[string]json.RawMessage),
			activeUIDKey: "activeUID",
			userListKey:  "userList",
		}
		c.credentialDocument = document
	}
	if document.data == nil {
		document.data = make(map[string]json.RawMessage)
	}
	if document.activeUIDKey == "" {
		document.activeUIDKey = "activeUID"
	}
	if document.userListKey == "" {
		document.userListKey = "userList"
	}

	users := rawUserList(document.data, document.userListKey)
	updated := false
	for index, userData := range users {
		userID := rawString(userData, "userId", "user_id", "uid")
		if userID != c.credentials.UserID {
			continue
		}
		users[index] = updateLegacyUser(userData, c.credentials)
		updated = true
		break
	}
	if c.credentials.OpenAPIAccess != "" && !updated {
		users = append(users, updateLegacyUser(make(map[string]json.RawMessage), c.credentials))
	}
	encodedUsers, err := json.Marshal(users)
	if err != nil {
		return fmt.Errorf("编码阿里云盘账号: %w", err)
	}
	document.data[document.userListKey] = encodedUsers
	activeUID := c.credentials.UserID
	encodedActiveUID, err := json.Marshal(activeUID)
	if err != nil {
		return fmt.Errorf("编码阿里云盘当前账号: %w", err)
	}
	document.data[document.activeUIDKey] = encodedActiveUID

	contents, err := json.MarshalIndent(document.data, "", "  ")
	if err != nil {
		return fmt.Errorf("编码阿里云盘配置: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(document.path), 0o750); err != nil {
		return fmt.Errorf("创建阿里云盘配置目录: %w", err)
	}
	temporary, err := os.CreateTemp(filepath.Dir(document.path), ".aliyunpan-config-*.tmp")
	if err != nil {
		return fmt.Errorf("创建阿里云盘配置临时文件: %w", err)
	}
	temporaryName := temporary.Name()
	cleanup := func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryName)
	}
	if err := temporary.Chmod(0o600); err != nil {
		cleanup()
		return err
	}
	if _, err := temporary.Write(contents); err != nil {
		cleanup()
		return fmt.Errorf("写入阿里云盘配置: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("同步阿里云盘配置: %w", err)
	}
	if err := temporary.Close(); err != nil {
		_ = os.Remove(temporaryName)
		return fmt.Errorf("关闭阿里云盘配置: %w", err)
	}
	if err := os.Rename(temporaryName, document.path); err != nil {
		_ = os.Remove(temporaryName)
		return fmt.Errorf("替换阿里云盘配置: %w", err)
	}
	return nil
}

func updateLegacyUser(userData map[string]json.RawMessage, credentials accountCredentials) map[string]json.RawMessage {
	if userData == nil {
		userData = make(map[string]json.RawMessage)
	}
	setJSONField(userData, []string{"userId", "user_id", "uid"}, credentials.UserID)
	setJSONField(userData, []string{"nickname", "nickName", "name"}, credentials.Nickname)
	setJSONField(userData, []string{"ticketId", "ticket_id"}, credentials.TicketID)
	setTokenField(userData, []string{"openapiToken", "openApiToken", "open_token", "openToken", "open_api_token"}, credentials.OpenAPIAccess, credentials.OpenAPIExpired, credentials.OpenAPIRefresh)
	if credentials.WebAPIAccess != "" {
		setTokenField(userData, []string{"webapiToken", "webApiToken", "web_token", "webToken", "web_api_token"}, credentials.WebAPIAccess, credentials.WebAPIExpired, credentials.WebAPIRefresh)
	}
	return userData
}

func setJSONField(fields map[string]json.RawMessage, names []string, value any) {
	key := findJSONKey(fields, names...)
	if key == "" {
		key = names[0]
	}
	encoded, err := json.Marshal(value)
	if err == nil {
		fields[key] = encoded
	}
}

func setTokenField(fields map[string]json.RawMessage, names []string, accessToken string, expired int64, refreshToken string) {
	key := findJSONKey(fields, names...)
	if key == "" {
		key = names[0]
	}
	token := make(map[string]json.RawMessage)
	if existing, ok := fields[key]; ok {
		_ = json.Unmarshal(existing, &token)
	}
	setJSONField(token, []string{"accessToken", "access_token"}, accessToken)
	setJSONField(token, []string{"expired", "expiresAt", "expires_at"}, expired)
	if refreshToken != "" {
		setJSONField(token, []string{"refreshToken", "refresh_token"}, refreshToken)
	}
	encoded, err := json.Marshal(token)
	if err == nil {
		fields[key] = encoded
	}
}
