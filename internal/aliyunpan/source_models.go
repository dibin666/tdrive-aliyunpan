package aliyunpan

import (
	"errors"
	"fmt"
	"path"
	"strings"

	aliyunpanapi "github.com/tickstep/aliyunpan-api/aliyunpan"
)

const (
	DriveBackup   = "backup"
	DriveResource = "resource"
)

// ErrNotLoggedIn is returned when no usable Aliyun Drive credential is loaded.
var ErrNotLoggedIn = errors.New("阿里云盘未登录")

// ErrPathNotFound means the requested cloud path does not exist.
var ErrPathNotFound = errors.New("云盘路径不存在")

// Account is the linked Aliyun Drive account.
type Account struct {
	UserID    string `json:"userId"`
	Nickname  string `json:"nickname"`
	DriveName string `json:"driveName"`
}

// BinaryState is kept in the public snapshot for clients of older plugin
// pages. It now describes the built-in source client rather than an installed
// executable.
type BinaryState struct {
	Path      string `json:"path"`
	Managed   bool   `json:"managed"`
	Installed bool   `json:"installed"`
	Version   string `json:"version,omitempty"`
	Error     string `json:"error,omitempty"`
}

func BuiltInBinaryState() BinaryState {
	return BinaryState{
		Path:      "内置源码客户端",
		Installed: true,
		Version:   "aliyunpan-api v0.2.9",
	}
}

// Drive is one usable Aliyun Drive exposed by an account.
type Drive struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Kind string `json:"kind"`
}

// Entry is one cloud file or directory.
type Entry struct {
	Name  string `json:"name"`
	Path  string `json:"path"`
	IsDir bool   `json:"isDir"`
	Size  int64  `json:"size"`
	// SHA1 is the content hash used by the queue's stable de-duplication key.
	SHA1       string `json:"sha1,omitempty"`
	FileID     string `json:"fileId,omitempty"`
	ModifiedAt string `json:"modifiedAt,omitempty"`
}

// NormalizeDriveName accepts the stable values used in plugin settings and
// the labels used by the Aliyun Drive UI.
func NormalizeDriveName(value string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", DriveBackup, "file", "备份盘", "文件":
		return DriveBackup, nil
	case DriveResource, "资源盘", "资源库":
		return DriveResource, nil
	default:
		return "", fmt.Errorf("网盘 %q 不支持，仅支持 backup（备份盘）或 resource（资源库）", value)
	}
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
	return Drive{}, fmt.Errorf("当前阿里云盘账号缺少%s，请检查网盘权限", label)
}

// convertFileEntity maps the upstream aliyunpan-api model into the plugin's
// stable model. Keeping this conversion in one place preserves Item.key even
// though the old CLI parser and the new API use different representations.
func convertFileEntity(file *aliyunpanapi.FileEntity, parentPath string) Entry {
	if file == nil {
		return Entry{}
	}
	filePath := file.Path
	if filePath == "" {
		filePath = joinCloudPath(parentPath, file.FileName)
	}
	return Entry{
		Name:       file.FileName,
		Path:       filePath,
		IsDir:      file.IsFolder(),
		Size:       file.FileSize,
		SHA1:       normalizeSHA1(file.ContentHash),
		FileID:     file.FileId,
		ModifiedAt: file.UpdatedAt,
	}
}

func joinCloudPath(parentPath, name string) string {
	if parentPath == "" || parentPath == "/" {
		return "/" + name
	}
	return path.Join(parentPath, name)
}

func normalizeSHA1(value string) string {
	if len(value) != 40 {
		return ""
	}
	for _, character := range value {
		if !isHexDigit(character) {
			return ""
		}
	}
	return strings.ToLower(value)
}

func isHexDigit(character rune) bool {
	return character >= '0' && character <= '9' ||
		character >= 'a' && character <= 'f' ||
		character >= 'A' && character <= 'F'
}
