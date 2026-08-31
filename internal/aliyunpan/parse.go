package aliyunpan

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Entry is one cloud file or directory.
type Entry struct {
	Name  string `json:"name"`
	Path  string `json:"path"`
	IsDir bool   `json:"isDir"`
	Size  int64  `json:"size"`
	// SHA1 is aliyunpan's content hash. It identifies a file across renames
	// and is what the queue keys on to decide something has already been
	// synced.
	SHA1       string `json:"sha1,omitempty"`
	FileID     string `json:"fileId,omitempty"`
	ModifiedAt string `json:"modifiedAt,omitempty"`
}

// `ll` prints a borderless table whose columns are separated by padding alone,
// so there is no separator to split on. The two timestamp columns are the only
// unambiguous landmark in a row: the name may contain spaces, and so may the
// columns to their left once a hash goes missing. Everything left of the
// timestamps is "file_id humanSize [sha1] rawSize", everything right of them
// is the name.
//
//	2  6512ab34ce  1.53MB  A1B2…  1604321  2024-01-02 15:04:05  2024-03-04 05:06:07  我的 视频.mkv
//
// The timestamps are matched strictly. Allowing "-" for them too would make
// the pattern ambiguous with the "-  -  -" a directory row puts in its size
// columns, and the shortest match would then be the wrong one.
var (
	timestamp  = `\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}`
	rowPattern = regexp.MustCompile(
		`^\s*(\d+)\s+(.*?)\s+(` + timestamp + `)\s+(` + timestamp + `)\s+(.*?)\s*$`)
	sha1Pattern = regexp.MustCompile(`^[0-9a-fA-F]{40}$`)
	whoPattern  = regexp.MustCompile(
		`当前帐号UID:\s*(\S*),\s*昵称:\s*(.*?),\s*三方权益包:.*?,\s*当前使用网盘：(.*?)\s*$`)
)

// ErrPathNotFound means the cloud path does not exist.
var ErrPathNotFound = errors.New("云盘路径不存在")

// parseList turns `ll <dir>` output into entries.
//
// A row that cannot be parsed aborts the whole listing rather than being
// skipped: silently dropping a row would silently drop a file from the sync,
// and a format change is something an operator needs to be told about.
func parseList(cloudPath, output string) ([]Entry, error) {
	if strings.Contains(output, "指定目录不存在") || strings.Contains(output, "目录路径不存在") {
		return nil, fmt.Errorf("%w: %s", ErrPathNotFound, cloudPath)
	}
	parent := strings.TrimSuffix(cloudPath, "/")

	entries := make([]Entry, 0, 16)
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimRight(line, "\r")
		if strings.TrimSpace(line) == "" {
			continue
		}
		match := rowPattern.FindStringSubmatch(line)
		if match == nil {
			// The header, the trailing totals row and any surrounding prose
			// legitimately fail to match; only rows that begin with an index
			// are data.
			if isDataRow(line) {
				return nil, fmt.Errorf("无法解析 aliyunpan 的目录输出，可能是 CLI 版本不兼容: %q", strings.TrimSpace(line))
			}
			continue
		}
		entry, err := parseRow(parent, match[2], match[4], match[5])
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// isDataRow reports whether a line looked like it was meant to be an entry.
// Rows start with a 1-based index; the header starts with "#" and the totals
// row starts with padding.
func isDataRow(line string) bool {
	trimmed := strings.TrimLeft(line, " \t")
	if trimmed == "" {
		return false
	}
	return trimmed[0] >= '0' && trimmed[0] <= '9'
}

// parseRow interprets the columns left of the timestamps plus the name.
//
// leading holds "file_id humanSize [sha1] rawSize"; the hash column is empty
// for a directory and, defensively, for a file the API did not hash.
func parseRow(parent, leading, modified, name string) (Entry, error) {
	fields := strings.Fields(leading)
	if len(fields) < 3 {
		return Entry{}, fmt.Errorf("目录输出的列数不符合预期: %q", leading)
	}
	entry := Entry{
		Name:       name,
		IsDir:      strings.HasSuffix(name, "/"),
		FileID:     fields[0],
		ModifiedAt: modified,
	}
	if entry.IsDir {
		entry.Name = strings.TrimSuffix(name, "/")
	}
	if entry.Name == "" {
		return Entry{}, fmt.Errorf("目录输出里出现了空文件名: %q", leading)
	}
	entry.Path = parent + "/" + entry.Name

	raw := fields[len(fields)-1]
	if raw != "-" {
		size, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return Entry{}, fmt.Errorf("无法解析文件大小 %q: %w", raw, err)
		}
		entry.Size = size
	}
	if hash := fields[len(fields)-2]; sha1Pattern.MatchString(hash) {
		entry.SHA1 = strings.ToLower(hash)
	}
	// A row whose name has no trailing slash but whose size column is "-" is
	// not something the CLI produces; treating it as a directory would make
	// the scanner descend into a file.
	if !entry.IsDir && raw == "-" {
		return Entry{}, fmt.Errorf("目录输出里的 %q 既不是目录也没有大小", entry.Name)
	}
	return entry, nil
}

// parseWho reads the single line `who` prints for a linked account.
func parseWho(output string) (Account, error) {
	match := whoPattern.FindStringSubmatch(output)
	if match == nil {
		return Account{}, fmt.Errorf("无法解析 aliyunpan 的账号信息: %q", strings.TrimSpace(output))
	}
	return Account{UserID: match[1], Nickname: match[2], DriveName: match[3]}, nil
}
