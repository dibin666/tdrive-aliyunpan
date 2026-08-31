package aliyunpan

import (
	"errors"
	"strings"
	"testing"
)

// The fixture is byte-for-byte what tablewriter produces with the settings
// aliyunpan's cmdtable applies (no border, no header line, empty column
// separator), so the parser is exercised against the real layout rather than a
// tidied-up approximation of it.
const listFixture = `  #   FILE ID     文件大小                    文件SHA1                  文件大小(原始)       创建日期                修改日期             文件(目录)
  1  6512ab34cd  -            -                                         -               2024-01-02 15:04:05  2024-03-04 05:06:07       影视 合集/
  2  6512ab34ce  1.53MB       A1B2C3D4E5F60718293A4B5C6D7E8F9012345678  1604321         2024-01-02 15:04:05  2024-03-04 05:06:07       我的 视频 v2.mkv
  3  6512ab34cf  12.00GB      0000000000000000000000000000000000000000  12884901888     2024-01-02 15:04:05  2024-03-04 05:06:07       big.iso
  4  6512ab34d0  999B         1111111111111111111111111111111111111111  999             2024-01-02 15:04:05  2024-03-04 05:06:07       tiny.txt
                 总: 12.00GB                                                                                 文件总数: 3, 目录总数: 1
`

func TestParseList(t *testing.T) {
	entries, err := parseList("/我的资源", listFixture)
	if err != nil {
		t.Fatalf("parseList: %v", err)
	}
	if len(entries) != 4 {
		t.Fatalf("got %d entries, want 4: %+v", len(entries), entries)
	}

	directory := entries[0]
	if !directory.IsDir {
		t.Errorf("entry 0 should be a directory: %+v", directory)
	}
	if directory.Name != "影视 合集" {
		t.Errorf("directory name = %q, want %q", directory.Name, "影视 合集")
	}
	if directory.Path != "/我的资源/影视 合集" {
		t.Errorf("directory path = %q", directory.Path)
	}
	if directory.Size != 0 {
		t.Errorf("directory size = %d, want 0", directory.Size)
	}

	// A name containing spaces is the case the whitespace-separated table
	// makes easy to get wrong.
	file := entries[1]
	if file.IsDir {
		t.Errorf("entry 1 should be a file")
	}
	if file.Name != "我的 视频 v2.mkv" {
		t.Errorf("file name = %q", file.Name)
	}
	if file.Size != 1604321 {
		t.Errorf("file size = %d, want 1604321", file.Size)
	}
	if file.SHA1 != "a1b2c3d4e5f60718293a4b5c6d7e8f9012345678" {
		t.Errorf("file sha1 = %q", file.SHA1)
	}
	if file.FileID != "6512ab34ce" {
		t.Errorf("file id = %q", file.FileID)
	}

	if entries[2].Size != 12884901888 {
		t.Errorf("large file size = %d", entries[2].Size)
	}
	if entries[3].Size != 999 {
		t.Errorf("small file size = %d", entries[3].Size)
	}
}

// A file the API never hashed leaves the SHA1 column blank, which shifts the
// leading columns. The row must still parse rather than abort the scan.
func TestParseListWithoutHash(t *testing.T) {
	line := "  1  6512ab34ce  1.53MB       " +
		"                                          1604321         " +
		"2024-01-02 15:04:05  2024-03-04 05:06:07       no-hash.bin  \n"
	entries, err := parseList("/a", line)
	if err != nil {
		t.Fatalf("parseList: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("got %d entries, want 1", len(entries))
	}
	if entries[0].SHA1 != "" {
		t.Errorf("sha1 = %q, want empty", entries[0].SHA1)
	}
	if entries[0].Size != 1604321 {
		t.Errorf("size = %d", entries[0].Size)
	}
}

func TestParseListRootPath(t *testing.T) {
	entries, err := parseList("/", listFixture)
	if err != nil {
		t.Fatalf("parseList: %v", err)
	}
	if entries[3].Path != "/tiny.txt" {
		t.Errorf("path under root = %q, want /tiny.txt", entries[3].Path)
	}
}

func TestParseListMissingDirectory(t *testing.T) {
	_, err := parseList("/nope", "指定目录不存在: /nope\n")
	if !errors.Is(err, ErrPathNotFound) {
		t.Fatalf("err = %v, want ErrPathNotFound", err)
	}
}

// A row that begins with an index but does not match the column layout means
// the CLI's output changed. Skipping it would silently drop a file from the
// sync, so it has to fail loudly.
func TestParseListRejectsUnknownRow(t *testing.T) {
	_, err := parseList("/a", "  1  totally different output\n")
	if err == nil {
		t.Fatal("expected an error for an unparseable data row")
	}
	if !strings.Contains(err.Error(), "无法解析") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestParseListIgnoresHeaderAndTotals(t *testing.T) {
	only := "  #   FILE ID  文件大小\n                 总: 0B\n\n"
	entries, err := parseList("/a", only)
	if err != nil {
		t.Fatalf("parseList: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("got %d entries, want 0", len(entries))
	}
}

func TestParseWho(t *testing.T) {
	account, err := parseWho("当前帐号UID: 1234abcd, 昵称: 张 三, 三方权益包: 未开通, 当前使用网盘：备份盘\n")
	if err != nil {
		t.Fatalf("parseWho: %v", err)
	}
	if account.UserID != "1234abcd" {
		t.Errorf("user id = %q", account.UserID)
	}
	if account.Nickname != "张 三" {
		t.Errorf("nickname = %q", account.Nickname)
	}
	if account.DriveName != "备份盘" {
		t.Errorf("drive name = %q", account.DriveName)
	}
}

func TestParseWhoRejectsGarbage(t *testing.T) {
	if _, err := parseWho("未登录账号\n"); err == nil {
		t.Fatal("expected an error")
	}
}

func TestStagedPath(t *testing.T) {
	// aliyunpan joins the save directory with the file's whole cloud path, not
	// just its name, so the staging tree mirrors the cloud tree.
	got := StagedPath("/var/stage", "/我的资源/影视/a.mkv")
	want := "/var/stage/我的资源/影视/a.mkv"
	if got != want {
		t.Errorf("StagedPath = %q, want %q", got, want)
	}
}

func TestAssetName(t *testing.T) {
	name, err := assetName("linux", "amd64")
	if err != nil {
		t.Fatalf("assetName: %v", err)
	}
	if name != "aliyunpan-"+ReleaseVersion+"-linux-amd64.zip" {
		t.Errorf("asset = %q", name)
	}
	if _, err := assetName("plan9", "amd64"); err == nil {
		t.Error("expected an error for an unpublished platform")
	}
}
