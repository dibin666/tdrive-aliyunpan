// Package aliyunpan drives the upstream aliyunpan command line tool.
//
// The plugin shells out rather than reimplementing the Aliyun Drive API: the
// CLI already handles OAuth, token refresh, device registration and the
// download protocol, and all of those change on Aliyun's schedule rather than
// on tdrive's. The cost is that the CLI's human-readable output has to be
// parsed, which is what parse.go is for.
package aliyunpan

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// CLI is a configured aliyunpan installation.
type CLI struct {
	binary    string
	configDir string
	// managed reports that binary is the copy this plugin downloaded, as
	// opposed to an operator-supplied path.
	managed bool

	// mu serializes the short commands. aliyunpan keeps its tokens in a config
	// file it rewrites on almost every command (Before: ReloadConfigFunc,
	// After: SaveConfigFunc), so two concurrent invocations can lose a
	// refreshed token.
	mu sync.Mutex
	// downloadMu serializes downloads separately; see Download for why they do
	// not share the lock above.
	downloadMu sync.Mutex
	// loginMu guards the interactive login session.
	loginMu sync.Mutex
	login   *LoginSession
}

// New builds a CLI. When override is empty the managed binary under dataDir is
// used, which is the normal case; an absolute override lets an operator supply
// their own build or bind-mount one into the container.
func New(dataDir, override string) *CLI {
	binary := filepath.Join(dataDir, "bin", executableName())
	managed := true
	if override != "" {
		binary = override
		managed = false
	}
	return &CLI{binary: binary, configDir: filepath.Join(dataDir, "config"), managed: managed}
}

// Binary is the resolved executable path.
func (c *CLI) Binary() string { return c.binary }

// Managed reports whether Install may replace this binary.
func (c *CLI) Managed() bool { return c.managed }

// InstallManaged downloads the pinned release into the managed location.
func (c *CLI) InstallManaged(ctx context.Context) error {
	if !c.managed {
		return errors.New("配置里指定了自定义 aliyunpan 路径，插件不会覆盖它")
	}
	return Install(ctx, c.binary)
}

type runOptions struct {
	timeout time.Duration
	// stdin is written to the child before its output is read. Only the login
	// flow uses it, and it uses the interactive helper below instead.
	stdin string
}

// run executes one aliyunpan command and returns its combined output.
//
// aliyunpan reports most failures on stdout with a zero exit status ("未登录
// 账号" is the common one), so callers must inspect the text; a non-zero exit
// is treated as an error only because it is unambiguous when it happens.
func (c *CLI) run(ctx context.Context, options runOptions, args ...string) (string, error) {
	if options.timeout <= 0 {
		options.timeout = 2 * time.Minute
	}
	if err := os.MkdirAll(c.configDir, 0o750); err != nil {
		return "", fmt.Errorf("创建 aliyunpan 配置目录: %w", err)
	}
	runCtx, cancel := context.WithTimeout(ctx, options.timeout)
	defer cancel()

	command := exec.CommandContext(runCtx, c.binary, args...)
	command.Env = c.environment()
	if options.stdin != "" {
		command.Stdin = strings.NewReader(options.stdin)
	} else {
		// A nil Stdin is /dev/null, which is what makes the CLI's interactive
		// prompts return immediately instead of blocking forever.
		command.Stdin = nil
	}
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output

	err := command.Run()
	text := output.String()
	if runCtx.Err() != nil {
		return text, fmt.Errorf("aliyunpan %s 超时", args[0])
	}
	if err != nil {
		return text, fmt.Errorf("aliyunpan %s 失败: %w: %s", args[0], err, strings.TrimSpace(text))
	}
	return text, nil
}

// environment isolates the CLI's state in the plugin's own directory so it
// never picks up — or clobbers — an aliyunpan installation the host may
// already have.
func (c *CLI) environment() []string {
	env := make([]string, 0, len(os.Environ())+1)
	for _, entry := range os.Environ() {
		if strings.HasPrefix(entry, "ALIYUNPAN_CONFIG_DIR=") {
			continue
		}
		env = append(env, entry)
	}
	return append(env, "ALIYUNPAN_CONFIG_DIR="+c.configDir)
}

// ErrNotLoggedIn is what every command degrades into before an account is
// linked. It is a sentinel because the scheduler has to distinguish "nothing
// to do" from "cannot do anything".
var ErrNotLoggedIn = errors.New("阿里云盘未登录")

const notLoggedInMarker = "未登录账号"

// Account is the linked Aliyun Drive account.
type Account struct {
	UserID    string `json:"userId"`
	Nickname  string `json:"nickname"`
	DriveName string `json:"driveName"`
}

// Who returns the currently linked account.
func (c *CLI) Who(ctx context.Context) (Account, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	output, err := c.run(ctx, runOptions{timeout: time.Minute}, "who")
	if strings.Contains(output, notLoggedInMarker) {
		return Account{}, ErrNotLoggedIn
	}
	if err != nil {
		return Account{}, err
	}
	return parseWho(output)
}

// Logout unlinks the account.
func (c *CLI) Logout(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	output, err := c.run(ctx, runOptions{timeout: time.Minute}, "logout", "-y")
	if strings.Contains(output, notLoggedInMarker) || strings.Contains(output, "未设置任何帐号") {
		return nil
	}
	return err
}

// SetDownloadRate applies a speed cap such as "2MB". An empty value clears
// nothing — aliyunpan has no "unset" — so callers simply skip the call.
func (c *CLI) SetDownloadRate(ctx context.Context, rate string) error {
	if strings.TrimSpace(rate) == "" {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	output, err := c.run(ctx, runOptions{timeout: time.Minute}, "config", "set", "-max_download_rate", rate)
	if err != nil {
		return err
	}
	if strings.Contains(output, "错误") {
		return fmt.Errorf("设置下载限速失败: %s", strings.TrimSpace(output))
	}
	return nil
}

// List reads one cloud directory. The `ll` alias is `ls -l`, whose table
// carries the exact byte size and content hash that `ls` alone omits.
func (c *CLI) List(ctx context.Context, cloudPath string) ([]Entry, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	output, err := c.run(ctx, runOptions{timeout: 3 * time.Minute}, "ll", cloudPath)
	if strings.Contains(output, notLoggedInMarker) {
		return nil, ErrNotLoggedIn
	}
	if err != nil {
		return nil, err
	}
	return parseList(cloudPath, output)
}

// Remove deletes a cloud path. It is only reachable from a job that opted into
// deleting its source, and only after the drive has committed the file.
func (c *CLI) Remove(ctx context.Context, cloudPath string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	output, err := c.run(ctx, runOptions{timeout: 2 * time.Minute}, "rm", cloudPath)
	if strings.Contains(output, notLoggedInMarker) {
		return ErrNotLoggedIn
	}
	if err != nil {
		return err
	}
	if strings.Contains(output, "失败") {
		return fmt.Errorf("删除云端文件失败: %s", strings.TrimSpace(output))
	}
	return nil
}
