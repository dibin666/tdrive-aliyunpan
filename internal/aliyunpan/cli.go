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

	// commandGate serializes every aliyunpan process, including downloads. The
	// CLI rewrites its config file after almost every command, so allowing a
	// concurrent `ll` or `who` to run beside a download can lose a refreshed
	// token or the selected drive.
	commandGate     chan struct{}
	commandGateInit sync.Once
	runningMu       sync.Mutex
	running         map[uint64]context.CancelFunc
	nextRunID       uint64

	// loginMu guards the interactive login session.
	loginMu sync.Mutex
	// loginStartMu prevents two callers from replacing each other's login while
	// still allowing CancelLogin to acquire loginMu during the URL wait.
	loginStartMu sync.Mutex
	login        *LoginSession
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
	return &CLI{
		binary:      binary,
		configDir:   filepath.Join(dataDir, "config"),
		managed:     managed,
		commandGate: make(chan struct{}, 1),
		running:     make(map[uint64]context.CancelFunc),
	}
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
	if ctx == nil {
		ctx = context.Background()
	}
	// Installing replaces the executable. Keep it behind the same gate as
	// probes, login and transfers so no command can be using a half-replaced
	// file (and so Windows is not asked to replace a running executable).
	installCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	unregister := c.registerRunning(cancel)
	defer unregister()
	if err := c.acquireCommand(installCtx); err != nil {
		return err
	}
	defer c.releaseCommand()
	return Install(installCtx, c.binary)
}

type runOptions struct {
	timeout time.Duration
	// stdin is written to the child before its output is read. Only the login
	// flow uses it, and it uses the interactive helper below instead.
	stdin string
}

func (c *CLI) initCommandGate() {
	c.commandGateInit.Do(func() {
		if c.commandGate == nil {
			c.commandGate = make(chan struct{}, 1)
		}
		if c.running == nil {
			c.running = make(map[uint64]context.CancelFunc)
		}
	})
}

func (c *CLI) acquireCommand(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	c.initCommandGate()
	select {
	case c.commandGate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *CLI) releaseCommand() {
	<-c.commandGate
}

func (c *CLI) runCommand(ctx context.Context, options runOptions, args ...string) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	// Register before waiting for the shared slot. Otherwise a queued download
	// is invisible to Logout and can jump in front of the logout command after
	// the currently running download is cancelled.
	commandCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	unregister := c.registerRunning(cancel)
	defer unregister()
	if err := c.acquireCommand(commandCtx); err != nil {
		return "", err
	}
	defer c.releaseCommand()
	return c.run(commandCtx, options, args...)
}

func (c *CLI) registerRunning(cancel context.CancelFunc) func() {
	c.initCommandGate()
	c.runningMu.Lock()
	c.nextRunID++
	id := c.nextRunID
	c.running[id] = cancel
	c.runningMu.Unlock()
	return func() {
		c.runningMu.Lock()
		delete(c.running, id)
		c.runningMu.Unlock()
	}
}

// CancelRunning interrupts a download or another non-interactive command so a
// logout can complete instead of waiting behind a multi-hour transfer.
func (c *CLI) CancelRunning() {
	c.runningMu.Lock()
	cancels := make([]context.CancelFunc, 0, len(c.running))
	for _, cancel := range c.running {
		cancels = append(cancels, cancel)
	}
	c.runningMu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
}

// run executes one aliyunpan command and returns its combined output.
//
// aliyunpan reports most failures on stdout with a zero exit status ("未登录
// 账号" is the common one), so callers must inspect the text; a non-zero exit
// is treated as an error only because it is unambiguous when it happens.
func (c *CLI) run(ctx context.Context, options runOptions, args ...string) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if options.timeout <= 0 {
		options.timeout = 2 * time.Minute
	}
	if len(args) == 0 {
		return "", errors.New("aliyunpan 命令不能为空")
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
		if errors.Is(runCtx.Err(), context.Canceled) {
			return text, fmt.Errorf("aliyunpan %s 已取消: %w", args[0], context.Canceled)
		}
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
		key, _, _ := strings.Cut(entry, "=")
		if strings.EqualFold(key, "ALIYUNPAN_CONFIG_DIR") {
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
	output, err := c.runCommand(ctx, runOptions{timeout: time.Minute}, "who")
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
	c.CancelLogin()
	c.CancelRunning()
	output, err := c.runCommand(ctx, runOptions{timeout: 20 * time.Second}, "logout", "-y")
	if strings.Contains(output, notLoggedInMarker) || strings.Contains(output, "未设置任何帐号") {
		return nil
	}
	return err
}

// SetDownloadRate applies a speed cap such as "2MB". An empty value explicitly
// writes 0, which is aliyunpan's documented value for unlimited; otherwise a
// limit previously saved in its config would survive after the plugin setting
// was cleared.
func (c *CLI) SetDownloadRate(ctx context.Context, rate string) error {
	rate = strings.TrimSpace(rate)
	if rate == "" {
		rate = "0"
	}
	output, err := c.runCommand(ctx, runOptions{timeout: time.Minute}, "config", "set", "-max_download_rate", rate)
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
func (c *CLI) List(ctx context.Context, cloudPath string, driveID ...string) ([]Entry, error) {
	args := []string{"ll", cloudPath}
	if id := firstString(driveID); id != "" {
		args = append(args, "-driveId", id)
	}
	output, err := c.runCommand(ctx, runOptions{timeout: 3 * time.Minute}, args...)
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
func (c *CLI) Remove(ctx context.Context, cloudPath string, driveID ...string) error {
	args := []string{"rm", cloudPath}
	if id := firstString(driveID); id != "" {
		args = append(args, "-driveId", id)
	}
	output, err := c.runCommand(ctx, runOptions{timeout: 2 * time.Minute}, args...)
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

func firstString(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return strings.TrimSpace(values[0])
}
