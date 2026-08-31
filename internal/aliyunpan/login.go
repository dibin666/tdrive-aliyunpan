package aliyunpan

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"time"
)

// aliyunpan's login is interactive by design: it prints an authorization URL,
// blocks on a "press Enter" prompt while the user completes a scan in their
// browser, and only then exchanges the ticket for tokens. There is no
// non-interactive variant, so the plugin becomes the terminal — it reads the
// URL out of the child's stdout, shows it in the 账号 tab, and writes the
// newline when the administrator says they are done.
//
//	请在浏览器打开以下链接进行登录，链接有效时间为5分钟。
//	注意：你需要进行一次授权一次扫码的两次登录。
//	https://openapi.alipan.com/oauth/authorize?client_id=…
//
//	请在浏览器里面完成扫码登录，然后再按Enter键继续...
var (
	authorizeURLPattern = regexp.MustCompile(`https://openapi\.alipan\.com/oauth/authorize\?\S+`)
	loginSuccessPattern = regexp.MustCompile(`阿里云盘登录成功:\s*(.+)`)
)

// LoginLinkTTL is how long the printed authorization link stays valid. The CLI
// states five minutes; the UI counts down against it.
const LoginLinkTTL = 5 * time.Minute

// LoginPhase is where an in-flight login has got to.
type LoginPhase string

const (
	LoginStarting  LoginPhase = "starting"
	LoginWaiting   LoginPhase = "waiting"
	LoginFinishing LoginPhase = "finishing"
	LoginDone      LoginPhase = "done"
	LoginFailed    LoginPhase = "failed"
)

// LoginState is the snapshot the 账号 tab renders.
type LoginState struct {
	Active    bool       `json:"active"`
	Phase     LoginPhase `json:"phase"`
	URL       string     `json:"url,omitempty"`
	Nickname  string     `json:"nickname,omitempty"`
	Error     string     `json:"error,omitempty"`
	ExpiresAt time.Time  `json:"expiresAt,omitempty"`
}

// LoginSession is one run of `aliyunpan login`.
type LoginSession struct {
	mu       sync.Mutex
	phase    LoginPhase
	url      string
	nickname string
	failure  string
	started  time.Time

	stdin   io.WriteCloser
	command *exec.Cmd
	cancel  context.CancelFunc
	done    chan struct{}
	// confirmOnce keeps a double-clicked "已完成登录" from writing twice into
	// a child that only reads one line.
	confirmOnce sync.Once
}

// StartLogin launches a login and returns once the authorization URL is known.
// A previous unfinished session is discarded, which is what a user pressing
// "重新开始" expects.
func (c *CLI) StartLogin(ctx context.Context) (LoginState, error) {
	c.loginMu.Lock()
	defer c.loginMu.Unlock()

	if c.login != nil {
		c.login.abort()
	}
	if err := os.MkdirAll(c.configDir, 0o750); err != nil {
		return LoginState{}, fmt.Errorf("创建 aliyunpan 配置目录: %w", err)
	}

	// The session outlives the HTTP request that started it, so it gets its
	// own context bounded by the link's own lifetime plus room to exchange the
	// ticket afterwards.
	sessionCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), LoginLinkTTL+2*time.Minute)
	command := exec.CommandContext(sessionCtx, c.binary, "login")
	command.Env = c.environment()

	stdin, err := command.StdinPipe()
	if err != nil {
		cancel()
		return LoginState{}, err
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		cancel()
		return LoginState{}, err
	}
	command.Stderr = command.Stdout
	if err := command.Start(); err != nil {
		cancel()
		return LoginState{}, fmt.Errorf("启动 aliyunpan login: %w", err)
	}

	session := &LoginSession{
		phase:   LoginStarting,
		started: time.Now(),
		stdin:   stdin,
		command: command,
		cancel:  cancel,
		done:    make(chan struct{}),
	}
	urlReady := make(chan struct{})
	go session.consume(stdout, urlReady)
	c.login = session

	select {
	case <-urlReady:
	case <-session.done:
	case <-time.After(90 * time.Second):
		session.abort()
		return LoginState{}, errors.New("aliyunpan 没有在 90 秒内返回授权链接")
	}
	state := session.State()
	if state.URL == "" {
		if state.Error != "" {
			return LoginState{}, errors.New(state.Error)
		}
		return LoginState{}, errors.New("aliyunpan 没有输出授权链接")
	}
	return state, nil
}

// consume reads the child's output, which is the only channel it has for both
// the URL and the outcome.
//
// The read is unbuffered by line on purpose: the "press Enter" prompt is
// written without a trailing newline, so a line scanner would sit on it and
// the URL two lines above would never be reported.
func (s *LoginSession) consume(stdout io.ReadCloser, urlReady chan struct{}) {
	defer close(s.done)
	defer s.cancel()

	var transcript strings.Builder
	announced := false
	buffer := make([]byte, 4<<10)
	for {
		read, err := stdout.Read(buffer)
		if read > 0 {
			transcript.Write(buffer[:read])
			if !announced {
				if url := authorizeURLPattern.FindString(transcript.String()); url != "" {
					s.set(func() {
						s.url = url
						s.phase = LoginWaiting
					})
					announced = true
					close(urlReady)
				}
			}
		}
		if err != nil {
			break
		}
		if transcript.Len() > 256<<10 {
			break
		}
	}
	_ = stdout.Close()
	waitErr := s.command.Wait()
	if !announced {
		close(urlReady)
	}

	text := transcript.String()
	if match := loginSuccessPattern.FindStringSubmatch(text); match != nil {
		s.set(func() {
			s.phase = LoginDone
			s.nickname = strings.TrimSpace(match[1])
		})
		return
	}
	s.set(func() {
		s.phase = LoginFailed
		switch {
		case strings.Contains(text, "登录失败"):
			s.failure = "登录失败，请重新发起并在链接失效前完成扫码"
		case waitErr != nil:
			s.failure = "aliyunpan login 退出: " + waitErr.Error()
		default:
			s.failure = "登录没有完成: " + lastLines(text, 3)
		}
	})
}

// Confirm tells the child the browser step is finished.
func (s *LoginSession) Confirm() error {
	s.mu.Lock()
	phase := s.phase
	s.mu.Unlock()
	if phase != LoginWaiting {
		return fmt.Errorf("当前没有等待确认的登录（状态: %s）", phase)
	}
	var err error
	s.confirmOnce.Do(func() {
		s.set(func() { s.phase = LoginFinishing })
		_, err = io.WriteString(s.stdin, "\n")
	})
	return err
}

// Wait blocks until the session ends or ctx is done.
func (s *LoginSession) Wait(ctx context.Context) {
	select {
	case <-s.done:
	case <-ctx.Done():
	}
}

func (s *LoginSession) abort() {
	s.cancel()
	_ = s.stdin.Close()
	<-s.done
}

func (s *LoginSession) set(mutate func()) {
	s.mu.Lock()
	mutate()
	s.mu.Unlock()
}

// State snapshots the session for the UI.
func (s *LoginSession) State() LoginState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return LoginState{
		Active:    s.phase == LoginStarting || s.phase == LoginWaiting || s.phase == LoginFinishing,
		Phase:     s.phase,
		URL:       s.url,
		Nickname:  s.nickname,
		Error:     s.failure,
		ExpiresAt: s.started.Add(LoginLinkTTL),
	}
}

// LoginState returns the current session's state, or an inactive one.
func (c *CLI) LoginState() LoginState {
	c.loginMu.Lock()
	session := c.login
	c.loginMu.Unlock()
	if session == nil {
		return LoginState{}
	}
	return session.State()
}

// ConfirmLogin completes the browser step of the running session.
func (c *CLI) ConfirmLogin(ctx context.Context) (LoginState, error) {
	c.loginMu.Lock()
	session := c.login
	c.loginMu.Unlock()
	if session == nil {
		return LoginState{}, errors.New("没有正在进行的登录")
	}
	if err := session.Confirm(); err != nil {
		return session.State(), err
	}
	// The token exchange takes a moment; waiting for it here means the UI's
	// next poll already carries the outcome instead of a spinner.
	waitCtx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	session.Wait(waitCtx)
	return session.State(), nil
}

// CancelLogin discards an in-flight session.
func (c *CLI) CancelLogin() {
	c.loginMu.Lock()
	session := c.login
	c.login = nil
	c.loginMu.Unlock()
	if session != nil {
		session.abort()
	}
}
