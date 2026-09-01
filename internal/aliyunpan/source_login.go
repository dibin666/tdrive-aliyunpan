package aliyunpan

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"time"
)

// LoginLinkTTL is how long the upstream token service says the authorization
// link remains valid. The UI uses the same value for its expiry caption.
const LoginLinkTTL = 5 * time.Minute

// LoginPhase is where an in-flight source login has reached.
type LoginPhase string

const (
	LoginStarting  LoginPhase = "starting"
	LoginWaiting   LoginPhase = "waiting"
	LoginFinishing LoginPhase = "finishing"
	LoginDone      LoginPhase = "done"
	LoginFailed    LoginPhase = "failed"
)

// LoginState is the snapshot rendered by the account tab.
type LoginState struct {
	Active    bool       `json:"active"`
	Phase     LoginPhase `json:"phase"`
	URL       string     `json:"url,omitempty"`
	Nickname  string     `json:"nickname,omitempty"`
	Error     string     `json:"error,omitempty"`
	ExpiresAt time.Time  `json:"expiresAt,omitempty"`
}

// LoginSession replaces the old CLI stdin/stdout session. The browser still
// sees the exact same URL and confirmation button, but the token exchange is
// now an HTTP request owned by this client.
type LoginSession struct {
	mu       sync.Mutex
	phase    LoginPhase
	url      string
	nickname string
	failure  string
	ticketID string
	started  time.Time
	cancel   context.CancelFunc
	done     chan struct{}
	doneOnce sync.Once
}

func (c *CLI) StartLogin(ctx context.Context) (LoginState, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	c.loginStartMu.Lock()
	defer c.loginStartMu.Unlock()

	c.loginMu.Lock()
	previous := c.login
	c.login = nil
	c.loginMu.Unlock()
	if previous != nil {
		previous.abort()
	}

	loginURL, ticketID, err := c.createLoginURL(ctx)
	if err != nil {
		return LoginState{}, err
	}
	sessionContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), LoginLinkTTL)
	session := &LoginSession{
		phase:    LoginWaiting,
		url:      loginURL,
		ticketID: ticketID,
		started:  time.Now(),
		cancel:   cancel,
		done:     make(chan struct{}),
	}
	// Keep the session context alive until confirmation or cancellation. The
	// context is not used for the initial request, which must honor the caller's
	// request deadline, but it bounds a forgotten browser session.
	go func() {
		select {
		case <-sessionContext.Done():
			session.setFailure("登录链接已过期")
		case <-session.done:
		}
	}()
	c.loginMu.Lock()
	c.login = session
	c.loginMu.Unlock()
	return session.State(), nil
}

func (c *CLI) createLoginURL(ctx context.Context) (string, string, error) {
	requestContext, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	key := make([]byte, 16)
	if _, err := rand.Read(key); err != nil {
		return "", "", fmt.Errorf("生成登录请求标识: %w", err)
	}
	query := url.Values{
		"ip":      {"127.0.0.1"},
		"os":      {runtime.GOOS},
		"arch":    {runtime.GOARCH},
		"version": {defaultAliyunpanVersion},
		"key":     {hex.EncodeToString(key)},
	}
	data, err := c.tokenServiceData(requestContext, http.MethodGet, "/auth/tickstep/aliyunpan/token/qrcode/create?"+query.Encode(), nil)
	if err != nil {
		return "", "", fmt.Errorf("获取阿里云盘登录链接: %w", err)
	}
	var result map[string]json.RawMessage
	if err := json.Unmarshal(data, &result); err != nil {
		return "", "", fmt.Errorf("解析阿里云盘登录链接: %w", err)
	}
	ticketID := rawString(result, "tokenId", "token_id")
	loginURL := rawString(result, "tokenUrl", "token_url", "url")
	if loginURL != "" {
		parsedURL, parseErr := url.Parse(loginURL)
		if parseErr == nil {
			if parsedTicket := parsedURL.Query().Get("tokenId"); ticketID == "" {
				ticketID = parsedTicket
			}
		}
	}
	if ticketID == "" {
		return "", "", errors.New("登录服务没有返回 token ID")
	}
	if loginURL == "" {
		loginURL = c.buildLoginURL(ticketID)
	}
	return loginURL, ticketID, nil
}

func (c *CLI) buildLoginURL(ticketID string) string {
	redirectURL := strings.TrimRight(c.tokenServiceURL, "/") + "/auth/tickstep/aliyunpan/token/openapi/" + url.PathEscape(ticketID) + "/auth2"
	query := url.Values{
		"client_id":    {c.clientID},
		"redirect_uri": {redirectURL},
		"scope":        {"user:base,file:all:read,file:all:write,file:share:write,album:shared:read"},
	}
	return defaultOpenAPIURL + "/oauth/authorize?" + query.Encode()
}

func (c *CLI) ConfirmLogin(ctx context.Context) (LoginState, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	c.loginMu.Lock()
	session := c.login
	c.loginMu.Unlock()
	if session == nil {
		return LoginState{}, errors.New("没有正在进行的登录")
	}
	if err := session.beginConfirmation(); err != nil {
		return session.State(), err
	}
	requestContext, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	data, err := c.tokenServiceData(requestContext, http.MethodGet, "/auth/tickstep/aliyunpan/token/common/"+url.PathEscape(session.ticketID)+"/login", nil)
	if err != nil {
		session.setFailure("登录失败，请重新发起并在链接失效前完成扫码: " + err.Error())
		return session.State(), err
	}
	openAccess, openExpired, openRefresh, webAccess, webExpired, webRefresh, parseErr := parseLoginTokens(data)
	if parseErr != nil {
		session.setFailure(parseErr.Error())
		return session.State(), parseErr
	}
	c.setCredentials(accountCredentials{
		TicketID:       session.ticketID,
		OpenAPIAccess:  openAccess,
		OpenAPIExpired: openExpired,
		OpenAPIRefresh: openRefresh,
		WebAPIAccess:   webAccess,
		WebAPIExpired:  webExpired,
		WebAPIRefresh:  webRefresh,
	})
	account, err := c.Who(requestContext)
	if err != nil {
		c.setCredentials(accountCredentials{})
		session.setFailure("登录后读取账号信息失败: " + err.Error())
		return session.State(), err
	}
	c.credentialsMu.Lock()
	c.credentials.UserID = account.UserID
	c.credentials.Nickname = account.Nickname
	c.credentialsMu.Unlock()
	if err := c.persistCredentials(); err != nil {
		session.setFailure("保存登录凭证失败: " + err.Error())
		return session.State(), err
	}
	session.setSuccess(account.Nickname)
	return session.State(), nil
}

func parseLoginTokens(data json.RawMessage) (string, int64, string, string, int64, string, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return "", 0, "", "", 0, "", fmt.Errorf("解析登录令牌: %w", err)
	}
	openRaw, openOK := findJSONRaw(fields, "openapi", "openApi", "openapiToken", "open_token")
	webRaw, webOK := findJSONRaw(fields, "webapi", "webApi", "webapiToken", "web_token")
	if !openOK {
		openRaw = data
	}
	openAccess, openExpired, openRefresh := parseToken(openRaw)
	webAccess, webExpired, webRefresh := "", int64(0), ""
	if webOK {
		webAccess, webExpired, webRefresh = parseToken(webRaw)
	}
	if openAccess == "" {
		return "", 0, "", "", 0, "", errors.New("登录服务没有返回 OpenAPI access token")
	}
	return openAccess, openExpired, openRefresh, webAccess, webExpired, webRefresh, nil
}

func (s *LoginSession) beginConfirmation() error {
	s.mu.Lock()
	if s.phase != LoginWaiting {
		s.mu.Unlock()
		return fmt.Errorf("当前没有等待确认的登录（状态: %s）", s.phase)
	}
	if !s.started.IsZero() && time.Now().After(s.started.Add(LoginLinkTTL)) {
		s.phase = LoginFailed
		s.failure = "登录链接已过期"
		s.mu.Unlock()
		s.close()
		return errors.New("登录链接已过期")
	}
	s.phase = LoginFinishing
	s.mu.Unlock()
	return nil
}

func (s *LoginSession) setSuccess(nickname string) {
	s.mu.Lock()
	s.phase = LoginDone
	s.nickname = nickname
	s.failure = ""
	s.mu.Unlock()
	s.close()
}

func (s *LoginSession) setFailure(message string) {
	s.mu.Lock()
	if s.phase != LoginDone {
		s.phase = LoginFailed
		s.failure = message
	}
	s.mu.Unlock()
	s.close()
}

func (s *LoginSession) close() {
	s.doneOnce.Do(func() {
		if s.cancel != nil {
			s.cancel()
		}
		close(s.done)
	})
}

func (s *LoginSession) abort() {
	s.setFailure("登录已取消")
}

func (s *LoginSession) Wait(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-s.done:
	case <-ctx.Done():
	}
}

func (s *LoginSession) State() LoginState {
	s.mu.Lock()
	defer s.mu.Unlock()
	active := s.phase == LoginStarting || s.phase == LoginWaiting || s.phase == LoginFinishing
	return LoginState{
		Active:    active,
		Phase:     s.phase,
		URL:       s.url,
		Nickname:  s.nickname,
		Error:     s.failure,
		ExpiresAt: s.started.Add(LoginLinkTTL),
	}
}

func (c *CLI) LoginState() LoginState {
	c.loginMu.Lock()
	session := c.login
	c.loginMu.Unlock()
	if session == nil {
		return LoginState{}
	}
	return session.State()
}

func (c *CLI) CancelLogin() {
	c.loginMu.Lock()
	session := c.login
	c.login = nil
	c.loginMu.Unlock()
	if session != nil {
		session.abort()
	}
}

// Logout clears the active account from the same config document used by the
// old CLI. It deliberately does not touch plugin settings, queue, or quota.
func (c *CLI) Logout(ctx context.Context) error {
	_ = ctx
	c.CancelLogin()
	return c.clearCredentials()
}
