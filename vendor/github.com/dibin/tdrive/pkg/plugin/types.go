package plugin

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"
)

type userIDContextKey struct{}
type hostCallContextKey struct{}

// WithUserID lets host services attach the authenticated account to an
// operation without exposing the host's private auth context type.
func WithUserID(ctx context.Context, userID string) context.Context {
	return context.WithValue(ctx, userIDContextKey{}, userID)
}

// UserIDFromContext returns the account associated with a host operation.
func UserIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	userID, _ := ctx.Value(userIDContextKey{}).(string)
	return userID
}

// WithHostCall marks a host operation initiated by a plugin. The core skips
// re-entering operation hooks for that call, preventing a plugin's own Host
// API usage from recursively invoking its hooks.
func WithHostCall(ctx context.Context) context.Context {
	return context.WithValue(ctx, hostCallContextKey{}, true)
}

// IsHostCall reports whether a context came from the reverse host bridge.
func IsHostCall(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	bypass, _ := ctx.Value(hostCallContextKey{}).(bool)
	return bypass
}

// deadlineUnixMilli and contextWithDeadline are the small wire-level bridge
// used by both directions of the RPC connection. Context values themselves
// cannot cross net/rpc, but a deadline can, and preserving it prevents a host
// request from being held open by a nested plugin/host call after its caller
// has already timed out.
func deadlineUnixMilli(ctx context.Context) int64 {
	if ctx == nil {
		return 0
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return 0
	}
	return deadline.UnixMilli()
}

func contextWithDeadline(deadline int64) (context.Context, context.CancelFunc) {
	if deadline <= 0 {
		return context.Background(), func() {}
	}
	return context.WithDeadline(context.Background(), time.UnixMilli(deadline))
}

// Operation is the generic interception envelope. Keeping the payload as
// JSON makes the API forward-compatible: a new host operation does not force
// every existing plugin to rebuild before it can load.
type Operation struct {
	Name    string          `json:"name"`
	UserID  string          `json:"userId,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
	// DeadlineUnixMilli is transport metadata, not part of the operation JSON.
	// It lets a hook's reverse Host call stop with the original request.
	DeadlineUnixMilli int64 `json:"-"`
}

// OperationResult lets a plugin reject an operation or replace its payload.
// Allowed defaults to true when a plugin does not implement BeforeHook.
type OperationResult struct {
	Allowed bool            `json:"allowed"`
	Payload json.RawMessage `json:"payload,omitempty"`
	Error   string          `json:"error,omitempty"`
}

// Event is delivered to plugins that list the event type in their manifest.
type Event struct {
	Type   string          `json:"type"`
	Data   json.RawMessage `json:"data"`
	At     time.Time       `json:"at"`
	UserID string          `json:"userId,omitempty"`
	// DeadlineUnixMilli is transport metadata, not event payload.
	DeadlineUnixMilli int64 `json:"-"`
}

// HTTPRequest is the serializable subset of an HTTP request exposed to a
// plugin route. Body sizes are bounded by the host route handler; large file
// operations should use Host.OpenStream instead.
type HTTPRequest struct {
	Method     string              `json:"method"`
	Path       string              `json:"path"`
	RawQuery   string              `json:"rawQuery,omitempty"`
	Headers    map[string][]string `json:"headers,omitempty"`
	Body       []byte              `json:"body,omitempty"`
	UserID     string              `json:"userId,omitempty"`
	RemoteAddr string              `json:"remoteAddr,omitempty"`
	// DeadlineUnixMilli carries the host HTTP request deadline across the RPC
	// boundary. Without it a plugin can keep a reverse Host call alive after the
	// host has already timed out the browser request, which leaves the plugin
	// process looking dead to the host.
	DeadlineUnixMilli int64 `json:"deadlineUnixMilli,omitempty"`
}

// HTTPResponse is returned by a plugin route.
type HTTPResponse struct {
	Status  int                 `json:"status"`
	Headers map[string][]string `json:"headers,omitempty"`
	Body    []byte              `json:"body,omitempty"`
}

// Host is the complete public host bridge. Call dispatches a named operation
// using JSON request/response values; the SDK keeps this generic so plugins
// can use newly added host APIs without importing private tdrive packages.
type Host interface {
	Call(ctx context.Context, method string, request any, response any) error
	OpenStream(ctx context.Context, method string, request any) (io.ReadWriteCloser, error)
}

// Call is a convenience helper for plugins that prefer a free function over a
// method on Host. It is also useful when response is a json.RawMessage.
func Call(ctx context.Context, host Host, method string, request any, response any) error {
	return host.Call(ctx, method, request, response)
}

// BeforeHook can rewrite or reject a host operation.
type BeforeHook interface {
	Before(ctx context.Context, operation Operation) (OperationResult, error)
}

// AfterHook observes a completed host operation. Errors are logged by the
// host and do not undo an operation that has already committed.
type AfterHook interface {
	After(ctx context.Context, operation Operation)
}

// Hooks is the host-side contract implemented by the plugin manager. The
// drive keeps a nil Hooks field until at least one plugin is active, preserving
// the original no-plugin fast path.
type Hooks interface {
	Before(ctx context.Context, operation Operation) (OperationResult, error)
	After(ctx context.Context, operation Operation)
}

// EventHook receives events declared by the plugin manifest.
type EventHook interface {
	OnEvent(ctx context.Context, event Event)
}

// HTTPHook handles plugin-owned HTTP routes.
type HTTPHook interface {
	HandleHTTP(ctx context.Context, request HTTPRequest) (HTTPResponse, error)
}

// Plugin is the only required interface for a plugin binary. Optional hooks
// are discovered at runtime through BeforeHook, AfterHook, EventHook, and
// HTTPHook, so a minimal plugin remains small.
type Plugin interface {
	Manifest() Manifest
	Initialize(ctx context.Context, host Host) error
}

// ShutdownHook is optional. A plugin that does not implement it is still
// terminated by the host process manager after the RPC connection closes.
type ShutdownHook interface {
	Shutdown(ctx context.Context) error
}

// Entry is the public file-tree representation used by host calls.
type Entry struct {
	Name         string    `json:"name"`
	Path         string    `json:"path"`
	IsDir        bool      `json:"isDir"`
	Size         int64     `json:"size"`
	MIME         string    `json:"mime,omitempty"`
	ID           string    `json:"id"`
	SegmentCount int       `json:"segmentCount,omitempty"`
	Status       string    `json:"status,omitempty"`
	ModifiedAt   time.Time `json:"modifiedAt"`
	CreatedAt    time.Time `json:"createdAt"`
}

// File is the logical file returned by upload host calls.
type File struct {
	ID           string    `json:"id"`
	DirID        string    `json:"dirId,omitempty"`
	Name         string    `json:"name"`
	Size         int64     `json:"size"`
	MIME         string    `json:"mime"`
	SegmentCount int       `json:"segmentCount"`
	Status       string    `json:"status"`
	CreatedAt    time.Time `json:"createdAt"`
	UpdatedAt    time.Time `json:"updatedAt"`
	OwnerID      string    `json:"ownerId,omitempty"`
}

// UploadRequest is accepted by the host upload API.
type UploadRequest struct {
	DirPath   string `json:"dirPath"`
	Name      string `json:"name"`
	Size      int64  `json:"size"`
	MIME      string `json:"mime,omitempty"`
	UserID    string `json:"userId,omitempty"`
	Source    string `json:"source,omitempty"`
	SourceURL string `json:"sourceUrl,omitempty"`
	Overwrite bool   `json:"overwrite,omitempty"`
}

// UploadJob is the public resumable upload state.
type UploadJob struct {
	ID            string `json:"id"`
	FileID        string `json:"fileId,omitempty"`
	Name          string `json:"name"`
	TotalSize     int64  `json:"totalSize"`
	SegmentSize   int64  `json:"segmentSize"`
	SegmentCount  int    `json:"segmentCount"`
	UploadedBytes int64  `json:"uploadedBytes"`
	Status        string `json:"status"`
	Error         string `json:"error,omitempty"`
}

// DownloadJob is the public staged-download state.
type DownloadJob struct {
	ID              string    `json:"id"`
	FileID          string    `json:"fileId,omitempty"`
	Name            string    `json:"name"`
	TotalSize       int64     `json:"totalSize"`
	DownloadedBytes int64     `json:"downloadedBytes"`
	Mode            string    `json:"mode"`
	Status          string    `json:"status"`
	Error           string    `json:"error,omitempty"`
	CreatedAt       time.Time `json:"createdAt"`
	UpdatedAt       time.Time `json:"updatedAt"`
}

// User is the non-secret public user representation.
type User struct {
	ID       string `json:"id"`
	Username string `json:"username"`
	Role     string `json:"role"`
	Enabled  bool   `json:"enabled"`
}

// RuntimeSettings is intentionally open-ended. Plugins should prefer the
// named settings host call and only depend on keys they actually use.
type RuntimeSettings map[string]any

// NewRequest converts the SDK request type into a standard request for plugin
// code that wants familiar HTTP helpers.
func (r HTTPRequest) NewRequest() *http.Request {
	request, _ := http.NewRequest(r.Method, r.Path, nil)
	request.URL.RawQuery = r.RawQuery
	request.Header = make(http.Header, len(r.Headers))
	for key, values := range r.Headers {
		for _, value := range values {
			request.Header.Add(key, value)
		}
	}
	request.Body = io.NopCloser(bytesReader(r.Body))
	return request
}

// bytesReader avoids exporting an implementation detail just for the helper
// above while retaining a zero-allocation empty body.
func bytesReader(data []byte) io.Reader {
	if len(data) == 0 {
		return emptyReader{}
	}
	return &sliceReader{data: data}
}

type emptyReader struct{}

func (emptyReader) Read([]byte) (int, error) { return 0, io.EOF }

type sliceReader struct {
	data []byte
	read int
}

func (reader *sliceReader) Read(buffer []byte) (int, error) {
	if reader.read >= len(reader.data) {
		return 0, io.EOF
	}
	count := copy(buffer, reader.data[reader.read:])
	reader.read += count
	return count, nil
}
