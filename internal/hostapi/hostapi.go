// Package hostapi wraps the generic tdrive Host bridge in typed calls.
//
// The SDK deliberately keeps Host.Call untyped so new host methods do not
// force plugins to rebuild. That is the right trade for the SDK and the wrong
// one for calling code, which would otherwise repeat anonymous request structs
// at every call site.
package hostapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	tdriveplugin "github.com/dibin/tdrive/pkg/plugin"
)

// Client is the typed view of the host.
type Client struct {
	host tdriveplugin.Host
}

func New(host tdriveplugin.Host) *Client { return &Client{host: host} }

// RuntimeSettings mirrors config.RuntimeSettings.
//
// That struct carries no JSON tags, so the host serializes it with Go field
// names — SegmentSize, not segmentSize. The WebUI's /api/settings uses a
// different, lowerCamel DTO; mixing them up yields a struct of zeroes, which
// is why the field names here are copied verbatim.
type RuntimeSettings struct {
	AppID               int
	LocalRoot           string
	SegmentSize         int64
	PoolSize            int64
	UploadThreads       int
	UploadPartSize      int64
	RateLimit           time.Duration
	StreamConcurrency   int
	UploadConcurrency   int
	DownloadConcurrency int
	WebDAVEnabled       bool
	CacheDir            string
	CacheLimit          int64
	MaxDownloadConns    int
}

// Settings reads tdrive's current transfer limits. The sync engine re-reads
// them before every file so a change made in 性能参数 takes effect without
// restarting the plugin.
func (c *Client) Settings(ctx context.Context) (RuntimeSettings, error) {
	var settings RuntimeSettings
	err := c.host.Call(ctx, "settings.get", struct{}{}, &settings)
	return settings, err
}

// Users lists the accounts the host is willing to tell this plugin about.
//
// Since tdrive made plugins per-account, that is exactly one account: the one
// that installed this plugin. Older hosts, where a plugin was a deployment-wide
// component, return every account instead — see OwnedByCaller.
func (c *Client) Users(ctx context.Context) ([]tdriveplugin.User, error) {
	var users []tdriveplugin.User
	err := c.host.Call(ctx, "users.list", struct{}{}, &users)
	return users, err
}

// OwnedByCaller reports whether the host scopes this plugin to one account.
//
// A host that owns plugins per account answers users.list with the single
// owning account; one from before that change answers with the whole user
// table. The count is therefore the version signal, and it needs no version
// string — which matters because minTdriveVersion is documentation that
// nothing enforces.
//
// A single-account deployment on an old host looks identical, and that is
// harmless: the only account there is the administrator, so both branches
// reach the same verdict.
func OwnedByCaller(users []tdriveplugin.User) bool { return len(users) == 1 }

// List reads one drive directory.
func (c *Client) List(ctx context.Context, path string) ([]tdriveplugin.Entry, error) {
	var entries []tdriveplugin.Entry
	err := c.host.Call(ctx, "files.list", map[string]string{"path": path}, &entries)
	return entries, err
}

// Stat resolves one drive path.
func (c *Client) Stat(ctx context.Context, path string) (tdriveplugin.Entry, error) {
	var entry tdriveplugin.Entry
	err := c.host.Call(ctx, "files.stat", map[string]string{"path": path}, &entry)
	return entry, err
}

// Mkdir creates a directory and its parents. The host's implementation is
// idempotent, so callers do not check for existence first.
func (c *Client) Mkdir(ctx context.Context, path string) error {
	return c.host.Call(ctx, "files.mkdir", map[string]string{"path": path}, nil)
}

// BeginUpload opens a resumable segmented upload. The returned job carries the
// segment geometry the caller must reproduce exactly.
func (c *Client) BeginUpload(ctx context.Context, request tdriveplugin.UploadRequest) (tdriveplugin.UploadJob, tdriveplugin.File, error) {
	var response struct {
		Job  tdriveplugin.UploadJob `json:"job"`
		File tdriveplugin.File      `json:"file"`
	}
	if err := c.host.Call(ctx, "files.beginUpload", request, &response); err != nil {
		return tdriveplugin.UploadJob{}, tdriveplugin.File{}, err
	}
	return response.Job, response.File, nil
}

// CompleteUpload commits an upload whose segments have all landed.
func (c *Client) CompleteUpload(ctx context.Context, jobID string) (tdriveplugin.File, error) {
	var file tdriveplugin.File
	err := c.host.Call(ctx, "files.completeUpload", map[string]string{"jobId": jobID}, &file)
	return file, err
}

// AbortUpload tears down an upload, removing whatever segments did land. The
// host only accepts "failed" and "cancelled" as the terminal state.
func (c *Client) AbortUpload(ctx context.Context, jobID, reason, state string) error {
	return c.host.Call(ctx, "files.abortUpload", map[string]string{
		"jobId": jobID, "reason": reason, "state": state,
	}, nil)
}

// Delete removes a drive path.
func (c *Client) Delete(ctx context.Context, path string) error {
	return c.host.Call(ctx, "files.delete", map[string]string{"path": path}, nil)
}

// GetData reads a plugin-namespaced value. It reports false when the key has
// never been written, which callers use to fall back to defaults.
//
// An unwritten key comes back as a not-found error rather than as a null, so
// the very first start of a plugin would otherwise look like a failure.
func (c *Client) GetData(ctx context.Context, key string, target any) (bool, error) {
	var raw json.RawMessage
	if err := c.host.Call(ctx, "data.get", map[string]string{"key": key}, &raw); err != nil {
		if IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	if len(raw) == 0 || string(raw) == "null" {
		return false, nil
	}
	if err := json.Unmarshal(raw, target); err != nil {
		return false, fmt.Errorf("decode plugin data %q: %w", key, err)
	}
	return true, nil
}

// SetData writes a plugin-namespaced value.
func (c *Client) SetData(ctx context.Context, key string, value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode plugin data %q: %w", key, err)
	}
	return c.host.Call(ctx, "data.set", map[string]any{"key": key, "value": json.RawMessage(encoded)}, nil)
}

const (
	// segmentChunk is how much is handed to the brokered connection at once.
	// It is small enough that the write deadline below is refreshed often even
	// on a slow link, and large enough not to add measurable syscall overhead.
	segmentChunk = 256 << 10
	// chunkTimeout is how long one chunk may take before the segment is
	// declared stuck. It is generous because tdrive's own rate limiter can
	// legitimately pace a chunk, and the failure it guards against is a hang,
	// not slowness.
	chunkTimeout = 5 * time.Minute
)

// PutSegment streams exactly one segment to Telegram.
//
// Two host-side details shape this. The host validates the declared size
// against its own segment geometry and rejects a mismatch, so size must be
// computed the same way the drive computes it. And when the host's segment
// upload fails early it stops draining its half of the pipe without closing
// it, so a plugin writing without a deadline blocks forever; the rolling
// deadline turns that hang into an error. Its byte counter is also the
// progress the sync page displays.
func (c *Client) PutSegment(
	ctx context.Context,
	jobID string,
	index int,
	size int64,
	reader io.Reader,
	progress func(delta int64),
) error {
	stream, err := c.host.OpenStream(ctx, "files.putSegment", map[string]any{
		"jobId": jobID, "index": index, "size": size,
	})
	if err != nil {
		return fmt.Errorf("open segment %d: %w", index, err)
	}
	// The brokered connection is a yamux stream, so it carries deadlines. The
	// type assertion is optional rather than required: a future transport
	// without deadlines still works, just without the hang guard.
	deadliner, _ := stream.(net.Conn)

	closed := false
	closeStream := func() error {
		if closed {
			return nil
		}
		closed = true
		return stream.Close()
	}
	defer func() { _ = closeStream() }()

	buffer := make([]byte, segmentChunk)
	var written int64
	for written < size {
		want := int64(len(buffer))
		if remaining := size - written; remaining < want {
			want = remaining
		}
		read, readErr := io.ReadFull(reader, buffer[:want])
		if read > 0 {
			if deadliner != nil {
				_ = deadliner.SetWriteDeadline(time.Now().Add(chunkTimeout))
			}
			if _, writeErr := stream.Write(buffer[:read]); writeErr != nil {
				return fmt.Errorf("write segment %d at %d/%d bytes: %w", index, written, size, writeErr)
			}
			written += int64(read)
			if progress != nil {
				progress(int64(read))
			}
		}
		if readErr != nil {
			return fmt.Errorf("read segment %d source at %d/%d bytes: %w", index, written, size, readErr)
		}
	}
	if deadliner != nil {
		_ = deadliner.SetWriteDeadline(time.Now().Add(chunkTimeout))
	}
	// Closing sends EOF to the drive, which commits the segment. The commit's
	// own error is not reported back over the bridge — the host closes this
	// connection before it waits for it — so success is confirmed later by
	// CompleteUpload rather than here.
	return closeStream()
}

// IsNotFound reports whether a host error means "there is nothing there".
//
// The host flattens its typed errors into strings across the RPC boundary, so
// there is nothing to match on but the message — and the two layers that
// produce it word it differently: the drive says "drive: no such file or
// directory" and the database says "database: not found".
func IsNotFound(err error) bool {
	if err == nil {
		return false
	}
	message := err.Error()
	return strings.Contains(message, "not found") ||
		strings.Contains(message, "no such file or directory")
}
