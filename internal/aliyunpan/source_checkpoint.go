package aliyunpan

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// A download is cut into fixed-size chunks rather than one range per
// connection. The old layout gave each connection a contiguous third of the
// file, so a 10 GiB file meant three ~3.4 GiB single HTTP responses: one
// dropped connection threw away gigabytes, and there was nothing on disk
// saying which bytes had survived. A chunk is small enough that losing one
// costs seconds and large enough that the per-request overhead disappears
// against the transfer itself.
const downloadChunkSize = 32 << 20

// chunkCheckpointVersion is bumped when the sidecar's meaning changes. A
// document from a future or unknown version is discarded rather than
// misinterpreted, which restarts the download instead of writing chunks into
// the wrong offsets.
const chunkCheckpointVersion = 1

// chunkCheckpoint records which chunks of a partially downloaded file are
// already on disk.
//
// It lives beside the .part file and is the only thing that makes a download
// survive a retry, a cancellation or a plugin restart. The identity fields are
// as important as the bitmap: resuming into a .part file that was written for a
// different revision of the cloud file would silently produce a corrupt result,
// so every one of them has to match before a single chunk is skipped.
type chunkCheckpoint struct {
	Version   int    `json:"version"`
	DriveID   string `json:"driveId"`
	FileID    string `json:"fileId"`
	Size      int64  `json:"size"`
	Hash      string `json:"contentHash,omitempty"`
	ChunkSize int64  `json:"chunkSize"`
	// Done is a base64 bitmap, one bit per chunk, least significant bit first.
	// A list of indices would be simpler to read but is rewritten after every
	// chunk; the bitmap keeps that write at a few hundred bytes even for a
	// hundred-gigabyte file.
	Done string `json:"done"`
}

// checkpointFile tracks completed chunks and persists them.
//
// Writes are serialized and atomic: the sidecar is rewritten through a
// temporary file and renamed, so a process killed mid-write leaves either the
// previous checkpoint or the new one, never a truncated document that would
// have to be thrown away along with the bytes it describes.
type checkpointFile struct {
	path  string
	mu    sync.Mutex
	done  []bool
	meta  chunkCheckpoint
	dirty bool
}

func checkpointPath(partial string) string {
	return partial + downloadProgressSuffix
}

// chunkCount is how many chunks a file of this size is cut into.
func chunkCount(size, chunkSize int64) int {
	if size <= 0 || chunkSize <= 0 {
		return 0
	}
	return int((size + chunkSize - 1) / chunkSize)
}

// chunkBounds returns the half-open byte range [begin, end) of one chunk.
func chunkBounds(index int, size, chunkSize int64) (int64, int64) {
	begin := int64(index) * chunkSize
	end := begin + chunkSize
	if end > size {
		end = size
	}
	return begin, end
}

// newCheckpoint starts a fresh record with nothing completed.
func newCheckpoint(partial string, meta chunkCheckpoint) *checkpointFile {
	meta.Version = chunkCheckpointVersion
	return &checkpointFile{
		path: checkpointPath(partial),
		done: make([]bool, chunkCount(meta.Size, meta.ChunkSize)),
		meta: meta,
	}
}

// loadCheckpoint reads a sidecar and reports whether it describes the same file
// the caller is about to download. Anything unreadable, stale or mismatched
// returns false, which the caller treats as "start over" rather than as an
// error: a broken checkpoint costs a re-download, while trusting one costs a
// corrupt file.
func loadCheckpoint(partial string, want chunkCheckpoint) (*checkpointFile, bool) {
	raw, err := os.ReadFile(checkpointPath(partial))
	if err != nil {
		return nil, false
	}
	var stored chunkCheckpoint
	if err := json.Unmarshal(raw, &stored); err != nil {
		return nil, false
	}
	if stored.Version != chunkCheckpointVersion ||
		stored.DriveID != want.DriveID ||
		stored.FileID != want.FileID ||
		stored.Size != want.Size ||
		stored.ChunkSize != want.ChunkSize {
		return nil, false
	}
	// An empty stored hash comes from a drive that did not report one. Only a
	// hash that is present on both sides and disagrees rules the file out.
	if stored.Hash != "" && want.Hash != "" && stored.Hash != want.Hash {
		return nil, false
	}
	total := chunkCount(want.Size, want.ChunkSize)
	done, ok := decodeChunkBitmap(stored.Done, total)
	if !ok {
		return nil, false
	}
	stored.Hash = want.Hash
	return &checkpointFile{path: checkpointPath(partial), done: done, meta: stored}, true
}

// checkpointBytesForSize reports how many bytes a sidecar claims are on disk
// for a file of this size, without knowing which cloud file it belongs to.
//
// It validates only the geometry, because its caller is the queue view rather
// than the downloader: showing a number that turns out to be slightly stale is
// harmless, whereas the identity checks in loadCheckpoint exist to stop bytes
// being written into the wrong file and are the downloader's business.
func checkpointBytesForSize(partial string, size int64) int64 {
	raw, err := os.ReadFile(checkpointPath(partial))
	if err != nil {
		return 0
	}
	var stored chunkCheckpoint
	if err := json.Unmarshal(raw, &stored); err != nil {
		return 0
	}
	if stored.Version != chunkCheckpointVersion || stored.Size != size || stored.ChunkSize <= 0 {
		return 0
	}
	done, ok := decodeChunkBitmap(stored.Done, chunkCount(size, stored.ChunkSize))
	if !ok {
		return 0
	}
	var total int64
	for index, complete := range done {
		if !complete {
			continue
		}
		begin, end := chunkBounds(index, size, stored.ChunkSize)
		total += end - begin
	}
	return total
}

// completed reports how many chunks have landed and how many bytes that is.
func (c *checkpointFile) completed() (int, int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	var total int64
	for index, done := range c.done {
		if !done {
			continue
		}
		count++
		begin, end := chunkBounds(index, c.meta.Size, c.meta.ChunkSize)
		total += end - begin
	}
	return count, total
}

// pending lists the chunks still to fetch, in file order so a resumed transfer
// keeps filling the file from the front.
func (c *checkpointFile) pending() []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	missing := make([]int, 0, len(c.done))
	for index, done := range c.done {
		if !done {
			missing = append(missing, index)
		}
	}
	return missing
}

// markDone records one finished chunk and persists the record.
//
// A failure to persist is deliberately not fatal. The bytes are already on
// disk and the transfer can still finish; all that is lost is the ability to
// resume, which is strictly better than aborting a download that is working.
func (c *checkpointFile) markDone(index int) error {
	c.mu.Lock()
	if index < 0 || index >= len(c.done) {
		c.mu.Unlock()
		return fmt.Errorf("分片序号 %d 超出范围", index)
	}
	c.done[index] = true
	document := c.meta
	document.Done = encodeChunkBitmap(c.done)
	c.mu.Unlock()
	return writeCheckpointDocument(c.path, document)
}

// save writes the current record even when no chunk has completed, so a
// resumable download is recoverable from the moment it starts.
func (c *checkpointFile) save() error {
	c.mu.Lock()
	document := c.meta
	document.Done = encodeChunkBitmap(c.done)
	c.mu.Unlock()
	return writeCheckpointDocument(c.path, document)
}

func (c *checkpointFile) remove() {
	_ = os.Remove(c.path)
}

func writeCheckpointDocument(path string, document chunkCheckpoint) error {
	encoded, err := json.Marshal(document)
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".checkpoint-*")
	if err != nil {
		return err
	}
	name := temporary.Name()
	if _, err := temporary.Write(encoded); err != nil {
		temporary.Close()
		_ = os.Remove(name)
		return err
	}
	if err := temporary.Close(); err != nil {
		_ = os.Remove(name)
		return err
	}
	if err := os.Rename(name, path); err != nil {
		_ = os.Remove(name)
		return err
	}
	return nil
}

func encodeChunkBitmap(done []bool) string {
	if len(done) == 0 {
		return ""
	}
	bytes := make([]byte, (len(done)+7)/8)
	for index, value := range done {
		if value {
			bytes[index/8] |= 1 << (index % 8)
		}
	}
	return base64.StdEncoding.EncodeToString(bytes)
}

// decodeChunkBitmap expands a stored bitmap to exactly total entries. A bitmap
// that is the wrong length for the file it claims to describe is rejected: it
// belongs to a different geometry, and reading it would mark the wrong chunks
// as already downloaded.
func decodeChunkBitmap(encoded string, total int) ([]bool, bool) {
	if total == 0 {
		return nil, encoded == ""
	}
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, false
	}
	if len(raw) != (total+7)/8 {
		return nil, false
	}
	done := make([]bool, total)
	for index := range done {
		done[index] = raw[index/8]&(1<<(index%8)) != 0
	}
	return done, true
}

// errChunkOutOfRange guards the worker loop against a chunk index that no
// longer matches the file, which can only happen if a checkpoint slipped past
// the identity checks.
var errChunkOutOfRange = errors.New("下载分片序号超出文件范围")
