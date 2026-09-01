// Package sync owns the queue, the schedule and the transfer pipeline that
// moves a cloud file into tdrive.
package sync

import (
	"crypto/rand"
	"encoding/hex"
	"strconv"
	"time"
)

// State is an item's lifecycle. The names are the ones tdrive's own transfer
// page already uses, so the sync page can reuse its labels verbatim rather
// than inventing a second vocabulary for the same thing.
type State string

const (
	StatePending   State = "pending"
	StateRunning   State = "running"
	StateComplete  State = "complete"
	StateFailed    State = "failed"
	StateCancelled State = "cancelled"
)

// Stage distinguishes the two halves of a running transfer. Only the second
// half spends the Telegram quota.
type Stage string

const (
	StageIdle        Stage = ""
	StageDownloading Stage = "downloading"
	StageUploading   Stage = "uploading"
	StageFinishing   Stage = "finishing"
)

// Item is one file on its way from Aliyun Drive into tdrive.
type Item struct {
	ID        string `json:"id"`
	JobID     string `json:"jobId"`
	JobName   string `json:"jobName"`
	DriveName string `json:"driveName,omitempty"`

	RemotePath string `json:"remotePath"`
	// FileID is populated by the source API scanner. It is optional for queue
	// records written by the old CLI integration, which fall back to the path.
	FileID     string `json:"fileId,omitempty"`
	TargetDir  string `json:"targetDir"`
	Name       string `json:"name"`
	Size       int64  `json:"size"`
	SHA1       string `json:"sha1,omitempty"`

	State State `json:"state"`
	Stage Stage `json:"stage,omitempty"`
	// Downloaded and Uploaded are counted separately because they measure
	// different scarce resources: disk on the way in, the Telegram API on the
	// way out. Only Uploaded is charged against the daily quota.
	Downloaded int64  `json:"downloaded"`
	Uploaded   int64  `json:"uploaded"`
	Error      string `json:"error,omitempty"`
	Attempts   int    `json:"attempts"`

	Overwrite   bool `json:"overwrite,omitempty"`
	DeleteAfter bool `json:"deleteAfter,omitempty"`

	CreatedAt  int64 `json:"createdAt"`
	StartedAt  int64 `json:"startedAt,omitempty"`
	FinishedAt int64 `json:"finishedAt,omitempty"`

	// UploadJobID is tdrive's own resumable upload job. It is kept so a
	// failure can be aborted cleanly instead of leaving orphaned segments in
	// the storage channel.
	UploadJobID string `json:"uploadJobId,omitempty"`

	// speed is the live rate the sync page shows. It is derived, not
	// persisted: a rate measured before a restart describes nothing.
	speed     float64
	speedAt   time.Time
	sampleAt  time.Time
	sampleSum int64
}

// TargetPath is where the file lands in the drive.
func (i *Item) TargetPath() string {
	if i.TargetDir == "/" {
		return "/" + i.Name
	}
	return i.TargetDir + "/" + i.Name
}

// Active reports whether the item still occupies the engine.
func (i *Item) Active() bool {
	return i.State == StatePending || i.State == StateRunning
}

// Finished reports whether the item has reached a terminal state.
func (i *Item) Finished() bool {
	return i.State == StateComplete || i.State == StateFailed || i.State == StateCancelled
}

// key identifies the same cloud file across scans. The content hash is part of
// it so that replacing a cloud file with a different one under the same name
// queues the new version instead of being mistaken for the old.
func (i *Item) key() string {
	// Size is part of the fallback identity when an upstream file has no SHA1;
	// otherwise a changed file can stay pending under the old size forever.
	return i.JobID + "\x00" + i.RemotePath + "\x00" + i.SHA1 + "\x00" + strconv.FormatInt(i.Size, 10)
}

// speedSampleWindow is how long a sample is accumulated before it becomes the
// reported rate. Shorter than this and the number flickers; longer and it lags
// behind what the row's progress bar is doing.
const speedSampleWindow = 1500 * time.Millisecond

// observe folds new bytes into the item's live rate.
func (i *Item) observe(delta int64, now time.Time) {
	if i.sampleAt.IsZero() {
		// The first observation only opens the window. Its bytes have no
		// elapsed time to be divided by, and counting them against a
		// zero-length interval would overstate the first rate the page shows.
		i.sampleAt = now
		return
	}
	i.sampleSum += delta
	elapsed := now.Sub(i.sampleAt)
	if elapsed < speedSampleWindow {
		return
	}
	i.speed = float64(i.sampleSum) / elapsed.Seconds()
	i.speedAt = now
	i.sampleAt = now
	i.sampleSum = 0
}

func (i *Item) resetSpeed() {
	i.speed = 0
	i.speedAt = time.Time{}
	i.sampleAt = time.Time{}
	i.sampleSum = 0
}

// QuotaState is the persisted daily counter.
type QuotaState struct {
	// Day names the quota period, as produced by settings.Quota.Day.
	Day string `json:"day"`
	// UsedBytes counts bytes committed to Telegram in that period.
	UsedBytes int64 `json:"usedBytes"`
}

func newID() string {
	buffer := make([]byte, 8)
	_, _ = rand.Read(buffer)
	return hex.EncodeToString(buffer)
}

func nowMillis() int64 { return time.Now().UnixMilli() }
