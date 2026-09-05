package sync

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/dibin/tdrive-aliyunpan/internal/aliyunpan"
)

var safeIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// StagedFile is one queue item's footprint in the staging area.
//
// A failed transfer keeps whatever it downloaded, so the staging area is no
// longer a place that empties itself. This is what the 下载文件 page shows so an
// operator can see what is holding the disk and decide what to do about it.
type StagedFile struct {
	ItemID     string `json:"itemId"`
	Name       string `json:"name"`
	RemotePath string `json:"remotePath"`
	TargetPath string `json:"targetPath"`
	JobName    string `json:"jobName"`
	// State and Stage are empty for an orphan, which by definition has no queue
	// row left to describe it.
	State State `json:"state,omitempty"`
	Stage Stage `json:"stage,omitempty"`
	Size  int64 `json:"size"`
	// Downloaded is how much of the file the checkpoint says is really there.
	Downloaded int64 `json:"downloaded"`
	// DiskBytes is what the directory actually occupies. It differs from
	// Downloaded on purpose: a .part file is pre-allocated at the final size
	// from the first moment, so a 10 GiB file holds 10 GiB of disk while only
	// 2 GiB of it has arrived. Showing one number without the other makes
	// whichever one is missing look like a bug.
	DiskBytes int64 `json:"diskBytes"`
	// Complete marks a download that finished and an upload that did not, which
	// is the case where deleting the local file is the expensive mistake.
	Complete   bool   `json:"complete"`
	Orphan     bool   `json:"orphan"`
	Path       string `json:"path"`
	ModifiedAt int64  `json:"modifiedAt"`
	Error      string `json:"error,omitempty"`
	Attempts   int    `json:"attempts"`
	Running    bool   `json:"running"`
}

// DownloadsView is everything the 下载文件 page renders.
type DownloadsView struct {
	Files      []StagedFile `json:"files"`
	StageDir   string       `json:"stageDir"`
	UsedBytes  int64        `json:"usedBytes"`
	LimitBytes int64        `json:"limitBytes"`
	// OrphanCount and OrphanBytes drive the "清理无主文件" button, which is
	// otherwise offering to delete something the operator cannot see.
	OrphanCount int   `json:"orphanCount"`
	OrphanBytes int64 `json:"orphanBytes"`
}

// Downloads reports what is in the staging area, joined to the queue.
//
// The staging directory rather than the queue is the source of truth here: the
// queue can forget a row while its bytes remain, and those are exactly the
// files this page exists to surface.
func (e *Engine) Downloads(ctx context.Context) DownloadsView {
	stageDir := e.StageDir()
	view := DownloadsView{StageDir: stageDir, Files: []StagedFile{}}
	if limits, err := e.host.Settings(ctx); err == nil {
		view.LimitBytes = e.stageLimit(limits)
	}

	e.mu.Lock()
	items := make(map[string]*Item, len(e.queue))
	for _, item := range e.queue {
		items[item.ID] = item
	}
	e.mu.Unlock()

	itemsRoot := filepath.Join(stageDir, stageItemsDir)
	entries, err := os.ReadDir(itemsRoot)
	if err != nil {
		return view
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		itemID := entry.Name()
		directory := filepath.Join(itemsRoot, itemID)
		file := StagedFile{ItemID: itemID, Path: directory}
		file.DiskBytes, file.ModifiedAt = measureStageDir(directory)

		if item, known := items[itemID]; known {
			e.mu.Lock()
			file.Name = item.Name
			file.RemotePath = item.RemotePath
			file.TargetPath = item.TargetPath()
			file.JobName = item.JobName
			file.State = item.State
			file.Stage = item.Stage
			file.Size = item.Size
			file.Error = item.Error
			file.Attempts = item.Attempts
			file.Running = item.State == StateRunning
			remotePath := item.RemotePath
			size := item.Size
			e.mu.Unlock()
			file.Downloaded = aliyunpan.StagedDownloadBytes(directory, remotePath, size)
			file.Complete = size >= 0 && file.Downloaded == size
			file.Path = aliyunpan.StagedFilePath(directory, remotePath)
		} else {
			file.Orphan = true
			view.OrphanCount++
			view.OrphanBytes += file.DiskBytes
		}
		view.UsedBytes += file.DiskBytes
		view.Files = append(view.Files, file)
	}

	// Biggest first: the reason to open this page is that something is filling
	// the disk, and the file responsible should not be somewhere in the middle.
	sort.Slice(view.Files, func(a, b int) bool {
		if view.Files[a].DiskBytes != view.Files[b].DiskBytes {
			return view.Files[a].DiskBytes > view.Files[b].DiskBytes
		}
		return view.Files[a].ItemID < view.Files[b].ItemID
	})
	return view
}

// measureStageDir sums one item's directory and reports when it last changed.
func measureStageDir(directory string) (int64, int64) {
	var total int64
	var newest int64
	_ = filepath.WalkDir(directory, func(_ string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return nil //nolint:nilerr // an unreadable staging tree is reported by the transfer itself
		}
		info, statErr := entry.Info()
		if statErr != nil {
			return nil
		}
		total += info.Size()
		if modified := info.ModTime().UnixMilli(); modified > newest {
			newest = modified
		}
		return nil
	})
	return total, newest
}

// DeleteStaged throws away the local copy of the named queue items, keeping
// their rows.
//
// It refuses a transfer that is running rather than deleting the file out from
// under it. The row stays because it is what the operator retries; only the
// bytes go, and the next attempt downloads them again from scratch.
func (e *Engine) DeleteStaged(ctx context.Context, ids ...string) (int, error) {
	if len(ids) == 0 {
		return 0, errors.New("未指定要删除的文件，请选择文件")
	}
	wanted := make(map[string]bool, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" || !safeIDPattern.MatchString(id) {
			return 0, fmt.Errorf("文件 ID %q 不合法，仅支持字母、数字、下划线和短横线", id)
		}
		wanted[id] = true
	}

	e.mu.Lock()
	if e.deleting == nil {
		e.deleting = make(map[string]int)
	}
	running := 0
	targets := make([]*Item, 0, len(ids))
	known := make(map[string]bool, len(ids))
	for _, item := range e.queue {
		if !wanted[item.ID] {
			continue
		}
		known[item.ID] = true
		if item.State == StateRunning || e.cancels[item.ID] != nil || e.cancelling[item.ID] || e.isDeletingLocked(item.ID) {
			running++
			continue
		}
		targets = append(targets, item)
		e.markDeletingLocked(item.ID)
	}
	orphans := make([]string, 0, len(ids))
	for id := range wanted {
		if !known[id] {
			if e.isDeletingLocked(id) {
				running++
				continue
			}
			orphans = append(orphans, id)
			e.markDeletingLocked(id)
		}
	}
	e.mu.Unlock()

	defer func() {
		e.mu.Lock()
		for _, item := range targets {
			e.unmarkDeletingLocked(item.ID)
		}
		for _, id := range orphans {
			e.unmarkDeletingLocked(id)
		}
		e.mu.Unlock()
	}()

	stageDir := e.StageDir()

	deleted := 0
	for _, item := range targets {
		e.discardStagedWork(item)
		e.mu.Lock()
		item.Downloaded = 0
		e.markQueueDirtyLocked()
		e.mu.Unlock()
		deleted++
	}
	for _, id := range orphans {
		if !safeIDPattern.MatchString(id) {
			continue
		}
		targetDir := itemStageDir(stageDir, id)
		itemsRoot := filepath.Join(stageDir, stageItemsDir)
		rel, err := filepath.Rel(itemsRoot, targetDir)
		if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, "..") || strings.Contains(rel, string(filepath.Separator)) {
			continue
		}
		if err := os.RemoveAll(targetDir); err != nil {
			e.logf("删除暂存目录 %s 失败: %v", id, err)
			continue
		}
		deleted++
	}

	if deleted > 0 {
		e.persistNow(ctx)
	}
	if deleted == 0 {
		if running > 0 {
			return 0, errors.New("选中的文件正在传输中，请先取消传输再删除本地文件")
		}
		return 0, errors.New("未找到可删除的本地暂存文件")
	}
	return deleted, nil
}

// PruneStaged deletes every staging directory the queue no longer refers to,
// on demand rather than only at startup. It reports how many went and how much
// disk that freed, so the page can say something more useful than "done".
func (e *Engine) PruneStaged() (int, int64) {
	stageDir := e.StageDir()
	live := e.liveItemIDs()

	itemsRoot := filepath.Join(stageDir, stageItemsDir)
	entries, err := os.ReadDir(itemsRoot)
	if err != nil {
		return 0, 0
	}
	removed := 0
	var freed int64
	for _, entry := range entries {
		if !entry.IsDir() || live[entry.Name()] {
			continue
		}
		directory := filepath.Join(itemsRoot, entry.Name())
		size, _ := measureStageDir(directory)
		if err := os.RemoveAll(directory); err != nil {
			e.logf("清理无主暂存目录 %s 失败: %v", entry.Name(), err)
			continue
		}
		removed++
		freed += size
	}
	return removed, freed
}
