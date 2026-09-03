package sync

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"
	"unicode"

	"github.com/dibin/tdrive-aliyunpan/internal/aliyunpan"
	"github.com/dibin/tdrive-aliyunpan/internal/hostapi"
	"github.com/dibin/tdrive-aliyunpan/internal/settings"
	tdriveplugin "github.com/dibin/tdrive/pkg/plugin"
)

// maxScanDirectories bounds one scan. A cloud account with a pathological tree
// should slow the plugin down, not wedge it.
const maxScanDirectories = 5000

// maxNameBytes mirrors tdrive's own limit on a file name. Checking it during
// the scan turns "this file cannot be stored" into a queue entry an operator
// can see, rather than a failure discovered after a multi-gigabyte download.
const maxNameBytes = 255

// StartScan kicks off a scan unless one is already running.
func (e *Engine) StartScan(_ context.Context) {
	// A scan started by an HTTP request is background work. The request context
	// only exists for the acknowledgement response and must not cancel the scan
	// as soon as that response is returned. Bind it to the plugin lifetime so a
	// shutdown can still cancel the scan.
	e.startScan(e.backgroundContext())
}

func (e *Engine) startScan(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	e.mu.Lock()
	if e.scanning || e.stopping {
		e.mu.Unlock()
		return
	}
	e.scanning = true
	jobs := append([]settings.Job(nil), e.settings.Jobs...)
	cli := e.cli
	e.workers.Add(1)
	e.mu.Unlock()

	go func() {
		defer e.workers.Done()
		err := e.scan(ctx, cli, jobs)
		e.mu.Lock()
		e.scanning = false
		e.lastScan = time.Now()
		if err != nil {
			e.scanError = err.Error()
		} else {
			e.scanError = ""
		}
		e.mu.Unlock()
		if err != nil {
			e.logger.Printf("扫描阿里云盘失败: %v", err)
		}
		e.persistNow(context.WithoutCancel(ctx))
		e.Wake()
	}()
}

// scan walks every enabled job and queues whatever the drive does not already
// hold.
func (e *Engine) scan(ctx context.Context, cli *aliyunpan.CLI, jobs []settings.Job) error {
	if cli == nil {
		return errors.New("aliyunpan 尚未配置")
	}
	hasEnabledJob := false
	for _, job := range jobs {
		if job.Enabled {
			hasEnabledJob = true
			break
		}
	}
	if !hasEnabledJob {
		return nil
	}
	drives, err := cli.Drives(ctx)
	if err != nil {
		if errors.Is(err, aliyunpan.ErrNotLoggedIn) {
			e.invalidateProbe()
			e.requestProbeRefresh()
		}
		return err
	}
	var failures []string
	for _, job := range jobs {
		if !job.Enabled {
			continue
		}
		kind, err := aliyunpan.NormalizeDriveName(job.DriveName)
		if err != nil {
			if errors.Is(err, aliyunpan.ErrNotLoggedIn) {
				return err
			}
			failures = append(failures, fmt.Sprintf("%s: %v", job.Name, err))
			continue
		}
		drive, err := resolveDriveForScan(kind, drives)
		if err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", job.Name, err))
			continue
		}
		if err := e.scanJob(ctx, cli, job, drive); err != nil {
			if errors.Is(err, aliyunpan.ErrNotLoggedIn) {
				e.invalidateProbe()
				e.requestProbeRefresh()
				return err
			}
			failures = append(failures, fmt.Sprintf("%s: %v", job.Name, err))
		}
	}
	if len(failures) > 0 {
		return errors.New(strings.Join(failures, "; "))
	}
	return nil
}

func resolveDriveForScan(kind string, drives []aliyunpan.Drive) (aliyunpan.Drive, error) {
	for _, drive := range drives {
		if drive.Kind == kind {
			return drive, nil
		}
	}
	label := "备份盘"
	if kind == aliyunpan.DriveResource {
		label = "资源库"
	}
	return aliyunpan.Drive{}, fmt.Errorf("当前阿里云盘账号没有%s", label)
}

func (e *Engine) scanJob(ctx context.Context, cli *aliyunpan.CLI, job settings.Job, selected ...aliyunpan.Drive) error {
	var drive aliyunpan.Drive
	if len(selected) > 0 {
		drive = selected[0]
	} else {
		var err error
		drive, err = cli.ResolveDrive(ctx, job.DriveName)
		if err != nil {
			return err
		}
	}
	excludes, err := compileExcludes(job.ExcludeNames)
	if err != nil {
		return err
	}
	cache := newDriveCache(e)

	// A job with an explicit selection is not mirroring a directory, so it does
	// not walk one.
	if job.HasSelection() {
		return e.scanChosen(ctx, cli, job, drive, excludes, cache)
	}
	return e.walkCloudDir(ctx, cli, job, drive, job.RemotePath, excludes, cache, new(int))
}

func compileExcludes(patterns []string) ([]*regexp.Regexp, error) {
	excludes := make([]*regexp.Regexp, 0, len(patterns))
	for _, pattern := range patterns {
		compiled, err := regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("排除规则 %q 无效: %w", pattern, err)
		}
		excludes = append(excludes, compiled)
	}
	return excludes, nil
}

// walkCloudDir queues everything under one cloud directory, rebasing each
// directory it descends into onto the job's target root so the tree that
// arrives in tdrive has the shape it had in the cloud.
//
// visited is shared across every root one job walks, because the ceiling exists
// to bound a single scan rather than a single directory.
func (e *Engine) walkCloudDir(
	ctx context.Context,
	cli *aliyunpan.CLI,
	job settings.Job,
	drive aliyunpan.Drive,
	root string,
	excludes []*regexp.Regexp,
	cache *driveCache,
	visited *int,
) error {
	// Breadth-first so the shallow files — usually the ones an operator is
	// watching for — are queued first.
	queue := []string{root}
	for len(queue) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		cloudDir := queue[0]
		queue = queue[1:]
		*visited++
		if *visited > maxScanDirectories {
			return fmt.Errorf("目录数超过 %d，停止扫描", maxScanDirectories)
		}

		entries, err := cli.List(ctx, cloudDir, drive.ID)
		if err != nil {
			return err
		}
		if len(entries) == 1 && !entries[0].IsDir && entries[0].Path == cloudDir {
			// Listing a file answers with the file itself. Rebasing that path as
			// though it were a directory would create a drive directory named
			// after the file and put the file inside it, so it is reported as a
			// selection that is no longer there.
			return fmt.Errorf("%w: %s 不是目录", aliyunpan.ErrPathNotFound, cloudDir)
		}
		targetDir := mapPath(job.RemotePath, job.TargetPath, cloudDir)
		existing, err := cache.at(ctx, targetDir)
		if err != nil {
			return err
		}

		for _, entry := range entries {
			if matchesAny(excludes, entry.Name) {
				continue
			}
			if entry.IsDir {
				queue = append(queue, entry.Path)
				continue
			}
			if job.MinSizeBytes > 0 && entry.Size < job.MinSizeBytes {
				continue
			}
			if job.MaxSizeBytes > 0 && entry.Size > job.MaxSizeBytes {
				continue
			}
			e.consider(ctx, job, entry, targetDir, existing, cache)
		}
	}
	return nil
}

// scanChosen queues exactly what a job's selection names, and nothing else.
//
// A ticked directory is walked whole, and the size and exclude filters do apply
// inside it: its contents were never named by hand, so they are described by the
// job's rules like any other directory. Ticked files are exempt from those
// filters, for the reason IncludeFiles documents.
func (e *Engine) scanChosen(
	ctx context.Context,
	cli *aliyunpan.CLI,
	job settings.Job,
	drive aliyunpan.Drive,
	excludes []*regexp.Regexp,
	cache *driveCache,
) error {
	missing, err := e.scanChosenFiles(ctx, cli, job, drive, cache)
	if err != nil {
		return err
	}
	visited := new(int)
	for _, cloudDir := range job.IncludeDirs {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := e.walkCloudDir(ctx, cli, job, drive, cloudDir, excludes, cache, visited)
		if errors.Is(err, aliyunpan.ErrPathNotFound) {
			missing = append(missing, cloudDir)
			continue
		}
		if err != nil {
			return err
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("勾选的内容在云盘上找不到: %s", strings.Join(missing, "、"))
	}
	return nil
}

// scanChosenFiles queues exactly the files a job names, and nothing else. It
// returns the ones that are no longer there rather than failing on them, so a
// missing file and a missing directory are reported to the operator together.
//
// The directories holding them are listed rather than walked: the point of
// picking files by hand is that the rest of the tree is not wanted, and a
// listing per directory is also how the size and identity of each file is
// learned without a call per file. A file that has since been moved or deleted
// in the cloud is reported rather than passed over, because a job that silently
// syncs nothing looks identical to one that is working.
func (e *Engine) scanChosenFiles(
	ctx context.Context,
	cli *aliyunpan.CLI,
	job settings.Job,
	drive aliyunpan.Drive,
	cache *driveCache,
) ([]string, error) {
	byDirectory := make(map[string][]string)
	order := make([]string, 0, len(job.IncludeFiles))
	for _, path := range job.IncludeFiles {
		parent := parentCloudDir(path)
		if _, seen := byDirectory[parent]; !seen {
			order = append(order, parent)
		}
		byDirectory[parent] = append(byDirectory[parent], path)
	}

	var missing []string
	for _, cloudDir := range order {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		entries, err := cli.List(ctx, cloudDir, drive.ID)
		if errors.Is(err, aliyunpan.ErrPathNotFound) {
			missing = append(missing, byDirectory[cloudDir]...)
			continue
		}
		if err != nil {
			return nil, err
		}
		found := make(map[string]aliyunpan.Entry, len(entries))
		for _, entry := range entries {
			if !entry.IsDir {
				found[entry.Path] = entry
			}
		}

		targetDir := mapPath(job.RemotePath, job.TargetPath, cloudDir)
		existing, err := cache.at(ctx, targetDir)
		if err != nil {
			return nil, err
		}
		for _, wanted := range byDirectory[cloudDir] {
			entry, ok := found[wanted]
			if !ok {
				missing = append(missing, wanted)
				continue
			}
			e.consider(ctx, job, entry, targetDir, existing, cache)
		}
	}
	return missing, nil
}

// parentCloudDir is the directory holding a cloud path.
func parentCloudDir(path string) string {
	index := strings.LastIndexByte(path, '/')
	if index <= 0 {
		return "/"
	}
	return path[:index]
}

// driveCache remembers the drive directories one job's scan has already read.
//
// A scan now consults the same directory from more than one place: once for the
// files being rebased onto it, and again whenever a file that has already been
// delivered has to be checked against the destination it was delivered to. Both
// are answered from one listing per directory.
//
// It is not safe for concurrent use, and does not need to be: one job's scan
// runs on one goroutine.
type driveCache struct {
	engine *Engine
	dirs   map[string]map[string]tdriveplugin.Entry
}

func newDriveCache(engine *Engine) *driveCache {
	return &driveCache{engine: engine, dirs: map[string]map[string]tdriveplugin.Entry{}}
}

func (c *driveCache) at(ctx context.Context, targetDir string) (map[string]tdriveplugin.Entry, error) {
	if cached, ok := c.dirs[targetDir]; ok {
		return cached, nil
	}
	index, err := c.engine.driveIndex(ctx, targetDir)
	if err != nil {
		return nil, err
	}
	c.dirs[targetDir] = index
	return index, nil
}

// driveIndex reads the drive directory a cloud directory maps onto, so the
// scan can tell what is already stored without a call per file.
//
// The drive is the index. Keeping a separate "already synced" ledger would
// drift the moment someone deleted a file from 文件, and would then refuse to
// restore it.
func (e *Engine) driveIndex(ctx context.Context, targetDir string) (map[string]tdriveplugin.Entry, error) {
	entries, err := e.host.List(ctx, targetDir)
	if err != nil {
		if hostapi.IsNotFound(err) {
			return map[string]tdriveplugin.Entry{}, nil
		}
		return nil, fmt.Errorf("读取 tdrive 目录 %s: %w", targetDir, err)
	}
	index := make(map[string]tdriveplugin.Entry, len(entries))
	for _, entry := range entries {
		if !entry.IsDir {
			index[entry.Name] = entry
		}
	}
	return index, nil
}

// consider queues one cloud file if it is neither already stored nor already
// waiting.
func (e *Engine) consider(
	ctx context.Context,
	job settings.Job,
	entry aliyunpan.Entry,
	targetDir string,
	existing map[string]tdriveplugin.Entry,
	cache *driveCache,
) {
	if stored, ok := existing[entry.Name]; ok {
		// Same name and same size is treated as the same file. The content
		// hash cannot be compared because tdrive does not keep one, and
		// re-uploading everything on every scan would defeat the quota.
		if stored.Size == entry.Size {
			return
		}
		if !job.Overwrite {
			return
		}
	}

	candidate := &Item{
		JobID:      job.ID,
		RemotePath: entry.Path,
		FileID:     entry.FileID,
		Size:       entry.Size,
		SHA1:       entry.SHA1,
	}
	e.mu.Lock()
	verdict := e.inspectLocked(job, candidate, targetDir)
	e.mu.Unlock()
	if verdict.stop {
		return
	}
	if e.deliveredElsewhere(ctx, entry, verdict.delivered, cache) {
		return
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	// The lock was released for those lookups, so nothing learned before them is
	// trusted: the job may have been edited and the file may have been queued by
	// a scan running beside this one.
	if e.inspectLocked(job, candidate, targetDir).stop {
		return
	}

	item := &Item{
		ID:          newID(),
		JobID:       job.ID,
		JobName:     job.Name,
		DriveName:   job.DriveName,
		RemotePath:  entry.Path,
		FileID:      entry.FileID,
		TargetDir:   targetDir,
		Name:        entry.Name,
		Size:        entry.Size,
		SHA1:        entry.SHA1,
		State:       StatePending,
		Overwrite:   job.Overwrite,
		DeleteAfter: job.DeleteAfterUpload,
		CreatedAt:   nowMillis(),
	}
	if reason := unstorableName(entry.Name); reason != "" {
		item.State = StateFailed
		item.Error = reason
		item.FinishedAt = item.CreatedAt
	}
	e.queue = append(e.queue, item)
	e.markQueueDirtyLocked()
}

// verdict is what one locked look at the engine says about a candidate.
type verdict struct {
	// stop means the file must not be queued at all.
	stop bool
	// delivered names the drive directories this exact file has already been
	// delivered to, when they are not the one it maps onto now.
	delivered []string
}

func (e *Engine) inspectLocked(job settings.Job, candidate *Item, targetDir string) verdict {
	currentJob, current := e.settings.JobByID(job.ID)
	if !current || !reflect.DeepEqual(currentJob, job) {
		// A scan may have started just before the operator edited this job. Do
		// not let its old snapshot enqueue work with the previous drive,
		// destination, filters, or delete policy.
		return verdict{stop: true}
	}
	var found verdict
	key := candidate.key()
	for _, item := range e.queue {
		if item.key() != key {
			continue
		}
		// A file already waiting or running is not queued twice. One that
		// failed is left alone too: retrying it is an explicit decision, so
		// the next scan does not silently loop on a permanent error.
		if item.Active() || item.State == StateFailed || item.State == StateCancelled {
			return verdict{stop: true}
		}
		if item.State == StateComplete && item.TargetDir != targetDir {
			found.delivered = append(found.delivered, item.TargetDir)
		}
	}
	return found
}

// deliveredElsewhere reports whether this file is already in the drive under a
// destination the job used to map it onto.
//
// The destination is derived, not stored: it is the file's cloud directory
// rebased from the job's cloud root, so editing a job moves it. The cloud root
// moves on its own, too — it is the deepest directory holding everything ticked,
// so ticking a second file in another folder lifts it a level and re-nests every
// destination under it. A file delivered under the old mapping is invisible to
// the listing of the new one, and the scan would then download and re-upload
// every byte of it to put a second copy somewhere else. Its own recorded
// destination is checked before that happens.
//
// The consequence is that re-pointing a job does not re-fetch what it has
// already delivered; the files stay where they landed and can be moved in 文件
// for free. What does bring a file back is deleting it from the drive, which is
// the point of indexing the drive rather than keeping a ledger of what was once
// synced.
func (e *Engine) deliveredElsewhere(
	ctx context.Context,
	entry aliyunpan.Entry,
	dirs []string,
	cache *driveCache,
) bool {
	for _, dir := range dirs {
		delivered, err := cache.at(ctx, dir)
		if err != nil {
			// The old destination could not be read, so nothing is known about
			// it. Queueing the file is the recoverable outcome: a needless
			// re-download costs quota, a skip loses the file.
			e.logf("检查 %s 之前的目标目录 %s 失败: %v", entry.Name, dir, err)
			continue
		}
		if stored, ok := delivered[entry.Name]; ok && stored.Size == entry.Size {
			return true
		}
	}
	return false
}

// unstorableName reports why tdrive would refuse a name, or "" if it would
// accept it. The rules mirror drive.ValidateName.
func unstorableName(name string) string {
	switch {
	case name == "" || name == "." || name == "..":
		return "文件名不合法"
	case unsafeFileName(name):
		return "文件名含有路径分隔符、Windows 保留字符或设备名"
	case len(name) > maxNameBytes:
		return fmt.Sprintf("文件名 %d 字节，超过 tdrive 的 %d 字节上限", len(name), maxNameBytes)
	}
	for _, r := range name {
		if r < 0x20 || r == 0x7f || (unicode.IsControl(r) && r != '\t') {
			return "文件名含有控制字符"
		}
	}
	return ""
}

func unsafeFileName(name string) bool {
	if strings.ContainsAny(name, `<>:"/\\|?*`) || strings.TrimRight(name, " .") != name {
		return true
	}
	base := strings.ToUpper(name)
	if dot := strings.IndexByte(base, '.'); dot >= 0 {
		base = base[:dot]
	}
	switch base {
	case "CON", "PRN", "AUX", "NUL",
		"COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
		"LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9":
		return true
	default:
		return false
	}
}

// mapPath rebases a cloud path from the job's source root onto its target
// root.
func mapPath(remoteRoot, targetRoot, cloudPath string) string {
	relative := cloudPath
	if cloudPath == remoteRoot {
		relative = ""
	} else if strings.HasPrefix(cloudPath, remoteRoot+"/") {
		relative = strings.TrimPrefix(cloudPath, remoteRoot)
	}
	relative = strings.Trim(relative, "/")
	if relative == "" {
		return targetRoot
	}
	if targetRoot == "/" {
		return "/" + relative
	}
	return targetRoot + "/" + relative
}

func matchesAny(patterns []*regexp.Regexp, name string) bool {
	for _, pattern := range patterns {
		if pattern.MatchString(name) {
			return true
		}
	}
	return false
}
