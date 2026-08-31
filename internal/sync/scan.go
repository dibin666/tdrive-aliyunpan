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
	drive := aliyunpan.Drive{}
	if len(selected) > 0 {
		drive = selected[0]
	} else {
		var err error
		drive, err = cli.ResolveDrive(ctx, job.DriveName)
		if err != nil {
			return err
		}
	}
	excludes := make([]*regexp.Regexp, 0, len(job.ExcludeNames))
	for _, pattern := range job.ExcludeNames {
		compiled, err := regexp.Compile(pattern)
		if err != nil {
			return fmt.Errorf("排除规则 %q 无效: %w", pattern, err)
		}
		excludes = append(excludes, compiled)
	}

	// Breadth-first so the shallow files — usually the ones an operator is
	// watching for — are queued first.
	queue := []string{job.RemotePath}
	visited := 0
	for len(queue) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		cloudDir := queue[0]
		queue = queue[1:]
		visited++
		if visited > maxScanDirectories {
			return fmt.Errorf("目录数超过 %d，停止扫描", maxScanDirectories)
		}

		entries, err := cli.List(ctx, cloudDir, drive.ID)
		if err != nil {
			return err
		}
		targetDir := mapPath(job.RemotePath, job.TargetPath, cloudDir)
		existing, err := e.driveIndex(ctx, targetDir)
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
			e.consider(job, entry, targetDir, existing)
		}
	}
	return nil
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
	job settings.Job,
	entry aliyunpan.Entry,
	targetDir string,
	existing map[string]tdriveplugin.Entry,
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

	e.mu.Lock()
	defer e.mu.Unlock()
	currentJob, current := e.settings.JobByID(job.ID)
	if !current || !reflect.DeepEqual(currentJob, job) {
		// A scan may have started just before the operator edited this job. Do
		// not let its old snapshot enqueue work with the previous drive,
		// destination, filters, or delete policy.
		return
	}
	candidate := &Item{
		JobID:      job.ID,
		RemotePath: entry.Path,
		Size:       entry.Size,
		SHA1:       entry.SHA1,
	}
	for _, item := range e.queue {
		if item.key() != candidate.key() {
			continue
		}
		// A file already waiting or running is not queued twice. One that
		// failed is left alone too: retrying it is an explicit decision, so
		// the next scan does not silently loop on a permanent error.
		if item.Active() || item.State == StateFailed || item.State == StateCancelled {
			return
		}
	}

	item := &Item{
		ID:          newID(),
		JobID:       job.ID,
		JobName:     job.Name,
		DriveName:   job.DriveName,
		RemotePath:  entry.Path,
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
