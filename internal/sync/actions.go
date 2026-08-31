package sync

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dibin/tdrive-aliyunpan/internal/aliyunpan"
	"github.com/dibin/tdrive-aliyunpan/internal/settings"
)

// InstallBinary downloads the pinned aliyunpan release in the background.
//
// It cannot run inside the request that triggered it: the host gives a plugin
// HTTP route thirty seconds, and the archive is large enough that a modest
// link needs longer. The 账号 tab polls Snapshot.Installing instead.
func (e *Engine) InstallBinary(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	cli := e.CLI()
	if cli == nil {
		return errors.New("aliyunpan 尚未配置")
	}
	if !cli.Managed() {
		return errors.New("配置里指定了自定义 aliyunpan 路径，插件不会覆盖它")
	}
	e.mu.Lock()
	stopping := e.stopping
	e.mu.Unlock()
	if stopping {
		return errors.New("插件正在停止")
	}

	e.probeMu.Lock()
	if e.installing {
		e.probeMu.Unlock()
		return errors.New("已经在下载了")
	}
	e.installing = true
	e.installError = ""
	e.probeMu.Unlock()
	e.mu.Lock()
	if e.stopping {
		e.mu.Unlock()
		e.probeMu.Lock()
		e.installing = false
		e.probeMu.Unlock()
		return errors.New("插件正在停止")
	}
	e.workers.Add(1)
	e.mu.Unlock()

	background := e.backgroundContext()
	go func() {
		defer e.workers.Done()
		err := cli.InstallManaged(background)

		e.probeMu.Lock()
		e.installing = false
		if err != nil {
			e.installError = err.Error()
		}
		e.probeMu.Unlock()
		// Force the next probe to re-read the binary rather than serve the
		// pre-install cache.
		e.invalidateProbe()

		if err != nil {
			e.logger.Printf("下载 aliyunpan 失败: %v", err)
			return
		}
		e.requestProbeRefresh()
		e.Wake()
	}()
	return nil
}

// StartLogin begins the interactive scan-code login.
func (e *Engine) StartLogin(ctx context.Context) (aliyunpan.LoginState, error) {
	cli := e.CLI()
	if cli == nil {
		return aliyunpan.LoginState{}, errors.New("aliyunpan 尚未配置")
	}
	return cli.StartLogin(ctx)
}

// ConfirmLogin tells the CLI the browser step is done and waits briefly for
// the token exchange so the caller's next poll already has the outcome.
func (e *Engine) ConfirmLogin(ctx context.Context) (aliyunpan.LoginState, error) {
	cli := e.CLI()
	if cli == nil {
		return aliyunpan.LoginState{}, errors.New("aliyunpan 尚未配置")
	}
	state, err := cli.ConfirmLogin(ctx)
	if err == nil && state.Phase == aliyunpan.LoginDone {
		e.invalidateProbe()
		e.requestProbeRefresh()
		e.Wake()
	}
	return state, err
}

// CancelLogin discards an in-flight login.
func (e *Engine) CancelLogin() {
	if cli := e.CLI(); cli != nil {
		cli.CancelLogin()
	}
}

// Logout unlinks the Aliyun Drive account.
func (e *Engine) Logout(ctx context.Context) error {
	cli := e.CLI()
	if cli == nil {
		return errors.New("aliyunpan 尚未配置")
	}
	e.invalidateProbe()
	err := cli.Logout(ctx)
	// A probe can have completed its `who` call just before Logout cancelled
	// it. Invalidate again after logout so that result cannot make the scheduler
	// believe the account is still linked.
	e.invalidateProbe()
	e.requestProbeRefresh()
	e.Wake()
	return err
}

// Browse lists a cloud directory for the target picker.
func (e *Engine) Browse(ctx context.Context, path string, driveName ...string) ([]aliyunpan.Entry, error) {
	cli := e.CLI()
	if cli == nil {
		return nil, errors.New("aliyunpan 尚未配置")
	}
	if path == "" {
		path = "/"
	}
	selectedDrive := ""
	if len(driveName) > 0 {
		selectedDrive = driveName[0]
	}
	drive, err := cli.ResolveDrive(ctx, selectedDrive)
	if err != nil {
		return nil, err
	}
	entries, err := cli.List(ctx, path, drive.ID)
	if err != nil {
		return nil, err
	}
	return entries, nil
}

// UpsertJob adds or replaces one job, leaving the rest of the document alone.
// Editing a single job through the whole-document endpoint would make two
// administrators with the page open overwrite each other's work.
func (e *Engine) UpsertJob(ctx context.Context, job settings.Job) error {
	// Based on the stored document rather than the cached one, so adding a job
	// cannot silently revert an edit made through the core's modal.
	current := e.ReloadSettings(ctx)
	if job.ID == "" {
		job.ID = newID()
	}
	replaced := false
	for i := range current.Jobs {
		if current.Jobs[i].ID == job.ID {
			current.Jobs[i] = job
			replaced = true
			break
		}
	}
	if !replaced {
		current.Jobs = append(current.Jobs, job)
	}
	if err := e.SaveSettings(ctx, current); err != nil {
		return err
	}
	// Pending candidates contain the old source/target/drive snapshot. Keeping
	// them after an edit would upload them to the old destination or from the
	// old drive; running items are left alone so an in-flight upload can finish
	// safely.
	e.mu.Lock()
	remaining := e.queue[:0]
	dropped := false
	for _, item := range e.queue {
		if replaced && item.JobID == job.ID && item.State != StateRunning && item.State != StateComplete {
			dropped = true
			continue
		}
		remaining = append(remaining, item)
	}
	e.queue = remaining
	if dropped {
		e.markQueueDirtyLocked()
	}
	e.mu.Unlock()
	if e.accountReady() {
		e.StartScan(ctx)
	} else {
		// The startup probe or a login completion will wake the scheduler once
		// there is an account to scan. Starting a CLI process before that only
		// creates noisy failed scans.
		e.Wake()
	}
	return nil
}

// DeleteJob removes a job. Queued items belonging to it are dropped too, since
// nothing would ever dispatch them again.
func (e *Engine) DeleteJob(ctx context.Context, id string) error {
	current := e.ReloadSettings(ctx)
	kept := make([]settings.Job, 0, len(current.Jobs))
	found := false
	for _, job := range current.Jobs {
		if job.ID == id {
			found = true
			continue
		}
		kept = append(kept, job)
	}
	if !found {
		return fmt.Errorf("找不到任务 %s", id)
	}
	current.Jobs = kept
	if err := e.SaveSettings(ctx, current); err != nil {
		return err
	}

	e.mu.Lock()
	remaining := e.queue[:0]
	for _, item := range e.queue {
		if item.JobID == id && item.State == StatePending {
			continue
		}
		remaining = append(remaining, item)
	}
	e.queue = remaining
	e.markQueueDirtyLocked()
	e.mu.Unlock()
	e.persistNow(ctx)
	return nil
}

func (e *Engine) invalidateProbe() {
	e.probeMu.Lock()
	e.probes.revision++
	e.probes.checkedAt = time.Time{}
	e.probes.account = AccountView{}
	if e.probes.refreshing {
		e.probes.rerun = true
	}
	e.probeMu.Unlock()
}
