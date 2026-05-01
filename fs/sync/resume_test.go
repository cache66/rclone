package sync

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/fs/resume"
	"github.com/rclone/rclone/fstest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errCopyResumeInjected = errors.New("injected copy resume error")

func TestSyncResumeScanLimitPersistsState(t *testing.T) {
	cacheDir := t.TempDir()
	require.NoError(t, config.SetCacheDir(cacheDir))

	ctx := context.Background()
	meta := resume.Meta{
		FormatVersion: resume.FormatVersion,
		JobID:         "resume-sync-limit",
		Op:            "copy",
		SrcConfig:     "src",
		DstConfig:     "dst",
	}
	initialScan := resume.ScanState{
		Phase:  resume.PhaseCopySource,
		Target: "src",
		Frames: []resume.ScanFrame{{Dir: ""}},
	}

	store, err := resume.Open(ctx, meta.JobID)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close(true))
	}()

	snapshot, _, err := store.LoadOrInit(meta, initialScan)
	require.NoError(t, err)

	s := &syncCopyMove{ci: &fs.ConfigInfo{ResumeErrorLimit: 0}}
	snapshot.Totals.PendingFailedCount = 1
	require.NoError(t, s.syncResumeScanLimit(store, &snapshot))

	loaded, err := store.Snapshot()
	require.NoError(t, err)
	assert.True(t, loaded.Scan.OverErrorLimit)

	snapshot.Totals.PendingFailedCount = 0
	require.NoError(t, s.syncResumeScanLimit(store, &snapshot))

	loaded, err = store.Snapshot()
	require.NoError(t, err)
	assert.False(t, loaded.Scan.OverErrorLimit)
}

func TestCopyDirResumeRetriesFailuresConcurrently(t *testing.T) {
	ctx := newCopyResumeTestContext(t, "copy-resume-retry-concurrency", 2)
	srcDir := t.TempDir()
	dstDir := t.TempDir()
	writeCopyResumeFile(t, srcDir, "one.txt", "1111")
	writeCopyResumeFile(t, srcDir, "two.txt", "2222")

	baseSrc := newCopyResumeLocalFs(t, ctx, srcDir)
	fdst := newCopyResumeLocalFs(t, ctx, dstDir)
	probe := &copyResumeProbe{delay: 150 * time.Millisecond}
	fsrc := &copyResumeProbeFs{Fs: baseSrc, probe: probe}
	seedCopyResumeFailures(t, ctx, fsrc, fdst, "one.txt", "two.txt")

	accounting.GlobalStats().ResetCounters()
	defer accounting.GlobalStats().ResetCounters()

	err := CopyDir(ctx, fdst, fsrc, false)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, probe.max.Load(), int32(2))
	assert.Equal(t, int64(2), accounting.GlobalStats().GetTransfers())
	assert.Equal(t, "1111", readCopyResumeFile(t, dstDir, "one.txt"))
	assert.Equal(t, "2222", readCopyResumeFile(t, dstDir, "two.txt"))
}

func TestCopyDirResumeSkipsDoneAndRestoresCounters(t *testing.T) {
	ctx := newCopyResumeTestContext(t, "copy-resume-skip-done", 2)
	srcDir := t.TempDir()
	dstDir := t.TempDir()
	writeCopyResumeFile(t, srcDir, "done.txt", "done")
	writeCopyResumeFile(t, dstDir, "done.txt", "done")
	writeCopyResumeFile(t, srcDir, "pending.txt", "pend")

	fsrc := newCopyResumeLocalFs(t, ctx, srcDir)
	fdst := newCopyResumeLocalFs(t, ctx, dstDir)
	seedCopyResumeDone(t, ctx, fsrc, fdst, "done.txt")

	accounting.GlobalStats().ResetCounters()
	defer accounting.GlobalStats().ResetCounters()

	err := CopyDir(ctx, fdst, fsrc, false)
	require.NoError(t, err)
	assert.Equal(t, int64(2), accounting.GlobalStats().GetTransfers())
	assert.Equal(t, "done", readCopyResumeFile(t, dstDir, "done.txt"))
	assert.Equal(t, "pend", readCopyResumeFile(t, dstDir, "pending.txt"))
}

func TestCopyDirResumeReusesDestinationListingAcrossSourcePages(t *testing.T) {
	ctx := newCopyResumeTestContext(t, "copy-resume-dst-list-cache", 1)
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	writeCopyResumeFile(t, srcDir, "a.txt", "a")
	writeCopyResumeFile(t, srcDir, "b.txt", "b")
	writeCopyResumeFile(t, srcDir, "c.txt", "c")
	writeCopyResumeFile(t, srcDir, "d.txt", "d")

	baseSrc := newCopyResumeLocalFs(t, ctx, srcDir)
	fsrc := &copyResumePagedSourceFs{Fs: baseSrc, pageSize: 2}
	fdst := &copyResumeCountingFs{Fs: newCopyResumeLocalFs(t, ctx, dstDir)}

	accounting.GlobalStats().ResetCounters()
	defer accounting.GlobalStats().ResetCounters()

	err := CopyDir(ctx, fdst, fsrc, false)
	require.NoError(t, err)
	assert.Equal(t, int32(1), fdst.listCalls.Load())
	assert.Equal(t, "a", readCopyResumeFile(t, dstDir, "a.txt"))
	assert.Equal(t, "d", readCopyResumeFile(t, dstDir, "d.txt"))
}

func TestCopyDirResumePersistsPendingBatchBeforeDirectoryError(t *testing.T) {
	ctx := newCopyResumeTestContext(t, "copy-resume-flush-before-dir-error", 1)
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	writeCopyResumeFile(t, srcDir, "a-before.txt", "before")
	writeCopyResumeFile(t, srcDir, "b-dir/child.txt", "child")

	baseSrc := newCopyResumeLocalFs(t, ctx, srcDir)
	fsrc := &copyResumeFailListOnceFs{
		Fs:      baseSrc,
		failDir: "b-dir",
		err:     errCopyResumeInjected,
	}
	fdst := newCopyResumeLocalFs(t, ctx, dstDir)
	store := pinCopyResumeStore(t, ctx)

	accounting.GlobalStats().ResetCounters()
	defer accounting.GlobalStats().ResetCounters()

	err := CopyDir(ctx, fdst, fsrc, false)
	require.ErrorContains(t, err, errCopyResumeInjected.Error())

	snapshot := copyResumeSnapshot(t, store)
	if snapshot.Scan.FileTree != nil {
		require.NotEmpty(t, snapshot.Scan.FileTree.Frames)
		assert.Empty(t, snapshot.Scan.FileTree.Frames[0].LastEntry)
	}

	err = CopyDir(ctx, fdst, baseSrc, false)
	require.NoError(t, err)
	assert.Equal(t, "before", readCopyResumeFile(t, dstDir, "a-before.txt"))
	assert.Equal(t, "child", readCopyResumeFile(t, dstDir, "b-dir/child.txt"))
}

func TestResumeObjectFrontierReadyHonorsContiguousPrefix(t *testing.T) {
	pending := map[int64]resumeObjectSegmentResult{
		2: {segment: resume.ObjectSegment{SegmentID: 2, Status: resume.ObjectSegmentDone, EndKey: "b.txt"}},
		3: {segment: resume.ObjectSegment{SegmentID: 3, Status: resume.ObjectSegmentDone, EndKey: "c.txt"}},
	}
	assert.Empty(t, resumeObjectFrontierReady(0, pending))

	pending[1] = resumeObjectSegmentResult{segment: resume.ObjectSegment{SegmentID: 1, Status: resume.ObjectSegmentDone, EndKey: "a.txt"}}
	ready := resumeObjectFrontierReady(0, pending)
	require.Len(t, ready, 3)
	assert.Equal(t, int64(1), ready[0].segment.SegmentID)
	assert.Equal(t, int64(3), ready[2].segment.SegmentID)

	pending[4] = resumeObjectSegmentResult{segment: resume.ObjectSegment{SegmentID: 4, Status: resume.ObjectSegmentFailed, EndKey: "d.txt"}}
	ready = resumeObjectFrontierReady(3, pending)
	require.Len(t, ready, 1)
	assert.Equal(t, resume.ObjectSegmentFailed, ready[0].segment.Status)
}

func TestResumeFileFrontierReadyHonorsContiguousPrefix(t *testing.T) {
	pending := map[int64]resumeFileTaskResult{
		2: {task: resume.FileTask{TaskID: 2, Status: resume.FileTaskDone, EndFile: "b.txt"}},
		3: {task: resume.FileTask{TaskID: 3, Status: resume.FileTaskDone, EndFile: "c.txt"}},
	}
	assert.Empty(t, resumeFileFrontierReady(0, pending))

	pending[1] = resumeFileTaskResult{task: resume.FileTask{TaskID: 1, Status: resume.FileTaskDone, EndFile: "a.txt"}}
	ready := resumeFileFrontierReady(0, pending)
	require.Len(t, ready, 3)
	assert.Equal(t, int64(1), ready[0].task.TaskID)
	assert.Equal(t, int64(3), ready[2].task.TaskID)

	pending[4] = resumeFileTaskResult{task: resume.FileTask{TaskID: 4, Status: resume.FileTaskFailed, EndFile: "d.txt"}}
	ready = resumeFileFrontierReady(3, pending)
	require.Len(t, ready, 1)
	assert.Equal(t, resume.FileTaskFailed, ready[0].task.Status)
}

func TestNewResumeFileTaskCapturesTaskMetadata(t *testing.T) {
	ctx := newCopyResumeTestContext(t, "copy-resume-file-task-meta", 1)
	srcDir := t.TempDir()
	writeCopyResumeFile(t, srcDir, "a/one.txt", "1")
	writeCopyResumeFile(t, srcDir, "a/two.txt", "22")
	fsrc := newCopyResumeLocalFs(t, ctx, srcDir)
	one, err := fsrc.NewObject(ctx, "a/one.txt")
	require.NoError(t, err)
	two, err := fsrc.NewObject(ctx, "a/two.txt")
	require.NoError(t, err)

	frames := []resume.ScanFrame{
		{Dir: "", DstDir: "", LastDoneEntryKey: "0:1", ContinuationToken: "tok", PageIndex: 2},
		{Dir: "a", DstDir: "a", LastDoneEntryKey: "0:3", PageIndex: 0},
	}
	startFrames := []resume.ScanFrame{
		{Dir: "", DstDir: "", LastDoneEntryKey: "0:0"},
		{Dir: "a", DstDir: "a"},
	}
	tasks := []resumeCopyTask{
		{src: one},
		{src: two},
	}

	task := newResumeFileTask(7, tasks, startFrames, frames)
	assert.Equal(t, int64(7), task.meta.TaskID)
	assert.Equal(t, 2, task.meta.FileCount)
	assert.Equal(t, "a/two.txt", task.meta.EndFile)
	require.Len(t, task.meta.StartFrameSnapshot, 2)
	assert.Equal(t, "", task.meta.StartFrameSnapshot[0].Dir)
	assert.Equal(t, "0:0", task.meta.StartFrameSnapshot[0].LastEntry)
	assert.Equal(t, "", task.meta.StartFrameSnapshot[0].ContinuationToken)
	assert.Equal(t, "", task.meta.StartFrameSnapshot[1].LastEntry)
	assert.Equal(t, frames, task.commitFrames)
}

func TestCommitResumeReadyFileTasksAdvancesContiguousFrontier(t *testing.T) {
	ctx := newCopyResumeTestContext(t, "copy-resume-file-frontier-commit", 1)
	meta := resume.Meta{
		FormatVersion: resume.FormatVersion,
		JobID:         fs.GetConfig(ctx).ResumeID,
		Op:            "copy",
		SrcConfig:     "src",
		DstConfig:     "dst",
	}
	initialScan := resume.ScanState{
		Phase:  resume.PhaseCopySource,
		Target: "src",
		Frames: []resume.ScanFrame{{Dir: "", DstDir: ""}},
	}
	store, err := resume.Open(ctx, meta.JobID)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close(true))
	}()
	snapshot, _, err := store.LoadOrInit(meta, initialScan)
	require.NoError(t, err)

	s := &syncCopyMove{ctx: ctx, ci: fs.GetConfig(ctx)}
	inflight := []resume.FileTask{
		{TaskID: 1}, {TaskID: 2}, {TaskID: 4},
	}
	pending := map[int64]resumeFileTaskResult{
		1: {
			task:         resume.FileTask{TaskID: 1, Status: resume.FileTaskDone, EndFile: "a.txt"},
			commitFrames: []resume.ScanFrame{{Dir: "", DstDir: "", LastDoneEntryKey: "0:1"}},
			successCommits: []resume.SuccessCommit{{
				Done:         resume.DoneRecord{WorkKey: "wk1", Name: "a.txt", Files: 1, Objects: 1, Bytes: 10},
				HistoryLimit: s.ci.ResumeHistoryLimit,
			}},
		},
		2: {
			task:         resume.FileTask{TaskID: 2, Status: resume.FileTaskDone, EndFile: "b.txt"},
			commitFrames: []resume.ScanFrame{{Dir: "", DstDir: "", LastDoneEntryKey: "0:2"}},
			successCommits: []resume.SuccessCommit{{
				Done:         resume.DoneRecord{WorkKey: "wk2", Name: "b.txt", Files: 1, Objects: 1, Bytes: 20},
				HistoryLimit: s.ci.ResumeHistoryLimit,
			}},
		},
		4: {
			task:         resume.FileTask{TaskID: 4, Status: resume.FileTaskDone, EndFile: "d.txt"},
			commitFrames: []resume.ScanFrame{{Dir: "", DstDir: "", LastDoneEntryKey: "0:4"}},
			successCommits: []resume.SuccessCommit{{
				Done:         resume.DoneRecord{WorkKey: "wk4", Name: "d.txt", Files: 1, Objects: 1, Bytes: 40},
				HistoryLimit: s.ci.ResumeHistoryLimit,
			}},
		},
	}

	nextFrontier, remaining, blocked, err := s.commitResumeReadyFileTasks(store, &snapshot, 0, pending, inflight)
	require.NoError(t, err)
	assert.False(t, blocked)
	assert.Equal(t, int64(2), nextFrontier)
	require.Len(t, remaining, 1)
	assert.Equal(t, int64(4), remaining[0].TaskID)
	assert.Equal(t, int64(2), snapshot.Totals.Files)
	assert.Equal(t, int64(30), snapshot.Totals.Bytes)
	require.NotNil(t, snapshot.Scan.FileTree)
	assert.Equal(t, int64(2), snapshot.Scan.FileTree.Window.CommitFrontierTaskID)
	require.Len(t, snapshot.Scan.FileTree.Window.InflightTasks, 1)
	assert.Equal(t, int64(4), snapshot.Scan.FileTree.Window.InflightTasks[0].TaskID)
}

func TestCommitResumeReadyFileTasksStopsAtFailure(t *testing.T) {
	ctx := newCopyResumeTestContext(t, "copy-resume-file-frontier-failure", 1)
	meta := resume.Meta{
		FormatVersion: resume.FormatVersion,
		JobID:         fs.GetConfig(ctx).ResumeID,
		Op:            "copy",
		SrcConfig:     "src",
		DstConfig:     "dst",
	}
	initialScan := resume.ScanState{
		Phase:  resume.PhaseCopySource,
		Target: "src",
		Frames: []resume.ScanFrame{{Dir: "", DstDir: ""}},
	}
	store, err := resume.Open(ctx, meta.JobID)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close(true))
	}()
	snapshot, _, err := store.LoadOrInit(meta, initialScan)
	require.NoError(t, err)

	s := &syncCopyMove{ctx: ctx, ci: fs.GetConfig(ctx)}
	inflight := []resume.FileTask{{TaskID: 1}, {TaskID: 2}}
	pending := map[int64]resumeFileTaskResult{
		1: {
			task:         resume.FileTask{TaskID: 1, Status: resume.FileTaskDone, EndFile: "a.txt"},
			commitFrames: []resume.ScanFrame{{Dir: "", DstDir: "", LastDoneEntryKey: "0:1"}},
			successCommits: []resume.SuccessCommit{{
				Done:         resume.DoneRecord{WorkKey: "wk1", Name: "a.txt", Files: 1, Objects: 1, Bytes: 10},
				HistoryLimit: s.ci.ResumeHistoryLimit,
			}},
		},
		2: {
			task:         resume.FileTask{TaskID: 2, Status: resume.FileTaskFailed, EndFile: "b.txt"},
			startFrames:  []resume.ScanFrame{{Dir: "", DstDir: "", LastDoneEntryKey: "0:1"}},
			commitFrames: []resume.ScanFrame{{Dir: "", DstDir: "", LastDoneEntryKey: "0:2"}},
			successCommits: []resume.SuccessCommit{{
				Done:         resume.DoneRecord{WorkKey: "wk2-ok", Name: "b-ok.txt", Files: 1, Objects: 1, Bytes: 11},
				HistoryLimit: s.ci.ResumeHistoryLimit,
			}},
			failureCommits: []resume.FailureCommit{{
				Failed:       resume.FailedRecord{WorkKey: "wk2", SrcRemote: "b.txt", Name: "b.txt", LastError: "boom", What: "transferring"},
				HistoryLimit: s.ci.ResumeHistoryLimit,
			}},
		},
	}

	nextFrontier, remaining, blocked, err := s.commitResumeReadyFileTasks(store, &snapshot, 0, pending, inflight)
	require.ErrorContains(t, err, "boom")
	assert.True(t, blocked)
	assert.Equal(t, int64(1), nextFrontier)
	assert.Empty(t, remaining)
	assert.Equal(t, int64(1), snapshot.Totals.Files)
	assert.Equal(t, int64(1), snapshot.Totals.PendingFailedCount)
	require.NotNil(t, snapshot.Scan.FileTree)
	assert.Equal(t, int64(1), snapshot.Scan.FileTree.Window.CommitFrontierTaskID)
	require.Len(t, snapshot.Scan.FileTree.Frames, 1)
	assert.Equal(t, "0:1", snapshot.Scan.FileTree.Frames[0].LastEntry)
	failed, err := store.ListFailed()
	require.NoError(t, err)
	require.Len(t, failed, 1)
	assert.Equal(t, "b.txt", failed[0].SrcRemote)
}

func TestCopyDirResumeRestoresDeepDirectoryStack(t *testing.T) {
	ctx := newCopyResumeTestContext(t, "copy-resume-deep-stack", 1)
	srcDir := t.TempDir()
	dstDir := t.TempDir()
	writeCopyResumeFile(t, srcDir, "a-parent/a-child/a-done.txt", "done")
	writeCopyResumeFile(t, srcDir, "a-parent/a-child/z-pending.txt", "pending")
	writeCopyResumeFile(t, srcDir, "a-parent/z-after.txt", "after")
	writeCopyResumeFile(t, srcDir, "z-root.txt", "root")
	writeCopyResumeFile(t, dstDir, "a-parent/a-child/a-done.txt", "done")

	fsrc := newCopyResumeLocalFs(t, ctx, srcDir)
	fdst := newCopyResumeLocalFs(t, ctx, dstDir)
	seedCopyResumeDone(t, ctx, fsrc, fdst, "a-parent/a-child/a-done.txt")
	seedCopyResumeScanFrames(t, ctx, fsrc, fdst, []resume.ScanFrame{
		copyResumeFrame(t, ctx, fsrc, fdst, "", "", "a-parent"),
		copyResumeFrame(t, ctx, fsrc, fdst, "a-parent", "a-parent", "a-parent/a-child"),
		copyResumeFrame(t, ctx, fsrc, fdst, "a-parent/a-child", "a-parent/a-child", "a-parent/a-child/a-done.txt"),
	})

	accounting.GlobalStats().ResetCounters()
	defer accounting.GlobalStats().ResetCounters()

	err := CopyDir(ctx, fdst, fsrc, false)
	require.NoError(t, err)
	assert.Equal(t, "done", readCopyResumeFile(t, dstDir, "a-parent/a-child/a-done.txt"))
	assert.Equal(t, "pending", readCopyResumeFile(t, dstDir, "a-parent/a-child/z-pending.txt"))
	assert.Equal(t, "after", readCopyResumeFile(t, dstDir, "a-parent/z-after.txt"))
	assert.Equal(t, "root", readCopyResumeFile(t, dstDir, "z-root.txt"))
	assert.Equal(t, int64(4), accounting.GlobalStats().GetTransfers())
}

func TestCopyDirResumeRestoresSameDirectoryFileMiddle(t *testing.T) {
	ctx := newCopyResumeTestContext(t, "copy-resume-same-dir-middle", 1)
	srcDir := t.TempDir()
	dstDir := t.TempDir()
	writeCopyResumeFile(t, srcDir, "a-done.txt", "done")
	writeCopyResumeFile(t, srcDir, "b-pending.txt", "pending-b")
	writeCopyResumeFile(t, srcDir, "c-pending.txt", "pending-c")
	writeCopyResumeFile(t, dstDir, "a-done.txt", "done")

	fsrc := newCopyResumeLocalFs(t, ctx, srcDir)
	fdst := newCopyResumeLocalFs(t, ctx, dstDir)
	seedCopyResumeDone(t, ctx, fsrc, fdst, "a-done.txt")
	seedCopyResumeScanFrames(t, ctx, fsrc, fdst, []resume.ScanFrame{
		copyResumeFrame(t, ctx, fsrc, fdst, "", "", "a-done.txt"),
	})

	accounting.GlobalStats().ResetCounters()
	defer accounting.GlobalStats().ResetCounters()

	err := CopyDir(ctx, fdst, fsrc, false)
	require.NoError(t, err)
	assert.Equal(t, "done", readCopyResumeFile(t, dstDir, "a-done.txt"))
	assert.Equal(t, "pending-b", readCopyResumeFile(t, dstDir, "b-pending.txt"))
	assert.Equal(t, "pending-c", readCopyResumeFile(t, dstDir, "c-pending.txt"))
	assert.Equal(t, int64(3), accounting.GlobalStats().GetTransfers())
}

func TestCopyDirResumeRestoresAfterFinishedChildBeforeParentPop(t *testing.T) {
	ctx := newCopyResumeTestContext(t, "copy-resume-before-parent-pop", 1)
	srcDir := t.TempDir()
	dstDir := t.TempDir()
	writeCopyResumeFile(t, srcDir, "a-parent/a-child/only.txt", "done")
	writeCopyResumeFile(t, srcDir, "a-parent/z-after.txt", "after")
	writeCopyResumeFile(t, srcDir, "z-root.txt", "root")
	writeCopyResumeFile(t, dstDir, "a-parent/a-child/only.txt", "done")

	fsrc := newCopyResumeLocalFs(t, ctx, srcDir)
	fdst := newCopyResumeLocalFs(t, ctx, dstDir)
	seedCopyResumeDone(t, ctx, fsrc, fdst, "a-parent/a-child/only.txt")
	seedCopyResumeScanFrames(t, ctx, fsrc, fdst, []resume.ScanFrame{
		copyResumeFrame(t, ctx, fsrc, fdst, "", "", "a-parent"),
		copyResumeFrame(t, ctx, fsrc, fdst, "a-parent", "a-parent", "a-parent/a-child"),
		copyResumeFrame(t, ctx, fsrc, fdst, "a-parent/a-child", "a-parent/a-child", "a-parent/a-child/only.txt"),
	})

	accounting.GlobalStats().ResetCounters()
	defer accounting.GlobalStats().ResetCounters()

	err := CopyDir(ctx, fdst, fsrc, false)
	require.NoError(t, err)
	assert.Equal(t, "done", readCopyResumeFile(t, dstDir, "a-parent/a-child/only.txt"))
	assert.Equal(t, "after", readCopyResumeFile(t, dstDir, "a-parent/z-after.txt"))
	assert.Equal(t, "root", readCopyResumeFile(t, dstDir, "z-root.txt"))
	assert.Equal(t, int64(3), accounting.GlobalStats().GetTransfers())
}

func TestCopyDirResumeRestoresFromFileTreeFrames(t *testing.T) {
	ctx := newCopyResumeTestContext(t, "copy-resume-file-tree-frames", 1)
	srcDir := t.TempDir()
	dstDir := t.TempDir()
	writeCopyResumeFile(t, srcDir, "a-parent/a-child/a-done.txt", "done")
	writeCopyResumeFile(t, srcDir, "a-parent/a-child/z-pending.txt", "pending")
	writeCopyResumeFile(t, srcDir, "a-parent/z-after.txt", "after")
	writeCopyResumeFile(t, srcDir, "z-root.txt", "root")
	writeCopyResumeFile(t, dstDir, "a-parent/a-child/a-done.txt", "done")

	fsrc := newCopyResumeLocalFs(t, ctx, srcDir)
	fdst := newCopyResumeLocalFs(t, ctx, dstDir)
	seedCopyResumeDone(t, ctx, fsrc, fdst, "a-parent/a-child/a-done.txt")

	meta := copyResumeMeta(ctx, fsrc, fdst, false)
	legacyFrames := []resume.ScanFrame{
		copyResumeFrame(t, ctx, fsrc, fdst, "", "", "a-parent"),
		copyResumeFrame(t, ctx, fsrc, fdst, "a-parent", "a-parent", "a-parent/a-child"),
		copyResumeFrame(t, ctx, fsrc, fdst, "a-parent/a-child", "a-parent/a-child", "a-parent/a-child/a-done.txt"),
	}
	fileTreeFrames := make([]resume.FileFrame, 0, len(legacyFrames))
	for _, frame := range legacyFrames {
		fileTreeFrames = append(fileTreeFrames, resume.FileFrame{
			Dir:               frame.Dir,
			DstDir:            frame.DstDir,
			LastEntry:         frame.LastDoneEntryKey,
			ContinuationToken: frame.ContinuationToken,
			PageIndex:         frame.PageIndex,
		})
	}
	seedCopyResumeSnapshot(t, ctx, meta, resume.ScanState{
		Phase:    resume.PhaseCopySource,
		Target:   "src",
		FileTree: &resume.FileTreeScanState{Frames: fileTreeFrames},
	})

	accounting.GlobalStats().ResetCounters()
	defer accounting.GlobalStats().ResetCounters()

	err := CopyDir(ctx, fdst, fsrc, false)
	require.NoError(t, err)
	assert.Equal(t, "done", readCopyResumeFile(t, dstDir, "a-parent/a-child/a-done.txt"))
	assert.Equal(t, "pending", readCopyResumeFile(t, dstDir, "a-parent/a-child/z-pending.txt"))
	assert.Equal(t, "after", readCopyResumeFile(t, dstDir, "a-parent/z-after.txt"))
	assert.Equal(t, "root", readCopyResumeFile(t, dstDir, "z-root.txt"))
	assert.Equal(t, int64(4), accounting.GlobalStats().GetTransfers())
}

func TestResumeLegacyFileFramesPrefersEarliestInflightTask(t *testing.T) {
	ctx := newCopyResumeTestContext(t, "copy-resume-file-tree-inflight", 1)
	srcDir := t.TempDir()
	dstDir := t.TempDir()
	writeCopyResumeFile(t, srcDir, "a.txt", "a")
	writeCopyResumeFile(t, srcDir, "b.txt", "b")

	fsrc := newCopyResumeLocalFs(t, ctx, srcDir)
	fdst := newCopyResumeLocalFs(t, ctx, dstDir)

	s := &syncCopyMove{
		ctx:  ctx,
		fsrc: fsrc,
		fdst: fdst,
	}
	snapshot := &resume.Snapshot{
		Scan: resume.ScanState{
			FileTree: &resume.FileTreeScanState{
				Frames: []resume.FileFrame{{
					Dir:       "",
					DstDir:    "",
					LastEntry: resume.CursorKey(0, 1),
				}},
				Window: resume.FileWindowState{
					InflightTasks: []resume.FileTask{
						{
							TaskID: 2,
							StartFrameSnapshot: []resume.FileFrame{{
								Dir:       "",
								DstDir:    "",
								LastEntry: resume.CursorKey(0, 1),
							}},
						},
						{
							TaskID: 1,
							StartFrameSnapshot: []resume.FileFrame{{
								Dir:       "",
								DstDir:    "",
								LastEntry: resume.CursorKey(0, 0),
							}},
						},
					},
				},
			},
		},
	}

	frames := s.resumeLegacyFileFrames(snapshot)
	require.Len(t, frames, 1)
	assert.Equal(t, resume.CursorKey(0, 0), frames[0].LastDoneEntryKey)
}

func TestCopyDirResumeRestoresEmptyDirectoriesAndDirModTimes(t *testing.T) {
	ctx := newCopyResumeTestContext(t, "copy-resume-empty-dirs", 1)
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	writeCopyResumeFile(t, srcDir, "a-before.txt", "before")
	writeCopyResumeFile(t, srcDir, "c-parent/z-after.txt", "after")
	mkdirCopyResumeDir(t, srcDir, "b-empty")
	mkdirCopyResumeDir(t, srcDir, "c-parent/c-empty")

	baseTime := time.Unix(1_700_000_000, 0).UTC()
	setCopyResumeDirModTime(t, ctx, newCopyResumeLocalFs(t, ctx, srcDir), "b-empty", baseTime.Add(1*time.Minute))
	setCopyResumeDirModTime(t, ctx, newCopyResumeLocalFs(t, ctx, srcDir), "c-parent", baseTime.Add(2*time.Minute))
	setCopyResumeDirModTime(t, ctx, newCopyResumeLocalFs(t, ctx, srcDir), "c-parent/c-empty", baseTime.Add(3*time.Minute))

	writeCopyResumeFile(t, dstDir, "a-before.txt", "before")
	mkdirCopyResumeDir(t, dstDir, "b-empty")
	fsrc := newCopyResumeLocalFs(t, ctx, srcDir)
	fdst := newCopyResumeLocalFs(t, ctx, dstDir)
	setCopyResumeDirModTime(t, ctx, fdst, "b-empty", baseTime.Add(1*time.Minute))

	seedCopyResumeDoneWithCopyEmptySrcDirs(t, ctx, fsrc, fdst, "a-before.txt", true)
	seedCopyResumeScanFramesWithCopyEmptySrcDirs(t, ctx, fsrc, fdst, []resume.ScanFrame{
		copyResumeFrame(t, ctx, fsrc, fdst, "", "", "b-empty"),
		{Dir: "b-empty", DstDir: "b-empty"},
	}, true)
	store := pinCopyResumeStore(t, ctx)

	accounting.GlobalStats().ResetCounters()
	defer accounting.GlobalStats().ResetCounters()

	err := CopyDir(ctx, fdst, fsrc, true)
	require.NoError(t, err)

	assert.True(t, copyResumeDirExists(dstDir, "b-empty"))
	assert.True(t, copyResumeDirExists(dstDir, "c-parent"))
	assert.True(t, copyResumeDirExists(dstDir, "c-parent/c-empty"))
	assert.Equal(t, "after", readCopyResumeFile(t, dstDir, "c-parent/z-after.txt"))
	assertCopyResumeDirModTime(t, ctx, fsrc, fdst, srcDir, dstDir, "b-empty")
	assertCopyResumeDirModTime(t, ctx, fsrc, fdst, srcDir, dstDir, "c-parent")
	assertCopyResumeDirModTime(t, ctx, fsrc, fdst, srcDir, dstDir, "c-parent/c-empty")
	assert.True(t, copyResumeStateCleared(t, store))
}

func TestCopyDirResumeSkipsEmptyDirectoriesWhenCopyEmptySrcDirsDisabled(t *testing.T) {
	ctx := newCopyResumeTestContext(t, "copy-resume-skip-empty-dirs", 1)
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	writeCopyResumeFile(t, srcDir, "a-before.txt", "before")
	writeCopyResumeFile(t, srcDir, "c-parent/z-after.txt", "after")
	mkdirCopyResumeDir(t, srcDir, "b-empty")
	mkdirCopyResumeDir(t, srcDir, "c-parent/c-empty")

	fsrc := newCopyResumeLocalFs(t, ctx, srcDir)
	fdst := newCopyResumeLocalFs(t, ctx, dstDir)
	writeCopyResumeFile(t, dstDir, "a-before.txt", "before")
	seedCopyResumeDone(t, ctx, fsrc, fdst, "a-before.txt")
	seedCopyResumeSourceState(t, ctx, fsrc, fdst, "a-before.txt")
	store := pinCopyResumeStore(t, ctx)

	accounting.GlobalStats().ResetCounters()
	defer accounting.GlobalStats().ResetCounters()

	err := CopyDir(ctx, fdst, fsrc, false)
	require.NoError(t, err)

	assert.False(t, copyResumeDirExists(dstDir, "b-empty"))
	assert.True(t, copyResumeDirExists(dstDir, "c-parent"))
	assert.False(t, copyResumeDirExists(dstDir, "c-parent/c-empty"))
	assert.Equal(t, "after", readCopyResumeFile(t, dstDir, "c-parent/z-after.txt"))
	assert.True(t, copyResumeStateCleared(t, store))
}

func TestCopyDirResumeRejectsDifferentJobBinding(t *testing.T) {
	const wantErr = "resume state is incompatible with the current job, source, or destination"

	testCases := []struct {
		name     string
		seedMeta func(t *testing.T, ctx context.Context, fsrc, fdst fs.Fs) resume.Meta
	}{
		{
			name: "different-source",
			seedMeta: func(t *testing.T, ctx context.Context, fsrc, fdst fs.Fs) resume.Meta {
				return copyResumeMeta(ctx, newCopyResumeLocalFs(t, ctx, t.TempDir()), fdst, false)
			},
		},
		{
			name: "different-destination",
			seedMeta: func(t *testing.T, ctx context.Context, fsrc, fdst fs.Fs) resume.Meta {
				return copyResumeMeta(ctx, fsrc, newCopyResumeLocalFs(t, ctx, t.TempDir()), false)
			},
		},
		{
			name: "different-op",
			seedMeta: func(_ *testing.T, ctx context.Context, fsrc, fdst fs.Fs) resume.Meta {
				meta := copyResumeMeta(ctx, fsrc, fdst, false)
				meta.Op = "move"
				return meta
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := newCopyResumeTestContext(t, "copy-resume-incompatible-"+tc.name, 1)
			srcDir := t.TempDir()
			dstDir := t.TempDir()
			writeCopyResumeFile(t, srcDir, "done.txt", "done")
			writeCopyResumeFile(t, srcDir, "pending.txt", "pending")
			fsrc := newCopyResumeLocalFs(t, ctx, srcDir)
			fdst := newCopyResumeLocalFs(t, ctx, dstDir)
			writeCopyResumeFile(t, dstDir, "done.txt", "done")

			meta := tc.seedMeta(t, ctx, fsrc, fdst)
			seedCopyResumeSnapshot(t, ctx, meta, resume.ScanState{
				Phase:  resume.PhaseCopySource,
				Target: "src",
				Frames: []resume.ScanFrame{{Dir: "", DstDir: ""}},
			})
			store := pinCopyResumeStore(t, ctx)

			accounting.GlobalStats().ResetCounters()
			defer accounting.GlobalStats().ResetCounters()

			err := CopyDir(ctx, fdst, fsrc, false)
			require.EqualError(t, err, wantErr)
			_, statErr := os.Stat(filepath.Join(dstDir, "pending.txt"))
			assert.Error(t, statErr)
			assert.False(t, copyResumeStateCleared(t, store))
		})
	}
}

func TestCopyDirResumeErrorLimitFail(t *testing.T) {
	ctx := newCopyResumeTestContext(t, "copy-resume-error-limit-fail", 1)
	ci := fs.GetConfig(ctx)
	ci.ResumeErrorLimit = 0
	ci.ResumeErrorLimitAction = fs.ResumeErrorLimitActionContinue

	srcDir := t.TempDir()
	dstDir := t.TempDir()
	writeCopyResumeFile(t, srcDir, "a-fail.txt", "fail")
	writeCopyResumeFile(t, srcDir, "sub/success.txt", "success")

	baseSrc := newCopyResumeLocalFs(t, ctx, srcDir)
	fsrc := &copyResumeFailingFs{
		Fs:   baseSrc,
		fail: map[string]error{"a-fail.txt": errCopyResumeInjected},
	}
	fdst := newCopyResumeLocalFs(t, ctx, dstDir)
	store := pinCopyResumeStore(t, ctx)

	accounting.GlobalStats().ResetCounters()
	defer accounting.GlobalStats().ResetCounters()

	err := CopyDir(ctx, fdst, fsrc, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "resume pending failed item limit exceeded")

	snapshot := copyResumeSnapshot(t, store)
	assert.Equal(t, int64(1), snapshot.Totals.PendingFailedCount)
	assert.True(t, snapshot.Scan.OverErrorLimit)

	failedRecords, err := store.ListFailed()
	require.NoError(t, err)
	require.Len(t, failedRecords, 1)
	assert.Equal(t, "a-fail.txt", failedRecords[0].SrcRemote)

	assert.Equal(t, "success", readCopyResumeFile(t, dstDir, "sub/success.txt"))
	assert.False(t, hasCopyResumeDone(t, ctx, store, fsrc, "sub/success.txt"))
}

func TestCopyDirResumeContinuesBelowErrorLimit(t *testing.T) {
	ctx := newCopyResumeTestContext(t, "copy-resume-below-error-limit", 1)
	ci := fs.GetConfig(ctx)
	ci.ResumeErrorLimit = 1

	srcDir := t.TempDir()
	dstDir := t.TempDir()
	writeCopyResumeFile(t, srcDir, "a-fail.txt", "fail")
	writeCopyResumeFile(t, srcDir, "sub/success.txt", "success")

	baseSrc := newCopyResumeLocalFs(t, ctx, srcDir)
	fsrc := &copyResumeFailingFs{
		Fs:   baseSrc,
		fail: map[string]error{"a-fail.txt": errCopyResumeInjected},
	}
	fdst := newCopyResumeLocalFs(t, ctx, dstDir)
	store := pinCopyResumeStore(t, ctx)

	accounting.GlobalStats().ResetCounters()
	defer accounting.GlobalStats().ResetCounters()

	err := CopyDir(ctx, fdst, fsrc, false)
	require.ErrorContains(t, err, errCopyResumeInjected.Error())

	snapshot := copyResumeSnapshot(t, store)
	assert.Equal(t, int64(1), snapshot.Totals.PendingFailedCount)
	assert.False(t, snapshot.Scan.OverErrorLimit)

	failedRecords, err := store.ListFailed()
	require.NoError(t, err)
	require.Len(t, failedRecords, 1)
	assert.Equal(t, "a-fail.txt", failedRecords[0].SrcRemote)

	assert.Equal(t, "success", readCopyResumeFile(t, dstDir, "sub/success.txt"))
	assert.False(t, hasCopyResumeDone(t, ctx, store, fsrc, "sub/success.txt"))
}

func TestCopyDirResumeClearsStateAndRestoresHistoryOnSuccess(t *testing.T) {
	ctx := newCopyResumeTestContext(t, "copy-resume-clear-state", 1)
	srcDir := t.TempDir()
	dstDir := t.TempDir()
	writeCopyResumeFile(t, srcDir, "done.txt", "done")
	writeCopyResumeFile(t, dstDir, "done.txt", "done")
	writeCopyResumeFile(t, srcDir, "pending.txt", "pending")

	fsrc := newCopyResumeLocalFs(t, ctx, srcDir)
	fdst := newCopyResumeLocalFs(t, ctx, dstDir)
	seedCopyResumeDone(t, ctx, fsrc, fdst, "done.txt")
	store := pinCopyResumeStore(t, ctx)

	accounting.GlobalStats().ResetCounters()
	defer accounting.GlobalStats().ResetCounters()

	err := CopyDir(ctx, fdst, fsrc, false)
	require.NoError(t, err)

	assert.Equal(t, "done", readCopyResumeFile(t, dstDir, "done.txt"))
	assert.Equal(t, "pending", readCopyResumeFile(t, dstDir, "pending.txt"))
	assert.True(t, copyResumeStateCleared(t, store))
	assert.Equal(t, int64(2), accounting.GlobalStats().GetTransfers())
	assert.Equal(t, int64(len("done")+len("pending")), accounting.GlobalStats().GetBytes())
	assert.ElementsMatch(t, []string{"done.txt", "pending.txt"}, copyResumeTransferredNames())
}

func TestCopyDirResumePreservesStateAfterFailure(t *testing.T) {
	ctx := newCopyResumeTestContext(t, "copy-resume-preserve-state", 1)
	srcDir := t.TempDir()
	dstDir := t.TempDir()
	writeCopyResumeFile(t, srcDir, "fail.txt", "fail")

	baseSrc := newCopyResumeLocalFs(t, ctx, srcDir)
	fsrc := &copyResumeFailingFs{
		Fs:   baseSrc,
		fail: map[string]error{"fail.txt": errCopyResumeInjected},
	}
	fdst := newCopyResumeLocalFs(t, ctx, dstDir)
	store := pinCopyResumeStore(t, ctx)

	accounting.GlobalStats().ResetCounters()
	defer accounting.GlobalStats().ResetCounters()

	err := CopyDir(ctx, fdst, fsrc, false)
	require.ErrorContains(t, err, errCopyResumeInjected.Error())

	snapshot := copyResumeSnapshot(t, store)
	assert.False(t, copyResumeStateCleared(t, store))
	assert.Equal(t, int64(1), snapshot.Totals.PendingFailedCount)
	assert.Equal(t, int64(1), snapshot.Totals.CumulativeErrorEvents)
	failedRecords, err := store.ListFailed()
	require.NoError(t, err)
	require.Len(t, failedRecords, 1)
	assert.Equal(t, "fail.txt", failedRecords[0].SrcRemote)
}

func TestCopyDirResumeRestoresTotalsAndHistoryAcrossRecovery(t *testing.T) {
	ctx := newCopyResumeTestContext(t, "copy-resume-restore-totals", 1)
	srcDir := t.TempDir()
	dstDir := t.TempDir()
	writeCopyResumeFile(t, srcDir, "done.txt", "done")
	writeCopyResumeFile(t, dstDir, "done.txt", "done")
	writeCopyResumeFile(t, srcDir, "retry.txt", "retry!")
	writeCopyResumeFile(t, srcDir, "pending.txt", "pending")

	fsrc := newCopyResumeLocalFs(t, ctx, srcDir)
	fdst := newCopyResumeLocalFs(t, ctx, dstDir)
	seedCopyResumeDone(t, ctx, fsrc, fdst, "done.txt")
	seedCopyResumeFailures(t, ctx, fsrc, fdst, "retry.txt")
	store := pinCopyResumeStore(t, ctx)

	accounting.GlobalStats().ResetCounters()
	defer accounting.GlobalStats().ResetCounters()

	err := CopyDir(ctx, fdst, fsrc, false)
	require.NoError(t, err)

	assert.Equal(t, "done", readCopyResumeFile(t, dstDir, "done.txt"))
	assert.Equal(t, "retry!", readCopyResumeFile(t, dstDir, "retry.txt"))
	assert.Equal(t, "pending", readCopyResumeFile(t, dstDir, "pending.txt"))
	assert.True(t, copyResumeStateCleared(t, store))
	assert.Equal(t, int64(3), accounting.GlobalStats().GetTransfers())
	assert.Equal(t, int64(len("done")+len("retry!")+len("pending")), accounting.GlobalStats().GetBytes())
	assert.Equal(t, int64(0), accounting.GlobalStats().GetErrors())
	names := copyResumeTransferredNames()
	assert.Contains(t, names, "done.txt")
	assert.Contains(t, names, "retry.txt")
	assert.Contains(t, names, "pending.txt")
	assert.GreaterOrEqual(t, len(names), 4)
}

func newCopyResumeTestContext(t *testing.T, jobID string, transfers int) context.Context {
	t.Helper()
	require.NoError(t, config.SetCacheDir(t.TempDir()))
	ctx, ci := fs.AddConfig(context.Background())
	ci.Resume = true
	ci.ResumeID = jobID
	ci.ResumeHistoryLimit = 16
	ci.ResumeErrorLimit = -1
	ci.Transfers = transfers
	ci.Checkers = transfers
	return ctx
}

func newCopyResumeLocalFs(t *testing.T, ctx context.Context, root string) fs.Fs {
	t.Helper()
	f, err := fs.NewFs(ctx, root)
	require.NoError(t, err)
	return f
}

func writeCopyResumeFile(t *testing.T, root, remote, content string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(remote))
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
}

func readCopyResumeFile(t *testing.T, root, remote string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(remote)))
	require.NoError(t, err)
	return string(data)
}

func seedCopyResumeFailures(t *testing.T, ctx context.Context, fsrc, fdst fs.Fs, remotes ...string) {
	seedCopyResumeFailuresWithCopyEmptySrcDirs(t, ctx, fsrc, fdst, remotes, false)
}

func seedCopyResumeFailuresWithCopyEmptySrcDirs(t *testing.T, ctx context.Context, fsrc, fdst fs.Fs, remotes []string, copyEmptySrcDirs bool) {
	t.Helper()
	meta := copyResumeMeta(ctx, fsrc, fdst, copyEmptySrcDirs)
	store, err := resume.Open(ctx, meta.JobID)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close(false))
	})

	initialScan := resume.ScanState{
		Phase:  resume.PhaseCopySource,
		Target: "src",
		Frames: []resume.ScanFrame{{Dir: "", DstDir: ""}},
	}
	snapshot, _, err := store.LoadOrInit(meta, initialScan)
	require.NoError(t, err)

	for _, remote := range remotes {
		srcObj, err := fsrc.NewObject(ctx, remote)
		require.NoError(t, err)
		now := time.Now()
		record := resume.FailedRecord{
			WorkKey:       resume.WorkKey("copy", srcObj.Remote(), resume.TargetRemote(ctx, srcObj), resume.EntryFingerprint(ctx, srcObj)),
			Op:            "copy",
			Kind:          "object",
			SrcRemote:     srcObj.Remote(),
			DstRemote:     resume.TargetRemote(ctx, srcObj),
			Fingerprint:   resume.EntryFingerprint(ctx, srcObj),
			Name:          srcObj.Remote(),
			Size:          srcObj.Size(),
			Checked:       false,
			What:          "transferring",
			FirstFailedAt: now,
			LastFailedAt:  now,
			LastError:     "seeded failure",
		}
		snapshot, err = store.CommitFailure(resume.FailureCommit{
			Failed:       record,
			Scan:         snapshot.Scan,
			Event:        resume.HistoryEvent{Kind: "failure", Name: record.Name, Size: record.Size, What: record.What, Error: record.LastError, StartedAt: now, CompletedAt: now},
			HistoryLimit: fs.GetConfig(ctx).ResumeHistoryLimit,
		})
		require.NoError(t, err)
	}
}

func seedCopyResumeDone(t *testing.T, ctx context.Context, fsrc, fdst fs.Fs, remote string) {
	seedCopyResumeDoneWithCopyEmptySrcDirs(t, ctx, fsrc, fdst, remote, false)
}

func seedCopyResumeDoneWithCopyEmptySrcDirs(t *testing.T, ctx context.Context, fsrc, fdst fs.Fs, remote string, copyEmptySrcDirs bool) {
	t.Helper()
	meta := copyResumeMeta(ctx, fsrc, fdst, copyEmptySrcDirs)
	store, err := resume.Open(ctx, meta.JobID)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close(false))
	})

	initialScan := resume.ScanState{
		Phase:  resume.PhaseCopySource,
		Target: "src",
		Frames: []resume.ScanFrame{{Dir: "", DstDir: ""}},
	}
	snapshot, _, err := store.LoadOrInit(meta, initialScan)
	require.NoError(t, err)

	srcObj, err := fsrc.NewObject(ctx, remote)
	require.NoError(t, err)
	done := resume.DoneRecord{
		WorkKey:     resume.WorkKey("copy", srcObj.Remote(), resume.TargetRemote(ctx, srcObj), resume.EntryFingerprint(ctx, srcObj)),
		Op:          "copy",
		Kind:        "object",
		SrcRemote:   srcObj.Remote(),
		DstRemote:   resume.TargetRemote(ctx, srcObj),
		Fingerprint: resume.EntryFingerprint(ctx, srcObj),
		Name:        srcObj.Remote(),
		Size:        srcObj.Size(),
		Files:       1,
		Objects:     1,
		Bytes:       srcObj.Size(),
		What:        "transferring",
		Outcome:     "copied",
		StartedAt:   time.Now(),
		CompletedAt: time.Now(),
	}
	_, err = store.CommitSuccess(resume.SuccessCommit{
		Done:         done,
		Scan:         snapshot.Scan,
		Event:        resume.HistoryEvent{Kind: "success", WorkKey: done.WorkKey, Name: done.Name, Size: done.Size, Bytes: done.Bytes, What: done.What, StartedAt: done.StartedAt, CompletedAt: done.CompletedAt},
		HistoryLimit: fs.GetConfig(ctx).ResumeHistoryLimit,
	})
	require.NoError(t, err)
}

func seedCopyResumeSourceState(t *testing.T, ctx context.Context, fsrc, fdst fs.Fs, lastDoneRemote string) {
	seedCopyResumeSourceStateWithCopyEmptySrcDirs(t, ctx, fsrc, fdst, lastDoneRemote, false)
}

func seedCopyResumeSourceStateWithCopyEmptySrcDirs(t *testing.T, ctx context.Context, fsrc, fdst fs.Fs, lastDoneRemote string, copyEmptySrcDirs bool) {
	t.Helper()
	seedCopyResumeScanFramesWithCopyEmptySrcDirs(t, ctx, fsrc, fdst, []resume.ScanFrame{
		copyResumeFrame(t, ctx, fsrc, fdst, "", "", lastDoneRemote),
	}, copyEmptySrcDirs)
}

func seedCopyResumeScanFrames(t *testing.T, ctx context.Context, fsrc, fdst fs.Fs, frames []resume.ScanFrame) {
	seedCopyResumeScanFramesWithCopyEmptySrcDirs(t, ctx, fsrc, fdst, frames, false)
}

func seedCopyResumeScanFramesWithCopyEmptySrcDirs(t *testing.T, ctx context.Context, fsrc, fdst fs.Fs, frames []resume.ScanFrame, copyEmptySrcDirs bool) {
	t.Helper()
	meta := copyResumeMeta(ctx, fsrc, fdst, copyEmptySrcDirs)
	scan := resume.ScanState{
		Phase:  resume.PhaseCopySource,
		Target: "src",
		Frames: append([]resume.ScanFrame(nil), frames...),
	}
	seedCopyResumeSnapshot(t, ctx, meta, scan)
}

func copyResumeFrame(t *testing.T, ctx context.Context, fsrc, fdst fs.Fs, dir, dstDir, lastDoneRemote string) resume.ScanFrame {
	t.Helper()
	keyer := resume.NewMatchKeyer(ctx, fdst)
	page, err := resume.ListDirPage(ctx, fsrc, dir, false, keyer.SrcKey, "")
	require.NoError(t, err)
	return resume.ScanFrame{
		Dir:              dir,
		DstDir:           dstDir,
		LastDoneEntryKey: copyResumeCursorKey(t, 0, page, lastDoneRemote),
		PageIndex:        0,
	}
}

func pinCopyResumeStore(t *testing.T, ctx context.Context) *resume.Store {
	t.Helper()
	store, err := resume.Open(ctx, fs.GetConfig(ctx).ResumeID)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close(false))
	})
	return store
}

func copyResumeSnapshot(t *testing.T, store *resume.Store) resume.Snapshot {
	t.Helper()
	snapshot, err := store.Snapshot()
	require.NoError(t, err)
	return snapshot
}

func copyResumeStateCleared(t *testing.T, store *resume.Store) bool {
	t.Helper()
	return copyResumeSnapshot(t, store).Meta.JobID == ""
}

func hasCopyResumeDone(t *testing.T, ctx context.Context, store *resume.Store, fsrc fs.Fs, remote string) bool {
	t.Helper()
	srcObj, err := fsrc.NewObject(ctx, remote)
	require.NoError(t, err)
	workKey := resume.WorkKey("copy", srcObj.Remote(), resume.TargetRemote(ctx, srcObj), resume.EntryFingerprint(ctx, srcObj))
	done, err := store.HasDone(workKey)
	require.NoError(t, err)
	return done
}

func copyResumeTransferredNames() []string {
	transferred := accounting.GlobalStats().Transferred()
	names := make([]string, 0, len(transferred))
	for _, snapshot := range transferred {
		names = append(names, snapshot.Name)
	}
	return names
}

func copyResumeCursorKey(t *testing.T, pageIndex int, page resume.DirPage, remote string) string {
	t.Helper()
	for index, entry := range page.Entries {
		if entry.Remote() == remote {
			return resume.CursorKey(pageIndex, index)
		}
	}
	t.Fatalf("entry %q not found in page", remote)
	return ""
}

func copyResumeMeta(ctx context.Context, fsrc, fdst fs.Fs, copyEmptySrcDirs bool) resume.Meta {
	meta := resume.NewMeta(ctx, "copy", fsrc, fdst)
	meta.JobID = fs.GetConfig(ctx).ResumeID
	_ = copyEmptySrcDirs
	return meta
}

func seedCopyResumeSnapshot(t *testing.T, ctx context.Context, meta resume.Meta, scan resume.ScanState) {
	t.Helper()
	store, err := resume.Open(ctx, meta.JobID)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close(false))
	})

	_, _, err = store.LoadOrInit(meta, resume.ScanState{
		Phase:  resume.PhaseCopySource,
		Target: "src",
		Frames: []resume.ScanFrame{{Dir: "", DstDir: ""}},
	})
	require.NoError(t, err)
	require.NoError(t, store.SaveScan(scan))
}

func mkdirCopyResumeDir(t *testing.T, root, remote string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Join(root, filepath.FromSlash(remote)), 0o755))
}

func setCopyResumeDirModTime(t *testing.T, ctx context.Context, f fs.Fs, remote string, modTime time.Time) {
	t.Helper()
	_, err := operations.SetDirModTime(ctx, f, nil, remote, modTime)
	require.NoError(t, err)
}

func copyResumeDirExists(root, remote string) bool {
	info, err := os.Stat(filepath.Join(root, filepath.FromSlash(remote)))
	return err == nil && info.IsDir()
}

func assertCopyResumeDirModTime(t *testing.T, ctx context.Context, fsrc, fdst fs.Fs, srcRoot, dstRoot, remote string) {
	t.Helper()
	precision := fs.GetModifyWindow(ctx, fsrc, fdst)
	srcInfo, err := os.Stat(filepath.Join(srcRoot, filepath.FromSlash(remote)))
	require.NoError(t, err)
	dstInfo, err := os.Stat(filepath.Join(dstRoot, filepath.FromSlash(remote)))
	require.NoError(t, err)
	fstest.AssertTimeEqualWithPrecision(t, remote, srcInfo.ModTime(), dstInfo.ModTime(), precision)
}

type copyResumeProbe struct {
	active atomic.Int32
	max    atomic.Int32
	delay  time.Duration
}

type copyResumeCountingFs struct {
	fs.Fs
	listCalls atomic.Int32
}

func (f *copyResumeCountingFs) List(ctx context.Context, dir string) (fs.DirEntries, error) {
	f.listCalls.Add(1)
	return f.Fs.List(ctx, dir)
}

type copyResumeFailListOnceFs struct {
	fs.Fs
	failDir string
	err     error
	failed  atomic.Bool
}

func (f *copyResumeFailListOnceFs) List(ctx context.Context, dir string) (fs.DirEntries, error) {
	if dir == f.failDir && !f.failed.Swap(true) {
		return nil, f.err
	}
	return f.Fs.List(ctx, dir)
}

type copyResumePagedSourceFs struct {
	fs.Fs
	pageSize int
}

func (f *copyResumePagedSourceFs) ResumeListDir(ctx context.Context, dir, continuationToken string) (entries fs.DirEntries, nextContinuationToken string, tokenAccepted bool, err error) {
	entries, err = f.Fs.List(ctx, dir)
	if err != nil {
		return nil, "", false, err
	}
	sort.SliceStable(entries, func(i, j int) bool {
		return entries[i].Remote() < entries[j].Remote()
	})
	start := 0
	if continuationToken != "" {
		start, err = strconv.Atoi(continuationToken)
		if err != nil {
			return nil, "", false, err
		}
	}
	if start >= len(entries) {
		return nil, "", true, nil
	}
	end := start + f.pageSize
	if end > len(entries) {
		end = len(entries)
	} else {
		nextContinuationToken = strconv.Itoa(end)
	}
	return append(fs.DirEntries(nil), entries[start:end]...), nextContinuationToken, true, nil
}

type copyResumeFailingFs struct {
	fs.Fs
	fail map[string]error
}

func (f *copyResumeFailingFs) List(ctx context.Context, dir string) (fs.DirEntries, error) {
	entries, err := f.Fs.List(ctx, dir)
	if err != nil {
		return nil, err
	}
	out := make(fs.DirEntries, 0, len(entries))
	for _, entry := range entries {
		if obj, ok := entry.(fs.Object); ok {
			out = append(out, &copyResumeFailingObject{Object: obj, parent: f})
			continue
		}
		out = append(out, entry)
	}
	return out, nil
}

func (f *copyResumeFailingFs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	obj, err := f.Fs.NewObject(ctx, remote)
	if err != nil {
		return nil, err
	}
	return &copyResumeFailingObject{Object: obj, parent: f}, nil
}

type copyResumeFailingObject struct {
	fs.Object
	parent *copyResumeFailingFs
}

func (o *copyResumeFailingObject) Fs() fs.Info {
	return o.parent
}

func (o *copyResumeFailingObject) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	if err := o.parent.fail[o.Object.Remote()]; err != nil {
		return nil, err
	}
	return o.Object.Open(ctx, options...)
}

type copyResumeProbeFs struct {
	fs.Fs
	probe *copyResumeProbe
}

func (f *copyResumeProbeFs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	obj, err := f.Fs.NewObject(ctx, remote)
	if err != nil {
		return nil, err
	}
	return &copyResumeProbeObject{Object: obj, parent: f, probe: f.probe}, nil
}

type copyResumeProbeObject struct {
	fs.Object
	parent *copyResumeProbeFs
	probe  *copyResumeProbe
}

func (o *copyResumeProbeObject) Fs() fs.Info {
	return o.parent
}

func (o *copyResumeProbeObject) Open(ctx context.Context, options ...fs.OpenOption) (rc io.ReadCloser, err error) {
	current := o.probe.active.Add(1)
	updateCopyResumeMax(&o.probe.max, current)
	time.Sleep(o.probe.delay)
	o.probe.active.Add(-1)
	return o.Object.Open(ctx, options...)
}

func updateCopyResumeMax(max *atomic.Int32, candidate int32) {
	for {
		current := max.Load()
		if candidate <= current {
			return
		}
		if max.CompareAndSwap(current, candidate) {
			return
		}
	}
}
