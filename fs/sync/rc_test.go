package sync

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/cache"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/rc"
	"github.com/rclone/rclone/fs/resume"
	"github.com/rclone/rclone/fstest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func rcNewRun(t *testing.T, method string) (*fstest.Run, *rc.Call) {
	if *fstest.RemoteName != "" {
		t.Skip("Skipping test on non local remote")
	}
	r := fstest.NewRun(t)
	call := rc.Calls.Get(method)
	assert.NotNil(t, call)
	cache.Put(r.LocalName, r.Flocal)
	cache.Put(r.FremoteName, r.Fremote)
	return r, call
}

// sync/copy: copy a directory from source remote to destination remote
func TestRcCopy(t *testing.T) {
	r, call := rcNewRun(t, "sync/copy")
	r.Mkdir(context.Background(), r.Fremote)

	file1 := r.WriteBoth(context.Background(), "file1", "file1 contents", t1)
	file2 := r.WriteFile("subdir/file2", "file2 contents", t2)
	file3 := r.WriteObject(context.Background(), "subdir/subsubdir/file3", "file3 contents", t3)

	r.CheckLocalItems(t, file1, file2)
	r.CheckRemoteItems(t, file1, file3)

	in := rc.Params{
		"srcFs": r.LocalName,
		"dstFs": r.FremoteName,
	}
	out, err := call.Fn(context.Background(), in)
	require.NoError(t, err)
	assert.Equal(t, rc.Params(nil), out)

	r.CheckLocalItems(t, file1, file2)
	r.CheckRemoteItems(t, file1, file2, file3)
}

func TestRcCopyWithResumeParams(t *testing.T) {
	require.NoError(t, config.SetCacheDir(t.TempDir()))
	r, call := rcNewRun(t, "sync/copy")
	r.Mkdir(context.Background(), r.Fremote)

	file1 := r.WriteBoth(context.Background(), "file1", "file1 contents", t1)
	file2 := r.WriteFile("subdir/file2", "file2 contents", t2)

	in := rc.Params{
		"srcFs":    r.LocalName,
		"dstFs":    r.FremoteName,
		"resume":   true,
		"resumeId": "resume-rc-copy",
	}
	out, err := call.Fn(context.Background(), in)
	require.NoError(t, err)
	assert.Equal(t, rc.Params(nil), out)

	r.CheckLocalItems(t, file1, file2)
	r.CheckRemoteItems(t, file1, file2)

	statusCall := rc.Calls.Get("sync/resume/status")
	require.NotNil(t, statusCall)
	statusOut, err := statusCall.Fn(context.Background(), rc.Params{"resumeId": "resume-rc-copy"})
	require.NoError(t, err)
	assert.Equal(t, "resume-rc-copy", statusOut["jobId"])
	assert.Equal(t, false, statusOut["found"])
}

// sync/move: move a directory from source remote to destination remote
func TestRcMove(t *testing.T) {
	r, call := rcNewRun(t, "sync/move")
	r.Mkdir(context.Background(), r.Fremote)

	file1 := r.WriteBoth(context.Background(), "file1", "file1 contents", t1)
	file2 := r.WriteFile("subdir/file2", "file2 contents", t2)
	file3 := r.WriteObject(context.Background(), "subdir/subsubdir/file3", "file3 contents", t3)

	r.CheckLocalItems(t, file1, file2)
	r.CheckRemoteItems(t, file1, file3)

	in := rc.Params{
		"srcFs": r.LocalName,
		"dstFs": r.FremoteName,
	}
	out, err := call.Fn(context.Background(), in)
	require.NoError(t, err)
	assert.Equal(t, rc.Params(nil), out)

	r.CheckLocalItems(t)
	r.CheckRemoteItems(t, file1, file2, file3)
}

// sync/sync: sync a directory from source remote to destination remote
func TestRcSync(t *testing.T) {
	r, call := rcNewRun(t, "sync/sync")
	r.Mkdir(context.Background(), r.Fremote)

	file1 := r.WriteBoth(context.Background(), "file1", "file1 contents", t1)
	file2 := r.WriteFile("subdir/file2", "file2 contents", t2)
	file3 := r.WriteObject(context.Background(), "subdir/subsubdir/file3", "file3 contents", t3)

	r.CheckLocalItems(t, file1, file2)
	r.CheckRemoteItems(t, file1, file3)

	in := rc.Params{
		"srcFs": r.LocalName,
		"dstFs": r.FremoteName,
	}
	out, err := call.Fn(context.Background(), in)
	require.NoError(t, err)
	assert.Equal(t, rc.Params(nil), out)

	r.CheckLocalItems(t, file1, file2)
	r.CheckRemoteItems(t, file1, file2)
}

func TestRcApplyResumeParams(t *testing.T) {
	ctx, err := rcApplyResumeParams(context.Background(), rc.Params{
		"resume":   true,
		"resumeId": "resume-apply-test",
	}, "copy")
	require.NoError(t, err)
	ci := fs.GetConfig(ctx)
	assert.True(t, ci.Resume)
	assert.Equal(t, "resume-apply-test", ci.ResumeID)

	ctx, err = rcApplyResumeParams(context.Background(), rc.Params{
		"resumeId": "resume-id-only",
	}, "copy")
	require.NoError(t, err)
	ci = fs.GetConfig(ctx)
	assert.True(t, ci.Resume)
	assert.Equal(t, "resume-id-only", ci.ResumeID)
}

func TestRcApplyResumeParamsRejectsNonCopy(t *testing.T) {
	_, err := rcApplyResumeParams(context.Background(), rc.Params{"resume": true}, "sync")
	require.EqualError(t, err, "resume parameters are supported for sync/copy only when calling copy")

	_, err = rcApplyResumeParams(context.Background(), rc.Params{"resumeId": "resume-nope"}, "move")
	require.EqualError(t, err, "resume parameters are supported for sync/copy only when calling copy")
}

func TestRcResumeStatusAndClear(t *testing.T) {
	require.NoError(t, config.SetCacheDir(t.TempDir()))
	r, call := rcNewRun(t, "sync/resume/status")
	clearCall := rc.Calls.Get("sync/resume/clear")
	require.NotNil(t, clearCall)

	ctx := context.Background()
	meta := resume.NewMeta(ctx, "copy", r.Flocal, r.Fremote)
	meta.JobID = "resume-rc-test"
	store, err := resume.Open(ctx, meta.JobID)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close(false))
	}()

	initialScan := resume.ScanState{
		Phase:  resume.PhaseCopySource,
		Target: "src",
		Frames: []resume.ScanFrame{{Dir: "", DstDir: ""}},
	}
	snapshot, _, err := store.LoadOrInit(meta, initialScan)
	require.NoError(t, err)
	now := time.Now()
	snapshot, err = store.CommitFailure(resume.FailureCommit{
		Failed: resume.FailedRecord{
			WorkKey:       "copy:file1",
			Op:            "copy",
			Kind:          "object",
			SrcRemote:     "file1",
			DstRemote:     "file1",
			Name:          "file1",
			Size:          5,
			What:          "transferring",
			FirstFailedAt: now,
			LastFailedAt:  now,
			LastError:     "boom",
		},
		Scan:         snapshot.Scan,
		Event:        resume.HistoryEvent{Kind: "failure", Name: "file1", Error: "boom", StartedAt: now, CompletedAt: now},
		HistoryLimit: 10,
	})
	require.NoError(t, err)

	out, err := call.Fn(ctx, rc.Params{"jobId": meta.JobID})
	require.NoError(t, err)
	assert.Equal(t, meta.JobID, out["jobId"])
	assert.Equal(t, true, out["found"])
	totals, ok := out["totals"].(resume.CounterState)
	require.True(t, ok)
	assert.Equal(t, int64(1), totals.PendingFailedCount)
	failed, ok := out["failed"].([]resume.FailedRecord)
	require.True(t, ok)
	require.Len(t, failed, 1)
	assert.Equal(t, "file1", failed[0].Name)

	clearOut, err := clearCall.Fn(ctx, rc.Params{"jobId": meta.JobID})
	require.NoError(t, err)
	assert.Equal(t, true, clearOut["found"])
	assert.Equal(t, true, clearOut["cleared"])

	out, err = call.Fn(ctx, rc.Params{"jobId": meta.JobID})
	require.NoError(t, err)
	assert.Equal(t, false, out["found"])
	failed, ok = out["failed"].([]resume.FailedRecord)
	require.True(t, ok)
	assert.Nil(t, failed)
}

func TestRcResumeImportRestoresSnapshot(t *testing.T) {
	require.NoError(t, config.SetCacheDir(t.TempDir()))
	r, statusCall := rcNewRun(t, "sync/resume/status")
	importCall := rc.Calls.Get("sync/resume/import")
	require.NotNil(t, importCall)

	ctx := context.Background()
	meta := resume.NewMeta(ctx, "copy", r.Flocal, r.Fremote)
	meta.JobID = "resume-rc-import"
	pinnedStore, err := resume.Open(ctx, meta.JobID)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, pinnedStore.Close(false))
	}()
	now := time.Now()
	payload := map[string]any{
		"jobId": meta.JobID,
		"found": true,
		"meta":  meta,
		"scan": resume.ScanState{
			Phase:  resume.PhaseCopySource,
			Target: "src",
			Frames: []resume.ScanFrame{{
				Dir:              "",
				DstDir:           "",
				LastDoneEntryKey: "0:10",
			}},
		},
		"totals": resume.CounterState{
			Files:              7,
			Bytes:              700,
			PendingFailedCount: 1,
			StartTime:          now,
		},
		"run": resume.CounterState{
			StartTime: now,
		},
		"history": []resume.HistoryEvent{{
			Kind:        "failure",
			Name:        "file7",
			Error:       "boom",
			StartedAt:   now,
			CompletedAt: now,
		}},
		"failed": []resume.FailedRecord{{
			WorkKey:       "copy:file7",
			Op:            "copy",
			Kind:          "object",
			SrcRemote:     "file7",
			DstRemote:     "file7",
			Name:          "file7",
			Size:          7,
			FirstFailedAt: now,
			LastFailedAt:  now,
			LastError:     "boom",
			Failures:      1,
		}},
	}
	data, err := json.Marshal(payload)
	require.NoError(t, err)
	payloadFile := filepath.Join(t.TempDir(), "resume-import.json")
	require.NoError(t, os.WriteFile(payloadFile, data, 0o600))

	importOut, err := importCall.Fn(ctx, rc.Params{
		"jobId":       meta.JobID,
		"payloadFile": payloadFile,
	})
	require.NoError(t, err)
	assert.Equal(t, meta.JobID, importOut["jobId"])
	assert.Equal(t, true, importOut["imported"])
	assert.Equal(t, false, importOut["previousFound"])

	out, err := statusCall.Fn(ctx, rc.Params{"jobId": meta.JobID})
	require.NoError(t, err)
	assert.Equal(t, true, out["found"])
	totals, ok := out["totals"].(resume.CounterState)
	require.True(t, ok)
	assert.Equal(t, int64(7), totals.Files)
	assert.Equal(t, int64(700), totals.Bytes)
	failed, ok := out["failed"].([]resume.FailedRecord)
	require.True(t, ok)
	require.Len(t, failed, 1)
	assert.Equal(t, "file7", failed[0].Name)
}

func TestRcResumeStatusWithoutState(t *testing.T) {
	require.NoError(t, config.SetCacheDir(t.TempDir()))
	_, call := rcNewRun(t, "sync/resume/status")

	out, err := call.Fn(context.Background(), rc.Params{"jobId": "missing-job"})
	require.NoError(t, err)
	assert.Equal(t, "missing-job", out["jobId"])
	assert.Equal(t, false, out["found"])
	failed, ok := out["failed"].([]resume.FailedRecord)
	require.True(t, ok)
	assert.Nil(t, failed)

	out, err = call.Fn(context.Background(), rc.Params{"resumeId": "missing-job"})
	require.NoError(t, err)
	assert.Equal(t, "missing-job", out["jobId"])
	assert.Equal(t, false, out["found"])
}

func TestRcResumeStatusAndClearDerivedJobID(t *testing.T) {
	require.NoError(t, config.SetCacheDir(t.TempDir()))
	r, call := rcNewRun(t, "sync/resume/status")
	clearCall := rc.Calls.Get("sync/resume/clear")
	require.NotNil(t, clearCall)

	ctx := context.Background()
	jobID := resume.DerivedJobID(resume.NewMeta(ctx, "copy", r.Flocal, r.Fremote))
	store, err := resume.Open(ctx, jobID)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close(false))
	}()

	_, _, err = store.LoadOrInit(resume.Meta{
		FormatVersion: resume.FormatVersion,
		JobID:         jobID,
		Op:            "copy",
		SrcConfig:     fs.ConfigStringFull(r.Flocal),
		DstConfig:     fs.ConfigStringFull(r.Fremote),
	}, resume.ScanState{
		Phase:  resume.PhaseCopySource,
		Target: "src",
		Frames: []resume.ScanFrame{{Dir: "", DstDir: ""}},
	})
	require.NoError(t, err)

	in := rc.Params{"srcFs": r.LocalName, "dstFs": r.FremoteName}
	out, err := call.Fn(ctx, in)
	require.NoError(t, err)
	assert.Equal(t, jobID, out["jobId"])
	assert.Equal(t, true, out["found"])

	clearOut, err := clearCall.Fn(ctx, in)
	require.NoError(t, err)
	assert.Equal(t, true, clearOut["cleared"])
}
