package sync

import (
	"context"
	"testing"
	"time"

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
}
