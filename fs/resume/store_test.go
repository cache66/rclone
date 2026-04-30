package resume

import (
	"context"
	"testing"
	"time"

	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/lib/kv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStoreFailureSuccessLifecycle(t *testing.T) {
	cacheDir := t.TempDir()
	require.NoError(t, config.SetCacheDir(cacheDir))

	ctx := context.Background()
	meta := Meta{
		FormatVersion: FormatVersion,
		JobID:         "resume-test",
		Op:            "copy",
		SrcConfig:     "src",
		DstConfig:     "dst",
	}
	initialScan := ScanState{
		Phase:  PhaseCopySource,
		Target: "src",
		Frames: []ScanFrame{{Dir: ""}},
	}

	store, err := Open(ctx, meta.JobID)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close(true))
	}()

	snapshot, created, err := store.LoadOrInit(meta, initialScan)
	require.NoError(t, err)
	assert.True(t, created)
	assert.Equal(t, int64(0), snapshot.Totals.PendingFailedCount)

	startedAt := time.Now()
	failed := FailedRecord{
		WorkKey:       "copy:file",
		Op:            "copy",
		Kind:          "object",
		SrcRemote:     "src/file.txt",
		DstRemote:     "dst/file.txt",
		Fingerprint:   "fingerprint",
		Name:          "src/file.txt",
		Size:          10,
		What:          "transferring",
		FirstFailedAt: startedAt,
		LastFailedAt:  startedAt,
		LastError:     "boom",
	}
	snapshot, err = store.CommitFailure(FailureCommit{
		Failed:       failed,
		Scan:         initialScan,
		Event:        HistoryEvent{Kind: "failure", Name: failed.Name, Error: failed.LastError},
		HistoryLimit: 10,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), snapshot.Totals.PendingFailedCount)
	assert.Equal(t, int64(1), snapshot.Totals.CumulativeErrorEvents)
	assert.Equal(t, int64(1), snapshot.Run.PendingFailedCount)

	failed.LastError = "boom again"
	failed.LastFailedAt = startedAt.Add(time.Second)
	snapshot, err = store.CommitFailure(FailureCommit{
		Failed:       failed,
		Scan:         initialScan,
		Event:        HistoryEvent{Kind: "failure", Name: failed.Name, Error: failed.LastError},
		HistoryLimit: 10,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), snapshot.Totals.PendingFailedCount)
	assert.Equal(t, int64(2), snapshot.Totals.CumulativeErrorEvents)

	failedRecords, err := store.ListFailed()
	require.NoError(t, err)
	require.Len(t, failedRecords, 1)
	assert.Equal(t, int64(2), failedRecords[0].Failures)
	assert.Equal(t, "boom again", failedRecords[0].LastError)

	done := DoneRecord{
		WorkKey:     failed.WorkKey,
		Op:          "copy",
		Kind:        "object",
		SrcRemote:   failed.SrcRemote,
		DstRemote:   failed.DstRemote,
		Fingerprint: failed.Fingerprint,
		Name:        failed.Name,
		Size:        failed.Size,
		Files:       1,
		Objects:     1,
		Bytes:       failed.Size,
		What:        "transferring",
		Outcome:     "copied",
		StartedAt:   startedAt,
		CompletedAt: startedAt.Add(2 * time.Second),
	}
	snapshot, err = store.CommitSuccess(SuccessCommit{
		Done:         done,
		Scan:         initialScan,
		Event:        HistoryEvent{Kind: "success", Name: done.Name, Bytes: done.Bytes},
		HistoryLimit: 10,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(0), snapshot.Totals.PendingFailedCount)
	assert.Equal(t, int64(1), snapshot.Totals.RecoveredFromError)
	assert.Equal(t, int64(1), snapshot.Totals.Files)
	assert.Equal(t, int64(1), snapshot.Totals.Objects)
	assert.Equal(t, int64(10), snapshot.Totals.Bytes)

	snapshot, err = store.CommitSuccess(SuccessCommit{
		Done:         done,
		Scan:         initialScan,
		Event:        HistoryEvent{Kind: "success", Name: done.Name, Bytes: done.Bytes},
		HistoryLimit: 10,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), snapshot.Totals.Files)
	assert.Equal(t, int64(1), snapshot.Totals.Objects)
	assert.Equal(t, int64(10), snapshot.Totals.Bytes)

	doneExists, err := store.HasDone(done.WorkKey)
	require.NoError(t, err)
	assert.True(t, doneExists)

	failedRecords, err = store.ListFailed()
	require.NoError(t, err)
	assert.Empty(t, failedRecords)
}

func TestLoadOrInitRejectsCorruptMeta(t *testing.T) {
	cacheDir := t.TempDir()
	require.NoError(t, config.SetCacheDir(cacheDir))

	ctx := context.Background()
	store, err := Open(ctx, "resume-corrupt-meta")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close(true))
	}()

	err = store.db.Do(true, bucketOp(func(ctx context.Context, b kv.Bucket) error {
		return b.Put([]byte(store.metaKey()), []byte("{not-json"))
	}))
	require.NoError(t, err)

	_, _, err = store.LoadOrInit(Meta{
		FormatVersion: FormatVersion,
		JobID:         "resume-corrupt-meta",
		Op:            "copy",
		SrcConfig:     "src",
		DstConfig:     "dst",
	}, ScanState{
		Phase:  PhaseCopySource,
		Target: "src",
		Frames: []ScanFrame{{Dir: ""}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decode resume meta")
}

func TestSnapshotReturnsEmptyWhenStateMissing(t *testing.T) {
	cacheDir := t.TempDir()
	require.NoError(t, config.SetCacheDir(cacheDir))

	ctx := context.Background()
	store, err := Open(ctx, "resume-empty-snapshot")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close(true))
	}()

	snapshot, err := store.Snapshot()
	require.NoError(t, err)
	assert.Equal(t, Snapshot{}, snapshot)
}

func TestCommitSuccessBatchAggregatesSnapshotRewrite(t *testing.T) {
	cacheDir := t.TempDir()
	require.NoError(t, config.SetCacheDir(cacheDir))

	ctx := context.Background()
	meta := Meta{
		FormatVersion: FormatVersion,
		JobID:         "resume-batch-success",
		Op:            "copy",
		SrcConfig:     "src",
		DstConfig:     "dst",
	}
	initialScan := ScanState{
		Phase:  PhaseCopySource,
		Target: "src",
		Frames: []ScanFrame{{Dir: ""}},
	}

	store, err := Open(ctx, meta.JobID)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close(true))
	}()

	_, _, err = store.LoadOrInit(meta, initialScan)
	require.NoError(t, err)

	startedAt := time.Now()
	snapshot, err := store.CommitSuccessBatch(SuccessBatchCommit{
		Commits: []SuccessCommit{
			{
				Done: DoneRecord{
					WorkKey:     "copy:file1",
					Op:          "copy",
					Kind:        "object",
					SrcRemote:   "src/file1",
					DstRemote:   "dst/file1",
					Fingerprint: "fp1",
					Name:        "src/file1",
					Size:        10,
					Files:       1,
					Objects:     1,
					Bytes:       10,
					What:        "transferring",
					Outcome:     "copied",
					StartedAt:   startedAt,
					CompletedAt: startedAt.Add(time.Second),
				},
				Scan:         initialScan,
				Event:        HistoryEvent{Kind: "success", Name: "src/file1", Bytes: 10},
				HistoryLimit: 10,
			},
			{
				Done: DoneRecord{
					WorkKey:     "copy:file2",
					Op:          "copy",
					Kind:        "object",
					SrcRemote:   "src/file2",
					DstRemote:   "dst/file2",
					Fingerprint: "fp2",
					Name:        "src/file2",
					Size:        20,
					Files:       1,
					Objects:     1,
					Bytes:       20,
					What:        "transferring",
					Outcome:     "copied",
					StartedAt:   startedAt,
					CompletedAt: startedAt.Add(2 * time.Second),
				},
				Scan:         initialScan,
				Event:        HistoryEvent{Kind: "success", Name: "src/file2", Bytes: 20},
				HistoryLimit: 10,
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(2), snapshot.Totals.Files)
	assert.Equal(t, int64(2), snapshot.Totals.Objects)
	assert.Equal(t, int64(30), snapshot.Totals.Bytes)
	assert.Len(t, snapshot.History, 2)

	done1, err := store.HasDone("copy:file1")
	require.NoError(t, err)
	assert.True(t, done1)
	done2, err := store.HasDone("copy:file2")
	require.NoError(t, err)
	assert.True(t, done2)
}
