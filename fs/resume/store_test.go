package resume

import (
	"context"
	"testing"
	"time"

	"github.com/rclone/rclone/fs/config"
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
