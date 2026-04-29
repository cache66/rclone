package resume

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/rclone/rclone/fs/accounting"
	"github.com/stretchr/testify/assert"
)

func TestRestoreAccounting(t *testing.T) {
	startTime := time.Now().Add(-time.Minute)
	ctx := accounting.WithStatsGroup(context.Background(), fmt.Sprintf("resume-restore-%d", time.Now().UnixNano()))
	stats := accounting.Stats(ctx)

	RestoreAccounting(ctx, Snapshot{
		Totals: CounterState{
			StartTime: startTime,
			Bytes:     123,
			Checks:    4,
			Files:     3,
			Objects:   9,
		},
		History: []HistoryEvent{
			{
				Name:        "checked.txt",
				Size:        7,
				Bytes:       7,
				Checked:     true,
				What:        "checking",
				StartedAt:   startTime.Add(3 * time.Second),
				CompletedAt: startTime.Add(4 * time.Second),
			},
			{
				Name:        "failed.txt",
				Size:        9,
				Bytes:       9,
				What:        "transferring",
				StartedAt:   startTime.Add(5 * time.Second),
				CompletedAt: startTime.Add(6 * time.Second),
				Error:       "boom",
			},
		},
	})

	assert.Equal(t, int64(123), stats.GetBytes())
	assert.Equal(t, int64(4), stats.GetChecks())
	assert.Equal(t, int64(3), stats.GetTransfers())
	assert.False(t, stats.Errored())

	transferred := stats.Transferred()
	assert.Len(t, transferred, 2)

	remote, err := stats.RemoteStats(true)
	assert.NoError(t, err)
	speed, ok := remote["speed"].(float64)
	assert.True(t, ok)
	assert.Greater(t, speed, 0.0)
}
