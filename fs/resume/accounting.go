package resume

import (
	"context"

	"github.com/rclone/rclone/fs/accounting"
)

// RestoreAccounting injects persisted totals and recent history into the live
// accounting stats so progress output remains continuous after a resume.
func RestoreAccounting(ctx context.Context, snapshot Snapshot) {
	accounting.Stats(ctx).RestoreCounters(accounting.RestoreCountersSnapshot{
		StartTime:           snapshot.Totals.StartTime,
		Bytes:               snapshot.Totals.Bytes,
		Checks:              snapshot.Totals.Checks,
		Transfers:           snapshot.Totals.Files,
		Listed:              snapshot.Totals.Objects,
		ServerSideCopies:    0,
		ServerSideCopyBytes: 0,
		ServerSideMoves:     0,
		ServerSideMoveBytes: 0,
	})
	events := make([]accounting.RestoreTransferEvent, 0, len(snapshot.History))
	for _, event := range snapshot.History {
		events = append(events, accounting.RestoreTransferEvent{
			Name:        event.Name,
			Size:        event.Size,
			Bytes:       event.Bytes,
			Checked:     event.Checked,
			What:        event.What,
			StartedAt:   event.StartedAt,
			CompletedAt: event.CompletedAt,
			Error:       event.Error,
		})
	}
	accounting.Stats(ctx).RestoreEventHistory(events)
}
