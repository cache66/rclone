package accounting

import (
	"errors"
	"time"
)

// RestoreCountersSnapshot contains persisted counter values that can be
// injected back into StatsInfo when resuming a job.
type RestoreCountersSnapshot struct {
	StartTime           time.Time
	Bytes               int64
	Checks              int64
	Transfers           int64
	Listed              int64
	ServerSideCopies    int64
	ServerSideCopyBytes int64
	ServerSideMoves     int64
	ServerSideMoveBytes int64
}

// RestoreTransferEvent is a persisted completed transfer/check event that can
// be replayed into StatsInfo so progress output remains continuous after a
// resume.
type RestoreTransferEvent struct {
	Name        string
	Size        int64
	Bytes       int64
	Checked     bool
	What        string
	StartedAt   time.Time
	CompletedAt time.Time
	Error       string
}

// RestoreCounters restores persisted counters without restoring historical
// errors, so current-run control flow remains unchanged.
func (s *StatsInfo) RestoreCounters(snapshot RestoreCountersSnapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.bytes = snapshot.Bytes
	s.checks = snapshot.Checks
	s.transfers = snapshot.Transfers
	s.listed = snapshot.Listed
	s.serverSideCopies = snapshot.ServerSideCopies
	s.serverSideCopyBytes = snapshot.ServerSideCopyBytes
	s.serverSideMoves = snapshot.ServerSideMoves
	s.serverSideMoveBytes = snapshot.ServerSideMoveBytes
	if !snapshot.StartTime.IsZero() {
		s.startTime = snapshot.StartTime
	}
}

// RestoreEventHistory injects completed transfer/check events back into the
// in-memory history window used by progress and rc stats output.
func (s *StatsInfo) RestoreEventHistory(events []RestoreTransferEvent) {
	if len(events) == 0 {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, event := range events {
		if event.Name == "" {
			continue
		}
		what := event.What
		if what == "" {
			if event.Checked {
				what = "checking"
			} else {
				what = "transferring"
			}
		}
		completedAt := event.CompletedAt
		if completedAt.IsZero() {
			completedAt = event.StartedAt
		}
		tr := &Transfer{
			stats:       s,
			remote:      event.Name,
			size:        event.Size,
			startedAt:   event.StartedAt,
			checking:    event.Checked,
			what:        what,
			err:         errorFromString(event.Error),
			completedAt: completedAt,
		}
		s.startedTransfers = append(s.startedTransfers, tr)
	}
}

func errorFromString(message string) error {
	if message == "" {
		return nil
	}
	return errors.New(message)
}
