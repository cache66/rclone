package resume

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/rclone/rclone/fs"
)

const (
	// FormatVersion is incremented whenever the on-disk resume state schema
	// changes in an incompatible way.
	FormatVersion = 5

	PhaseCopySource = "copy_source"
)

// Meta is the persisted signature of a resumable job.
type Meta struct {
	FormatVersion int       `json:"format_version"`
	JobID         string    `json:"job_id"`
	CreatedAt     time.Time `json:"created_at"`
	Op            string    `json:"op"`
	SrcConfig     string    `json:"src_config"`
	DstConfig     string    `json:"dst_config"`
}

// NewMeta builds the persisted job signature used to validate resume state.
func NewMeta(ctx context.Context, op string, fsrc, fdst fs.Fs) Meta {
	_ = ctx
	return Meta{
		FormatVersion: FormatVersion,
		CreatedAt:     time.Now(),
		Op:            op,
		SrcConfig:     fs.ConfigStringFull(fsrc),
		DstConfig:     fs.ConfigStringFull(fdst),
	}
}

// DerivedJobID returns a stable identifier for the job when the user did not
// explicitly provide --resume-id.
func DerivedJobID(meta Meta) string {
	return hashString(fmt.Sprintf("%s|%s|%s", meta.Op, meta.SrcConfig, meta.DstConfig))
}

// Equal reports whether both persisted job signatures are compatible.
func (m Meta) Equal(other Meta) bool {
	return m.FormatVersion == other.FormatVersion &&
		m.Op == other.Op &&
		m.SrcConfig == other.SrcConfig &&
		m.DstConfig == other.DstConfig
}

// ScanFrame is a persisted DFS frame for the current directory traversal.
type ScanFrame struct {
	Dir               string `json:"dir"`
	DstDir            string `json:"dst_dir,omitempty"`
	LastDoneEntryKey  string `json:"last_done_entry_key,omitempty"` // cursor key within the current directory page
	ContinuationToken string `json:"continuation_token,omitempty"`  // backend page token for resumable listings
	PageIndex         int    `json:"page_index,omitempty"`          // logical page index within the current directory
}

// ScanState stores the resumable scan cursor.
type ScanState struct {
	Phase          string      `json:"phase"`
	Target         string      `json:"target"`
	Frames         []ScanFrame `json:"frames"`
	Complete       bool        `json:"complete"`
	OverErrorLimit bool        `json:"over_error_limit"`
}

// CounterState stores persisted counters for the lifetime of the job or the
// current process.
type CounterState struct {
	Files                 int64     `json:"files"`
	Objects               int64     `json:"objects"`
	Bytes                 int64     `json:"bytes"`
	Checks                int64     `json:"checks"`
	PendingFailedCount    int64     `json:"pending_failed_count"`
	CumulativeErrorEvents int64     `json:"cumulative_error_events"`
	RecoveredFromError    int64     `json:"recovered_from_error"`
	StartTime             time.Time `json:"start_time"`
}

// HistoryEvent stores recent completed success/failure events so progress and
// rc output can be restored.
type HistoryEvent struct {
	Kind        string    `json:"kind"`
	WorkKey     string    `json:"work_key"`
	Name        string    `json:"name"`
	Size        int64     `json:"size"`
	Bytes       int64     `json:"bytes"`
	Checked     bool      `json:"checked"`
	What        string    `json:"what"`
	StartedAt   time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at"`
	Error       string    `json:"error,omitempty"`
}

// DoneRecord stores a completed work item.
type DoneRecord struct {
	WorkKey     string    `json:"work_key"`
	Op          string    `json:"op"`
	Kind        string    `json:"kind"`
	SrcRemote   string    `json:"src_remote,omitempty"`
	DstRemote   string    `json:"dst_remote,omitempty"`
	Fingerprint string    `json:"fingerprint,omitempty"`
	Name        string    `json:"name"`
	Size        int64     `json:"size"`
	Files       int64     `json:"files"`
	Objects     int64     `json:"objects"`
	Bytes       int64     `json:"bytes"`
	Checks      int64     `json:"checks"`
	Checked     bool      `json:"checked"`
	What        string    `json:"what,omitempty"`
	Outcome     string    `json:"outcome,omitempty"`
	StartedAt   time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at"`
}

// FailedRecord stores an unresolved failed work item.
type FailedRecord struct {
	WorkKey       string    `json:"work_key"`
	Op            string    `json:"op"`
	Kind          string    `json:"kind"`
	SrcRemote     string    `json:"src_remote,omitempty"`
	DstRemote     string    `json:"dst_remote,omitempty"`
	Fingerprint   string    `json:"fingerprint,omitempty"`
	Name          string    `json:"name"`
	Size          int64     `json:"size"`
	Checked       bool      `json:"checked"`
	What          string    `json:"what,omitempty"`
	FirstFailedAt time.Time `json:"first_failed_at"`
	LastFailedAt  time.Time `json:"last_failed_at"`
	LastError     string    `json:"last_error"`
	Failures      int64     `json:"failures"`
}

// Snapshot is the materialized persisted state for a job.
type Snapshot struct {
	Meta    Meta           `json:"meta"`
	Scan    ScanState      `json:"scan"`
	Totals  CounterState   `json:"stats_total"`
	Run     CounterState   `json:"stats_run"`
	History []HistoryEvent `json:"history"`
}

func hashString(in string) string {
	sum := sha256.Sum256([]byte(in))
	return hex.EncodeToString(sum[:])
}
