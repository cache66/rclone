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

type ObjectSegmentStatus string

const (
	ObjectSegmentScanned ObjectSegmentStatus = "SCANNED"
	ObjectSegmentRunning ObjectSegmentStatus = "RUNNING"
	ObjectSegmentDone    ObjectSegmentStatus = "DONE"
	ObjectSegmentFailed  ObjectSegmentStatus = "FAILED"
)

// ObjectSegment describes one object checkpoint segment in the lightweight
// S3/object resume pipeline.
type ObjectSegment struct {
	SegmentID     int64               `json:"segment_id"`
	StartAfterKey string              `json:"start_after_key,omitempty"`
	EndKey        string              `json:"end_key,omitempty"`
	ObjectCount   int                 `json:"object_count"`
	Status        ObjectSegmentStatus `json:"status,omitempty"`
}

// ObjectWindowState tracks the current in-flight object segments and the
// latest committed frontier segment.
type ObjectWindowState struct {
	WindowSize              int             `json:"window_size,omitempty"`
	InflightSegments        []ObjectSegment `json:"inflight_segments,omitempty"`
	CommitFrontierSegmentID int64           `json:"commit_frontier_segment_id,omitempty"`
}

// ObjectScanState stores the lightweight object scan cursor for bucket-based
// remotes such as S3.
type ObjectScanState struct {
	Bucket           string            `json:"bucket,omitempty"`
	RootPrefix       string            `json:"root_prefix,omitempty"`
	LastCommittedKey string            `json:"last_committed_key,omitempty"`
	LastScannedKey   string            `json:"last_scanned_key,omitempty"`
	Complete         bool              `json:"complete,omitempty"`
	OverErrorLimit   bool              `json:"over_error_limit,omitempty"`
	Window           ObjectWindowState `json:"window,omitempty"`
}

type FileTaskStatus string

const (
	FileTaskScanned FileTaskStatus = "SCANNED"
	FileTaskRunning FileTaskStatus = "RUNNING"
	FileTaskDone    FileTaskStatus = "DONE"
	FileTaskFailed  FileTaskStatus = "FAILED"
)

// FileFrame stores one lightweight directory traversal frame for the planned
// file/NAS frontier pipeline.
type FileFrame struct {
	Dir         string `json:"dir"`
	LastEntry   string `json:"last_entry,omitempty"`
	AlreadyInto bool   `json:"already_into,omitempty"`
}

// FileTreeScanState stores the lightweight file scan cursor for the planned
// file/NAS frontier pipeline.
type FileTreeScanState struct {
	Frames []FileFrame `json:"frames,omitempty"`
}

// FileTask describes one batched file task for the planned file/NAS frontier
// pipeline.
type FileTask struct {
	TaskID             int64          `json:"task_id"`
	StartFrameSnapshot []FileFrame    `json:"start_frame_snapshot,omitempty"`
	EndFile            string         `json:"end_file,omitempty"`
	FileCount          int            `json:"file_count"`
	Status             FileTaskStatus `json:"status,omitempty"`
}

// FileWindowState tracks the current in-flight file tasks and the latest
// committed file frontier.
type FileWindowState struct {
	WindowSize           int        `json:"window_size,omitempty"`
	InflightTasks        []FileTask `json:"inflight_tasks,omitempty"`
	CommitFrontierTaskID int64      `json:"commit_frontier_task_id,omitempty"`
}

// ScanState stores the resumable scan cursor.
type ScanState struct {
	Phase          string             `json:"phase"`
	Target         string             `json:"target"`
	Frames         []ScanFrame        `json:"frames"`
	Complete       bool               `json:"complete"`
	OverErrorLimit bool               `json:"over_error_limit"`
	Object         *ObjectScanState   `json:"object,omitempty"`
	FileTree       *FileTreeScanState `json:"file_tree,omitempty"`
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
