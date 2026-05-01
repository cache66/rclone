package resume

import (
	"sync/atomic"
	"time"
)

var resumeMetrics = struct {
	saveScanTotal                   atomic.Int64
	saveScanNanosTotal              atomic.Int64
	commitSuccessBatchTotal         atomic.Int64
	commitSuccessBatchItemsTotal    atomic.Int64
	commitSuccessBatchNanosTotal    atomic.Int64
	objectFrontierCommitTotal       atomic.Int64
	objectFrontierCommittedSegments atomic.Int64
	objectFrontierBlockedTotal      atomic.Int64
	objectFrontierInflight          atomic.Int64
	objectFrontierPending           atomic.Int64
	objectSegmentsCreatedTotal      atomic.Int64
	objectSegmentObjectsTotal       atomic.Int64
	objectSegmentBytesTotal         atomic.Int64
	fileFrontierCommitTotal         atomic.Int64
	fileFrontierCommittedTasks      atomic.Int64
	fileFrontierBlockedTotal        atomic.Int64
	fileFrontierInflight            atomic.Int64
	fileFrontierPending             atomic.Int64
	fileScanPagesTotal              atomic.Int64
	fileScanDirectoriesTotal        atomic.Int64
	fileTasksCreatedTotal           atomic.Int64
	fileTaskFilesTotal              atomic.Int64
	fileTaskBytesTotal              atomic.Int64
	committedFiles                  atomic.Int64
	committedBytes                  atomic.Int64
}{}

// MetricsSnapshot exposes lightweight resume diagnostics for Prometheus.
type MetricsSnapshot struct {
	SaveScanTotal                   int64
	SaveScanSecondsTotal            float64
	CommitSuccessBatchTotal         int64
	CommitSuccessBatchItemsTotal    int64
	CommitSuccessBatchSecondsTotal  float64
	ObjectFrontierCommitTotal       int64
	ObjectFrontierCommittedSegments int64
	ObjectFrontierBlockedTotal      int64
	ObjectFrontierInflight          int64
	ObjectFrontierPending           int64
	ObjectSegmentsCreatedTotal      int64
	ObjectSegmentObjectsTotal       int64
	ObjectSegmentBytesTotal         int64
	FileFrontierCommitTotal         int64
	FileFrontierCommittedTasks      int64
	FileFrontierBlockedTotal        int64
	FileFrontierInflight            int64
	FileFrontierPending             int64
	FileScanPagesTotal              int64
	FileScanDirectoriesTotal        int64
	FileTasksCreatedTotal           int64
	FileTaskFilesTotal              int64
	FileTaskBytesTotal              int64
	CommittedFiles                  int64
	CommittedBytes                  int64
}

// Metrics returns a copy of the current process-wide resume diagnostics.
func Metrics() MetricsSnapshot {
	return MetricsSnapshot{
		SaveScanTotal:                   resumeMetrics.saveScanTotal.Load(),
		SaveScanSecondsTotal:            float64(resumeMetrics.saveScanNanosTotal.Load()) / float64(time.Second),
		CommitSuccessBatchTotal:         resumeMetrics.commitSuccessBatchTotal.Load(),
		CommitSuccessBatchItemsTotal:    resumeMetrics.commitSuccessBatchItemsTotal.Load(),
		CommitSuccessBatchSecondsTotal:  float64(resumeMetrics.commitSuccessBatchNanosTotal.Load()) / float64(time.Second),
		ObjectFrontierCommitTotal:       resumeMetrics.objectFrontierCommitTotal.Load(),
		ObjectFrontierCommittedSegments: resumeMetrics.objectFrontierCommittedSegments.Load(),
		ObjectFrontierBlockedTotal:      resumeMetrics.objectFrontierBlockedTotal.Load(),
		ObjectFrontierInflight:          resumeMetrics.objectFrontierInflight.Load(),
		ObjectFrontierPending:           resumeMetrics.objectFrontierPending.Load(),
		ObjectSegmentsCreatedTotal:      resumeMetrics.objectSegmentsCreatedTotal.Load(),
		ObjectSegmentObjectsTotal:       resumeMetrics.objectSegmentObjectsTotal.Load(),
		ObjectSegmentBytesTotal:         resumeMetrics.objectSegmentBytesTotal.Load(),
		FileFrontierCommitTotal:         resumeMetrics.fileFrontierCommitTotal.Load(),
		FileFrontierCommittedTasks:      resumeMetrics.fileFrontierCommittedTasks.Load(),
		FileFrontierBlockedTotal:        resumeMetrics.fileFrontierBlockedTotal.Load(),
		FileFrontierInflight:            resumeMetrics.fileFrontierInflight.Load(),
		FileFrontierPending:             resumeMetrics.fileFrontierPending.Load(),
		FileScanPagesTotal:              resumeMetrics.fileScanPagesTotal.Load(),
		FileScanDirectoriesTotal:        resumeMetrics.fileScanDirectoriesTotal.Load(),
		FileTasksCreatedTotal:           resumeMetrics.fileTasksCreatedTotal.Load(),
		FileTaskFilesTotal:              resumeMetrics.fileTaskFilesTotal.Load(),
		FileTaskBytesTotal:              resumeMetrics.fileTaskBytesTotal.Load(),
		CommittedFiles:                  resumeMetrics.committedFiles.Load(),
		CommittedBytes:                  resumeMetrics.committedBytes.Load(),
	}
}

// RecordCommittedTotals publishes the safely persisted resume totals. These
// counters reflect only the contiguous frontier that has been committed to the
// resume store, not speculative or out-of-order completed work.
func RecordCommittedTotals(files, bytes int64) {
	resumeMetrics.committedFiles.Store(files)
	resumeMetrics.committedBytes.Store(bytes)
}

func recordSaveScan(duration time.Duration) {
	resumeMetrics.saveScanTotal.Add(1)
	resumeMetrics.saveScanNanosTotal.Add(duration.Nanoseconds())
}

func recordCommitSuccessBatch(items int, duration time.Duration) {
	resumeMetrics.commitSuccessBatchTotal.Add(1)
	resumeMetrics.commitSuccessBatchItemsTotal.Add(int64(items))
	resumeMetrics.commitSuccessBatchNanosTotal.Add(duration.Nanoseconds())
}

// RecordObjectSegmentCreated increments object resume segment diagnostics.
func RecordObjectSegmentCreated(objects int, bytes int64) {
	resumeMetrics.objectSegmentsCreatedTotal.Add(1)
	resumeMetrics.objectSegmentObjectsTotal.Add(int64(objects))
	resumeMetrics.objectSegmentBytesTotal.Add(bytes)
}

// RecordObjectFrontierCommit updates object-resume frontier diagnostics.
func RecordObjectFrontierCommit(committedSegments, pending, inflight int, blocked bool) {
	if committedSegments > 0 {
		resumeMetrics.objectFrontierCommitTotal.Add(1)
		resumeMetrics.objectFrontierCommittedSegments.Add(int64(committedSegments))
	}
	if blocked {
		resumeMetrics.objectFrontierBlockedTotal.Add(1)
	}
	resumeMetrics.objectFrontierPending.Store(int64(pending))
	resumeMetrics.objectFrontierInflight.Store(int64(inflight))
}

// RecordFileFrontierCommit updates file-resume frontier diagnostics.
func RecordFileFrontierCommit(committedTasks, pending, inflight int, blocked bool) {
	if committedTasks > 0 {
		resumeMetrics.fileFrontierCommitTotal.Add(1)
		resumeMetrics.fileFrontierCommittedTasks.Add(int64(committedTasks))
	}
	if blocked {
		resumeMetrics.fileFrontierBlockedTotal.Add(1)
	}
	resumeMetrics.fileFrontierPending.Store(int64(pending))
	resumeMetrics.fileFrontierInflight.Store(int64(inflight))
}

// RecordFileScanPage increments the file scan page counter.
func RecordFileScanPage() {
	resumeMetrics.fileScanPagesTotal.Add(1)
}

// RecordFileScanDirectory increments the file directory descent counter.
func RecordFileScanDirectory() {
	resumeMetrics.fileScanDirectoriesTotal.Add(1)
}

// RecordFileTaskCreated increments file resume task diagnostics.
func RecordFileTaskCreated(files int, bytes int64) {
	resumeMetrics.fileTasksCreatedTotal.Add(1)
	resumeMetrics.fileTaskFilesTotal.Add(int64(files))
	resumeMetrics.fileTaskBytesTotal.Add(bytes)
}
