package resume

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/rclone/rclone/lib/kv"
)

const facility = "resume"

type bucketOp func(context.Context, kv.Bucket) error

func (op bucketOp) Do(ctx context.Context, b kv.Bucket) error {
	return op(ctx, b)
}

// Store is the persisted resume state for a single job ID.
type Store struct {
	ctx   context.Context
	db    *kv.DB
	jobID string
}

// SuccessCommit describes the data written when a work item completed.
type SuccessCommit struct {
	Done         DoneRecord
	Scan         ScanState
	Event        HistoryEvent
	HistoryLimit int
}

// SuccessBatchCommit describes a group of completed work items that can be
// persisted with a single snapshot rewrite.
type SuccessBatchCommit struct {
	Commits []SuccessCommit
}

// FailureCommit describes the data written when a work item failed.
type FailureCommit struct {
	Failed       FailedRecord
	Scan         ScanState
	Event        HistoryEvent
	HistoryLimit int
}

// Open opens or creates the persisted resume store for the provided job ID.
func Open(ctx context.Context, jobID string) (*Store, error) {
	db, err := kv.Start(ctx, facility, nil)
	if err != nil {
		return nil, err
	}
	return &Store{ctx: ctx, db: db, jobID: jobID}, nil
}

// Close releases the underlying key/value database handle.
func (s *Store) Close(remove bool) error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Stop(remove)
}

// LoadOrInit validates or creates the persisted state for a job.
func (s *Store) LoadOrInit(meta Meta, initialScan ScanState) (snapshot Snapshot, created bool, err error) {
	err = s.db.Do(false, bucketOp(func(ctx context.Context, b kv.Bucket) error {
		data := b.Get([]byte(s.metaKey()))
		if len(data) == 0 {
			created = true
			return nil
		}
		return snapshot.loadExisting(s, data, b)
	}))
	if err != nil && !errors.Is(err, kv.ErrEmpty) {
		return Snapshot{}, false, err
	}
	if created || errors.Is(err, kv.ErrEmpty) {
		created = true
		if meta.JobID == "" {
			meta.JobID = s.jobID
		}
		snapshot = Snapshot{
			Meta:   meta,
			Scan:   initialScan,
			Totals: CounterState{StartTime: time.Now()},
			Run:    CounterState{StartTime: time.Now()},
		}
		err = s.writeSnapshot(snapshot)
		return snapshot, true, err
	}
	if !snapshot.Meta.Equal(meta) {
		return Snapshot{}, false, fmt.Errorf("resume state is incompatible with the current job, source, or destination")
	}
	return snapshot, false, nil
}

func (snapshot *Snapshot) loadExisting(s *Store, metaData []byte, b kv.Bucket) error {
	if err := json.Unmarshal(metaData, &snapshot.Meta); err != nil {
		return fmt.Errorf("decode resume meta: %w", err)
	}
	_ = readJSONValue(b, s.scanKey(), &snapshot.Scan)
	_ = readJSONValue(b, s.totalKey(), &snapshot.Totals)
	_ = readJSONValue(b, s.runKey(), &snapshot.Run)
	_ = readJSONValue(b, s.historyKey(), &snapshot.History)
	return nil
}

// ResetRunCounters starts a fresh per-process counter window while leaving the
// lifetime totals untouched.
func (s *Store) ResetRunCounters(start time.Time) error {
	return s.db.Do(true, bucketOp(func(ctx context.Context, b kv.Bucket) error {
		run := CounterState{StartTime: start}
		return writeJSONValue(b, s.runKey(), run)
	}))
}

// Snapshot loads the latest persisted snapshot.
func (s *Store) Snapshot() (snapshot Snapshot, err error) {
	err = s.db.Do(false, bucketOp(func(ctx context.Context, b kv.Bucket) error {
		metaData := b.Get([]byte(s.metaKey()))
		if len(metaData) == 0 {
			return kv.ErrEmpty
		}
		return snapshot.loadExisting(s, metaData, b)
	}))
	if errors.Is(err, kv.ErrEmpty) {
		return Snapshot{}, nil
	}
	return snapshot, err
}

// SaveScan persists the current scan cursor.
func (s *Store) SaveScan(scan ScanState) error {
	started := time.Now()
	err := s.db.Do(true, bucketOp(func(ctx context.Context, b kv.Bucket) error {
		return writeJSONValue(b, s.scanKey(), scan)
	}))
	if err == nil {
		recordSaveScan(time.Since(started))
	}
	return err
}

// HasDone reports whether the work item was already completed.
func (s *Store) HasDone(workKey string) (found bool, err error) {
	err = s.db.Do(false, bucketOp(func(ctx context.Context, b kv.Bucket) error {
		found = len(b.Get([]byte(s.doneKey(workKey)))) != 0
		return nil
	}))
	if errors.Is(err, kv.ErrEmpty) {
		return false, nil
	}
	return found, err
}

// ListFailed returns all unresolved failed work items.
func (s *Store) ListFailed() (records []FailedRecord, err error) {
	err = s.db.Do(false, bucketOp(func(ctx context.Context, b kv.Bucket) error {
		records = make([]FailedRecord, 0)
		return scanJSONPrefix(b, s.failedPrefix(), func(data []byte) error {
			var record FailedRecord
			if err := json.Unmarshal(data, &record); err != nil {
				return err
			}
			records = append(records, record)
			return nil
		})
	}))
	if errors.Is(err, kv.ErrEmpty) {
		return nil, nil
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].LastFailedAt.Equal(records[j].LastFailedAt) {
			return records[i].Name < records[j].Name
		}
		return records[i].LastFailedAt.Before(records[j].LastFailedAt)
	})
	return records, err
}

// CommitSuccess persists a completed work item, updates totals once, removes a
// previously pending failure if present, updates history, then advances the
// scan cursor.
func (s *Store) CommitSuccess(commit SuccessCommit) (snapshot Snapshot, err error) {
	err = s.db.Do(true, bucketOp(func(ctx context.Context, b kv.Bucket) error {
		return s.commitSuccessLocked(b, commit, &snapshot)
	}))
	return snapshot, err
}

// CommitSuccessBatch persists multiple completed work items while rewriting the
// snapshot only once. This dramatically reduces resume bookkeeping overhead for
// healthy small-object workloads.
func (s *Store) CommitSuccessBatch(commit SuccessBatchCommit) (snapshot Snapshot, err error) {
	if len(commit.Commits) == 0 {
		return s.Snapshot()
	}
	started := time.Now()
	err = s.db.Do(true, bucketOp(func(ctx context.Context, b kv.Bucket) error {
		metaData := b.Get([]byte(s.metaKey()))
		if len(metaData) == 0 {
			return kv.ErrEmpty
		}
		if err := snapshot.loadExisting(s, metaData, b); err != nil {
			return err
		}
		for _, item := range commit.Commits {
			if err := s.applySuccessLocked(b, item, &snapshot); err != nil {
				return err
			}
		}
		return s.writeSnapshotLocked(b, snapshot)
	}))
	if err == nil {
		recordCommitSuccessBatch(len(commit.Commits), time.Since(started))
	}
	if errors.Is(err, kv.ErrEmpty) {
		return Snapshot{}, nil
	}
	return snapshot, err
}

func (s *Store) commitSuccessLocked(b kv.Bucket, commit SuccessCommit, snapshot *Snapshot) error {
	if err := snapshot.loadExisting(s, b.Get([]byte(s.metaKey())), b); err != nil {
		return err
	}
	if err := s.applySuccessLocked(b, commit, snapshot); err != nil {
		return err
	}
	return s.writeSnapshotLocked(b, *snapshot)
}

func (s *Store) applySuccessLocked(b kv.Bucket, commit SuccessCommit, snapshot *Snapshot) error {
	doneExists := len(b.Get([]byte(s.doneKey(commit.Done.WorkKey)))) != 0
	failedKey := s.failedKey(commit.Done.WorkKey)
	failedExists := len(b.Get([]byte(failedKey))) != 0

	if !doneExists {
		if err := writeJSONValue(b, s.doneKey(commit.Done.WorkKey), commit.Done); err != nil {
			return err
		}
		snapshot.Totals.Files += commit.Done.Files
		snapshot.Totals.Objects += commit.Done.Objects
		snapshot.Totals.Bytes += commit.Done.Bytes
		snapshot.Totals.Checks += commit.Done.Checks
		snapshot.Run.Files += commit.Done.Files
		snapshot.Run.Objects += commit.Done.Objects
		snapshot.Run.Bytes += commit.Done.Bytes
		snapshot.Run.Checks += commit.Done.Checks
		if commit.Event.Name != "" {
			snapshot.History = appendHistory(snapshot.History, commit.Event, commit.HistoryLimit)
		}
	}

	if failedExists {
		if err := b.Delete([]byte(failedKey)); err != nil {
			return err
		}
		if snapshot.Totals.PendingFailedCount > 0 {
			snapshot.Totals.PendingFailedCount--
		}
		snapshot.Totals.RecoveredFromError++
		if snapshot.Run.PendingFailedCount > 0 {
			snapshot.Run.PendingFailedCount--
		}
		snapshot.Run.RecoveredFromError++
	}

	snapshot.Scan = commit.Scan
	return nil
}

// CommitFailure persists or updates a failed work item, updates error
// counters/history, then advances the scan cursor.
func (s *Store) CommitFailure(commit FailureCommit) (snapshot Snapshot, err error) {
	err = s.db.Do(true, bucketOp(func(ctx context.Context, b kv.Bucket) error {
		return s.commitFailureLocked(b, commit, &snapshot)
	}))
	return snapshot, err
}

func (s *Store) commitFailureLocked(b kv.Bucket, commit FailureCommit, snapshot *Snapshot) error {
	if err := snapshot.loadExisting(s, b.Get([]byte(s.metaKey())), b); err != nil {
		return err
	}

	if len(b.Get([]byte(s.doneKey(commit.Failed.WorkKey)))) != 0 {
		snapshot.Scan = commit.Scan
		return s.writeSnapshotLocked(b, *snapshot)
	}

	existing := FailedRecord{}
	exists := readJSONValue(b, s.failedKey(commit.Failed.WorkKey), &existing) == nil
	if exists {
		commit.Failed.FirstFailedAt = existing.FirstFailedAt
		commit.Failed.Failures = existing.Failures + 1
	} else {
		commit.Failed.Failures = maxInt64(1, commit.Failed.Failures)
		snapshot.Totals.PendingFailedCount++
		snapshot.Run.PendingFailedCount++
	}
	snapshot.Totals.CumulativeErrorEvents++
	snapshot.Run.CumulativeErrorEvents++

	if err := writeJSONValue(b, s.failedKey(commit.Failed.WorkKey), commit.Failed); err != nil {
		return err
	}
	if commit.Event.Name != "" {
		snapshot.History = appendHistory(snapshot.History, commit.Event, commit.HistoryLimit)
	}
	snapshot.Scan = commit.Scan
	return s.writeSnapshotLocked(b, *snapshot)
}

// Clear removes the persisted state for the current job ID.
func (s *Store) Clear() error {
	return s.db.Do(true, bucketOp(func(ctx context.Context, b kv.Bucket) error {
		return deletePrefix(b, s.prefix(""))
	}))
}

func (s *Store) writeSnapshot(snapshot Snapshot) error {
	return s.db.Do(true, bucketOp(func(ctx context.Context, b kv.Bucket) error {
		return s.writeSnapshotLocked(b, snapshot)
	}))
}

func (s *Store) writeSnapshotLocked(b kv.Bucket, snapshot Snapshot) error {
	if err := writeJSONValue(b, s.metaKey(), snapshot.Meta); err != nil {
		return err
	}
	if err := writeJSONValue(b, s.scanKey(), snapshot.Scan); err != nil {
		return err
	}
	if err := writeJSONValue(b, s.totalKey(), snapshot.Totals); err != nil {
		return err
	}
	if err := writeJSONValue(b, s.runKey(), snapshot.Run); err != nil {
		return err
	}
	return writeJSONValue(b, s.historyKey(), snapshot.History)
}

func (s *Store) prefix(suffix string) string {
	if suffix == "" {
		return s.jobID + ":"
	}
	return s.jobID + ":" + suffix
}

func (s *Store) metaKey() string    { return s.prefix("meta") }
func (s *Store) scanKey() string    { return s.prefix("scan") }
func (s *Store) totalKey() string   { return s.prefix("stats_total") }
func (s *Store) runKey() string     { return s.prefix("stats_run") }
func (s *Store) historyKey() string { return s.prefix("history") }
func (s *Store) doneKey(workKey string) string {
	return s.prefix("done:" + workKey)
}
func (s *Store) failedKey(workKey string) string {
	return s.prefix("failed:" + workKey)
}
func (s *Store) failedPrefix() string { return s.prefix("failed:") }

func writeJSONValue(b kv.Bucket, key string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return b.Put([]byte(key), data)
}

func readJSONValue(b kv.Bucket, key string, out any) error {
	data := b.Get([]byte(key))
	if len(data) == 0 {
		return errors.New("not found")
	}
	return json.Unmarshal(data, out)
}

func scanJSONPrefix(b kv.Bucket, prefix string, fn func([]byte) error) error {
	cur := b.Cursor()
	key, data := cur.Seek([]byte(prefix))
	for key != nil {
		k := string(key)
		if !strings.HasPrefix(k, prefix) {
			break
		}
		if err := fn(slices.Clone(data)); err != nil {
			return err
		}
		key, data = cur.Next()
	}
	return nil
}

func deletePrefix(b kv.Bucket, prefix string) error {
	cur := b.Cursor()
	keys := make([]string, 0)
	key, _ := cur.Seek([]byte(prefix))
	for key != nil {
		k := string(key)
		if !strings.HasPrefix(k, prefix) {
			break
		}
		keys = append(keys, k)
		key, _ = cur.Next()
	}
	for _, key := range keys {
		if err := b.Delete([]byte(key)); err != nil {
			return err
		}
	}
	return nil
}

func appendHistory(history []HistoryEvent, event HistoryEvent, limit int) []HistoryEvent {
	history = append(history, event)
	if limit <= 0 {
		return history[:0]
	}
	if len(history) > limit {
		history = slices.Clone(history[len(history)-limit:])
	}
	return history
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
