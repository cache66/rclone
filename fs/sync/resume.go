package sync

import (
	"errors"
	"fmt"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/fs/resume"
	"golang.org/x/sync/errgroup"
)

const (
	resumeCopyCommitBatchSize     = 1000
	resumeCopyCommitBatchInterval = 5 * time.Second
)

type resumeObjectSegment struct {
	meta  resume.ObjectSegment
	tasks []resumeCopyTask
}

type resumeObjectSegmentResult struct {
	segment        resume.ObjectSegment
	successCommits []resume.SuccessCommit
	failureCommits []resume.FailureCommit
	firstFailure   error
}

type resumeFileTask struct {
	meta         resume.FileTask
	tasks        []resumeCopyTask
	startFrames  []resume.ScanFrame
	commitFrames []resume.ScanFrame
}

type resumeFileTaskResult struct {
	task           resume.FileTask
	startFrames    []resume.ScanFrame
	commitFrames   []resume.ScanFrame
	successCommits []resume.SuccessCommit
	failureCommits []resume.FailureCommit
	firstFailure   error
}

func newResumeFileTask(taskID int64, tasks []resumeCopyTask, startFrames, commitFrames []resume.ScanFrame) resumeFileTask {
	meta := resume.FileTask{
		TaskID:    taskID,
		FileCount: len(tasks),
		ByteCount: resumeCopyTaskBytes(tasks),
	}
	if len(tasks) > 0 {
		meta.EndFile = tasks[len(tasks)-1].src.Remote()
	}
	if len(startFrames) > 0 {
		meta.StartFrameSnapshot = resumeFileFramesStatic(startFrames)
	}
	return resumeFileTask{
		meta:         meta,
		tasks:        tasks,
		startFrames:  append([]resume.ScanFrame(nil), startFrames...),
		commitFrames: append([]resume.ScanFrame(nil), commitFrames...),
	}
}

func resumeCopyTaskSize(task resumeCopyTask) int64 {
	if task.src == nil {
		return 0
	}
	size := task.src.Size()
	if size < 0 {
		return 0
	}
	return size
}

func resumeCopyTaskBytes(tasks []resumeCopyTask) int64 {
	var bytes int64
	for _, task := range tasks {
		bytes += resumeCopyTaskSize(task)
	}
	return bytes
}

func resumeShouldDispatchBySize(itemCount int, byteCount int64, itemLimit int, byteLimit int64) bool {
	if itemLimit > 0 && itemCount >= itemLimit {
		return true
	}
	return byteLimit > 0 && byteCount >= byteLimit
}

func (s *syncCopyMove) resumeRun() error {
	if err := s.resumeValidate(); err != nil {
		return err
	}

	meta := resume.NewMeta(s.ctx, "copy", s.fsrc, s.fdst)
	meta.JobID = s.ci.ResumeID
	if meta.JobID == "" {
		meta.JobID = resume.DerivedJobID(meta)
	}

	store, err := resume.Open(s.ctx, meta.JobID)
	if err != nil {
		return err
	}
	defer func() {
		_ = store.Close(false)
	}()

	initialScan := resume.ScanState{
		Phase:  resume.PhaseCopySource,
		Target: "src",
		Frames: []resume.ScanFrame{{
			Dir:    s.dir,
			DstDir: s.dir,
		}},
	}
	if s.resumeUseObjectMode() {
		initialScan.Object = &resume.ObjectScanState{
			RootPrefix: s.dir,
			Window: resume.ObjectWindowState{
				WindowSize: s.resumeObjectWindowSize(),
			},
		}
	}
	snapshot, created, err := store.LoadOrInit(meta, initialScan)
	if err != nil {
		return err
	}
	resume.RestoreAccounting(s.ctx, snapshot)
	if err = store.ResetRunCounters(time.Now()); err != nil {
		return err
	}

	fs.Infof(nil, "Resume enabled for copy job %q", meta.JobID)
	if snapshot.Totals.PendingFailedCount > 0 {
		fs.Infof(nil, "Found %d pending failed item(s) to retry before continuing the scan", snapshot.Totals.PendingFailedCount)
	}

	if err = s.retryResumeFailures(store, &snapshot); err != nil {
		return err
	}
	if err = s.runResumeSourceScan(store, &snapshot, created); err != nil {
		return err
	}

	if s.setDirModTime && s.setDirModTimeAfter {
		s.processError(s.setDelayedDirModTimes(s.ctx))
	}

	s.processError(s.ctx.Err())
	s.processError(s.inCtx.Err())

	if s.currentError() == nil && snapshot.Totals.PendingFailedCount == 0 {
		if err = store.Clear(); err != nil {
			return err
		}
		fs.Infof(nil, "Resume state cleared for copy job %q", meta.JobID)
	}
	return s.currentError()
}

func (s *syncCopyMove) resumeValidate() error {
	switch {
	case s.deleteMode != fs.DeleteModeOff:
		return fserrors.FatalError(errors.New("--resume currently supports copy only, not sync deletes"))
	case s.DoMove:
		return fserrors.FatalError(errors.New("--resume currently does not support move"))
	case s.ci.DryRun:
		return fserrors.FatalError(errors.New("--resume does not support --dry-run"))
	case s.ci.Interactive:
		return fserrors.FatalError(errors.New("--resume does not support --interactive"))
	case s.noTraverse:
		return fserrors.FatalError(errors.New("--resume does not support --no-traverse"))
	case s.noCheckDest:
		return fserrors.FatalError(errors.New("--resume does not support --no-check-dest"))
	default:
		return nil
	}
}

func (s *syncCopyMove) retryResumeFailures(store *resume.Store, snapshot *resume.Snapshot) error {
	failed, err := store.ListFailed()
	if err != nil {
		return err
	}
	batch := make([]resume.FailedRecord, 0, len(failed))
	for _, record := range failed {
		if record.Op == "copy" {
			batch = append(batch, record)
		}
	}
	if len(batch) == 0 {
		return nil
	}
	results := make([]resumeCopyRetryResult, len(batch))
	g := new(errgroup.Group)
	g.SetLimit(s.ci.Transfers)
	for i := range batch {
		i := i
		g.Go(func() error {
			results[i] = s.runRetryResumeFailure(batch[i])
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	for _, result := range results {
		if result.fatalErr != nil {
			return result.fatalErr
		}
		if result.err != nil {
			if err := s.commitRetryFailure(store, snapshot, result.failedRecord, result.err); err != nil {
				return err
			}
			continue
		}
		newSnapshot, commitErr := store.CommitSuccess(resume.SuccessCommit{
			Done:         result.done,
			Scan:         snapshot.Scan,
			Event:        resumeSuccessEvent(result.done),
			HistoryLimit: s.ci.ResumeHistoryLimit,
		})
		if commitErr != nil {
			return commitErr
		}
		if err := s.syncResumeScanLimit(store, &newSnapshot); err != nil {
			return err
		}
		*snapshot = newSnapshot
	}
	return nil
}

func (s *syncCopyMove) commitRetryFailure(store *resume.Store, snapshot *resume.Snapshot, record resume.FailedRecord, err error) error {
	failed := record
	failed.LastError = err.Error()
	if failed.FirstFailedAt.IsZero() {
		failed.FirstFailedAt = time.Now()
	}
	failed.LastFailedAt = time.Now()
	newSnapshot, commitErr := store.CommitFailure(resume.FailureCommit{
		Failed:       failed,
		Scan:         snapshot.Scan,
		Event:        resumeFailureEvent(failed.Name, failed.Size, failed.Checked, failed.What, failed.LastError),
		HistoryLimit: s.ci.ResumeHistoryLimit,
	})
	if commitErr != nil {
		return commitErr
	}
	if err = s.syncResumeScanLimit(store, &newSnapshot); err != nil {
		return err
	}
	*snapshot = newSnapshot
	s.processError(err)
	return s.stopForErrorLimit(snapshot)
}

type resumeCopyRetryResult struct {
	done         resume.DoneRecord
	failedRecord resume.FailedRecord
	err          error
	fatalErr     error
}

func (s *syncCopyMove) runRetryResumeFailure(record resume.FailedRecord) resumeCopyRetryResult {
	src, err := s.fsrc.NewObject(s.ctx, record.SrcRemote)
	if err != nil {
		return resumeCopyRetryResult{failedRecord: record, err: err}
	}
	if src == nil {
		return resumeCopyRetryResult{failedRecord: record, err: fs.ErrorObjectNotFound}
	}
	dstRemote := record.DstRemote
	if dstRemote == "" {
		dstRemote = resume.TargetRemote(s.ctx, src)
	}
	var dst fs.Object
	if record.DstRemote != "" {
		dst, _ = s.fdst.NewObject(s.ctx, record.DstRemote)
	}
	if dst == nil && dstRemote != "" {
		dst, _ = s.fdst.NewObject(s.ctx, dstRemote)
	}
	done, failed, err := s.copyResumeObject(src, dst, record.WorkKey, true)
	if err != nil {
		return resumeCopyRetryResult{
			failedRecord: failed,
			err:          err,
		}
	}
	return resumeCopyRetryResult{done: done}
}

func (s *syncCopyMove) runResumeSourceScan(store *resume.Store, snapshot *resume.Snapshot, skipDoneLookup bool) error {
	if s.resumeUseObjectMode() {
		return s.runResumeObjectScan(store, snapshot)
	}
	return s.runResumeFileScan(store, snapshot, skipDoneLookup)
}

func (s *syncCopyMove) runResumeFileScan(store *resume.Store, snapshot *resume.Snapshot, skipDoneLookup bool) error {
	keyer := resume.NewMatchKeyer(s.ctx, s.fdst)
	frames := s.resumeLegacyFileFrames(snapshot)
	windowSize := s.resumeFileWindowSize()
	fileMaxBytes := s.resumeFileTaskMaxBytes()
	checkpointInterval := s.resumeSuccessCheckpointInterval()
	frontierTaskID := s.resumeFileFrontierTaskID(snapshot)
	var nextTaskID int64
	if frontierTaskID > 0 {
		nextTaskID = frontierTaskID + 1
	} else {
		nextTaskID = 1
	}
	var batch []resumeCopyTask
	var batchBytes int64
	var batchOpenedAt time.Time
	var batchStartFrames []resume.ScanFrame
	var batchCommitFrames []resume.ScanFrame
	var inflight []resume.FileTask
	inflightCount := 0
	resultsCh := make(chan resumeFileTaskResult, windowSize)
	pending := make(map[int64]resumeFileTaskResult)
	frontierBlocked := false
	var lastPersistedAt time.Time
	type dstDirListing struct {
		byKey map[string]fs.DirEntry
	}
	type srcDirPageCache struct {
		page resume.DirPage
	}
	dstListings := make(map[string]dstDirListing)
	srcPages := make(map[string]srcDirPageCache)
	if len(frames) == 0 {
		frames = []resume.ScanFrame{{Dir: s.dir, DstDir: s.dir}}
	}

	persistFileScan := func(force, complete bool) error {
		if !force && !complete && checkpointInterval > 0 && !lastPersistedAt.IsZero() && time.Since(lastPersistedAt) < checkpointInterval {
			return nil
		}
		snapshot.Scan = s.resumeFileScanState(snapshot, frames, frontierTaskID, inflight, complete)
		if err := store.SaveScan(snapshot.Scan); err != nil {
			return err
		}
		lastPersistedAt = time.Now()
		return nil
	}

	dispatchTask := func() error {
		if len(batch) == 0 {
			return nil
		}
		task := newResumeFileTask(nextTaskID, batch, batchStartFrames, batchCommitFrames)
		resume.RecordFileTaskCreated(task.meta.FileCount, task.meta.ByteCount)
		task.meta.Status = resume.FileTaskRunning
		inflight = append(inflight, task.meta)
		inflightCount++
		if err := persistFileScan(true, false); err != nil {
			return err
		}
		go func(fileTask resumeFileTask) {
			resultsCh <- s.runResumeFileTask(fileTask)
		}(task)
		nextTaskID++
		batch = nil
		batchBytes = 0
		batchOpenedAt = time.Time{}
		batchStartFrames = nil
		batchCommitFrames = nil
		return nil
	}

	waitForOne := func() error {
		result := <-resultsCh
		inflightCount--
		pending[result.task.TaskID] = result
		nextFrontierTaskID, remainingInflight, blocked, err := s.commitResumeReadyFileTasks(store, snapshot, frontierTaskID, pending, inflight)
		if err != nil {
			return err
		}
		frontierTaskID = nextFrontierTaskID
		inflight = remainingInflight
		if blocked {
			frontierBlocked = true
		}
		return nil
	}

	checkpointScanIfNoPendingBatch := func(complete bool) error {
		if len(batch) > 0 {
			return nil
		}
		return persistFileScan(false, complete)
	}

	dispatchBatchAndThrottle := func() error {
		if err := dispatchTask(); err != nil {
			return err
		}
		for inflightCount >= windowSize && !frontierBlocked {
			if err := waitForOne(); err != nil {
				return err
			}
		}
		return nil
	}

	for len(frames) > 0 {
		if frontierBlocked {
			break
		}
		frame := &frames[len(frames)-1]
		srcPageKey := frame.Dir + "\x00" + frame.ContinuationToken
		srcPage, ok := srcPages[srcPageKey]
		if !ok {
			page, err := resume.ListDirPage(s.ctx, s.fsrc, frame.Dir, false, keyer.SrcKey, frame.ContinuationToken)
			if err != nil {
				return err
			}
			resume.RecordFileScanPage()
			if !page.NativeOrder {
				page.Entries = append(fs.DirEntries(nil), page.Entries...)
				srcPages[srcPageKey] = srcDirPageCache{page: page}
			}
			srcPage = srcDirPageCache{page: page}
		}
		pageIndex := frame.PageIndex
		if !srcPage.page.TokenAccepted {
			frame.ContinuationToken = ""
			frame.PageIndex = 0
			pageIndex = 0
		}
		listingKey := frame.Dir + "\x00" + frame.DstDir
		dstListing, ok := dstListings[listingKey]
		if !ok {
			dstEntries, err := resume.SortedDirEntries(s.ctx, s.fdst, frame.DstDir, false, keyer.DstKey)
			if err != nil && !errors.Is(err, fs.ErrorDirNotFound) {
				return err
			}
			if errors.Is(err, fs.ErrorDirNotFound) {
				dstEntries = nil
			}
			dstListing = dstDirListing{
				byKey: resume.DirByKey(dstEntries, keyer.DstKey),
			}
			dstListings[listingKey] = dstListing
		}
		descended := false
		for entryIndex, srcEntry := range srcPage.page.Entries {
			matchKey := keyer.SrcKey(srcEntry)
			cursorKey := resume.CursorKey(pageIndex, entryIndex)
			if frame.LastDoneEntryKey != "" && cursorKey <= frame.LastDoneEntryKey {
				continue
			}
			switch x := srcEntry.(type) {
			case fs.Directory:
				resume.RecordFileScanDirectory()
				nextDstDir, dirErr := s.handleResumeDirectory(x, dstListing.byKey[matchKey])
				if dirErr != nil {
					return dirErr
				}
				frame.LastDoneEntryKey = cursorKey
				frames = append(frames, resume.ScanFrame{Dir: x.Remote(), DstDir: nextDstDir})
				if err := checkpointScanIfNoPendingBatch(false); err != nil {
					return err
				}
				descended = true
			case fs.Object:
				// File/NAS resume now restores from the earliest uncommitted task
				// frontier, so the hot path no longer needs per-file done lookups.
				// This keeps restart correctness while avoiding one KV read per file.
				task, alreadyDone, fileErr := s.prepareResumeCopyTask(store, x, dstListing.byKey[matchKey], cursorKey, nil, true, snapshot.Totals.PendingFailedCount == 0)
				if fileErr != nil {
					return fileErr
				}
				if alreadyDone {
					frame.LastDoneEntryKey = cursorKey
					continue
				}
				if len(batch) == 0 {
					batchOpenedAt = time.Now()
					batchStartFrames = append([]resume.ScanFrame(nil), frames...)
				}
				batchCommitFrames = append([]resume.ScanFrame(nil), frames...)
				batchCommitFrames[len(batchCommitFrames)-1].LastDoneEntryKey = cursorKey
				batch = append(batch, task)
				batchBytes += resumeCopyTaskSize(task)
				if resumeShouldDispatchBySize(len(batch), batchBytes, resumeCopyCommitBatchSize, fileMaxBytes) ||
					(!batchOpenedAt.IsZero() && time.Since(batchOpenedAt) >= resumeCopyCommitBatchInterval) {
					if err := dispatchBatchAndThrottle(); err != nil {
						return err
					}
				}
			default:
				return fmt.Errorf("unsupported source entry %T", srcEntry)
			}
			if descended || frontierBlocked {
				break
			}
		}
		if descended {
			continue
		}
		if srcPage.page.NextContinuationToken != "" {
			frame.ContinuationToken = srcPage.page.NextContinuationToken
			frame.PageIndex = pageIndex + 1
			if err := checkpointScanIfNoPendingBatch(false); err != nil {
				return err
			}
			continue
		}
		delete(dstListings, listingKey)
		delete(srcPages, srcPageKey)
		frames = frames[:len(frames)-1]
		if err := checkpointScanIfNoPendingBatch(len(frames) == 0 && inflightCount == 0); err != nil {
			return err
		}
	}

	if !frontierBlocked {
		if err := dispatchTask(); err != nil {
			return err
		}
	}
	for inflightCount > 0 {
		if err := waitForOne(); err != nil {
			return err
		}
		if frontierBlocked {
			break
		}
	}

	snapshot.Scan = s.resumeFileScanState(snapshot, frames, frontierTaskID, inflight, !frontierBlocked && len(frames) == 0 && inflightCount == 0 && len(batch) == 0)
	return store.SaveScan(snapshot.Scan)
}

func (s *syncCopyMove) runResumeObjectScan(store *resume.Store, snapshot *resume.Snapshot) error {
	keyer := resume.NewMatchKeyer(s.ctx, s.fdst)
	frames := []resume.ScanFrame{{Dir: s.dir, DstDir: s.dir}}
	if len(snapshot.Scan.Frames) > 0 {
		frames = append([]resume.ScanFrame(nil), snapshot.Scan.Frames...)
	}
	state := s.resumeObjectState(snapshot)
	lastCommittedKey := state.LastCommittedKey
	lastScannedKey := state.LastScannedKey
	nextSegmentID := state.Window.CommitFrontierSegmentID + 1
	frontierSegmentID := state.Window.CommitFrontierSegmentID
	windowSize := s.resumeObjectWindowSize()
	segmentSize := s.resumeObjectSegmentSize()
	segmentMaxBytes := s.resumeObjectSegmentMaxBytes()
	checkpointInterval := s.resumeSuccessCheckpointInterval()
	dstListings := make(map[string]map[string]fs.DirEntry)
	resultsCh := make(chan resumeObjectSegmentResult, windowSize)
	pending := make(map[int64]resumeObjectSegmentResult)

	current := resumeObjectSegment{
		meta: resume.ObjectSegment{
			SegmentID:     nextSegmentID,
			StartAfterKey: lastCommittedKey,
			Status:        resume.ObjectSegmentScanned,
		},
	}
	var segmentOpenedAt time.Time
	var inflight []resume.ObjectSegment
	var inflightCount int
	var frontierBlocked bool

	persistObjectScan := func(complete bool) error {
		snapshot.Scan = s.resumeObjectScanState(snapshot, state.RootPrefix, lastCommittedKey, lastScannedKey, frontierSegmentID, inflight, complete)
		return store.SaveScan(snapshot.Scan)
	}

	dispatchSegment := func() error {
		if len(current.tasks) == 0 {
			return nil
		}
		segment := current
		segment.meta.Status = resume.ObjectSegmentRunning
		segment.meta.ObjectCount = len(segment.tasks)
		resume.RecordObjectSegmentCreated(segment.meta.ObjectCount, segment.meta.ByteCount)
		inflight = append(inflight, segment.meta)
		inflightCount++
		if err := persistObjectScan(false); err != nil {
			return err
		}
		go func(seg resumeObjectSegment) {
			resultsCh <- s.runResumeObjectSegment(seg)
		}(segment)
		nextSegmentID++
		current = resumeObjectSegment{
			meta: resume.ObjectSegment{
				SegmentID:     nextSegmentID,
				StartAfterKey: segment.meta.EndKey,
				Status:        resume.ObjectSegmentScanned,
			},
		}
		segmentOpenedAt = time.Time{}
		return nil
	}

	commitFailureBatch := func(commits []resume.FailureCommit) error {
		for _, commit := range commits {
			commit.Scan = s.resumeObjectScanState(snapshot, state.RootPrefix, lastCommittedKey, lastScannedKey, frontierSegmentID, inflight, false)
			newSnapshot, commitErr := store.CommitFailure(commit)
			if commitErr != nil {
				return commitErr
			}
			if err := s.syncResumeScanLimit(store, &newSnapshot); err != nil {
				return err
			}
			*snapshot = newSnapshot
			s.processError(errors.New(commit.Failed.LastError))
			if err := s.stopForErrorLimit(snapshot); err != nil {
				return err
			}
		}
		return nil
	}

	popInflight := func(segmentID int64) {
		next := inflight[:0]
		for _, segment := range inflight {
			if segment.SegmentID != segmentID {
				next = append(next, segment)
			}
		}
		inflight = next
	}

	advanceFrontier := func() error {
		committedSegments := 0
		ready := resumeObjectFrontierReady(frontierSegmentID, pending)
		if len(ready) == 0 {
			resume.RecordObjectFrontierCommit(0, len(pending), len(inflight), false)
			return persistObjectScan(false)
		}
		for _, result := range ready {
			segmentID := result.segment.SegmentID
			if result.segment.Status == resume.ObjectSegmentFailed {
				if err := commitFailureBatch(result.failureCommits); err != nil {
					return err
				}
				delete(pending, segmentID)
				popInflight(segmentID)
				frontierBlocked = true
				resume.RecordObjectFrontierCommit(committedSegments, len(pending), len(inflight), true)
				return persistObjectScan(false)
			}
			if len(result.successCommits) > 0 {
				newSnapshot, commitErr := store.CommitSuccessBatch(resume.SuccessBatchCommit{Commits: result.successCommits})
				if commitErr != nil {
					return commitErr
				}
				if err := s.syncResumeScanLimit(store, &newSnapshot); err != nil {
					return err
				}
				*snapshot = newSnapshot
			}
			lastCommittedKey = result.segment.EndKey
			frontierSegmentID = segmentID
			delete(pending, segmentID)
			popInflight(segmentID)
			committedSegments++
		}
		resume.RecordObjectFrontierCommit(committedSegments, len(pending), len(inflight), false)
		return persistObjectScan(false)
	}

	waitForOne := func() error {
		result := <-resultsCh
		inflightCount--
		pending[result.segment.SegmentID] = result
		if result.segment.Status == resume.ObjectSegmentFailed {
			frontierBlocked = true
		}
		return advanceFrontier()
	}

	for len(frames) > 0 {
		if frontierBlocked {
			break
		}
		frame := &frames[len(frames)-1]
		srcPage, err := resume.ListDirPage(s.ctx, s.fsrc, frame.Dir, false, keyer.SrcKey, frame.ContinuationToken)
		if err != nil {
			return err
		}
		pageIndex := frame.PageIndex
		if !srcPage.TokenAccepted {
			frame.ContinuationToken = ""
			frame.PageIndex = 0
			pageIndex = 0
		}
		listingKey := frame.Dir + "\x00" + frame.DstDir
		dstByKey, ok := dstListings[listingKey]
		if !ok {
			dstEntries, err := resume.SortedDirEntries(s.ctx, s.fdst, frame.DstDir, false, keyer.DstKey)
			if err != nil && !errors.Is(err, fs.ErrorDirNotFound) {
				return err
			}
			if errors.Is(err, fs.ErrorDirNotFound) {
				dstEntries = nil
			}
			dstByKey = resume.DirByKey(dstEntries, keyer.DstKey)
			dstListings[listingKey] = dstByKey
		}

		descended := false
		for entryIndex, srcEntry := range srcPage.Entries {
			matchKey := keyer.SrcKey(srcEntry)
			cursorKey := resume.CursorKey(pageIndex, entryIndex)
			if frame.LastDoneEntryKey != "" && cursorKey <= frame.LastDoneEntryKey {
				continue
			}
			switch x := srcEntry.(type) {
			case fs.Directory:
				nextDstDir, dirErr := s.handleResumeDirectory(x, dstByKey[matchKey])
				if dirErr != nil {
					return dirErr
				}
				frame.LastDoneEntryKey = cursorKey
				frames = append(frames, resume.ScanFrame{Dir: x.Remote(), DstDir: nextDstDir})
				descended = true
			case fs.Object:
				sourceKey := x.Remote()
				if lastCommittedKey != "" && sourceKey <= lastCommittedKey {
					frame.LastDoneEntryKey = cursorKey
					continue
				}
				task, _, taskErr := s.prepareResumeCopyTask(store, x, dstByKey[matchKey], cursorKey, nil, true, false)
				if taskErr != nil {
					return taskErr
				}
				if len(current.tasks) == 0 {
					segmentOpenedAt = time.Now()
					current.meta.StartAfterKey = lastCommittedKey
				}
				current.tasks = append(current.tasks, task)
				current.meta.EndKey = sourceKey
				current.meta.ObjectCount = len(current.tasks)
				current.meta.ByteCount += resumeCopyTaskSize(task)
				lastScannedKey = sourceKey
				frame.LastDoneEntryKey = cursorKey
				if resumeShouldDispatchBySize(len(current.tasks), current.meta.ByteCount, segmentSize, segmentMaxBytes) ||
					(!segmentOpenedAt.IsZero() && checkpointInterval > 0 && time.Since(segmentOpenedAt) >= checkpointInterval) {
					if err := dispatchSegment(); err != nil {
						return err
					}
					for inflightCount >= windowSize && !frontierBlocked {
						if err := waitForOne(); err != nil {
							return err
						}
					}
				}
			default:
				return fmt.Errorf("unsupported source entry %T", srcEntry)
			}
			if descended || frontierBlocked {
				break
			}
		}
		if descended {
			continue
		}
		if srcPage.NextContinuationToken != "" {
			frame.ContinuationToken = srcPage.NextContinuationToken
			frame.PageIndex = pageIndex + 1
			continue
		}
		delete(dstListings, listingKey)
		frames = frames[:len(frames)-1]
	}

	if !frontierBlocked {
		if err := dispatchSegment(); err != nil {
			return err
		}
	}
	for inflightCount > 0 {
		if err := waitForOne(); err != nil {
			return err
		}
		if frontierBlocked {
			break
		}
	}

	snapshot.Scan = s.resumeObjectScanState(snapshot, state.RootPrefix, lastCommittedKey, lastScannedKey, frontierSegmentID, inflight, !frontierBlocked && inflightCount == 0 && len(current.tasks) == 0)
	return store.SaveScan(snapshot.Scan)
}

type resumeCopyTask struct {
	cursorKey  string
	workKey    string
	src        fs.Object
	dst        fs.Object
	scanFrames []resume.ScanFrame
}

type resumeCopyResult struct {
	task   resumeCopyTask
	done   resume.DoneRecord
	failed resume.FailedRecord
	err    error
}

func (s *syncCopyMove) prepareResumeCopyTask(store *resume.Store, src fs.Object, dstEntry fs.DirEntry, cursorKey string, scanFrames []resume.ScanFrame, skipDoneLookup, deferWorkKey bool) (task resumeCopyTask, alreadyDone bool, err error) {
	targetRemote := resume.TargetRemote(s.ctx, src)
	dstRemote := targetRemote
	var dstObj fs.Object
	if dstEntry != nil {
		dstRemote = dstEntry.Remote()
		if object, ok := dstEntry.(fs.Object); ok {
			dstObj = object
		}
	}
	workKey := ""
	if !deferWorkKey || !skipDoneLookup {
		workKey = resume.WorkKey("copy", src.Remote(), dstRemote, resume.EntryFingerprint(s.ctx, src))
	}
	if !skipDoneLookup {
		done, err := store.HasDone(workKey)
		if err != nil {
			return task, false, err
		}
		if done {
			return task, true, nil
		}
	}
	if len(scanFrames) > 0 {
		scanFrames[len(scanFrames)-1].LastDoneEntryKey = cursorKey
	}
	return resumeCopyTask{
		cursorKey:  cursorKey,
		workKey:    workKey,
		src:        src,
		dst:        dstObj,
		scanFrames: scanFrames,
	}, false, nil
}

func (s *syncCopyMove) runResumeObjectSegment(segment resumeObjectSegment) resumeObjectSegmentResult {
	results := make([]resumeCopyResult, len(segment.tasks))
	g := new(errgroup.Group)
	g.SetLimit(s.ci.Transfers)
	for i := range segment.tasks {
		i := i
		g.Go(func() error {
			done, failed, err := s.copyResumeObject(segment.tasks[i].src, segment.tasks[i].dst, segment.tasks[i].workKey, false)
			results[i] = resumeCopyResult{
				task:   segment.tasks[i],
				done:   done,
				failed: failed,
				err:    err,
			}
			return nil
		})
	}
	_ = g.Wait()

	result := resumeObjectSegmentResult{
		segment: segment.meta,
	}
	for _, item := range results {
		if item.err != nil {
			result.segment.Status = resume.ObjectSegmentFailed
			if result.firstFailure == nil {
				result.firstFailure = item.err
			}
			result.failureCommits = append(result.failureCommits, resume.FailureCommit{
				Failed:       item.failed,
				Scan:         resume.ScanState{},
				Event:        resumeFailureEvent(item.failed.Name, item.failed.Size, item.failed.Checked, item.failed.What, item.failed.LastError),
				HistoryLimit: s.ci.ResumeHistoryLimit,
			})
			continue
		}
		historyEvent := resume.HistoryEvent{}
		if item.done.Bytes > 0 || item.done.Checks > 0 {
			historyEvent = resumeSuccessEvent(item.done)
		}
		result.successCommits = append(result.successCommits, resume.SuccessCommit{
			Done:         item.done,
			Scan:         resume.ScanState{},
			Event:        historyEvent,
			HistoryLimit: s.ci.ResumeHistoryLimit,
		})
	}
	if result.segment.Status == "" {
		result.segment.Status = resume.ObjectSegmentDone
	}
	return result
}

func (s *syncCopyMove) runResumeFileTask(task resumeFileTask) resumeFileTaskResult {
	results := make([]resumeCopyResult, len(task.tasks))
	g := new(errgroup.Group)
	g.SetLimit(s.ci.Transfers)
	for i := range task.tasks {
		i := i
		g.Go(func() error {
			done, failed, err := s.copyResumeObject(task.tasks[i].src, task.tasks[i].dst, task.tasks[i].workKey, false)
			results[i] = resumeCopyResult{
				task:   task.tasks[i],
				done:   done,
				failed: failed,
				err:    err,
			}
			return nil
		})
	}
	_ = g.Wait()

	result := resumeFileTaskResult{
		task:         task.meta,
		startFrames:  append([]resume.ScanFrame(nil), task.startFrames...),
		commitFrames: append([]resume.ScanFrame(nil), task.commitFrames...),
	}
	for _, item := range results {
		if item.err != nil {
			result.task.Status = resume.FileTaskFailed
			if result.firstFailure == nil {
				result.firstFailure = item.err
			}
			result.failureCommits = append(result.failureCommits, resume.FailureCommit{
				Failed:       item.failed,
				Event:        resumeFailureEvent(item.failed.Name, item.failed.Size, item.failed.Checked, item.failed.What, item.failed.LastError),
				HistoryLimit: s.ci.ResumeHistoryLimit,
			})
			continue
		}
		historyEvent := resume.HistoryEvent{}
		if item.done.Bytes > 0 || item.done.Checks > 0 {
			historyEvent = resumeSuccessEvent(item.done)
		}
		result.successCommits = append(result.successCommits, resume.SuccessCommit{
			Done:         item.done,
			Event:        historyEvent,
			HistoryLimit: s.ci.ResumeHistoryLimit,
		})
	}
	if result.task.Status == "" {
		result.task.Status = resume.FileTaskDone
	}
	return result
}

func (s *syncCopyMove) commitResumeReadyFileTasks(store *resume.Store, snapshot *resume.Snapshot, frontierTaskID int64, pending map[int64]resumeFileTaskResult, inflight []resume.FileTask) (nextFrontierTaskID int64, remainingInflight []resume.FileTask, blocked bool, err error) {
	nextFrontierTaskID = frontierTaskID
	remainingInflight = append([]resume.FileTask(nil), inflight...)
	ready := resumeFileFrontierReady(frontierTaskID, pending)
	if len(ready) == 0 {
		resume.RecordFileFrontierCommit(0, len(pending), len(remainingInflight), false)
		return nextFrontierTaskID, remainingInflight, false, nil
	}
	committedTasks := 0
	popInflight := func(taskID int64) {
		next := remainingInflight[:0]
		for _, task := range remainingInflight {
			if task.TaskID != taskID {
				next = append(next, task)
			}
		}
		remainingInflight = next
	}

	successReady := make([]resumeFileTaskResult, 0, len(ready))
	var failureResult *resumeFileTaskResult
	for i := range ready {
		result := ready[i]
		if result.task.Status == resume.FileTaskFailed {
			failureResult = &result
			break
		}
		successReady = append(successReady, result)
	}

	if len(successReady) > 0 {
		lastSuccess := successReady[len(successReady)-1]
		successScan := s.resumeFileScanState(snapshot, lastSuccess.commitFrames, nextFrontierTaskID, remainingInflight, false)
		commits := make([]resume.SuccessCommit, 0)
		for _, result := range successReady {
			for _, commit := range result.successCommits {
				commit.Scan = successScan
				commits = append(commits, commit)
			}
		}
		if len(commits) > 0 {
			newSnapshot, commitErr := store.CommitSuccessBatch(resume.SuccessBatchCommit{
				Commits:            commits,
				SkipDoneRecords:    true,
				SkipFailureRecords: snapshot.Totals.PendingFailedCount == 0,
			})
			if commitErr != nil {
				return nextFrontierTaskID, remainingInflight, false, commitErr
			}
			if syncErr := s.syncResumeScanLimit(store, &newSnapshot); syncErr != nil {
				return nextFrontierTaskID, remainingInflight, false, syncErr
			}
			*snapshot = newSnapshot
		}
		for _, result := range successReady {
			nextFrontierTaskID = result.task.TaskID
			delete(pending, result.task.TaskID)
			popInflight(result.task.TaskID)
			committedTasks++
		}
	}

	if failureResult != nil {
		result := *failureResult
		if result.firstFailure != nil {
			s.processError(result.firstFailure)
		}
		for _, commit := range result.failureCommits {
			commit.Scan = s.resumeFileScanState(snapshot, result.startFrames, nextFrontierTaskID, remainingInflight, false)
			newSnapshot, commitErr := store.CommitFailure(commit)
			if commitErr != nil {
				return nextFrontierTaskID, remainingInflight, true, commitErr
			}
			if syncErr := s.syncResumeScanLimit(store, &newSnapshot); syncErr != nil {
				return nextFrontierTaskID, remainingInflight, true, syncErr
			}
			*snapshot = newSnapshot
			s.processError(errors.New(commit.Failed.LastError))
			if limitErr := s.stopForErrorLimit(snapshot); limitErr != nil {
				return nextFrontierTaskID, remainingInflight, true, limitErr
			}
		}
		delete(pending, result.task.TaskID)
		popInflight(result.task.TaskID)
		blocked = true
		snapshot.Scan = s.resumeFileScanState(snapshot, result.startFrames, nextFrontierTaskID, remainingInflight, false)
		if saveErr := store.SaveScan(snapshot.Scan); saveErr != nil {
			return nextFrontierTaskID, remainingInflight, true, saveErr
		}
		resume.RecordFileFrontierCommit(committedTasks, len(pending), len(remainingInflight), true)
		if result.firstFailure != nil {
			return nextFrontierTaskID, remainingInflight, true, result.firstFailure
		}
		if len(result.failureCommits) > 0 {
			return nextFrontierTaskID, remainingInflight, true, errors.New(result.failureCommits[0].Failed.LastError)
		}
		return nextFrontierTaskID, remainingInflight, true, nil
	}

	snapshot.Scan = s.resumeFileScanState(snapshot, s.resumeLegacyFileFrames(snapshot), nextFrontierTaskID, remainingInflight, false)
	if err := store.SaveScan(snapshot.Scan); err != nil {
		return nextFrontierTaskID, remainingInflight, false, err
	}
	resume.RecordFileFrontierCommit(committedTasks, len(pending), len(remainingInflight), false)
	return nextFrontierTaskID, remainingInflight, false, nil
}

func (s *syncCopyMove) flushResumeCopyBatch(store *resume.Store, snapshot *resume.Snapshot, batch []resumeCopyTask) error {
	if len(batch) == 0 {
		return nil
	}

	results := make([]resumeCopyResult, len(batch))
	g := new(errgroup.Group)
	g.SetLimit(s.ci.Transfers)
	for i := range batch {
		i := i
		g.Go(func() error {
			done, failed, err := s.copyResumeObject(batch[i].src, batch[i].dst, batch[i].workKey, false)
			results[i] = resumeCopyResult{
				task:   batch[i],
				done:   done,
				failed: failed,
				err:    err,
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	successCommits := make([]resume.SuccessCommit, 0, len(results))
	for _, result := range results {
		scan := s.resumeCopyScanState(result.task.scanFrames, false, snapshot.Totals.PendingFailedCount)
		if result.err != nil {
			newSnapshot, commitErr := store.CommitFailure(resume.FailureCommit{
				Failed:       result.failed,
				Scan:         scan,
				Event:        resumeFailureEvent(result.failed.Name, result.failed.Size, result.failed.Checked, result.failed.What, result.failed.LastError),
				HistoryLimit: s.ci.ResumeHistoryLimit,
			})
			if commitErr != nil {
				return commitErr
			}
			if syncErr := s.syncResumeScanLimit(store, &newSnapshot); syncErr != nil {
				return syncErr
			}
			s.processError(result.err)
			if limitErr := s.stopForErrorLimit(&newSnapshot); limitErr != nil {
				*snapshot = newSnapshot
				return limitErr
			}
			*snapshot = newSnapshot
			continue
		}

		historyEvent := resume.HistoryEvent{}
		if result.done.Bytes > 0 || result.done.Checks > 0 {
			historyEvent = resumeSuccessEvent(result.done)
		}
		successCommits = append(successCommits, resume.SuccessCommit{
			Done:         result.done,
			Scan:         scan,
			Event:        historyEvent,
			HistoryLimit: s.ci.ResumeHistoryLimit,
		})
	}

	if len(successCommits) > 0 {
		newSnapshot, commitErr := store.CommitSuccessBatch(resume.SuccessBatchCommit{
			Commits: successCommits,
		})
		if commitErr != nil {
			return commitErr
		}
		if syncErr := s.syncResumeScanLimit(store, &newSnapshot); syncErr != nil {
			return syncErr
		}
		*snapshot = newSnapshot
	}

	return nil
}

func (s *syncCopyMove) handleResumeDirectory(src fs.Directory, dstEntry fs.DirEntry) (nextDstDir string, err error) {
	src = fs.NewOverrideDirectory(src, resume.TargetRemote(s.ctx, src))
	switch dstX := dstEntry.(type) {
	case nil:
		s.markParentNotEmpty(src)
		s.logger(s.ctx, operations.MissingOnDst, src, nil, fs.ErrorIsDir)
		s.copyDirMetadata(s.ctx, s.fdst, nil, src.Remote(), src)
		s.markDirModified(src.Remote())
		return src.Remote(), nil
	case fs.Directory:
		s.markParentNotEmpty(src)
		s.logger(s.ctx, operations.Match, src, dstX, fs.ErrorIsDir)
		s.copyDirMetadata(s.ctx, s.fdst, dstX, "", src)
		if s.ci.FixCase && !s.ci.Immutable && src.Remote() != dstX.Remote() {
			err = operations.DirMoveCaseInsensitive(s.ctx, s.fdst, dstX.Remote(), src.Remote())
			if err != nil {
				fs.Errorf(dstX, "Error while attempting to rename to %s: %v", src.Remote(), err)
				s.processError(err)
				return "", err
			}
			fs.Infof(dstX, "Fixed case by renaming to: %s", src.Remote())
			return src.Remote(), nil
		}
		return dstX.Remote(), nil
	default:
		err = errors.New("can't overwrite file with directory")
		fs.Errorf(dstEntry, "%v", err)
		s.processError(err)
		return "", err
	}
}

func (s *syncCopyMove) copyResumeObject(src fs.Object, dst fs.Object, workKey string, retry bool) (resume.DoneRecord, resume.FailedRecord, error) {
	name := src.Remote()
	startedAt := time.Now()
	fingerprint := ""
	if workKey != "" {
		fingerprint = resume.EntryFingerprint(s.ctx, src)
	}
	doneRecord := resume.DoneRecord{
		WorkKey:     workKey,
		Op:          "copy",
		Kind:        "object",
		SrcRemote:   src.Remote(),
		DstRemote:   resume.TargetRemote(s.ctx, src),
		Fingerprint: fingerprint,
		Name:        name,
		Size:        src.Size(),
		Checked:     false,
		What:        "transferring",
		StartedAt:   startedAt,
		CompletedAt: startedAt,
	}
	failedRecord := resume.FailedRecord{
		WorkKey:       workKey,
		Op:            "copy",
		Kind:          "object",
		SrcRemote:     src.Remote(),
		DstRemote:     doneRecord.DstRemote,
		Fingerprint:   fingerprint,
		Name:          name,
		Size:          src.Size(),
		Checked:       false,
		What:          "transferring",
		FirstFailedAt: startedAt,
		LastFailedAt:  startedAt,
	}
	ensureFingerprint := func() string {
		if fingerprint == "" {
			fingerprint = resume.EntryFingerprint(s.ctx, src)
			doneRecord.Fingerprint = fingerprint
			failedRecord.Fingerprint = fingerprint
		}
		return fingerprint
	}
	ensureWorkKey := func() string {
		if workKey == "" {
			workKey = resume.WorkKey("copy", src.Remote(), doneRecord.DstRemote, ensureFingerprint())
		}
		doneRecord.WorkKey = workKey
		failedRecord.WorkKey = workKey
		return workKey
	}

	if !src.Storable() {
		s.markParentNotEmpty(src)
		doneRecord.Outcome = "skipped"
		return doneRecord, failedRecord, nil
	}
	s.markParentNotEmpty(src)
	if dst != nil {
		doneRecord.DstRemote = dst.Remote()
		failedRecord.DstRemote = dst.Remote()
	}
	if dst == nil {
		fs.Debugf(src, "Need to transfer - File not found at Destination")
		s.logger(s.ctx, operations.MissingOnDst, src, nil, nil)
	} else if s.ci.FixCase && !s.ci.Immutable && src.Remote() != dst.Remote() {
		newDst, err := operations.Move(s.ctx, s.fdst, nil, src.Remote(), dst)
		if err != nil {
			fs.Errorf(dst, "Error while attempting to rename to %s: %v", src.Remote(), err)
			failedRecord.LastError = err.Error()
			return doneRecord, failedRecord, err
		}
		fs.Infof(dst, "Fixed case by renaming to: %s", src.Remote())
		dst = newDst
		doneRecord.DstRemote = dst.Remote()
		failedRecord.DstRemote = dst.Remote()
	}

	needTransfer := operations.NeedTransfer(s.ctx, dst, src)
	if needTransfer {
		noNeedTransfer, err := operations.CompareOrCopyDest(s.ctx, s.fdst, dst, src, s.compareCopyDest, s.backupDir)
		if err != nil {
			ensureWorkKey()
			failedRecord.LastError = err.Error()
			return doneRecord, failedRecord, err
		}
		if noNeedTransfer {
			needTransfer = false
		}
	}

	if !needTransfer {
		if len(s.ci.CopyDest) > 0 {
			doneRecord.Files = 1
			doneRecord.Objects = 1
			doneRecord.Bytes = src.Size()
			doneRecord.Outcome = "copied"
		} else {
			doneRecord.Outcome = "skipped"
		}
		doneRecord.CompletedAt = time.Now()
		return doneRecord, failedRecord, nil
	}
	if s.ci.Immutable && dst != nil {
		err := fserrors.NoRetryError(fs.ErrorImmutableModified)
		ensureWorkKey()
		failedRecord.LastError = err.Error()
		return doneRecord, failedRecord, err
	}
	if dst != nil && s.backupDir != nil {
		s.markDirModifiedObject(dst)
		err := operations.MoveBackupDir(s.ctx, s.backupDir, dst)
		if err != nil {
			ensureWorkKey()
			failedRecord.LastError = err.Error()
			return doneRecord, failedRecord, err
		}
		dst = nil
	} else if dst != nil {
		s.markDirModifiedObject(dst)
	} else {
		s.markDirModifiedObject(src)
	}
	if dst == nil && retry && doneRecord.DstRemote != "" {
		dst, _ = s.fdst.NewObject(s.ctx, doneRecord.DstRemote)
	}
	_, err := operations.Copy(s.ctx, s.fdst, dst, src.Remote(), src)
	if err != nil {
		ensureWorkKey()
		failedRecord.LastError = err.Error()
		return doneRecord, failedRecord, err
	}

	doneRecord.Files = 1
	doneRecord.Objects = 1
	doneRecord.Bytes = src.Size()
	doneRecord.Outcome = "copied"
	doneRecord.CompletedAt = time.Now()
	return doneRecord, failedRecord, nil
}

func buildCopyScanState(frames []resume.ScanFrame, overLimit bool, complete bool) resume.ScanState {
	return resume.ScanState{
		Phase:          resume.PhaseCopySource,
		Target:         "src",
		Frames:         append([]resume.ScanFrame(nil), frames...),
		Complete:       complete,
		OverErrorLimit: overLimit,
	}
}

func resumeSuccessEvent(done resume.DoneRecord) resume.HistoryEvent {
	return resume.HistoryEvent{
		Kind:        "success",
		WorkKey:     done.WorkKey,
		Name:        done.Name,
		Size:        done.Size,
		Bytes:       done.Bytes,
		Checked:     done.Checked,
		What:        done.What,
		StartedAt:   done.StartedAt,
		CompletedAt: done.CompletedAt,
	}
}

func resumeFailureEvent(name string, size int64, checked bool, what, failure string) resume.HistoryEvent {
	now := time.Now()
	return resume.HistoryEvent{
		Kind:        "failure",
		Name:        name,
		Size:        size,
		Checked:     checked,
		What:        what,
		StartedAt:   now,
		CompletedAt: now,
		Error:       failure,
	}
}

func (s *syncCopyMove) stopForErrorLimit(snapshot *resume.Snapshot) error {
	limit := s.ci.ResumeErrorLimit
	if s.resumeOverErrorLimit(snapshot.Totals.PendingFailedCount) {
		return fmt.Errorf("resume pending failed item limit exceeded: %d > %d", snapshot.Totals.PendingFailedCount, limit)
	}
	return nil
}

func (s *syncCopyMove) resumeOverErrorLimit(pendingFailed int64) bool {
	return s.ci.ResumeErrorLimit >= 0 && pendingFailed > s.ci.ResumeErrorLimit
}

func (s *syncCopyMove) resumeUseObjectMode() bool {
	return s.fsrc.Features().BucketBased && s.fdst.Features().BucketBased
}

func (s *syncCopyMove) resumeObjectWindowSize() int {
	if s.ci.ResumeObjectWindowSize > 0 {
		return s.ci.ResumeObjectWindowSize
	}
	return 3
}

func (s *syncCopyMove) resumeObjectSegmentSize() int {
	if s.ci.ResumeObjectSegmentSize > 0 {
		return s.ci.ResumeObjectSegmentSize
	}
	return 1000
}

func (s *syncCopyMove) resumeObjectSegmentMaxBytes() int64 {
	if s.ci.ResumeObjectSegmentMaxBytes > 0 {
		return int64(s.ci.ResumeObjectSegmentMaxBytes)
	}
	return 0
}

func (s *syncCopyMove) resumeFileTaskMaxBytes() int64 {
	if s.ci.ResumeFileTaskMaxBytes > 0 {
		return int64(s.ci.ResumeFileTaskMaxBytes)
	}
	return 0
}

func (s *syncCopyMove) resumeSuccessCheckpointInterval() time.Duration {
	if s.ci.ResumeSuccessCheckpointInterval > 0 {
		return time.Duration(s.ci.ResumeSuccessCheckpointInterval)
	}
	return 5 * time.Second
}

func (s *syncCopyMove) resumeObjectState(snapshot *resume.Snapshot) *resume.ObjectScanState {
	if snapshot.Scan.Object != nil {
		state := *snapshot.Scan.Object
		if state.RootPrefix == "" {
			state.RootPrefix = s.dir
		}
		if state.Window.WindowSize <= 0 {
			state.Window.WindowSize = s.resumeObjectWindowSize()
		}
		return &state
	}
	return &resume.ObjectScanState{
		RootPrefix: s.dir,
		Window: resume.ObjectWindowState{
			WindowSize: s.resumeObjectWindowSize(),
		},
	}
}

func (s *syncCopyMove) resumeFileWindowSize() int {
	if s.ci.ResumeFileWindowSize > 0 {
		return s.ci.ResumeFileWindowSize
	}
	return 2
}

func (s *syncCopyMove) resumeFileFrontierTaskID(snapshot *resume.Snapshot) int64 {
	if snapshot.Scan.FileTree != nil && snapshot.Scan.FileTree.Window.CommitFrontierTaskID > 0 {
		return snapshot.Scan.FileTree.Window.CommitFrontierTaskID
	}
	return 0
}

func (s *syncCopyMove) resumeFileFrames(frames []resume.ScanFrame) []resume.FileFrame {
	return resumeFileFramesStatic(frames)
}

func (s *syncCopyMove) resumeScanFramesFromFileTree(frames []resume.FileFrame) []resume.ScanFrame {
	out := make([]resume.ScanFrame, 0, len(frames))
	for _, frame := range frames {
		out = append(out, resume.ScanFrame{
			Dir:               frame.Dir,
			DstDir:            frame.DstDir,
			LastDoneEntryKey:  frame.LastEntry,
			ContinuationToken: frame.ContinuationToken,
			PageIndex:         frame.PageIndex,
		})
	}
	return out
}

func (s *syncCopyMove) resumeLegacyFileFrames(snapshot *resume.Snapshot) []resume.ScanFrame {
	if snapshot != nil && snapshot.Scan.FileTree != nil {
		if earliest := s.resumeEarliestInflightFileFrames(snapshot.Scan.FileTree.Window.InflightTasks); len(earliest) > 0 {
			return earliest
		}
		if len(snapshot.Scan.FileTree.Frames) > 0 {
			return s.resumeScanFramesFromFileTree(snapshot.Scan.FileTree.Frames)
		}
	}
	return append([]resume.ScanFrame(nil), snapshot.Scan.Frames...)
}

func (s *syncCopyMove) resumeEarliestInflightFileFrames(tasks []resume.FileTask) []resume.ScanFrame {
	var earliest *resume.FileTask
	for i := range tasks {
		task := &tasks[i]
		if len(task.StartFrameSnapshot) == 0 {
			continue
		}
		if earliest == nil || task.TaskID < earliest.TaskID {
			earliest = task
		}
	}
	if earliest == nil {
		return nil
	}
	return s.resumeScanFramesFromFileTree(earliest.StartFrameSnapshot)
}

func resumeFileFramesStatic(frames []resume.ScanFrame) []resume.FileFrame {
	out := make([]resume.FileFrame, 0, len(frames))
	for _, frame := range frames {
		out = append(out, resume.FileFrame{
			Dir:               frame.Dir,
			DstDir:            frame.DstDir,
			LastEntry:         frame.LastDoneEntryKey,
			ContinuationToken: frame.ContinuationToken,
			PageIndex:         frame.PageIndex,
		})
	}
	return out
}

func (s *syncCopyMove) resumeObjectScanState(snapshot *resume.Snapshot, rootPrefix, lastCommittedKey, lastScannedKey string, frontierSegmentID int64, inflight []resume.ObjectSegment, complete bool) resume.ScanState {
	scan := buildCopyScanState([]resume.ScanFrame{{
		Dir:    s.dir,
		DstDir: s.dir,
	}}, s.resumeOverErrorLimit(snapshot.Totals.PendingFailedCount), complete)
	scan.Object = &resume.ObjectScanState{
		RootPrefix:       rootPrefix,
		LastCommittedKey: lastCommittedKey,
		LastScannedKey:   lastScannedKey,
		Complete:         complete,
		OverErrorLimit:   scan.OverErrorLimit,
		Window: resume.ObjectWindowState{
			WindowSize:              s.resumeObjectWindowSize(),
			InflightSegments:        append([]resume.ObjectSegment(nil), inflight...),
			CommitFrontierSegmentID: frontierSegmentID,
		},
	}
	return scan
}

func (s *syncCopyMove) resumeFileScanState(snapshot *resume.Snapshot, frames []resume.ScanFrame, frontierTaskID int64, inflight []resume.FileTask, complete bool) resume.ScanState {
	scan := buildCopyScanState(frames, s.resumeOverErrorLimit(snapshot.Totals.PendingFailedCount), complete)
	scan.FileTree = &resume.FileTreeScanState{
		Frames: s.resumeFileFrames(frames),
		Window: resume.FileWindowState{
			WindowSize:           s.resumeFileWindowSize(),
			InflightTasks:        append([]resume.FileTask(nil), inflight...),
			CommitFrontierTaskID: frontierTaskID,
		},
	}
	return scan
}

func (s *syncCopyMove) resumeCopyScanState(frames []resume.ScanFrame, complete bool, pendingFailed int64) resume.ScanState {
	scan := buildCopyScanState(frames, s.resumeOverErrorLimit(pendingFailed), complete)
	scan.FileTree = &resume.FileTreeScanState{
		Frames: s.resumeFileFrames(frames),
	}
	return scan
}

func (s *syncCopyMove) syncResumeScanLimit(store *resume.Store, snapshot *resume.Snapshot) error {
	overLimit := s.resumeOverErrorLimit(snapshot.Totals.PendingFailedCount)
	if snapshot.Scan.OverErrorLimit == overLimit {
		return nil
	}
	snapshot.Scan.OverErrorLimit = overLimit
	return store.SaveScan(snapshot.Scan)
}

func resumeObjectFrontierReady(frontierSegmentID int64, pending map[int64]resumeObjectSegmentResult) []resumeObjectSegmentResult {
	ready := make([]resumeObjectSegmentResult, 0)
	for {
		segmentID := frontierSegmentID + int64(len(ready)) + 1
		result, ok := pending[segmentID]
		if !ok {
			return ready
		}
		ready = append(ready, result)
		if result.segment.Status == resume.ObjectSegmentFailed {
			return ready
		}
	}
}

func resumeFileFrontierReady(frontierTaskID int64, pending map[int64]resumeFileTaskResult) []resumeFileTaskResult {
	ready := make([]resumeFileTaskResult, 0)
	for {
		taskID := frontierTaskID + int64(len(ready)) + 1
		result, ok := pending[taskID]
		if !ok {
			return ready
		}
		ready = append(ready, result)
		if result.task.Status == resume.FileTaskFailed {
			return ready
		}
	}
}
