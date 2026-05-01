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

	keyer := resume.NewMatchKeyer(s.ctx, s.fdst)
	frames := snapshot.Scan.Frames
	var batch []resumeCopyTask
	var batchOpenedAt time.Time
	type dstDirListing struct {
		byKey map[string]fs.DirEntry
	}
	dstListings := make(map[string]dstDirListing)
	if len(frames) == 0 {
		frames = []resume.ScanFrame{{Dir: s.dir, DstDir: s.dir}}
	}
	for len(frames) > 0 {
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
		dstListing, ok := dstListings[listingKey]
		if !ok {
			dstEntries, err := resume.SortedDirEntries(s.ctx, s.fdst, frame.DstDir, false, keyer.DstKey)
			if err != nil && !errors.Is(err, fs.ErrorDirNotFound) {
				return err
			}
			if errors.Is(err, fs.ErrorDirNotFound) {
				dstEntries = nil
				err = nil
			}
			dstListing = dstDirListing{
				byKey: resume.DirByKey(dstEntries, keyer.DstKey),
			}
			dstListings[listingKey] = dstListing
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
				if err = s.flushResumeCopyBatch(store, snapshot, batch); err != nil {
					return err
				}
				batch = nil
				batchOpenedAt = time.Time{}
				nextDstDir, dirErr := s.handleResumeDirectory(x, dstListing.byKey[matchKey])
				if dirErr != nil {
					return dirErr
				}
				frame.LastDoneEntryKey = cursorKey
				frames = append(frames, resume.ScanFrame{Dir: x.Remote(), DstDir: nextDstDir})
				scan := s.resumeCopyScanState(frames, false, snapshot.Totals.PendingFailedCount)
				if err = store.SaveScan(scan); err != nil {
					return err
				}
				snapshot.Scan = scan
				descended = true
			case fs.Object:
				task, alreadyDone, fileErr := s.prepareResumeCopyTask(store, x, dstListing.byKey[matchKey], cursorKey, append([]resume.ScanFrame(nil), frames...), skipDoneLookup)
				if fileErr != nil {
					return fileErr
				}
				if alreadyDone {
					frame.LastDoneEntryKey = cursorKey
					continue
				}
				if len(batch) == 0 {
					batchOpenedAt = time.Now()
				}
				batch = append(batch, task)
				if len(batch) >= resumeCopyCommitBatchSize || (!batchOpenedAt.IsZero() && time.Since(batchOpenedAt) >= resumeCopyCommitBatchInterval) {
					if err = s.flushResumeCopyBatch(store, snapshot, batch); err != nil {
						return err
					}
					batch = nil
					batchOpenedAt = time.Time{}
				}
			default:
				return fmt.Errorf("unsupported source entry %T", srcEntry)
			}
			if descended {
				break
			}
		}
		if descended {
			continue
		}
		if err = s.flushResumeCopyBatch(store, snapshot, batch); err != nil {
			return err
		}
		batch = nil
		batchOpenedAt = time.Time{}
		if srcPage.NextContinuationToken != "" {
			frame.ContinuationToken = srcPage.NextContinuationToken
			frame.PageIndex = pageIndex + 1
			scan := s.resumeCopyScanState(frames, false, snapshot.Totals.PendingFailedCount)
			if err = store.SaveScan(scan); err != nil {
				return err
			}
			snapshot.Scan = scan
			continue
		}
		delete(dstListings, listingKey)
		frames = frames[:len(frames)-1]
		scan := s.resumeCopyScanState(frames, len(frames) == 0, snapshot.Totals.PendingFailedCount)
		if err = store.SaveScan(scan); err != nil {
			return err
		}
		snapshot.Scan = scan
	}
	snapshot.Scan = s.resumeCopyScanState(nil, true, snapshot.Totals.PendingFailedCount)
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
		for _, result := range resumeObjectFrontierReady(frontierSegmentID, pending) {
			segmentID := result.segment.SegmentID
			if result.segment.Status == resume.ObjectSegmentFailed {
				if err := commitFailureBatch(result.failureCommits); err != nil {
					return err
				}
				delete(pending, segmentID)
				popInflight(segmentID)
				frontierBlocked = true
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
		}
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
				task, _, taskErr := s.prepareResumeCopyTask(store, x, dstByKey[matchKey], cursorKey, nil, true)
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
				lastScannedKey = sourceKey
				frame.LastDoneEntryKey = cursorKey
				if len(current.tasks) >= segmentSize || (!segmentOpenedAt.IsZero() && checkpointInterval > 0 && time.Since(segmentOpenedAt) >= checkpointInterval) {
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

func (s *syncCopyMove) prepareResumeCopyTask(store *resume.Store, src fs.Object, dstEntry fs.DirEntry, cursorKey string, scanFrames []resume.ScanFrame, skipDoneLookup bool) (task resumeCopyTask, alreadyDone bool, err error) {
	targetRemote := resume.TargetRemote(s.ctx, src)
	dstRemote := targetRemote
	var dstObj fs.Object
	if dstEntry != nil {
		dstRemote = dstEntry.Remote()
		if object, ok := dstEntry.(fs.Object); ok {
			dstObj = object
		}
	}
	workKey := resume.WorkKey("copy", src.Remote(), dstRemote, resume.EntryFingerprint(s.ctx, src))
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
	doneRecord := resume.DoneRecord{
		WorkKey:     workKey,
		Op:          "copy",
		Kind:        "object",
		SrcRemote:   src.Remote(),
		DstRemote:   resume.TargetRemote(s.ctx, src),
		Fingerprint: resume.EntryFingerprint(s.ctx, src),
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
		Fingerprint:   doneRecord.Fingerprint,
		Name:          name,
		Size:          src.Size(),
		Checked:       false,
		What:          "transferring",
		FirstFailedAt: startedAt,
		LastFailedAt:  startedAt,
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
		failedRecord.LastError = err.Error()
		return doneRecord, failedRecord, err
	}
	if dst != nil && s.backupDir != nil {
		s.markDirModifiedObject(dst)
		err := operations.MoveBackupDir(s.ctx, s.backupDir, dst)
		if err != nil {
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

func (s *syncCopyMove) resumeCopyScanState(frames []resume.ScanFrame, complete bool, pendingFailed int64) resume.ScanState {
	return buildCopyScanState(frames, s.resumeOverErrorLimit(pendingFailed), complete)
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
