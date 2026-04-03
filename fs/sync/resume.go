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
	snapshot, _, err := store.LoadOrInit(meta, initialScan)
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
	if err = s.runResumeSourceScan(store, &snapshot); err != nil {
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
		return fserrors.FatalError(errors.New("--resume currently supports copy/check only, not sync deletes"))
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

func (s *syncCopyMove) runResumeSourceScan(store *resume.Store, snapshot *resume.Snapshot) error {
	keyer := resume.NewMatchKeyer(s.ctx, s.fdst)
	frames := snapshot.Scan.Frames
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
		dstEntries, err := resume.SortedDirEntries(s.ctx, s.fdst, frame.DstDir, false, keyer.DstKey)
		if err != nil && !errors.Is(err, fs.ErrorDirNotFound) {
			return err
		}
		if errors.Is(err, fs.ErrorDirNotFound) {
			dstEntries = nil
			err = nil
		}
		dstByKey := resume.DirByKey(dstEntries, keyer.DstKey)
		var batch []resumeCopyTask

		descended := false
		for entryIndex, srcEntry := range srcPage.Entries {
			matchKey := keyer.SrcKey(srcEntry)
			cursorKey := resume.CursorKey(pageIndex, entryIndex)
			if frame.LastDoneEntryKey != "" && cursorKey <= frame.LastDoneEntryKey {
				continue
			}
			switch x := srcEntry.(type) {
			case fs.Directory:
				if err = s.flushResumeCopyBatch(store, snapshot, &frames, batch); err != nil {
					return err
				}
				batch = nil
				frame = &frames[len(frames)-1]
				nextDstDir, dirErr := s.handleResumeDirectory(x, dstByKey[matchKey])
				if dirErr != nil {
					return dirErr
				}
				frame.LastDoneEntryKey = cursorKey
				scan := s.resumeCopyScanState(frames, false, snapshot.Totals.PendingFailedCount)
				if err = store.SaveScan(scan); err != nil {
					return err
				}
				frames = append(frames, resume.ScanFrame{Dir: x.Remote(), DstDir: nextDstDir})
				descended = true
			case fs.Object:
				task, alreadyDone, fileErr := s.prepareResumeCopyTask(store, x, dstByKey[matchKey], cursorKey)
				if fileErr != nil {
					return fileErr
				}
				if alreadyDone {
					frame.LastDoneEntryKey = cursorKey
					continue
				}
				batch = append(batch, task)
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
		if err = s.flushResumeCopyBatch(store, snapshot, &frames, batch); err != nil {
			return err
		}
		frame = &frames[len(frames)-1]
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

type resumeCopyTask struct {
	cursorKey string
	workKey   string
	src       fs.Object
	dst       fs.Object
}

type resumeCopyResult struct {
	task   resumeCopyTask
	done   resume.DoneRecord
	failed resume.FailedRecord
	err    error
}

func (s *syncCopyMove) prepareResumeCopyTask(store *resume.Store, src fs.Object, dstEntry fs.DirEntry, cursorKey string) (task resumeCopyTask, alreadyDone bool, err error) {
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
	done, err := store.HasDone(workKey)
	if err != nil {
		return task, false, err
	}
	if done {
		return task, true, nil
	}
	return resumeCopyTask{
		cursorKey: cursorKey,
		workKey:   workKey,
		src:       src,
		dst:       dstObj,
	}, false, nil
}

func (s *syncCopyMove) flushResumeCopyBatch(store *resume.Store, snapshot *resume.Snapshot, frames *[]resume.ScanFrame, batch []resumeCopyTask) error {
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

	for _, result := range results {
		currentFrames := *frames
		currentFrames[len(currentFrames)-1].LastDoneEntryKey = result.task.cursorKey
		scan := s.resumeCopyScanState(currentFrames, false, snapshot.Totals.PendingFailedCount)
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
				*frames = newSnapshot.Scan.Frames
				return limitErr
			}
			*snapshot = newSnapshot
			*frames = newSnapshot.Scan.Frames
			continue
		}

		historyEvent := resume.HistoryEvent{}
		if result.done.Bytes > 0 || result.done.Checks > 0 {
			historyEvent = resumeSuccessEvent(result.done)
		}
		newSnapshot, commitErr := store.CommitSuccess(resume.SuccessCommit{
			Done:         result.done,
			Scan:         scan,
			Event:        historyEvent,
			HistoryLimit: s.ci.ResumeHistoryLimit,
		})
		if commitErr != nil {
			return commitErr
		}
		if syncErr := s.syncResumeScanLimit(store, &newSnapshot); syncErr != nil {
			return syncErr
		}
		*snapshot = newSnapshot
		*frames = newSnapshot.Scan.Frames
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
