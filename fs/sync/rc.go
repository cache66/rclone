package sync

import (
	"context"
	"fmt"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/rc"
	"github.com/rclone/rclone/fs/resume"
)

func init() {
	for _, name := range []string{"sync", "copy", "move"} {
		moveHelp := ""
		resumeHelp := ""
		if name == "move" {
			moveHelp = "- deleteEmptySrcDirs - delete empty src directories if set\n"
		}
		if name == "copy" {
			resumeHelp = "- resume - enable Resume V1 for copy (boolean, optional)\n" +
				"- resumeId - explicit Resume V1 job ID for copy (string, optional)\n"
		}
		rc.Add(rc.Call{
			Path:         "sync/" + name,
			AuthRequired: true,
			Fn: func(ctx context.Context, in rc.Params) (rc.Params, error) {
				return rcSyncCopyMove(ctx, in, name)
			},
			Title: name + " a directory from source remote to destination remote",
			Help: `This takes the following parameters:

- srcFs - a remote name string e.g. "drive:src" for the source
- dstFs - a remote name string e.g. "drive:dst" for the destination
- createEmptySrcDirs - create empty src directories on destination if set
` + moveHelp + resumeHelp + `

See the [` + name + `](/commands/rclone_` + name + `/) command for more information on the above.`,
		})
	}

	rc.Add(rc.Call{
		Path:         "sync/resume/status",
		AuthRequired: true,
		Fn:           rcResumeStatus,
		Title:        "Show persisted copy resume state",
		Help: `This shows the persisted resume state for a copy job.

Supply either:

- jobId - explicit resume job ID
- resumeId - explicit resume job ID

or:

- srcFs - source remote name
- dstFs - destination remote name

When srcFs and dstFs are supplied, the job ID is derived the same way as copy resume.`,
	})

	rc.Add(rc.Call{
		Path:         "sync/resume/clear",
		AuthRequired: true,
		Fn:           rcResumeClear,
		Title:        "Clear persisted copy resume state",
		Help: `This clears the persisted resume state for a copy job.

Supply either:

- jobId - explicit resume job ID
- resumeId - explicit resume job ID

or:

- srcFs - source remote name
- dstFs - destination remote name`,
	})
}

// Sync/Copy/Move a file
func rcSyncCopyMove(ctx context.Context, in rc.Params, name string) (out rc.Params, err error) {
	srcFs, err := rc.GetFsNamed(ctx, in, "srcFs")
	if err != nil {
		return nil, err
	}
	dstFs, err := rc.GetFsNamed(ctx, in, "dstFs")
	if err != nil {
		return nil, err
	}
	createEmptySrcDirs, err := in.GetBool("createEmptySrcDirs")
	if rc.NotErrParamNotFound(err) {
		return nil, err
	}
	ctx, err = rcApplyResumeParams(ctx, in, name)
	if err != nil {
		return nil, err
	}
	switch name {
	case "sync":
		return nil, Sync(ctx, dstFs, srcFs, createEmptySrcDirs)
	case "copy":
		return nil, CopyDir(ctx, dstFs, srcFs, createEmptySrcDirs)
	case "move":
		deleteEmptySrcDirs, err := in.GetBool("deleteEmptySrcDirs")
		if rc.NotErrParamNotFound(err) {
			return nil, err
		}
		return nil, MoveDir(ctx, dstFs, srcFs, deleteEmptySrcDirs, createEmptySrcDirs)
	}
	panic("unknown rcSyncCopyMove type")
}

func rcApplyResumeParams(ctx context.Context, in rc.Params, name string) (context.Context, error) {
	resumeEnabled, err := in.GetBool("resume")
	if rc.NotErrParamNotFound(err) {
		return nil, err
	}
	resumeID, err := in.GetString("resumeId")
	if rc.NotErrParamNotFound(err) {
		return nil, err
	}
	if !resumeEnabled && resumeID == "" {
		return ctx, nil
	}
	if name != "copy" {
		return nil, fmt.Errorf("resume parameters are supported for sync/copy only when calling copy")
	}
	ctx, ci := fs.AddConfig(ctx)
	ci.Resume = true
	if resumeID != "" {
		ci.ResumeID = resumeID
	}
	return ctx, nil
}

func rcResumeStatus(ctx context.Context, in rc.Params) (out rc.Params, err error) {
	jobID, err := rcResumeJobID(ctx, in)
	if err != nil {
		return nil, err
	}
	store, err := resume.Open(ctx, jobID)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = store.Close(false)
	}()

	snapshot, err := store.Snapshot()
	if err != nil {
		return nil, err
	}
	found := snapshot.Meta.JobID != ""
	failed, err := store.ListFailed()
	if err != nil {
		return nil, err
	}
	if !found {
		failed = nil
	}

	return rc.Params{
		"jobId":   jobID,
		"found":   found,
		"meta":    snapshot.Meta,
		"scan":    snapshot.Scan,
		"totals":  snapshot.Totals,
		"run":     snapshot.Run,
		"history": snapshot.History,
		"failed":  failed,
	}, nil
}

func rcResumeClear(ctx context.Context, in rc.Params) (out rc.Params, err error) {
	jobID, err := rcResumeJobID(ctx, in)
	if err != nil {
		return nil, err
	}
	store, err := resume.Open(ctx, jobID)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = store.Close(false)
	}()

	snapshot, err := store.Snapshot()
	if err != nil {
		return nil, err
	}
	found := snapshot.Meta.JobID != ""
	if found {
		if err = store.Clear(); err != nil {
			return nil, err
		}
	}
	return rc.Params{
		"jobId":   jobID,
		"found":   found,
		"cleared": found,
	}, nil
}

func rcResumeJobID(ctx context.Context, in rc.Params) (string, error) {
	jobID, err := in.GetString("jobId")
	if err == nil {
		return jobID, nil
	}
	if rc.NotErrParamNotFound(err) {
		return "", err
	}
	jobID, err = in.GetString("resumeId")
	if err == nil {
		return jobID, nil
	}
	if rc.NotErrParamNotFound(err) {
		return "", err
	}
	srcFs, err := rc.GetFsNamed(ctx, in, "srcFs")
	if err != nil {
		return "", err
	}
	dstFs, err := rc.GetFsNamed(ctx, in, "dstFs")
	if err != nil {
		return "", err
	}
	return resume.DerivedJobID(resume.NewMeta(ctx, "copy", srcFs, dstFs)), nil
}
