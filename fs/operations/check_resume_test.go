package operations_test

import (
	"context"
	"testing"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/operations"
	"github.com/stretchr/testify/require"
)

func TestCheckFnRejectsResume(t *testing.T) {
	require.NoError(t, config.SetCacheDir(t.TempDir()))

	ctx, ci := fs.AddConfig(context.Background())
	ci.Resume = true

	srcDir := t.TempDir()
	dstDir := t.TempDir()
	fsrc, err := fs.NewFs(ctx, srcDir)
	require.NoError(t, err)
	fdst, err := fs.NewFs(ctx, dstDir)
	require.NoError(t, err)

	err = operations.CheckFn(ctx, &operations.CheckOpt{
		Fsrc: fsrc,
		Fdst: fdst,
		Check: func(ctx context.Context, dst, src fs.Object) (differ bool, noHash bool, err error) {
			return false, false, nil
		},
	})
	require.EqualError(t, err, "--resume currently supports copy only")
}
