package resume

import (
	"context"
	"testing"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fstest/mockdir"
	"github.com/rclone/rclone/fstest/mockfs"
	"github.com/rclone/rclone/fstest/mockobject"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWorkKeyStable(t *testing.T) {
	keyA := WorkKey("copy", "src/file.txt", "dst/file.txt", "fingerprint")
	keyB := WorkKey("copy", "src/file.txt", "dst/file.txt", "fingerprint")
	keyC := WorkKey("copy", "src/file.txt", "dst/other.txt", "fingerprint")

	assert.Equal(t, keyA, keyB)
	assert.NotEqual(t, keyA, keyC)
}

func TestEntryFingerprintObjectUsesFsFingerprint(t *testing.T) {
	ctx := context.Background()
	base, err := mockfs.NewFs(ctx, "mock", "", nil)
	require.NoError(t, err)
	f := base.(*mockfs.Fs)
	obj := mockobject.New("file.txt").WithContent([]byte("alpha"), mockobject.SeekModeRegular)
	f.AddObject(obj)

	expected := fs.Fingerprint(ctx, obj, true)
	assert.NotEmpty(t, expected)
	assert.Equal(t, expected, EntryFingerprint(ctx, obj))
}

func TestEntryFingerprintDirectoryUsesRemote(t *testing.T) {
	ctx := context.Background()
	dir := mockdir.New("dir")

	assert.Equal(t, "dir:dir", EntryFingerprint(ctx, dir))
}
