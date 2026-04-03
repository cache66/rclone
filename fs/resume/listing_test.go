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

type pagedMockFs struct {
	*mockfs.Fs
	pages map[string]mockPage
}

type mockPage struct {
	entries       fs.DirEntries
	nextToken     string
	tokenAccepted bool
}

func (f *pagedMockFs) ResumeListDir(ctx context.Context, dir, continuationToken string) (entries fs.DirEntries, nextContinuationToken string, tokenAccepted bool, err error) {
	page, ok := f.pages[continuationToken]
	if !ok {
		page = f.pages[""]
	}
	return append(fs.DirEntries(nil), page.entries...), page.nextToken, page.tokenAccepted, nil
}

func TestListDirPageUsesNativeCursor(t *testing.T) {
	ctx := context.Background()
	base, err := mockfs.NewFs(ctx, "mock", "", nil)
	require.NoError(t, err)

	f := &pagedMockFs{
		Fs: base.(*mockfs.Fs),
		pages: map[string]mockPage{
			"stale": {
				entries: fs.DirEntries{
					mockobject.New("dir/file.txt"),
					mockdir.New("dir/file.txt"),
				},
				nextToken:     "next-page",
				tokenAccepted: false,
			},
		},
	}

	page, err := ListDirPage(ctx, f, "dir", false, func(entry fs.DirEntry) string {
		return entry.Remote()
	}, "stale")
	require.NoError(t, err)
	require.Len(t, page.Entries, 2)
	assert.True(t, page.NativeOrder)
	assert.False(t, page.TokenAccepted)
	assert.Equal(t, "next-page", page.NextContinuationToken)
	assert.Less(t, CursorKey(0, 0), CursorKey(0, 1))
}

func TestSortedDirEntriesIgnoreCaseDuplicatesHaveDistinctCursorBoundaries(t *testing.T) {
	ctx, ci := fs.AddConfig(context.Background())
	ci.IgnoreCaseSync = true

	base, err := mockfs.NewFs(ctx, "mock", "", nil)
	require.NoError(t, err)
	f := base.(*mockfs.Fs)
	first := mockobject.New("a.txt").WithContent([]byte("a"), mockobject.SeekModeRegular)
	second := mockobject.New("A.txt").WithContent([]byte("A"), mockobject.SeekModeRegular)
	f.AddObject(first)
	f.AddObject(second)

	keyer := NewMatchKeyer(ctx, f)
	entries, err := SortedDirEntries(ctx, f, "", false, keyer.SrcKey)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	assert.Equal(t, "a.txt", entries[0].Remote())
	assert.Equal(t, "A.txt", entries[1].Remote())
	assert.Equal(t, keyer.SrcKey(entries[0]), keyer.SrcKey(entries[1]))
	assert.Less(t, CursorKey(0, 0), CursorKey(0, 1))
}

func TestSortedDirEntriesUnicodeDuplicatesHaveDistinctCursorBoundaries(t *testing.T) {
	ctx := context.Background()

	base, err := mockfs.NewFs(ctx, "mock", "", nil)
	require.NoError(t, err)
	f := base.(*mockfs.Fs)
	first := mockobject.New("\u00e9.txt").WithContent([]byte("a"), mockobject.SeekModeRegular)
	second := mockobject.New("e\u0301.txt").WithContent([]byte("b"), mockobject.SeekModeRegular)
	f.AddObject(first)
	f.AddObject(second)

	keyer := NewMatchKeyer(ctx, f)
	entries, err := SortedDirEntries(ctx, f, "", false, keyer.SrcKey)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	assert.Equal(t, "\u00e9.txt", entries[0].Remote())
	assert.Equal(t, "e\u0301.txt", entries[1].Remote())
	assert.Equal(t, keyer.SrcKey(entries[0]), keyer.SrcKey(entries[1]))
	assert.Less(t, CursorKey(0, 0), CursorKey(0, 1))
}

func TestMatchKeyerDistinguishesFileAndDirectory(t *testing.T) {
	ctx := context.Background()
	base, err := mockfs.NewFs(ctx, "mock", "", nil)
	require.NoError(t, err)

	keyer := NewMatchKeyer(ctx, base)
	assert.NotEqual(t, keyer.SrcKey(mockobject.New("name")), keyer.SrcKey(mockdir.New("name")))
}
