package resume

import (
	"context"
	"testing"

	"github.com/rclone/rclone/fstest/mockfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewMetaUsesLightweightJobBinding(t *testing.T) {
	ctx := context.Background()
	base, err := mockfs.NewFs(ctx, "mock", "", nil)
	require.NoError(t, err)

	srcA := base
	srcB := base
	dst := base

	metaA := NewMeta(ctx, "copy", srcA, dst)
	metaB := NewMeta(ctx, "copy", srcB, dst)

	assert.Equal(t, metaA.Op, metaB.Op)
	assert.Equal(t, metaA.SrcConfig, metaB.SrcConfig)
	assert.Equal(t, metaA.DstConfig, metaB.DstConfig)
	assert.True(t, metaA.Equal(metaB))
}
