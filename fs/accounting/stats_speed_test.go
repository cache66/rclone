package accounting

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSpeedFallsBackToTransferredBytesOverElapsedTime(t *testing.T) {
	stats := NewStats(context.Background())
	stats.mu.Lock()
	stats.bytes = 4096
	stats.startTime = time.Now().Add(-2 * time.Second)
	stats.average.speed = 0
	stats.mu.Unlock()

	stats.mu.RLock()
	stats.average.mu.Lock()
	speed := stats._speed()
	stats.average.mu.Unlock()
	stats.mu.RUnlock()

	assert.Greater(t, speed, 0.0)
	assert.InDelta(t, 2048.0, speed, 512.0)
}

