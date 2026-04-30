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

func TestSpeedIgnoresStaleAverageWhenLoopStopped(t *testing.T) {
	stats := NewStats(context.Background())
	stats.mu.Lock()
	stats.bytes = 8192
	stats.startTime = time.Now().Add(-4 * time.Second)
	stats.average.speed = 999999
	stats.average.started = false
	stats.mu.Unlock()

	stats.mu.RLock()
	stats.average.mu.Lock()
	speed := stats._speed()
	stats.average.mu.Unlock()
	stats.mu.RUnlock()

	assert.InDelta(t, 2048.0, speed, 512.0)
}

func TestSpeedUsesRecentPendingBytesWhenAvailable(t *testing.T) {
	stats := NewStats(context.Background())
	stats.mu.Lock()
	stats.bytes = 8192
	stats.startTime = time.Now().Add(-8 * time.Second)
	stats.average.speed = 1234
	stats.average.started = true
	stats.average.lpTime = time.Now().Add(-500 * time.Millisecond)
	stats.average.lpBytes = 4096
	stats.mu.Unlock()

	stats.mu.RLock()
	stats.average.mu.Lock()
	speed := stats._speed()
	stats.average.mu.Unlock()
	stats.mu.RUnlock()

	assert.Greater(t, speed, 4000.0)
	assert.NotEqual(t, 1234.0, speed)
}
