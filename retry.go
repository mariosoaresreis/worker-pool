//go:build ignore

package retry

import (
	"context"
	"math"
	"math/rand"
	"time"
)

// Config controls retry behaviour.
type Config struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	Multiplier  float64
}

// Default returns a sensible retry configuration.
func Default() Config {
	return Config{
		MaxAttempts: 3,
		BaseDelay:   200 * time.Millisecond,
		MaxDelay:    10 * time.Second,
		Multiplier:  2.0,
	}
}

// ShouldRetry returns true when attempts < max and the error is retryable.
func (c Config) ShouldRetry(attempts int) bool {
	return attempts < c.MaxAttempts
}

// Delay returns the backoff duration for the given attempt (1-indexed),
// with full jitter to spread thundering-herd retries.
func (c Config) Delay(attempt int) time.Duration {
	exp := math.Pow(c.Multiplier, float64(attempt-1))
	base := float64(c.BaseDelay) * exp
	if base > float64(c.MaxDelay) {
		base = float64(c.MaxDelay)
	}
	// Full jitter: sleep between 0 and calculated delay.
	jittered := time.Duration(rand.Float64() * base)
	return jittered
}

// Sleep blocks for the backoff duration or until ctx is cancelled.
// Returns ctx.Err() if cancelled, nil otherwise.
func (c Config) Sleep(ctx context.Context, attempt int) error {
	d := c.Delay(attempt)
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
