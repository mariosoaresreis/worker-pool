package main_test

import (
	"context"
	"testing"
	"time"

	"github.com/example/workerpool/pkg/retry"
)

func TestConfig_ShouldRetry(t *testing.T) {
	cfg := retry.Config{MaxAttempts: 3}

	cases := []struct {
		attempt int
		want    bool
	}{
		{0, true},
		{1, true},
		{2, true},
		{3, false},
		{4, false},
	}

	for _, tc := range cases {
		if got := cfg.ShouldRetry(tc.attempt); got != tc.want {
			t.Errorf("ShouldRetry(%d) = %v, want %v", tc.attempt, got, tc.want)
		}
	}
}

func TestConfig_Delay_BoundedByMax(t *testing.T) {
	cfg := retry.Config{
		MaxAttempts: 10,
		BaseDelay:   100 * time.Millisecond,
		MaxDelay:    500 * time.Millisecond,
		Multiplier:  2.0,
	}

	for attempt := 1; attempt <= 10; attempt++ {
		d := cfg.Delay(attempt)
		if d > cfg.MaxDelay {
			t.Errorf("attempt %d: delay %v exceeds MaxDelay %v", attempt, d, cfg.MaxDelay)
		}
		if d < 0 {
			t.Errorf("attempt %d: negative delay %v", attempt, d)
		}
	}
}

func TestConfig_Sleep_RespectsContextCancel(t *testing.T) {
	cfg := retry.Config{
		MaxAttempts: 3,
		BaseDelay:   10 * time.Second, // long enough to test cancellation
		MaxDelay:    10 * time.Second,
		Multiplier:  1,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := cfg.Sleep(ctx, 1)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected context error, got nil")
	}
	if elapsed > 500*time.Millisecond {
		t.Errorf("Sleep did not respect context cancellation, took %v", elapsed)
	}
}
