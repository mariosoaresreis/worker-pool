package main_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/example/workerpool/internal/job"
	"github.com/example/workerpool/internal/result"
	"github.com/example/workerpool/internal/worker"
	"github.com/example/workerpool/pkg/retry"
)

var workerTestLogger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

// run launches a worker and returns its channels.
func run(t *testing.T, handler worker.HandlerFunc) (
	jobCh chan job.Job,
	resultCh chan result.Result,
	cancel context.CancelFunc,
	done chan struct{},
) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	jobCh = make(chan job.Job, 16)
	resultCh = make(chan result.Result, 16)
	done = make(chan struct{})

	retryCfg := retry.Config{MaxAttempts: 1} // no retries by default in tests
	w := worker.New(1, handler, retryCfg, workerTestLogger)

	go func() {
		defer close(done)
		w.Run(ctx, jobCh, resultCh)
	}()
	return
}

// TestWorker_HappyPath verifies a successful job produces a result with no error.
func TestWorker_HappyPath(t *testing.T) {
	handler := func(_ context.Context, j job.Job) (any, error) {
		return "ok-" + j.ID, nil
	}

	jobCh, resultCh, cancel, done := run(t, handler)
	defer cancel()

	jobCh <- job.New("j1", job.TypeGeneric, nil)

	select {
	case r := <-resultCh:
		if r.Err != nil {
			t.Fatalf("expected no error, got %v", r.Err)
		}
		if r.Output != "ok-j1" {
			t.Fatalf("unexpected output: %v", r.Output)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for result")
	}

	cancel()
	<-done
}

// TestWorker_PanicRecovery verifies a panicking handler does not kill the worker.
func TestWorker_PanicRecovery(t *testing.T) {
	calls := 0
	handler := func(_ context.Context, j job.Job) (any, error) {
		calls++
		if calls == 1 {
			panic("deliberate panic")
		}
		return "recovered", nil
	}

	jobCh, resultCh, cancel, done := run(t, handler)
	defer cancel()

	jobCh <- job.New("panic-job", job.TypeGeneric, nil)

	r := <-resultCh
	if r.Err == nil {
		t.Fatal("expected error wrapping panic, got nil")
	}

	// Worker must still be alive to process a second job.
	jobCh <- job.New("after-panic", job.TypeGeneric, nil)
	r2 := <-resultCh
	if r2.Err != nil {
		t.Fatalf("expected success after panic recovery, got %v", r2.Err)
	}

	cancel()
	<-done
}

// TestWorker_ContextCancellation verifies the worker exits promptly when ctx is cancelled.
func TestWorker_ContextCancellation(t *testing.T) {
	handler := func(ctx context.Context, _ job.Job) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	_, _, cancel, done := run(t, handler)

	cancel()

	select {
	case <-done:
		// Correct: worker exited.
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not exit after context cancellation")
	}
}

// TestWorker_DrainOnShutdown verifies buffered jobs are processed after ctx cancel.
func TestWorker_DrainOnShutdown(t *testing.T) {
	var mu sync.Mutex
	processed := []string{}

	handler := func(_ context.Context, j job.Job) (any, error) {
		mu.Lock()
		processed = append(processed, j.ID)
		mu.Unlock()
		return nil, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	jobCh := make(chan job.Job, 10)
	resultCh := make(chan result.Result, 10)
	done := make(chan struct{})

	retryCfg := retry.Config{MaxAttempts: 1}
	w := worker.New(1, handler, retryCfg, workerTestLogger)

	go func() {
		defer close(done)
		w.Run(ctx, jobCh, resultCh)
	}()

	// Fill the buffer before cancelling so all jobs are in-flight at shutdown.
	const numJobs = 5
	for i := range numJobs {
		jobCh <- job.New(fmt.Sprintf("drain-%d", i), job.TypeGeneric, nil)
	}

	// Cancel before any jobs are picked up (worker is blocked in select).
	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()
	if len(processed) != numJobs {
		t.Fatalf("expected %d drained jobs, got %d", numJobs, len(processed))
	}
}

// TestWorker_RetryOnTransientError verifies the retry loop fires on failure.
func TestWorker_RetryOnTransientError(t *testing.T) {
	attempts := 0
	handler := func(_ context.Context, j job.Job) (any, error) {
		attempts++
		if attempts < 3 {
			return nil, errors.New("transient")
		}
		return "done", nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	jobCh := make(chan job.Job, 4)
	resultCh := make(chan result.Result, 4)

	retryCfg := retry.Config{
		MaxAttempts: 3,
		BaseDelay:   1 * time.Millisecond,
		MaxDelay:    5 * time.Millisecond,
		Multiplier:  2,
	}
	w := worker.New(1, handler, retryCfg, workerTestLogger)

	go w.Run(ctx, jobCh, resultCh)

	jobCh <- job.New("retry-me", job.TypeGeneric, nil)

	select {
	case r := <-resultCh:
		if r.Err != nil {
			t.Fatalf("expected success after retries, got %v", r.Err)
		}
		if attempts != 3 {
			t.Fatalf("expected 3 attempts, got %d", attempts)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for retried result")
	}
}
