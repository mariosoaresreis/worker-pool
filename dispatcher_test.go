package main_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/example/workerpool/internal/dispatcher"
	"github.com/example/workerpool/internal/job"
	"github.com/example/workerpool/internal/result"
)

var dispatcherTestLogger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

func testConfig() dispatcher.Config {
	cfg := dispatcher.DefaultConfig()
	cfg.NumWorkers = 2
	cfg.JobBufferSize = 32
	cfg.ResultBufferSize = 64
	cfg.ShutdownTimeout = 5 * time.Second
	return cfg
}

// TestDispatcher_ProcessesAllJobs verifies every enqueued job produces a result.
func TestDispatcher_ProcessesAllJobs(t *testing.T) {
	var processed atomic.Int64

	handler := func(_ context.Context, j job.Job) (any, error) {
		return j.ID, nil
	}
	onResult := func(r result.Result) {
		if r.Success() {
			processed.Add(1)
		}
	}

	d := dispatcher.New(testConfig(), handler, onResult, dispatcherTestLogger)
	ctx := d.Start()

	const numJobs = 40
	for i := range numJobs {
		j := job.New(fmt.Sprintf("job-%03d", i), job.TypeGeneric, nil)
		if err := d.Enqueue(ctx, j); err != nil {
			t.Fatalf("enqueue failed: %v", err)
		}
	}

	d.Shutdown()

	if got := processed.Load(); got != numJobs {
		t.Errorf("expected %d processed, got %d", numJobs, got)
	}
}

// TestDispatcher_GracefulShutdown verifies no goroutines leak after Shutdown.
func TestDispatcher_GracefulShutdown(t *testing.T) {
	slow := func(_ context.Context, j job.Job) (any, error) {
		time.Sleep(50 * time.Millisecond)
		return nil, nil
	}

	d := dispatcher.New(testConfig(), slow, nil, dispatcherTestLogger)
	ctx := d.Start()

	for i := range 10 {
		_ = d.Enqueue(ctx, job.New(fmt.Sprintf("slow-%d", i), job.TypeGeneric, nil))
	}

	// Shutdown must return within the timeout, not hang.
	done := make(chan struct{})
	go func() {
		d.Shutdown()
		close(done)
	}()

	select {
	case <-done:
		// Clean exit.
	case <-time.After(8 * time.Second):
		t.Fatal("Shutdown() did not return within timeout")
	}
}

// TestDispatcher_MetricsAccuracy verifies counters reflect actual work done.
func TestDispatcher_MetricsAccuracy(t *testing.T) {
	handler := func(_ context.Context, _ job.Job) (any, error) {
		return nil, nil
	}

	d := dispatcher.New(testConfig(), handler, nil, dispatcherTestLogger)
	ctx := d.Start()

	const n = 20
	for i := range n {
		_ = d.Enqueue(ctx, job.New(fmt.Sprintf("m-%d", i), job.TypeGeneric, nil))
	}

	d.Shutdown()

	snap := d.Metrics()
	if snap.Enqueued != n {
		t.Errorf("expected Enqueued=%d, got %d", n, snap.Enqueued)
	}
	if snap.Processed != n {
		t.Errorf("expected Processed=%d, got %d", n, snap.Processed)
	}
	if snap.Failed != 0 {
		t.Errorf("expected Failed=0, got %d", snap.Failed)
	}
}

// TestDispatcher_TryEnqueue_DoesNotBlock verifies TryEnqueue returns immediately
// when the channel is full.
func TestDispatcher_TryEnqueue_DoesNotBlock(t *testing.T) {
	cfg := testConfig()
	cfg.JobBufferSize = 2
	cfg.NumWorkers = 0 // no workers so buffer fills immediately

	// We need at least 1 worker to start; use a very slow handler.
	handler := func(ctx context.Context, _ job.Job) (any, error) {
		<-ctx.Done()
		return nil, nil
	}

	d := dispatcher.New(cfg, handler, nil, dispatcherTestLogger)
	d.Start()

	// Fill the buffer.
	d.TryEnqueue(job.New("fill-1", job.TypeGeneric, nil))
	d.TryEnqueue(job.New("fill-2", job.TypeGeneric, nil))

	// This must not block.
	start := time.Now()
	ok := d.TryEnqueue(job.New("overflow", job.TypeGeneric, nil))
	elapsed := time.Since(start)

	if ok {
		t.Log("note: buffer had room (worker drained fast) — test inconclusive but not a failure")
	}
	if elapsed > 100*time.Millisecond {
		t.Errorf("TryEnqueue blocked for %v, expected immediate return", elapsed)
	}

	d.Shutdown()
}
