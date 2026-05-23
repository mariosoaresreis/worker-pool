//go:build ignore

package worker

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/example/workerpool/internal/job"
	"github.com/example/workerpool/internal/result"
	"github.com/example/workerpool/pkg/retry"
)

// HandlerFunc processes a job and returns an output value or error.
type HandlerFunc func(ctx context.Context, j job.Job) (any, error)

// Worker is a single goroutine that reads from jobCh and writes to resultCh.
type Worker struct {
	id       int
	handler  HandlerFunc
	retryCfg retry.Config
	logger   *slog.Logger
}

// New creates a Worker. Call Run to start the goroutine.
func New(id int, handler HandlerFunc, retryCfg retry.Config, logger *slog.Logger) *Worker {
	return &Worker{
		id:       id,
		handler:  handler,
		retryCfg: retryCfg,
		logger:   logger.With("worker_id", id),
	}
}

// Run starts the worker's select loop. It blocks until ctx is cancelled,
// then drains any remaining jobs already in the channel before returning.
// Call wg.Done() in the goroutine that launches Run.
func (w *Worker) Run(ctx context.Context, jobCh <-chan job.Job, resultCh chan<- result.Result) {
	w.logger.Info("worker started")
	defer w.logger.Info("worker stopped")

	for {
		select {
		// ── happy path: pick up a job ──────────────────────────────────────
		case j, ok := <-jobCh:
			if !ok {
				// Channel was closed by dispatcher; drain complete.
				return
			}
			w.process(ctx, j, resultCh)

		// ── shutdown path: context cancelled ──────────────────────────────
		case <-ctx.Done():
			w.logger.Info("context cancelled, draining remaining jobs")
			w.drain(jobCh, resultCh)
			return
		}
	}
}

// process executes the handler with retry logic and panic recovery.
func (w *Worker) process(ctx context.Context, j job.Job, resultCh chan<- result.Result) {
	startedAt := time.Now()

	var (
		output any
		err    error
	)

	for j.Attempts = 0; ; j.Attempts++ {
		output, err = w.safeRun(ctx, j)
		if err == nil {
			break
		}

		if !w.retryCfg.ShouldRetry(j.Attempts + 1) {
			w.logger.Warn("job failed, max retries reached",
				"job_id", j.ID, "attempts", j.Attempts+1, "error", err)
			break
		}

		w.logger.Info("job failed, retrying",
			"job_id", j.ID, "attempt", j.Attempts+1, "error", err)

		if sleepErr := w.retryCfg.Sleep(ctx, j.Attempts+1); sleepErr != nil {
			// Context cancelled during backoff sleep — stop retrying.
			err = fmt.Errorf("retry aborted: %w", sleepErr)
			break
		}
	}

	res := result.Result{
		Job:      j,
		Output:   output,
		Err:      err,
		WorkerID: w.id,
		StartedAt: startedAt,
		EndedAt:  time.Now(),
	}

	// Non-blocking send: if resultCh is full, log and drop rather than block.
	select {
	case resultCh <- res:
	default:
		w.logger.Warn("result channel full, dropping result", "job_id", j.ID)
	}
}

// safeRun calls the handler inside a recover so a panicking job never
// kills the worker goroutine.
func (w *Worker) safeRun(ctx context.Context, j job.Job) (output any, err error) {
	defer func() {
		if r := recover(); r != nil {
			stack := string(debug.Stack())
			err = fmt.Errorf("panic in job %s: %v\n%s", j.ID, r, stack)
			w.logger.Error("recovered panic", "job_id", j.ID, "panic", r)
		}
	}()
	return w.handler(ctx, j)
}

// drain processes all jobs already buffered in jobCh after ctx is cancelled.
// This prevents silent data loss when the process shuts down.
func (w *Worker) drain(jobCh <-chan job.Job, resultCh chan<- result.Result) {
	// Use a background context so the handler itself is not cancelled.
	drainCtx := context.Background()
	drained := 0
	for {
		select {
		case j, ok := <-jobCh:
			if !ok {
				w.logger.Info("drain complete, channel closed", "drained", drained)
				return
			}
			w.logger.Info("draining job", "job_id", j.ID)
			w.process(drainCtx, j, resultCh)
			drained++
		default:
			// Nothing left in the buffer.
			w.logger.Info("drain complete, channel empty", "drained", drained)
			return
		}
	}
}
