package dispatcher

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/example/workerpool/internal/job"
	"github.com/example/workerpool/internal/metrics"
	"github.com/example/workerpool/internal/result"
	"github.com/example/workerpool/internal/shutdown"
	"github.com/example/workerpool/internal/worker"
	"github.com/example/workerpool/pkg/retry"
)

// Config holds all tunable parameters for the Dispatcher.
type Config struct {
	NumWorkers       int
	JobBufferSize    int
	ResultBufferSize int
	ShutdownTimeout  time.Duration
	Retry            retry.Config
}

// DefaultConfig returns production-ready defaults.
func DefaultConfig() Config {
	return Config{
		NumWorkers:       8,
		JobBufferSize:    256,
		ResultBufferSize: 512,
		ShutdownTimeout:  15 * time.Second,
		Retry:            retry.Default(),
	}
}

// Dispatcher owns the job/result channels, the worker pool, and the
// collector goroutine. It is the single entry point for producers.
type Dispatcher struct {
	cfg      Config
	jobCh    chan job.Job
	resultCh chan result.Result

	// workerWg tracks only worker goroutines so we can close resultCh
	// exactly once all workers have exited (and thus will never send again).
	workerWg sync.WaitGroup

	ctrl     *shutdown.Controller
	metrics  *metrics.Counters
	handler  worker.HandlerFunc
	onResult func(result.Result)
	logger   *slog.Logger
}

// New creates a Dispatcher but does not start any goroutines.
func New(
	cfg Config,
	handler worker.HandlerFunc,
	onResult func(result.Result),
	logger *slog.Logger,
) *Dispatcher {
	return &Dispatcher{
		cfg:      cfg,
		jobCh:    make(chan job.Job, cfg.JobBufferSize),
		resultCh: make(chan result.Result, cfg.ResultBufferSize),
		ctrl:     shutdown.New(cfg.ShutdownTimeout, logger),
		metrics:  &metrics.Counters{},
		handler:  handler,
		onResult: onResult,
		logger:   logger,
	}
}

// Start launches workers, the collector, and the metrics ticker.
// Returns the root context (cancelled on Shutdown or OS signal).
func (d *Dispatcher) Start() context.Context {
	ctx := d.ctrl.Context()

	// ── spawn workers ───────────────────────────────────────────────────────
	for i := range d.cfg.NumWorkers {
		w := worker.New(i+1, d.wrappedHandler(), d.cfg.Retry, d.logger)
		d.workerWg.Add(1)
		d.ctrl.Add(1)
		go func() {
			defer d.workerWg.Done()
			defer d.ctrl.Done()
			w.Run(ctx, d.jobCh, d.resultCh)
		}()
	}

	// ── spawn collector (fan-in) ────────────────────────────────────────────
	// The collector exits when resultCh is closed (after all workers finish).
	d.ctrl.Add(1)
	go func() {
		defer d.ctrl.Done()
		d.collect()
	}()

	// ── spawn metrics ticker ────────────────────────────────────────────────
	d.ctrl.Add(1)
	go func() {
		defer d.ctrl.Done()
		d.reportMetrics(ctx)
	}()

	d.logger.Info("dispatcher started",
		"workers", d.cfg.NumWorkers,
		"job_buffer", d.cfg.JobBufferSize,
	)
	return ctx
}

// Enqueue submits a job. Blocks when the buffer is full (backpressure).
// Returns an error if ctx is cancelled before the job is accepted.
func (d *Dispatcher) Enqueue(ctx context.Context, j job.Job) error {
	select {
	case d.jobCh <- j:
		d.metrics.Enqueued.Add(1)
		d.logger.Debug("job enqueued", "job_id", j.ID, "type", j.Type)
		return nil
	case <-ctx.Done():
		d.metrics.Dropped.Add(1)
		return fmt.Errorf("enqueue cancelled: %w", ctx.Err())
	}
}

// TryEnqueue submits a job without blocking.
// Returns false immediately if the buffer is full.
func (d *Dispatcher) TryEnqueue(j job.Job) bool {
	select {
	case d.jobCh <- j:
		d.metrics.Enqueued.Add(1)
		return true
	default:
		d.metrics.Dropped.Add(1)
		d.logger.Warn("job dropped, channel full", "job_id", j.ID)
		return false
	}
}

// Shutdown drains all in-flight work and stops every goroutine cleanly.
// Sequence:
//  1. close(jobCh)    → workers stop accepting new jobs.
//  2. workerWg.Wait() → all workers finish draining and exit.
//  3. close(resultCh) → collector drains remaining results, then exits via range.
//  4. ctrl.Shutdown() → waits for collector + metrics goroutines, then returns.
func (d *Dispatcher) Shutdown() {
	close(d.jobCh)

	// Close resultCh only after every worker has exited so no sender remains.
	go func() {
		d.workerWg.Wait()
		close(d.resultCh)
	}()

	d.ctrl.Shutdown()
}

// Metrics returns a point-in-time snapshot of runtime counters.
func (d *Dispatcher) Metrics() metrics.Snapshot {
	return d.metrics.Snapshot()
}

// ── internal ────────────────────────────────────────────────────────────────

func (d *Dispatcher) wrappedHandler() worker.HandlerFunc {
	return func(ctx context.Context, j job.Job) (any, error) {
		d.metrics.InFlight.Add(1)
		defer d.metrics.InFlight.Add(-1)

		if j.Attempts > 0 {
			d.metrics.Retried.Add(1)
		}

		output, err := d.handler(ctx, j)
		if err != nil {
			d.metrics.Failed.Add(1)
		} else {
			d.metrics.Processed.Add(1)
		}
		return output, err
	}
}

// collect ranges over resultCh until it is closed, calling onResult for each.
// It exits naturally when the channel is closed by Shutdown.
func (d *Dispatcher) collect() {
	d.logger.Info("collector started")
	defer d.logger.Info("collector stopped")

	for r := range d.resultCh {
		if d.onResult != nil {
			d.onResult(r)
		}
	}
}

// reportMetrics logs a metrics snapshot every 5 seconds.
func (d *Dispatcher) reportMetrics(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			d.logger.Info("metrics", "snapshot", d.metrics.Snapshot().String())
		case <-ctx.Done():
			return
		}
	}
}

