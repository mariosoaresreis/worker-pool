package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/example/workerpool/internal/dispatcher"
	"github.com/example/workerpool/internal/job"
	"github.com/example/workerpool/internal/result"
	"github.com/example/workerpool/internal/worker"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// ── 1. configure the pool ───────────────────────────────────────────────
	cfg := dispatcher.DefaultConfig()
	cfg.NumWorkers = 4
	cfg.JobBufferSize = 32
	cfg.ShutdownTimeout = 10 * time.Second

	// ── 2. define what a worker does ────────────────────────────────────────
	handler := func(ctx context.Context, j job.Job) (any, error) {
		// Simulate variable-duration work.
		workDuration := time.Duration(30+rand.Intn(120)) * time.Millisecond

		select {
		case <-time.After(workDuration):
		case <-ctx.Done():
			return nil, ctx.Err()
		}

		// Simulate ~15% transient failure rate to exercise retries.
		if rand.Float32() < 0.15 {
			return nil, fmt.Errorf("transient error processing %s", j.ID)
		}

		return fmt.Sprintf("processed(%s)", j.ID), nil
	}

	// ── 3. define how results are collected ─────────────────────────────────
	var (
		mu         sync.Mutex
		successIDs []string
		failedIDs  []string
	)

	onResult := func(r result.Result) {
		mu.Lock()
		defer mu.Unlock()
		if r.Success() {
			successIDs = append(successIDs, r.Job.ID)
			logger.Debug("job succeeded",
				"job_id", r.Job.ID,
				"worker", r.WorkerID,
				"duration_ms", r.Duration().Milliseconds(),
			)
		} else {
			failedIDs = append(failedIDs, r.Job.ID)
			logger.Warn("job failed permanently",
				"job_id", r.Job.ID,
				"worker", r.WorkerID,
				"error", r.Err,
			)
		}
	}

	// ── 4. start the dispatcher ─────────────────────────────────────────────
	d := dispatcher.New(cfg, handler, onResult, logger)
	ctx := d.Start()

	// ── 5. simulate producers ───────────────────────────────────────────────
	//
	// Three concurrent producers to show goroutine-safe enqueuing.
	// Each producer sends a batch of jobs then exits.
	const (
		numProducers  = 3
		jobsPerBatch  = 20
	)

	jobTypes := []job.Type{job.TypeEmail, job.TypeResize, job.TypeExport, job.TypeGeneric}

	var producerWg sync.WaitGroup
	for p := range numProducers {
		producerWg.Add(1)
		go func(producerID int) {
			defer producerWg.Done()
			for i := range jobsPerBatch {
				jType := jobTypes[rand.Intn(len(jobTypes))]
				j := job.New(
					fmt.Sprintf("p%d-job-%03d", producerID, i),
					jType,
					map[string]any{"producer": producerID, "seq": i},
				)

				if err := d.Enqueue(ctx, j); err != nil {
					logger.Warn("producer enqueue failed",
						"producer", producerID, "job_id", j.ID, "error", err)
					return
				}

				// Stagger enqueues slightly to spread the load.
				time.Sleep(time.Duration(rand.Intn(20)) * time.Millisecond)
			}
			logger.Info("producer finished", "producer_id", producerID, "jobs_sent", jobsPerBatch)
		}(p)
	}

	// Wait for all producers to finish enqueuing, then shut down.
	producerWg.Wait()
	logger.Info("all producers done, shutting down")

	// ── 6. graceful shutdown ────────────────────────────────────────────────
	d.Shutdown()

	// ── 7. final report ─────────────────────────────────────────────────────
	snap := d.Metrics()
	fmt.Println()
	fmt.Println("══════════════════════════════════════")
	fmt.Println("           FINAL REPORT               ")
	fmt.Println("══════════════════════════════════════")
	fmt.Printf("  Enqueued : %d\n", snap.Enqueued)
	fmt.Printf("  Processed: %d\n", snap.Processed)
	fmt.Printf("  Failed   : %d\n", snap.Failed)
	fmt.Printf("  Retried  : %d\n", snap.Retried)
	fmt.Printf("  Dropped  : %d\n", snap.Dropped)
	fmt.Printf("  Success  : %d jobs\n", len(successIDs))
	fmt.Printf("  Permanent failures: %d jobs\n", len(failedIDs))
	fmt.Println("══════════════════════════════════════")

	// ── 8. use worker.HandlerFunc in output so the import is satisfied ───────
	_ = worker.HandlerFunc(handler)
}
