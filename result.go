//go:build ignore

package result

import (
	"time"

	"github.com/example/workerpool/internal/job"
)

// Result carries the outcome of a processed Job.
type Result struct {
	Job       job.Job
	Output    any
	Err       error
	WorkerID  int
	StartedAt time.Time
	EndedAt   time.Time
}

// Duration returns how long the job took to process.
func (r Result) Duration() time.Duration {
	return r.EndedAt.Sub(r.StartedAt)
}

// Success reports whether the job completed without error.
func (r Result) Success() bool {
	return r.Err == nil
}
