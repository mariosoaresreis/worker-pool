package metrics

import (
	"fmt"
	"sync/atomic"
)

// Counters holds all runtime metrics for the worker pool.
// All fields are updated atomically and safe for concurrent access.
type Counters struct {
	Enqueued  atomic.Int64
	Processed atomic.Int64
	Failed    atomic.Int64
	Retried   atomic.Int64
	InFlight  atomic.Int64
	Dropped   atomic.Int64 // jobs dropped due to full channel
}

// Snapshot is a point-in-time copy of all counters.
type Snapshot struct {
	Enqueued  int64
	Processed int64
	Failed    int64
	Retried   int64
	InFlight  int64
	Dropped   int64
}

// Snapshot returns an immutable point-in-time view.
func (c *Counters) Snapshot() Snapshot {
	return Snapshot{
		Enqueued:  c.Enqueued.Load(),
		Processed: c.Processed.Load(),
		Failed:    c.Failed.Load(),
		Retried:   c.Retried.Load(),
		InFlight:  c.InFlight.Load(),
		Dropped:   c.Dropped.Load(),
	}
}

func (s Snapshot) String() string {
	return fmt.Sprintf(
		"enqueued=%-6d processed=%-6d failed=%-6d retried=%-6d in_flight=%-4d dropped=%-4d",
		s.Enqueued, s.Processed, s.Failed, s.Retried, s.InFlight, s.Dropped,
	)
}

