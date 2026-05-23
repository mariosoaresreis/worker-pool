//go:build ignore

package job

import (
	"fmt"
	"time"
)

// Type identifies the kind of work to be done.
type Type string

const (
	TypeEmail   Type = "email"
	TypeResize  Type = "resize"
	TypeExport  Type = "export"
	TypeGeneric Type = "generic"
)

// Status represents the lifecycle stage of a Job.
type Status string

const (
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusDone       Status = "done"
	StatusFailed     Status = "failed"
	StatusRetrying   Status = "retrying"
)

// Job is the unit of work passed through the pipeline.
type Job struct {
	ID        string
	Type      Type
	Payload   any
	Attempts  int
	MaxRetry  int
	CreatedAt time.Time
}

// New creates a Job with sensible defaults.
func New(id string, t Type, payload any) Job {
	return Job{
		ID:        id,
		Type:      t,
		Payload:   payload,
		MaxRetry:  3,
		CreatedAt: time.Now(),
	}
}

func (j Job) String() string {
	return fmt.Sprintf("Job{id=%s type=%s attempt=%d/%d}", j.ID, j.Type, j.Attempts, j.MaxRetry)
}
