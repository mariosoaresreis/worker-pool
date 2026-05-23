package shutdown

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// Controller coordinates graceful shutdown across the whole pool.
// It owns the root context and the WaitGroup that tracks all goroutines.
type Controller struct {
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	timeout time.Duration
	logger  *slog.Logger
}

// New creates a Controller with the given drain timeout.
func New(timeout time.Duration, logger *slog.Logger) *Controller {
	return &Controller{
		timeout: timeout,
		logger:  logger,
	}
}

// Context returns a context that is cancelled when Shutdown is called
// or when SIGTERM / SIGINT is received.
func (c *Controller) Context() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel

	// Listen for OS signals in a dedicated goroutine.
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
		select {
		case sig := <-sigCh:
			c.logger.Info("signal received, initiating shutdown", "signal", sig)
			cancel()
		case <-ctx.Done():
			// Already cancelled externally — nothing to do.
		}
		signal.Stop(sigCh)
	}()

	return ctx
}

// Add increments the internal WaitGroup by delta.
// Must be called before the goroutine starts (same rules as sync.WaitGroup).
func (c *Controller) Add(delta int) {
	c.wg.Add(delta)
}

// Done decrements the WaitGroup counter by one.
func (c *Controller) Done() {
	c.wg.Done()
}

// Shutdown signals all goroutines to stop and waits for them to finish,
// up to the configured timeout. Returns true if all goroutines exited cleanly.
func (c *Controller) Shutdown() bool {
	c.logger.Info("shutdown initiated")
	c.cancel()

	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		c.logger.Info("all goroutines exited cleanly")
		return true
	case <-time.After(c.timeout):
		c.logger.Warn("shutdown timeout exceeded, forcing exit", "timeout", c.timeout)
		return false
	}
}

