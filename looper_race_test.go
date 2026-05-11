package looper

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestStartStop_NoRaceOnContextCancel exercises the start → stop sequence
// rapidly so the race detector can observe writes to j.contextCancel
// (in (*Job).start) against reads in (*Looper).Stop. Run with -race; pre-fix
// this test triggers a data-race report deterministically within a handful
// of iterations.
//
// Background: (*Job).start sets j.contextCancel while holding no lock, but
// (*Looper).Stop reads it inside j.mu.Lock to cancel an in-flight job. The
// pattern shows up in any consumer that calls Looper.Stop shortly after
// Looper.Start (test fixtures, controlled shutdowns during boot failure,
// container OOM during startup).
func TestStartStop_NoRaceOnContextCancel(t *testing.T) {
	const iterations = 50

	for i := 0; i < iterations; i++ {
		// StartupTime: 1ms — default is 1s per job, which would add ~50s to
		// the test for no benefit (we don't need staggered startup here, we
		// need the start→stop race window).
		l := New(&Config{StartupTime: time.Millisecond})

		var runs atomic.Int64
		if err := l.AddJob(context.Background(), &Job{
			Name:             "test-job",
			Active:           true,
			WaitAfterSuccess: 10 * time.Millisecond,
			WaitAfterError:   10 * time.Millisecond,
			Timeout:          200 * time.Millisecond,
			JobFn: func(ctx context.Context) error {
				runs.Add(1)
				return nil
			},
		}); err != nil {
			t.Fatalf("iter %d: AddJob: %v", i, err)
		}

		l.Start()
		// A tiny yield is enough to land the startLoop goroutine inside
		// (*Job).start, where the unguarded write to j.contextCancel used
		// to race with the Stop-side read. Without any sleep the race
		// window is too narrow to catch reliably on fast machines.
		time.Sleep(time.Millisecond)
		l.Stop()
	}
}
