package looper

import (
	"context"
	"errors"
	"time"
)

var (
	ErrFailedToConnectToLocker    = errors.New("looper - failed to connect to locker")
	ErrFailedToObtainLock         = errors.New("looper - failed to obtain lock")
	ErrFailedToReleaseLock        = errors.New("looper - failed to release lock")
	ErrFailedToCreateLockTable    = errors.New("looper - failed to create lock table")
	ErrFailedToCheckLockExistence = errors.New("looper - failed to check lock existence")
)

// Lock if an error is returned by lock, the job will not be scheduled.
type Locker interface {
	Lock(ctx context.Context, key string, timeout time.Duration) (Lock, error)
}

// Lock represents an obtained Lock
type Lock interface {
	Unlock(ctx context.Context) error
}

// Nop locker

func newNopLocker() Locker {
	return &nopLocker{}
}

// Locker
var _ Locker = (*nopLocker)(nil)

type nopLocker struct{}

func (r *nopLocker) Lock(_ context.Context, _ string, _ time.Duration) (Lock, error) {
	return &nopLock{}, nil
}

// Lock
var _ Lock = (*nopLock)(nil)

type nopLock struct{}

func (r *nopLock) Unlock(ctx context.Context) error {
	return nil
}
