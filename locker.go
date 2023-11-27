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

type lockerKind int

const (
	lockerNop lockerKind = iota
	lockerRedis
)

// Lock if an error is returned by lock, the job will not be scheduled.
type locker interface {
	lock(ctx context.Context, key string, timeout time.Duration) (lock, error)
}

// lock represents an obtained lock
type lock interface {
	unlock(ctx context.Context) error
}
