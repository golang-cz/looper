package looper

import (
	"context"
	"time"
)

func newNopLocker() locker {
	return &nopLocker{}
}

// Locker
var _ locker = (*nopLocker)(nil)

type nopLocker struct{}

func (r *nopLocker) lock(ctx context.Context, key string, timeout time.Duration) (lock, error) {
	return &nopLock{}, nil
}

// Lock
var _ lock = (*nopLock)(nil)

type nopLock struct{}

func (r *nopLock) unlock(ctx context.Context) error {
	return nil
}
