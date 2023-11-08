package looper

// inpired by https://github.com/go-co-op/gocron-redis-lock

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"
)

var (
	ErrFailedToConnectToRedis = errors.New("looper - failed to connect to redis")
	ErrFailedToObtainLock     = errors.New("looper - failed to obtain lock")
	ErrFailedToReleaseLock    = errors.New("looper - failed to release lock")
)

type locker interface {
	lock(ctx context.Context, key string) (lock, error)
}

type lock interface {
	unlock(ctx context.Context) error
}

func newRedisLocker(ctx context.Context, r redis.UniversalClient, options ...redsync.Option) (locker, error) {
	err := r.Ping(ctx).Err()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", ErrFailedToConnectToRedis, err)
	}

	return newLocker(r, options...), nil
}

func newLocker(r redis.UniversalClient, options ...redsync.Option) locker {
	pool := goredis.NewPool(r)
	rs := redsync.New(pool)
	return &redisLocker{rs: rs, options: options}
}

var _ locker = (*redisLocker)(nil)

type redisLocker struct {
	rs      *redsync.Redsync
	options []redsync.Option
}

func (r *redisLocker) lock(ctx context.Context, key string) (lock, error) {
	mu := r.rs.NewMutex(key, r.options...)
	err := mu.LockContext(ctx)
	if err != nil {
		return nil, ErrFailedToObtainLock
	}

	rl := &redisLock{
		mu: mu,
	}

	return rl, nil
}

var _ lock = (*redisLock)(nil)

type redisLock struct {
	mu *redsync.Mutex
}

func (r *redisLock) unlock(ctx context.Context) error {
	unlocked, err := r.mu.UnlockContext(ctx)
	if err != nil {
		return ErrFailedToReleaseLock
	}

	if !unlocked {
		return ErrFailedToReleaseLock
	}

	return nil
}
