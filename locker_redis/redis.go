package lockerredis

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"

	"github.com/golang-cz/looper"
)

// redisOptions := &redis.Options{
//     Addr: conf.Redis.Host,
// }
//
// redisClient := redis.NewClient(redisOptions)
//
// looperRedis, err := looper.RedisLocker(ctx, redisClient)
// if err != nil {
//     return err
// }

// RedisLocker provides an implementation of the Locker interface using
// redis for storage.
func RedisLocker(ctx context.Context, rc redis.UniversalClient) (looper.Locker, error) {
	err := rc.Ping(ctx).Err()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", looper.ErrFailedToConnectToLocker, err)
	}

	pool := goredis.NewPool(rc)
	rs := redsync.New(pool)

	l := redisLocker{rs: rs}

	return &l, nil
}

// Locker
var _ looper.Locker = (*redisLocker)(nil)

type redisLocker struct {
	rs *redsync.Redsync
}

func (r *redisLocker) Lock(ctx context.Context, key string, timeout time.Duration) (looper.Lock, error) {
	options := []redsync.Option{
		redsync.WithTries(1),
		redsync.WithExpiry(timeout + time.Second),
	}
	mu := r.rs.NewMutex(key, options...)
	err := mu.LockContext(ctx)
	if err != nil {
		return nil, looper.ErrFailedToObtainLock
	}

	rl := &redisLock{
		mu: mu,
	}

	return rl, nil
}

// Lock
var _ looper.Lock = (*redisLock)(nil)

type redisLock struct {
	mu *redsync.Mutex
}

func (r *redisLock) Unlock(ctx context.Context) error {
	unlocked, err := r.mu.UnlockContext(ctx)
	if err != nil {
		return looper.ErrFailedToReleaseLock
	}

	if !unlocked {
		return looper.ErrFailedToReleaseLock
	}

	return nil
}
