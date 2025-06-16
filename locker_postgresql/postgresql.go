package lockerpostgresql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/golang-cz/looper"
)

const defaultTableName = "looper_lock"

// PostgresLocker provides an implementation of the Locker interface using
// a PostgreSQL table for storage.
func PostgresLocker(ctx context.Context, db *sql.DB, tableName string) (looper.Locker, error) {
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("%w: %v", looper.ErrFailedToConnectToLocker, err)
	}

	if tableName == "" {
		tableName = defaultTableName
	}

	// Ensure the lock table exists, create it if necessary
	if err := createLockTable(ctx, db, tableName); err != nil {
		return nil, err
	}

	pl := &postgresLocker{
		db:    db,
		table: tableName,
	}

	return pl, nil
}

// Locker
var _ looper.Locker = (*postgresLocker)(nil)

type postgresLocker struct {
	db    *sql.DB
	table string
}

func createLockTable(ctx context.Context, db *sql.DB, table string) error {
	var tableExists bool
	err := db.QueryRowContext(
		ctx,
		fmt.Sprintf(`
			SELECT EXISTS (
				SELECT 1
				FROM information_schema.tables
				WHERE table_name = '%s'
			);`,
			table,
		),
	).Scan(&tableExists)
	if err != nil {
		return fmt.Errorf("%w: %v", looper.ErrFailedToCheckLockExistence, err)
	}

	if !tableExists {
		_, err := db.ExecContext(
			ctx,
			fmt.Sprintf(`
				CREATE TABLE %s (
					job_name VARCHAR(255) PRIMARY KEY,
					created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT (now() AT TIME ZONE 'UTC')
				);`,
				table,
			))
		if err != nil {
			return fmt.Errorf("%w: %v", looper.ErrFailedToCreateLockTable, err)
		}
	}

	return nil
}

func (p *postgresLocker) Lock(ctx context.Context, key string, timeout time.Duration) (looper.Lock, error) {
	// Create a row in the lock table to acquire the lock
	q := fmt.Sprintf(`
			INSERT INTO %s (job_name) 
			VALUES ('%s');`,
		p.table,
		key,
	)
	if _, err := p.db.ExecContext(ctx, q); err != nil {
		q := fmt.Sprintf(`
				SELECT created_at
				FROM %s
				WHERE job_name = '%s';`,
			p.table,
			key,
		)

		var createdAt time.Time
		if err := p.db.QueryRowContext(ctx, q).Scan(&createdAt); err != nil {
			return nil, looper.ErrFailedToCheckLockExistence
		}

		if createdAt.Before(time.Now().Add(-timeout)) {
			q := fmt.Sprintf(`
					DELETE FROM %s
					WHERE job_name = '%s';`,
				p.table,
				key,
			)
			if _, err := p.db.ExecContext(ctx, q); err != nil {
				return nil, looper.ErrFailedToReleaseLock
			}

			return p.Lock(ctx, key, timeout)
		}

		return nil, looper.ErrFailedToObtainLock
	}

	pl := &postgresLock{
		db:    p.db,
		table: p.table,
		key:   key,
	}

	return pl, nil
}

// Lock
var _ looper.Lock = (*postgresLock)(nil)

type postgresLock struct {
	db    *sql.DB
	table string
	key   string
}

func (p *postgresLock) Unlock(ctx context.Context) error {
	// Release the lock by deleting the row
	_, err := p.db.ExecContext(
		ctx,
		fmt.Sprintf(`
			DELETE FROM %s
			WHERE job_name = '%s';`,
			p.table,
			p.key,
		))
	if err != nil {
		return looper.ErrFailedToReleaseLock
	}

	return nil
}
