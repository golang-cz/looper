package looper

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

const defaultTableName = "looper_lock"

// PostgresLocker provides an implementation of the Locker interface using
// a PostgreSQL table for storage.
func PostgresLocker(ctx context.Context, db *sql.DB, tableName string) (locker, error) {
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrFailedToConnectToLocker, err)
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
var _ locker = (*postgresLocker)(nil)

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
		return fmt.Errorf("%w: %v", ErrFailedToCheckLockExistence, err)
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
			return fmt.Errorf("%w: %v", ErrFailedToCreateLockTable, err)
		}
	}

	return nil
}

func (p *postgresLocker) lock(
	ctx context.Context,
	key string,
	timeout time.Duration,
) (lock, error) {
	// Create a row in the lock table to acquire the lock
	_, err := p.db.ExecContext(
		ctx,
		fmt.Sprintf(`
			INSERT INTO %s (job_name) 
			VALUES ('%s');`,
			p.table,
			key,
		))
	if err != nil {
		var createdAt time.Time
		err := p.db.QueryRowContext(
			ctx,
			fmt.Sprintf(`
				SELECT created_at
				FROM %s
				WHERE job_name = '%s';`,
				p.table,
				key,
			)).Scan(&createdAt)
		if err != nil {
			return nil, ErrFailedToCheckLockExistence
		}

		if createdAt.Before(time.Now().Add(-timeout)) {
			_, err := p.db.ExecContext(
				ctx,
				fmt.Sprintf(`
					DELETE FROM %s
					WHERE job_name = '%s';`,
					p.table,
					key,
				))
			if err != nil {
				return nil, ErrFailedToReleaseLock
			}

			return p.lock(ctx, key, timeout)
		}

		return nil, ErrFailedToObtainLock
	}

	pl := &postgresLock{
		db:    p.db,
		table: p.table,
		key:   key,
	}

	return pl, nil
}

// Lock
var _ lock = (*postgresLock)(nil)

type postgresLock struct {
	db    *sql.DB
	table string
	key   string
}

func (p *postgresLock) unlock(ctx context.Context) error {
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
		return ErrFailedToReleaseLock
	}

	return nil
}
