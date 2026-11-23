// Package migrations holds the Postgres schema, embedded into every binary that needs it.
package migrations

import (
	"context"
	_ "embed"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed 001_init.sql
var initSQL string

//go:embed 002_usage_daily_key_idx.sql
var usageDailyKeyIdxSQL string

var files = []string{initSQL, usageDailyKeyIdxSQL} // each must be idempotent; re-run on every startup

// Apply runs the schema under an advisory lock to avoid concurrent CREATE TABLE collisions.
func Apply(ctx context.Context, pool *pgxpool.Pool) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock(727001)"); err != nil {
		return err
	}
	defer func() {
		if _, uerr := conn.Exec(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock(727001)"); uerr != nil {
			_ = uerr // best-effort; lock is session-scoped and releases on disconnect
		}
	}()
	for _, f := range files {
		if _, err := conn.Exec(ctx, f); err != nil {
			return err
		}
	}
	return nil
}
