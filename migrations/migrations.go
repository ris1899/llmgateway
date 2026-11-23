// Package migrations holds the Postgres schema, embedded into every binary
// that needs it (gateway with auth, usage consumer, keyctl).
package migrations

import (
	"context"
	_ "embed"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed 001_init.sql
var initSQL string

// Apply runs the idempotent schema under an advisory lock. Several processes
// may start at once (gateway replicas, consumer); without the lock, concurrent
// CREATE TABLE IF NOT EXISTS can still collide in the system catalog.
// A real project would use a migration tool (goose, atlas) with versioning;
// one idempotent file is enough at this size.
func Apply(ctx context.Context, pool *pgxpool.Pool) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock(727001)"); err != nil {
		return err
	}
	defer conn.Exec(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock(727001)")
	_, err = conn.Exec(ctx, initSQL)
	return err
}
