// keyctl manages API keys.
//
//	DATABASE_URL=postgres://... keyctl create -name demo -rps 10 -burst 20 -cap 5
//	keyctl list
//	keyctl revoke -id key_abc
//
// The plaintext key is printed once at creation and never stored.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"llmgateway/internal/auth"
	"llmgateway/migrations"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		fail("DATABASE_URL is not set")
	}
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		fail(err.Error())
	}
	defer pool.Close()
	if err := migrations.Apply(ctx, pool); err != nil {
		fail("migrate: " + err.Error())
	}

	switch os.Args[1] {
	case "create":
		fs := flag.NewFlagSet("create", flag.ExitOnError)
		name := fs.String("name", "", "human-readable name (required)")
		rps := fs.Float64("rps", 0, "requests per second (0 = unlimited)")
		burst := fs.Int("burst", 0, "burst size (0 = ceil(rps))")
		capUSD := fs.Float64("cap", 0, "monthly spend cap in USD (0 = none)")
		strategy := fs.String("strategy", "", "default routing strategy for this key")
		_ = fs.Parse(os.Args[2:])
		if err := validateCreateFlags(*name, *rps, *burst, *capUSD, *strategy); err != nil {
			fail(err.Error())
		}
		plain, hash := auth.Generate()
		id := auth.NewID()
		_, err := pool.Exec(ctx, `INSERT INTO api_keys (id, key_hash, name, rps, burst, monthly_cap_usd, default_strategy)
			VALUES ($1,$2,$3,$4,$5,$6,$7)`, id, hash[:], *name, *rps, *burst, *capUSD, *strategy)
		if err != nil {
			fail(err.Error())
		}
		fmt.Printf("id:  %s\nkey: %s\n\nStore the key now; it cannot be shown again.\n", id, plain)
	case "list":
		rows, err := pool.Query(ctx, `SELECT id, name, rps, burst, monthly_cap_usd, default_strategy, created_at, revoked_at IS NOT NULL
			FROM api_keys ORDER BY created_at`)
		if err != nil {
			fail(err.Error())
		}
		tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		if _, err := fmt.Fprintln(tw, "ID\tNAME\tRPS\tBURST\tCAP_USD\tSTRATEGY\tCREATED\tREVOKED"); err != nil {
			fail(err.Error())
		}
		for rows.Next() {
			var id, name, strategy string
			var rps, capUSD float64
			var burst int
			var created time.Time
			var revoked bool
			if err := rows.Scan(&id, &name, &rps, &burst, &capUSD, &strategy, &created, &revoked); err != nil {
				fail(err.Error())
			}
			if _, err := fmt.Fprintf(tw, "%s\t%s\t%g\t%d\t%g\t%s\t%s\t%v\n", id, name, rps, burst, capUSD, strategy, created.Format(time.DateTime), revoked); err != nil {
				fail(err.Error())
			}
		}
		if err := tw.Flush(); err != nil {
			fail(err.Error())
		}
	case "revoke":
		fs := flag.NewFlagSet("revoke", flag.ExitOnError)
		id := fs.String("id", "", "key id")
		_ = fs.Parse(os.Args[2:])
		tag, err := pool.Exec(ctx, `UPDATE api_keys SET revoked_at = now() WHERE id = $1 AND revoked_at IS NULL`, *id)
		if err != nil {
			fail(err.Error())
		}
		if tag.RowsAffected() == 0 {
			fail("no active key with that id")
		}
		fmt.Println("revoked (gateways stop accepting it within auth.cache_ttl)")
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: keyctl create|list|revoke [flags]")
	os.Exit(2)
}

func fail(msg string) {
	fmt.Fprintln(os.Stderr, "keyctl:", msg)
	os.Exit(1)
}
