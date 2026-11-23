// usageconsumer reads usage events from Kafka and aggregates them into
// Postgres. Separate binary from the gateway: it scales independently (one
// consumer per partition max), and a Postgres outage only stalls this process
// while the gateway keeps serving and Kafka holds the backlog.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"llmgateway/internal/usage"
	"llmgateway/migrations"
)

func main() {
	brokers := flag.String("brokers", "localhost:19092", "comma-separated Kafka brokers")
	topic := flag.String("topic", "llm-usage", "usage topic")
	group := flag.String("group", "llmgw-usage", "consumer group")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		log.Error("DATABASE_URL is not set")
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		log.Error("postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()
	for i := 0; ; i++ {
		if err = migrations.Apply(ctx, pool); err == nil || i == 30 || ctx.Err() != nil {
			break
		}
		log.Warn("waiting for postgres", "err", err)
		time.Sleep(time.Second)
	}
	if err != nil {
		log.Error("migrate", "err", err)
		os.Exit(1)
	}

	c, err := usage.NewConsumer(strings.Split(*brokers, ","), *topic, *group, pool, log)
	if err != nil {
		log.Error("kafka", "err", err)
		os.Exit(1)
	}
	log.Info("usage consumer started", "brokers", *brokers, "topic", *topic, "group", *group)
	if err := c.Run(ctx); err != nil && ctx.Err() == nil {
		log.Error("consumer stopped", "err", err)
		os.Exit(1)
	}
	log.Info("usage consumer stopped")
}
