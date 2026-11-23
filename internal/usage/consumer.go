package usage

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Consumer gets exactly-once effect from Kafka's at-least-once delivery:
// insert-dedupe by event_id, aggregate only new rows, commit tx, then
// commit offsets. A crash anywhere just redelivers the batch as a no-op.
type Consumer struct {
	cl  *kgo.Client
	db  *pgxpool.Pool
	log *slog.Logger
}

func NewConsumer(brokers []string, topic, group string, db *pgxpool.Pool, log *slog.Logger) (*Consumer, error) {
	c := &Consumer{db: db, log: log}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.AllowAutoTopicCreation(),
		kgo.OnPartitionsRevoked(func(ctx context.Context, cl *kgo.Client, _ map[string][]int32) {
			if err := cl.CommitUncommittedOffsets(ctx); err != nil {
				c.log.Error("offset commit on rebalance revoke failed (safe: redelivery is deduped)", "err", err)
			}
		}),
	)
	if err != nil {
		return nil, err
	}
	c.cl = cl
	return c, nil
}

func (c *Consumer) Run(ctx context.Context) error {
	defer c.cl.Close()
	for {
		fetches := c.cl.PollRecords(ctx, 5000)
		if ctx.Err() != nil {
			return nil
		}
		fetches.EachError(func(t string, p int32, err error) {
			c.log.Error("fetch error", "topic", t, "partition", p, "err", err)
		})
		var events []Event
		fetches.EachRecord(func(r *kgo.Record) {
			var e Event
			if err := json.Unmarshal(r.Value, &e); err != nil {
				c.log.Error("bad usage event, skipping", "offset", r.Offset, "err", err)
				return
			}
			events = append(events, e)
		})
		if len(events) > 0 {
			if err := c.writeWithRetry(ctx, events); err != nil {
				return err
			}
		}
		if err := c.cl.CommitUncommittedOffsets(ctx); err != nil {
			c.log.Error("offset commit failed (batch will be redelivered and deduped)", "err", err)
		}
	}
}

func (c *Consumer) writeWithRetry(ctx context.Context, events []Event) error {
	backoff := 200 * time.Millisecond
	for {
		inserted, err := c.write(ctx, events)
		if err == nil {
			c.log.Info("usage batch written", "received", len(events), "new", inserted)
			return nil
		}
		c.log.Error("write failed, retrying", "err", err, "backoff", backoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 10*time.Second)
	}
}

func (c *Consumer) write(ctx context.Context, events []Event) (int, error) {
	tx, err := c.db.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op once Commit succeeds

	if _, err := tx.Exec(ctx, `CREATE TEMP TABLE batch (LIKE usage_events) ON COMMIT DROP`); err != nil {
		return 0, err
	}
	rows := make([][]any, len(events))
	for i, e := range events {
		rows[i] = []any{e.EventID, e.TS, e.RequestID, e.KeyID, e.Model, e.Deployment, e.Status,
			e.Stream, e.CacheHit, e.Attempts, e.InputTokens, e.OutputTokens, e.Estimated,
			e.LatencyMs, e.TTFTMs, e.CostUSD}
	}
	cols := []string{"event_id", "ts", "request_id", "key_id", "model", "deployment", "status",
		"stream", "cache_hit", "attempts", "input_tokens", "output_tokens", "estimated",
		"latency_ms", "ttft_ms", "cost_usd"}
	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"batch"}, cols, pgx.CopyFromRows(rows)); err != nil {
		return 0, err
	}
	tag, err := tx.Exec(ctx, `
		WITH ins AS (
			INSERT INTO usage_events SELECT DISTINCT ON (event_id) * FROM batch
			ON CONFLICT (event_id) DO NOTHING
			RETURNING *
		)
		INSERT INTO usage_daily AS d (day, key_id, model, deployment, requests, errors, cache_hits,
			input_tokens, output_tokens, cost_usd)
		SELECT (ts AT TIME ZONE 'UTC')::date, key_id, model, deployment, count(*),
			count(*) FILTER (WHERE status <> 'ok'), count(*) FILTER (WHERE cache_hit),
			sum(input_tokens), sum(output_tokens), sum(cost_usd)
		FROM ins GROUP BY 1, 2, 3, 4
		ON CONFLICT (day, key_id, model, deployment) DO UPDATE SET
			requests = d.requests + EXCLUDED.requests,
			errors = d.errors + EXCLUDED.errors,
			cache_hits = d.cache_hits + EXCLUDED.cache_hits,
			input_tokens = d.input_tokens + EXCLUDED.input_tokens,
			output_tokens = d.output_tokens + EXCLUDED.output_tokens,
			cost_usd = d.cost_usd + EXCLUDED.cost_usd`)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}
