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

// Consumer reads usage events and writes them to Postgres exactly once in
// effect, from at-least-once delivery:
//
//  1. Poll a batch.
//  2. In ONE transaction: insert raw events with ON CONFLICT DO NOTHING
//     (dedupe by event_id), then add only the newly inserted rows to the
//     daily aggregates.
//  3. Commit the transaction, THEN commit Kafka offsets.
//
// If we crash between 3a and 3b, the batch is redelivered, every insert hits
// a conflict, nothing is added twice. If we crash before 3a, the transaction
// rolls back and the batch is redelivered. Either way aggregates are correct.
// Committing offsets first instead would lose data on a crash.
type Consumer struct {
	cl  *kgo.Client
	db  *pgxpool.Pool
	log *slog.Logger
}

func NewConsumer(brokers []string, topic, group string, db *pgxpool.Pool, log *slog.Logger) (*Consumer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(), // we commit only after Postgres commits
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return nil, err
	}
	return &Consumer{cl: cl, db: db, log: log}, nil
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
				// Poison message: log and skip. Retrying it forever would block
				// the partition. Production: send to a dead-letter topic.
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
		// Postgres down: keep retrying the same batch, don't advance offsets.
		// Kafka retains the backlog, which is the point of having Kafka here.
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
	defer tx.Rollback(ctx)

	// Stage the batch, then do insert-dedupe-aggregate as set operations in
	// SQL: one round trip per step instead of one per event.
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
