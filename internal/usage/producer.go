package usage

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer sends usage events to Kafka asynchronously.
//
// Delivery guarantees, and where they stop:
//   - acks=all + idempotent producer: once Kafka confirms, the event is on
//     every in-sync replica, and producer retries can't create duplicates
//     within a session.
//   - Kafka down briefly: franz-go keeps retrying buffered records; nothing is
//     lost while the buffer has room.
//   - Kafka down long enough to fill MaxBuffered: new events are DROPPED and
//     counted (llmgw_usage_events_total{result="dropped"}). Blocking requests
//     on billing telemetry would turn a Kafka outage into a gateway outage.
//   - Graceful shutdown (SIGTERM): Close flushes everything buffered.
//   - SIGKILL / OOM / power loss: buffered events are lost. Zero loss there
//     would need a local write-ahead log; noted as a future option.
//
// Emit never blocks the request path: Produce with a full buffer fails fast
// (ManualFlushing off, MaxBufferedRecords + a non-blocking context).
type Producer struct {
	cl      *kgo.Client
	topic   string
	log     *slog.Logger
	counter *prometheus.CounterVec
}

func NewProducer(brokers []string, topic string, maxBuffered int, log *slog.Logger, counter *prometheus.CounterVec) (*Producer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.MaxBufferedRecords(maxBuffered),
		// Batch up to 10ms: ~100x fewer produce requests at high rps for a
		// latency nobody notices on billing data.
		kgo.ProducerLinger(10*time.Millisecond),
		kgo.ProducerBatchCompression(kgo.Lz4Compression()),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return nil, err
	}
	return &Producer{cl: cl, topic: topic, log: log, counter: counter}, nil
}

// Emit queues an event. Key = key id so one key's events land on the same
// partition, in order, which keeps per-key aggregation simple downstream.
func (p *Producer) Emit(e Event) {
	b, err := json.Marshal(e)
	if err != nil {
		return
	}
	rec := &kgo.Record{Key: []byte(e.KeyID), Value: b}
	// TryProduce fails immediately with ErrMaxBuffered instead of blocking
	// when the buffer is full. That is the drop-don't-block policy.
	p.cl.TryProduce(context.Background(), rec, func(_ *kgo.Record, err error) {
		switch {
		case err == kgo.ErrMaxBuffered:
			p.counter.WithLabelValues("dropped").Inc()
		case err != nil:
			p.counter.WithLabelValues("failed").Inc()
			p.log.Warn("usage event failed", "err", err, "request_id", e.RequestID)
		default:
			p.counter.WithLabelValues("delivered").Inc()
		}
	})
	p.counter.WithLabelValues("queued").Inc()
}

// Close flushes buffered events (bounded) and closes the client.
func (p *Producer) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.cl.Flush(ctx); err != nil {
		p.log.Error("usage flush on shutdown incomplete", "err", err)
	}
	p.cl.Close()
}
