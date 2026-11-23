package usage

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer sends usage events to Kafka async; a full buffer drops events
// rather than blocking the request path.
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
		kgo.ProducerLinger(10*time.Millisecond), // batch up to 10ms
		kgo.ProducerBatchCompression(kgo.Lz4Compression()),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return nil, err
	}
	return &Producer{cl: cl, topic: topic, log: log, counter: counter}, nil
}

func (p *Producer) Emit(e Event) {
	b, err := json.Marshal(e)
	if err != nil {
		p.counter.WithLabelValues("dropped").Inc()
		p.log.Error("usage event marshal failed, dropping", "err", err, "request_id", e.RequestID)
		return
	}
	rec := &kgo.Record{Key: []byte(e.KeyID), Value: b} // keyed so one key's events stay ordered
	p.cl.TryProduce(context.Background(), rec, func(_ *kgo.Record, err error) {
		switch {
		case errors.Is(err, kgo.ErrMaxBuffered):
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

func (p *Producer) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.cl.Flush(ctx); err != nil {
		p.log.Error("usage flush on shutdown incomplete", "err", err)
	}
	p.cl.Close()
}
