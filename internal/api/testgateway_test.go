package api

import (
	"context"
	"io"
	"log/slog"
	"time"

	"llmgateway/internal/config"
	"llmgateway/internal/gateway"
	"llmgateway/internal/llm"
	"llmgateway/internal/metrics"
	"llmgateway/internal/provider"
	"llmgateway/internal/router"
)

const testModel = "g"

type fakeProvider struct {
	resp    *llm.ChatResponse
	err     error
	chunks  []llm.StreamChunk
	failAt  int
	failErr error
	block   bool // hang Chat/ChatStream until ctx is canceled, to simulate a disconnect

	streamCtx context.Context // ctx passed to ChatStream, for disconnect assertions
}

func (p *fakeProvider) Name() string { return "fake" }

func (p *fakeProvider) Chat(ctx context.Context, _ string, _ *llm.ChatRequest) (*llm.ChatResponse, error) {
	if p.block {
		<-ctx.Done()
		return nil, &llm.Error{Provider: "fake", Kind: llm.KindCanceled, Err: ctx.Err()}
	}
	if p.err != nil {
		return nil, p.err
	}
	return p.resp, nil
}

func (p *fakeProvider) ChatStream(ctx context.Context, _ string, _ *llm.ChatRequest) (provider.Stream, error) {
	if p.err != nil {
		return nil, p.err
	}
	p.streamCtx = ctx
	return &fakeStream{ctx: ctx, chunks: p.chunks, failAt: p.failAt, failErr: p.failErr, block: p.block}, nil
}

type fakeStream struct {
	ctx     context.Context
	chunks  []llm.StreamChunk
	i       int
	failAt  int
	failErr error
	block   bool
}

func (s *fakeStream) Recv() (llm.StreamChunk, error) {
	if s.failAt > 0 && s.i >= s.failAt {
		return llm.StreamChunk{}, s.failErr
	}
	if s.i < len(s.chunks) {
		c := s.chunks[s.i]
		s.i++
		return c, nil
	}
	if s.block {
		// Every scripted chunk has been delivered; now hang until the
		// request context is canceled, simulating a provider still
		// generating when the client disconnects.
		<-s.ctx.Done()
		return llm.StreamChunk{}, &llm.Error{Provider: "fake", Kind: llm.KindCanceled, Err: s.ctx.Err()}
	}
	return llm.StreamChunk{}, io.EOF
}

func (s *fakeStream) Close() error { return nil }

func generousBreaker() config.Breaker {
	return config.Breaker{ConsecutiveFailures: 100, ErrorRate: 1, MinRequests: 1_000_000, Cooldown: time.Minute, HalfOpenSuccesses: 1}
}

func newTestGateway(providers ...provider.Provider) *gateway.Gateway {
	deployments := make([]*router.Deployment, len(providers))
	for i, p := range providers {
		deployments[i] = &router.Deployment{
			ID: "fake/dep" + string(rune('0'+i)), Group: testModel, Provider: p, Model: "m",
			InputPricePerM: 1, OutputPricePerM: 2,
			Stats: router.NewStats(time.Second), Breaker: router.NewBreaker(generousBreaker(), nil),
		}
	}
	grp := &router.Group{Name: testModel, Strategy: config.StrategyPriority, Deployments: deployments}
	r := router.New(map[string]*router.Group{testModel: grp}, router.NewBudget(1, 1000))
	return gateway.New(r, nil, nil, metrics.New(), slog.New(slog.DiscardHandler))
}

func chatBody(stream bool) string {
	if stream {
		return `{"model":"g","messages":[{"role":"user","content":"hi"}],"stream":true}`
	}
	return `{"model":"g","messages":[{"role":"user","content":"hi"}]}`
}
