package gateway

import (
	"context"
	"io"
	"sync/atomic"

	"llmgateway/internal/llm"
	"llmgateway/internal/provider"
)

// fakeProvider replays script entries per call, repeating the last once exhausted.
type fakeProvider struct {
	name   string
	script []fakeCall
	calls  atomic.Int64
}

// block, if set, makes the call hang until closed or ctx is canceled.
type fakeCall struct {
	err  error // non-nil: Chat/ChatStream fails with this
	resp *llm.ChatResponse

	chunks  []llm.StreamChunk // ChatStream: chunks to emit before EOF/failErr
	failAt  int               // ChatStream: emit this many chunks, then failErr
	failErr error

	block   <-chan struct{}
	blocked chan struct{} // closed once the call starts blocking
}

func newFakeProvider(name string, script ...fakeCall) *fakeProvider {
	return &fakeProvider{name: name, script: script}
}

func (p *fakeProvider) Name() string { return p.name }

func (p *fakeProvider) next() fakeCall {
	n := p.calls.Add(1) - 1
	if len(p.script) == 0 {
		return fakeCall{} // no script: default to an empty success
	}
	if int(n) >= len(p.script) {
		return p.script[len(p.script)-1]
	}
	return p.script[n]
}

func (p *fakeProvider) waitBlock(ctx context.Context, c fakeCall) error {
	if c.block == nil {
		return nil
	}
	if c.blocked != nil {
		close(c.blocked)
	}
	select {
	case <-c.block:
		return nil
	case <-ctx.Done():
		return &llm.Error{Provider: p.name, Kind: llm.KindCanceled, Err: ctx.Err()}
	}
}

func (p *fakeProvider) Chat(ctx context.Context, _ string, _ *llm.ChatRequest) (*llm.ChatResponse, error) {
	c := p.next()
	if err := p.waitBlock(ctx, c); err != nil {
		return nil, err
	}
	if c.err != nil {
		return nil, c.err
	}
	if c.resp != nil {
		return c.resp, nil
	}
	return &llm.ChatResponse{ID: "fake", Content: "ok", FinishReason: "stop"}, nil
}

func (p *fakeProvider) ChatStream(ctx context.Context, _ string, _ *llm.ChatRequest) (provider.Stream, error) {
	c := p.next()
	if err := p.waitBlock(ctx, c); err != nil {
		return nil, err
	}
	if c.err != nil {
		return nil, c.err
	}
	return &fakeStream{chunks: c.chunks, failAt: c.failAt, failErr: c.failErr}, nil
}

type fakeStream struct {
	chunks  []llm.StreamChunk
	i       int
	failAt  int
	failErr error
	closed  bool
}

func (s *fakeStream) Recv() (llm.StreamChunk, error) {
	if s.failAt > 0 && s.i >= s.failAt {
		return llm.StreamChunk{}, s.failErr
	}
	if s.i >= len(s.chunks) {
		return llm.StreamChunk{}, io.EOF
	}
	c := s.chunks[s.i]
	s.i++
	return c, nil
}

func (s *fakeStream) Close() error { s.closed = true; return nil }

func retryableErr(kind llm.ErrorKind) error {
	return &llm.Error{Provider: "fake", Kind: kind}
}

func blockingCall() (call fakeCall, release chan struct{}) {
	release = make(chan struct{})
	call = fakeCall{block: release, blocked: make(chan struct{})}
	return call, release
}
