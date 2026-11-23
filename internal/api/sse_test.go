package api

import (
	"bufio"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"llmgateway/internal/llm"
)

func TestStreamChatSuccessChunksAndDone(t *testing.T) {
	t.Parallel()
	_, h := newChatTestServer(&fakeProvider{chunks: []llm.StreamChunk{
		{Delta: "hel"}, {Delta: "lo"}, {FinishReason: "stop"},
	}})

	rec := doChat(h, chatBody(true))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
	}
	body := rec.Body.String()
	if got := strings.Count(body, "data: "); got != 4 { // 3 chunks + [DONE]
		t.Errorf("got %d SSE events, want 4:\n%s", got, body)
	}
	if !strings.Contains(body, "\"content\":\"hel\"") {
		t.Errorf("missing first delta:\n%s", body)
	}
	if !strings.HasSuffix(strings.TrimRight(body, "\n"), "data: [DONE]") {
		t.Errorf("stream did not end with [DONE]:\n%s", body)
	}
}

func TestStreamChatMidStreamErrorNoDone(t *testing.T) {
	t.Parallel()
	_, h := newChatTestServer(&fakeProvider{
		chunks:  []llm.StreamChunk{{Delta: "hi"}},
		failAt:  1,
		failErr: &llm.Error{Kind: llm.KindUnavailable},
	})

	rec := doChat(h, chatBody(true))
	if rec.Code != http.StatusOK { // status is already committed by the time the stream breaks
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
	}
	body := rec.Body.String()
	if strings.Contains(body, "[DONE]") {
		t.Errorf("a mid-stream failure must not send [DONE]:\n%s", body)
	}
	if !strings.Contains(body, "upstream_error") {
		t.Errorf("missing error event:\n%s", body)
	}
}

func TestStreamChatFailureBeforeFirstChunkIsAJSONError(t *testing.T) {
	t.Parallel()
	_, h := newChatTestServer(&fakeProvider{err: &llm.Error{Kind: llm.KindBadRequest}})
	rec := doChat(h, chatBody(true))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body=%s", rec.Code, rec.Body)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json (no SSE headers were sent)", ct)
	}
}

// fake stream yields one chunk then hangs on ctx.Done(); if disconnect
// didn't cancel the request context, this test would time out instead.
func TestStreamChatClientDisconnectCancelsUpstream(t *testing.T) {
	t.Parallel()
	done := make(chan struct{})
	p := &fakeProvider{chunks: []llm.StreamChunk{{Delta: "hi"}}, block: true}
	_, h := newChatTestServer(p)

	srv := httptest.NewServer(h)
	defer srv.Close()

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/chat/completions", strings.NewReader(chatBody(true)))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("stream never started: %v", err)
	}
	br := bufio.NewReader(resp.Body)
	line, err := br.ReadString('\n')
	if err != nil || !strings.Contains(line, "\"hi\"") {
		t.Fatalf("first chunk = %q, err = %v, want the scripted delta", line, err)
	}

	go func() {
		<-p.streamCtx.Done()
		close(done)
	}()
	_ = resp.Body.Close() // disconnect while provider is still "generating"

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("upstream context was not canceled within 2s of client disconnect")
	}
}
