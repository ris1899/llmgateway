package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"llmgateway/internal/gateway"
	"llmgateway/internal/llm"
)

type chunkChoice struct {
	Index        int               `json:"index"`
	Delta        map[string]string `json:"delta"`
	FinishReason *string           `json:"finish_reason"`
}

type chunk struct {
	ID      string         `json:"id"`
	Object  string         `json:"object"`
	Created int64          `json:"created"`
	Model   string         `json:"model"`
	Choices []chunkChoice  `json:"choices"`
	Usage   map[string]int `json:"usage,omitempty"`
}

// streamChat proxies a stream to the client in OpenAI's SSE format.
//
// Lifecycle:
//  1. gw.ChatStream fails over across providers until one yields a first
//     chunk. Nothing has been sent yet, so on failure we can still return a
//     normal JSON error with a proper status code.
//  2. Headers go out (status 200 is now committed) and every chunk is flushed
//     immediately. Buffering here would turn time-to-first-token into
//     time-to-last-token.
//  3. If the upstream fails mid-stream, we can't change the status or switch
//     providers (the client already has partial text from one model). We send
//     an SSE error event and end the stream without [DONE], so clients can tell
//     a cut stream from a complete one.
//  4. If the client disconnects, r.Context() is canceled, which aborts the
//     upstream request too, so we stop paying for tokens nobody will read.
func (s *Server) streamChat(w http.ResponseWriter, r *http.Request, req *llm.ChatRequest, opt gateway.Options, includeUsage bool, start time.Time) {
	st, err := s.gw.ChatStream(r.Context(), req, opt)
	if err != nil {
		status := s.writeUpstreamError(w, r, err)
		s.finish(r, req, start, status, llm.AsError(err).Kind.String(), nil, 0)
		return
	}
	defer st.Close()
	s.m.ActiveStreams.Inc()
	defer s.m.ActiveStreams.Dec()

	// Gateway overhead to first token: total time so far minus time spent
	// waiting for the provider's first chunk.
	overhead := time.Since(start) - st.TTFT
	deployID := st.DeploymentID("cache")
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no") // tell nginx-style proxies not to buffer
	h.Set(HeaderProvider, deployID)
	h.Set(HeaderAttempts, strconv.Itoa(st.Attempts))
	h.Set(HeaderOverhead, ms(overhead))
	h.Set(HeaderCache, cacheLabel(st.CacheHit, req, opt, s.cacheOn))
	if st.Sticky != "" {
		h.Set(HeaderSticky, st.Sticky)
	}
	w.WriteHeader(http.StatusOK)
	rc := http.NewResponseController(w)
	if !st.CacheHit {
		s.m.Overhead.WithLabelValues("true").Observe(overhead.Seconds())
	}
	ttft := time.Since(start)

	id := "chatcmpl-" + RequestID(r.Context())
	created := time.Now().Unix()
	send := func(c chunk) bool {
		c.ID, c.Object, c.Created, c.Model = id, "chat.completion.chunk", created, req.Model
		b, _ := json.Marshal(c)
		if _, err := w.Write(append(append([]byte("data: "), b...), '\n', '\n')); err != nil {
			return false
		}
		return rc.Flush() == nil
	}

	var usage *llm.Usage
	var outChars int
	first := true
	o := &outcome{deployID: deployID, d: st.Deployment, attempts: st.Attempts, cacheHit: st.CacheHit}
	for {
		c, err := st.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			e := llm.AsError(err)
			if e.Kind != llm.KindCanceled {
				s.log.Warn("stream failed mid-way", "request_id", RequestID(r.Context()),
					"provider", deployID, "kind", e.Kind.String(), "err", e.Err)
				b, _ := json.Marshal(map[string]any{"error": map[string]string{
					"message": "upstream stream interrupted: " + e.Kind.String(), "type": "upstream_error"}})
				_, _ = w.Write(append(append([]byte("data: "), b...), '\n', '\n'))
				_ = rc.Flush()
			}
			// Bill what was produced so far: the provider charges for it.
			o.usage = llm.Usage{OutputTokens: outChars/4 + 1, Estimated: true}
			s.costOf(o)
			s.finish(r, req, start, http.StatusOK, "midstream_"+e.Kind.String(), o, ttft)
			return
		}
		if c.Usage != nil {
			usage = c.Usage
		}
		if c.Delta == "" && c.FinishReason == "" {
			continue // usage-only chunk; sent at the end if requested
		}
		delta := map[string]string{}
		if first {
			delta["role"] = "assistant"
			first = false
		}
		if c.Delta != "" {
			delta["content"] = c.Delta
			outChars += len(c.Delta)
		}
		var fr *string
		if c.FinishReason != "" {
			fr = &c.FinishReason
		}
		if !send(chunk{Choices: []chunkChoice{{Delta: delta, FinishReason: fr}}}) {
			// Client went away. Returning closes the upstream (defer st.Close),
			// which stops generation and further billing.
			o.usage = llm.Usage{OutputTokens: outChars/4 + 1, Estimated: true}
			s.costOf(o)
			s.finish(r, req, start, 499, "client_disconnected", o, ttft)
			return
		}
	}

	if usage == nil {
		// Provider didn't report usage; estimate output (~4 chars/token) and flag
		// it, so cost reports never present a guess as a billed amount.
		usage = &llm.Usage{OutputTokens: outChars/4 + 1, Estimated: true}
	}
	if includeUsage {
		send(chunk{Choices: []chunkChoice{}, Usage: map[string]int{
			"prompt_tokens": usage.InputTokens, "completion_tokens": usage.OutputTokens,
			"total_tokens": usage.InputTokens + usage.OutputTokens,
		}})
	}
	_, _ = w.Write([]byte("data: [DONE]\n\n"))
	_ = rc.Flush()

	o.usage = *usage
	s.costOf(o)
	s.finish(r, req, start, http.StatusOK, "ok", o, ttft)
}

func (s *Server) costOf(o *outcome) {
	if o.d != nil && !o.cacheHit {
		o.cost = o.d.Cost(o.usage)
	}
}
