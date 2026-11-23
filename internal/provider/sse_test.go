package provider

import (
	"errors"
	"io"
	"strings"
	"testing"

	"llmgateway/internal/llm"
)

func TestSSEReader_Next(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		body    string
		want    []SSEEvent
		wantErr error // sentinel to compare with errors.Is-style ==, or nil
	}{
		{
			name: "simple data event with LF",
			body: "data: hello\n\n",
			want: []SSEEvent{{Data: []byte("hello")}},
		},
		{
			name: "CRLF line endings",
			body: "data: hello\r\n\r\n",
			want: []SSEEvent{{Data: []byte("hello")}},
		},
		{
			name: "multi-line data joined with newline",
			body: "data: line1\ndata: line2\n\n",
			want: []SSEEvent{{Data: []byte("line1\nline2")}},
		},
		{
			name: "event field carried alongside data",
			body: "event: message_start\ndata: {\"a\":1}\n\n",
			want: []SSEEvent{{Event: "message_start", Data: []byte(`{"a":1}`)}},
		},
		{
			name: "comment lines are skipped",
			body: ": keep-alive\ndata: hello\n\n",
			want: []SSEEvent{{Data: []byte("hello")}},
		},
		{
			name: "two events back to back",
			body: "data: one\n\ndata: two\n\n",
			want: []SSEEvent{{Data: []byte("one")}, {Data: []byte("two")}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := NewSSEReader(strings.NewReader(tt.body))
			var got []SSEEvent
			for {
				ev, err := r.Next()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Fatalf("Next() error = %v", err)
				}
				got = append(got, ev)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("got %d events, want %d: %+v", len(got), len(tt.want), got)
			}
			for i := range got {
				if got[i].Event != tt.want[i].Event || string(got[i].Data) != string(tt.want[i].Data) {
					t.Errorf("event %d = %+v, want %+v", i, got[i], tt.want[i])
				}
			}
		})
	}
}

// Cut before the blank line: must be io.ErrUnexpectedEOF, not io.EOF.
func TestSSEReader_MissingTrailingBlankLine(t *testing.T) {
	t.Parallel()
	r := NewSSEReader(strings.NewReader("data: partial"))
	_, err := r.Next()
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("Next() error = %v, want io.ErrUnexpectedEOF", err)
	}
}

// Nothing left after the final blank line is a clean end.
func TestSSEReader_CleanEOF(t *testing.T) {
	t.Parallel()
	r := NewSSEReader(strings.NewReader("data: done\n\n"))
	if _, err := r.Next(); err != nil {
		t.Fatalf("first Next() error = %v", err)
	}
	if _, err := r.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("second Next() error = %v, want io.EOF", err)
	}
}

func TestSSEReader_OversizedLine(t *testing.T) {
	t.Parallel()
	big := strings.Repeat("x", maxLineSize+1)
	r := NewSSEReader(strings.NewReader("data: " + big + "\n\n"))
	_, err := r.Next()
	e := llm.AsError(err)
	if e.Kind != llm.KindUnavailable {
		t.Fatalf("err kind = %v, want KindUnavailable; err=%v", e.Kind, err)
	}
}

// serves an unterminated line of 'x', tracks bytes consumed
type countingXReader struct{ served, limit int }

func (r *countingXReader) Read(p []byte) (int, error) {
	if r.served >= r.limit {
		return 0, io.EOF
	}
	n := min(len(p), r.limit-r.served)
	for i := range n {
		p[i] = 'x'
	}
	r.served += n
	return n, nil
}

// guards against unbounded read after the size limit trips
func TestSSEReader_OversizedLineStopsReading(t *testing.T) {
	t.Parallel()
	src := &countingXReader{limit: 16 * maxLineSize}
	r := NewSSEReader(io.MultiReader(strings.NewReader("data: "), src))
	_, err := r.Next()
	if e := llm.AsError(err); e.Kind != llm.KindUnavailable {
		t.Fatalf("err kind = %v, want KindUnavailable; err=%v", e.Kind, err)
	}
	if src.served > 2*maxLineSize {
		t.Fatalf("read %d bytes after the limit tripped, want at most ~%d", src.served, 2*maxLineSize)
	}
}

func TestSSEReader_OversizedEventAcrossLines(t *testing.T) {
	t.Parallel()
	// No single line exceeds maxLineSize, but the accumulated event does.
	var b strings.Builder
	chunk := strings.Repeat("x", 1<<16) // 64 KiB per line
	for i := 0; i < (maxEventSize/(1<<16))+2; i++ {
		b.WriteString("data: ")
		b.WriteString(chunk)
		b.WriteByte('\n')
	}
	b.WriteByte('\n')

	r := NewSSEReader(strings.NewReader(b.String()))
	_, err := r.Next()
	e := llm.AsError(err)
	if e.Kind != llm.KindUnavailable {
		t.Fatalf("err kind = %v, want KindUnavailable; err=%v", e.Kind, err)
	}
}
