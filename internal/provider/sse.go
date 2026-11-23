package provider

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"

	"llmgateway/internal/llm"
)

type SSEEvent struct {
	Event string
	Data  []byte
}

// Bounds a malicious/misbehaving upstream that never sends a blank line
// from growing our buffer without limit.
const (
	maxLineSize  = 1 << 20
	maxEventSize = 1 << 20
)

type SSEReader struct {
	r *bufio.Reader
}

func NewSSEReader(r io.Reader) *SSEReader {
	return &SSEReader{r: bufio.NewReaderSize(r, 32<<10)}
}

// Next returns io.ErrUnexpectedEOF if the body ends mid-event; an oversized
// line/event reports as a classified llm.Error, not an unbounded read.
func (s *SSEReader) Next() (SSEEvent, error) {
	var ev SSEEvent
	var hasData bool
	for {
		line, err := readBoundedLine(s.r, maxLineSize)
		if err != nil {
			if errors.Is(err, io.EOF) {
				if len(bytes.TrimSpace(line)) == 0 && !hasData && ev.Event == "" {
					return SSEEvent{}, io.EOF
				}
				return SSEEvent{}, io.ErrUnexpectedEOF
			}
			if errors.Is(err, errLineTooLong) {
				return SSEEvent{}, &llm.Error{Kind: llm.KindUnavailable, Err: fmt.Errorf("sse: line exceeds %d bytes", maxLineSize)}
			}
			return SSEEvent{}, err
		}
		line = bytes.TrimRight(line, "\r\n")
		if len(line) == 0 {
			if hasData || ev.Event != "" {
				return ev, nil
			}
			continue
		}
		if line[0] == ':' {
			continue
		}
		field, value, _ := bytes.Cut(line, []byte(":"))
		value = bytes.TrimPrefix(value, []byte(" "))
		switch string(field) {
		case "event":
			ev.Event = string(value)
		case "data":
			if len(ev.Data)+len(value)+1 > maxEventSize {
				return SSEEvent{}, &llm.Error{Kind: llm.KindUnavailable, Err: fmt.Errorf("sse: event exceeds %d bytes", maxEventSize)}
			}
			if hasData {
				ev.Data = append(ev.Data, '\n')
			}
			ev.Data = append(ev.Data, value...)
			hasData = true
		}
	}
}

var errLineTooLong = errors.New("sse: line too long")

// Like bufio.Reader.ReadBytes('\n') but bails out past max instead of
// growing the buffer unboundedly.
func readBoundedLine(r *bufio.Reader, max int) ([]byte, error) {
	var line []byte
	for {
		frag, err := r.ReadSlice('\n')
		if errors.Is(err, bufio.ErrBufferFull) {
			if len(line)+len(frag) > max {
				// no skip-to-next-line: that read would be unbounded too
				return append(line, frag...), errLineTooLong
			}
			line = append(line, frag...)
			continue
		}
		if len(line)+len(frag) > max {
			return append(line, frag...), errLineTooLong
		}
		line = append(line, frag...)
		return line, err
	}
}
