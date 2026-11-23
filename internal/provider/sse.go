package provider

import (
	"bufio"
	"bytes"
	"io"
)

// SSEEvent is one Server-Sent Event: optional event name plus data.
type SSEEvent struct {
	Event string
	Data  []byte
}

// SSEReader parses a text/event-stream body. Events are separated by a blank
// line; multiple data: lines are joined with "\n"; lines starting with ':' are
// comments (often keep-alive pings) and are skipped.
type SSEReader struct {
	r *bufio.Reader
}

func NewSSEReader(r io.Reader) *SSEReader {
	return &SSEReader{r: bufio.NewReaderSize(r, 32<<10)}
}

// Next returns the next event, io.EOF at a clean end, or io.ErrUnexpectedEOF if
// the body ended in the middle of an event (a cut connection).
func (s *SSEReader) Next() (SSEEvent, error) {
	var ev SSEEvent
	var hasData bool
	for {
		line, err := s.r.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				if len(bytes.TrimSpace(line)) == 0 && !hasData && ev.Event == "" {
					return SSEEvent{}, io.EOF
				}
				return SSEEvent{}, io.ErrUnexpectedEOF
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
			if hasData {
				ev.Data = append(ev.Data, '\n')
			}
			ev.Data = append(ev.Data, value...)
			hasData = true
		}
	}
}
