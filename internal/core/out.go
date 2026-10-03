package core

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
)

// Out is the process stdout writer: it serializes every Emit/Raw/Log call
// into one atomic line under a mutex, matching the TS console.log grammar.
type Out struct {
	mu sync.Mutex
	w  io.Writer
}

var _ Sink = (*Out)(nil)

// NewOut wraps w in an Out.
func NewOut(w io.Writer) *Out { return &Out{w: w} }

// Emit writes "PREFIX <json>\n", encoding the payload without HTML
// escaping.
func (o *Out) Emit(prefix string, payload any) {
	b, err := marshalNoEscape(payload)
	if err != nil {
		fmt.Fprintf(os.Stderr, "omosense: emit %s: %v\n", prefix, err)
		return
	}
	o.line(prefix + " " + string(b))
}

// Raw writes "PREFIX text\n" without JSON-encoding the text.
func (o *Out) Raw(prefix, text string) { o.line(prefix + " " + text) }

// Log writes "LOG msg\n".
func (o *Out) Log(msg string) { o.line("LOG " + msg) }

func (o *Out) line(s string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	io.WriteString(o.w, s+"\n")
}

func marshalNoEscape(v any) ([]byte, error) {
	if m, ok := v.(*OMap); ok {
		return m.Marshal()
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}
