package core

import (
	"bytes"
	"regexp"
	"strings"
	"sync"
	"testing"
)

func TestOutEmitGrammar(t *testing.T) {
	var buf bytes.Buffer
	o := NewOut(&buf)
	o.Emit("EVENT", map[string]any{"a": 1})
	if got := buf.String(); got != "EVENT {\"a\":1}\n" {
		t.Errorf("emit = %q", got)
	}
}

func TestOutEmitNoHTMLEscapeKorean(t *testing.T) {
	var buf bytes.Buffer
	o := NewOut(&buf)
	o.Emit("EVENT", map[string]any{"t": "a<b>&c한글"})
	if got := buf.String(); got != "EVENT {\"t\":\"a<b>&c한글\"}\n" {
		t.Errorf("emit = %q", got)
	}
}

func TestOutRawGrammar(t *testing.T) {
	var buf bytes.Buffer
	o := NewOut(&buf)
	o.Raw("REMIND sent", `{"ok":true}`)
	if got := buf.String(); got != "REMIND sent {\"ok\":true}\n" {
		t.Errorf("raw = %q", got)
	}
}

func TestOutLogGrammar(t *testing.T) {
	var buf bytes.Buffer
	o := NewOut(&buf)
	o.Log("telegram b failing 60x in a row: x")
	if got := buf.String(); got != "LOG telegram b failing 60x in a row: x\n" {
		t.Errorf("log = %q", got)
	}
}

func TestOutLinesAreAtomic(t *testing.T) {
	var buf bytes.Buffer
	o := NewOut(&buf)
	const n = 64
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			o.Emit("X", map[string]any{"i": i})
		}(i)
	}
	wg.Wait()

	out := strings.TrimSuffix(buf.String(), "\n")
	lines := strings.Split(out, "\n")
	if len(lines) != n {
		t.Fatalf("got %d lines, want %d", len(lines), n)
	}
	re := regexp.MustCompile(`^X \{"i":\d+\}$`)
	seen := map[string]bool{}
	for _, ln := range lines {
		if !re.MatchString(ln) {
			t.Fatalf("corrupt line %q (interleaved write)", ln)
		}
		if seen[ln] {
			t.Fatalf("duplicate line %q", ln)
		}
		seen[ln] = true
	}
}

func TestOutImplementsSink(t *testing.T) {
	var _ Sink = (*Out)(nil)
}
