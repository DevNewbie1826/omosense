package core

import (
	"time"
)

// ISO formats t like the JS Date.prototype.toISOString: UTC with
// millisecond precision, e.g. 2006-01-02T15:04:05.000Z.
func ISO(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}

// Trunc cuts s to at most n runes, so multibyte text (Korean) is never
// split mid-rune.
func Trunc(s string, n int) string {
	if n <= 0 {
		return ""
	}
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}
