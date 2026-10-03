package core

import (
	"testing"
	"time"
)

func TestISO(t *testing.T) {
	kst := time.FixedZone("KST", 9*3600)
	got := ISO(time.Date(2026, 10, 3, 9, 41, 5, 123999999, kst))
	if got != "2026-10-03T00:41:05.123Z" {
		t.Errorf("ISO = %q", got)
	}
	got = ISO(time.Date(2026, 1, 2, 3, 4, 5, 6000000, time.UTC))
	if got != "2026-01-02T03:04:05.006Z" {
		t.Errorf("ISO = %q", got)
	}
}

func TestTruncKorean(t *testing.T) {
	cases := []struct {
		in   string
		n    int
		want string
	}{
		{"안녕하세요세상", 5, "안녕하세요"},
		{"안녕하세요", 5, "안녕하세요"},
		{"안녕", 5, "안녕"},
		{"", 3, ""},
		{"안녕하세요", 0, ""},
		{"한글and english", 5, "한글and"},
	}
	for _, c := range cases {
		if got := Trunc(c.in, c.n); got != c.want {
			t.Errorf("Trunc(%q,%d) = %q, want %q", c.in, c.n, got, c.want)
		}
	}
}
