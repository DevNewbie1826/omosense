package tidy

import (
	"math"
	"strconv"
	"strings"

	"github.com/DevNewbie1826/omosense/internal/core"
)

// Default thresholds in minutes, memory-tidy.ts's minutes() fallbacks.
const (
	defaultCheckMin = 10
	defaultQuietMin = 60
	defaultMaxMin   = 240
)

type minutesFlag struct {
	flag     string
	fallback float64
}

// minutesFlags order fixes the parse order: check, quiet, max, so the
// first bad flag is the one the TS would exit on.
var minutesFlags = []minutesFlag{
	{"--check-min", defaultCheckMin},
	{"--quiet-min", defaultQuietMin},
	{"--max-min", defaultMaxMin},
}

// parseMinutes reads the three threshold flags from raw argv (both --x=v
// and "--x v") and returns them as milliseconds. A bad value logs
// "memory-tidy bad <flag>" and reports ok=false (the caller exits 2).
func parseMinutes(sink core.Sink, args []string) (checkMs, quietMs, maxMs float64, ok bool) {
	vals := make([]float64, len(minutesFlags))
	for i, mf := range minutesFlags {
		v, found, bad := minutesValue(args, mf.flag)
		if bad {
			sink.Log("memory-tidy bad " + mf.flag)
			return 0, 0, 0, false
		}
		if !found {
			v = mf.fallback
		}
		vals[i] = v * 60_000
	}
	return vals[0], vals[1], vals[2], true
}

// minutesValue finds the first "--x" or "--x=v" occurrence and applies JS
// Number semantics to the value: the segment after the first "=" for the
// equals form (argv[i].split("=")[1]), the next argv element for the bare
// form (Number(undefined) = NaN when absent). bad reports a non-finite or
// negative result.
func minutesValue(args []string, flag string) (v float64, found, bad bool) {
	for i, a := range args {
		if a != flag && !strings.HasPrefix(a, flag+"=") {
			continue
		}
		var raw string
		defined := true
		if a == flag {
			if i+1 < len(args) {
				raw = args[i+1]
			} else {
				defined = false
			}
		} else {
			raw = strings.Split(a, "=")[1]
		}
		if !defined {
			return 0, true, true
		}
		n, finite := jsNumber(raw)
		if !finite || n < 0 {
			return 0, true, true
		}
		return n, true, false
	}
	return 0, false, false
}

// jsNumber applies JS Number() semantics to s: whitespace-trimmed, "" is
// 0, unsigned 0x/0o/0b literals decode by base (a signed hex string is
// NaN, like the spec's grammar), Infinity is non-finite, and anything
// strconv cannot parse as a decimal is non-finite (NaN).
func jsNumber(s string) (float64, bool) {
	t := strings.TrimSpace(s)
	if t == "" {
		return 0, true
	}
	if strings.ContainsRune(t, '_') {
		// Go literal syntax allows underscores; the JS Number string
		// grammar does not.
		return math.NaN(), false
	}
	switch strings.ToLower(t) {
	case "infinity", "+infinity":
		return math.Inf(1), false
	case "-infinity":
		return math.Inf(-1), false
	}
	lower := strings.ToLower(t)
	var base int
	var digits string
	switch {
	case strings.HasPrefix(lower, "0x"):
		base, digits = 16, t[2:]
	case strings.HasPrefix(lower, "0o"):
		base, digits = 8, t[2:]
	case strings.HasPrefix(lower, "0b"):
		base, digits = 2, t[2:]
	}
	if base != 0 {
		u, err := strconv.ParseUint(digits, base, 64)
		if err != nil {
			return math.NaN(), false
		}
		return float64(u), true
	}
	f, err := strconv.ParseFloat(t, 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
		return math.NaN(), false
	}
	return f, true
}
