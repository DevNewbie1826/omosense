package remind

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestCancelledMarkerSkipsSend is the scheduler half of IS-2: a truthy
// cancelled field is terminal, exactly like sent/skipped/failed, and a
// falsy cancelled (empty string) is still pending and sends once.
func TestCancelledMarkerSkipsSend(t *testing.T) {
	ctx := loadCtx(t)
	file := remindersFile(ctx)
	body := `[{"id":"keep","at":"2026-10-03T10:00:00.000Z","platform":"telegram","target":{"chat_id":1},"text":"keep-me","cancelled":"2026-10-03T09:00:00.000Z"},{"id":"send","at":"2026-10-03T10:00:00.000Z","platform":"telegram","target":{"chat_id":2},"text":"send-me","cancelled":""}]`
	if err := os.WriteFile(file, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	capture := filepath.Join(t.TempDir(), "captured-args")
	sleep := newFakeSleeper()
	withHooks(t, fixedTime, writeFakeSay(t, capture), sleep.sleep)

	sink := newChanSink()
	cancel, done := runSource(t, ctx, sink)
	sink.waitLine(t, "LOG reminder scheduler starting")
	sleep.waitCalls(t, 1)

	got := readCapture(t, capture)
	if n := strings.Count(got, "\n"); n != 1 || !strings.Contains(got, "send-me") || strings.Contains(got, "keep-me") {
		t.Fatalf("say invocations = %q, want exactly one send of send-me and none of keep-me", got)
	}
	sentLines := 0
	for _, ln := range sink.snapshot() {
		if !strings.HasPrefix(ln, "REMIND sent ") {
			continue
		}
		sentLines++
		if !strings.Contains(ln, "send-me") || strings.Contains(ln, "keep-me") {
			t.Errorf("REMIND sent line = %q, want only send-me", ln)
		}
	}
	if sentLines != 1 {
		t.Fatalf("REMIND sent lines = %d, want 1", sentLines)
	}
	after := string(mustRead(t, file))
	if strings.Count(after, `"sent"`) != 1 {
		t.Fatalf("sent markers = %d, want 1 (only the falsy cancelled entry):\n%s", strings.Count(after, `"sent"`), after)
	}
	if !strings.Contains(after, `"cancelled": "2026-10-03T09:00:00.000Z"`) && !strings.Contains(after, `"cancelled":"2026-10-03T09:00:00.000Z"`) {
		t.Fatalf("truthy cancelled marker was dropped:\n%s", after)
	}

	sleep.nextTick(t)
	if again := readCapture(t, capture); strings.Count(again, "\n") != 1 {
		t.Fatalf("second tick sent again: %q", again)
	}

	cancel()
	if err := waitDone(t, done); err != nil {
		t.Errorf("Run returned %v, want nil after cancel", err)
	}
}

// cancelBefore is the indent-2 layout marshalIndent2Array writes. Terminal
// entries (truthy sent/skipped/failed/cancelled) must stay byte-identical;
// the other entries are pending, including falsy markers.
const cancelBefore = `[
  {
    "zz": 1,
    "id": "sent1",
    "at": "2026-10-03T10:00:00.000Z",
    "platform": "telegram",
    "target": {
      "b": 2,
      "a": 1
    },
    "text": "hi",
    "sent": "2026-10-03T09:00:00.000Z"
  },
  {
    "id": "skip1",
    "skipped": "2026-10-03T09:00:00.000Z"
  },
  {
    "id": "fail1",
    "failed": "2026-10-03T09:00:00.000Z",
    "error": "boom"
  },
  {
    "id": "old",
    "cancelled": "2026-10-01T00:00:00.000Z",
    "extra": true
  },
  {
    "id": "truthy-sent",
    "sent": true
  },
  {
    "id": "pend",
    "at": "2030-01-01T00:00:00.000Z",
    "note": "keep",
    "text": "later"
  },
  {
    "id": "blank-sent",
    "sent": "",
    "at": "2030-01-01T00:00:00.000Z"
  },
  {
    "id": "blank-skip",
    "skipped": false
  },
  {
    "id": "zero-fail",
    "failed": 0
  },
  {
    "id": "blank-cancel",
    "cancelled": ""
  }
]`

const cancelAfter = `[
  {
    "zz": 1,
    "id": "sent1",
    "at": "2026-10-03T10:00:00.000Z",
    "platform": "telegram",
    "target": {
      "b": 2,
      "a": 1
    },
    "text": "hi",
    "sent": "2026-10-03T09:00:00.000Z"
  },
  {
    "id": "skip1",
    "skipped": "2026-10-03T09:00:00.000Z"
  },
  {
    "id": "fail1",
    "failed": "2026-10-03T09:00:00.000Z",
    "error": "boom"
  },
  {
    "id": "old",
    "cancelled": "2026-10-01T00:00:00.000Z",
    "extra": true
  },
  {
    "id": "truthy-sent",
    "sent": true
  },
  {
    "id": "pend",
    "at": "2030-01-01T00:00:00.000Z",
    "note": "keep",
    "text": "later",
    "cancelled": "2026-10-03T10:05:00.000Z"
  },
  {
    "id": "blank-sent",
    "sent": "",
    "at": "2030-01-01T00:00:00.000Z",
    "cancelled": "2026-10-03T10:05:00.000Z"
  },
  {
    "id": "blank-skip",
    "skipped": false,
    "cancelled": "2026-10-03T10:05:00.000Z"
  },
  {
    "id": "zero-fail",
    "failed": 0,
    "cancelled": "2026-10-03T10:05:00.000Z"
  },
  {
    "id": "blank-cancel",
    "cancelled": "2026-10-03T10:05:00.000Z"
  }
]`

func TestCancelPendingMarksOnlyPending(t *testing.T) {
	ctx := loadCtx(t)
	file := remindersFile(ctx)
	if err := os.WriteFile(file, []byte(cancelBefore), 0o644); err != nil {
		t.Fatal(err)
	}
	n, err := CancelPending(ctx, fixedTime)
	if err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Fatalf("marked %d entries, want 5 pending", n)
	}
	if got := string(mustRead(t, file)); got != cancelAfter {
		t.Fatalf("file after CancelPending:\n got %q\nwant %q", got, cancelAfter)
	}
}

func TestCancelPendingMissingFile(t *testing.T) {
	ctx := loadCtx(t)
	n, err := CancelPending(ctx, fixedTime)
	if err != nil || n != 0 {
		t.Fatalf("missing file: n=%d err=%v, want 0, nil", n, err)
	}
	if _, err := os.Stat(remindersFile(ctx)); !os.IsNotExist(err) {
		t.Fatalf("missing reminders file was created: %v", err)
	}
}

func TestCancelPendingDoesNotRewriteWhenNothingPending(t *testing.T) {
	ctx := loadCtx(t)
	file := remindersFile(ctx)
	odd := "[\n{\"id\":\"s\",\"sent\":\"yes\"} ]\n"
	if err := os.WriteFile(file, []byte(odd), 0o644); err != nil {
		t.Fatal(err)
	}
	n, err := CancelPending(ctx, fixedTime)
	if err != nil || n != 0 {
		t.Fatalf("nothing pending: n=%d err=%v, want 0, nil", n, err)
	}
	if got := string(mustRead(t, file)); got != odd {
		t.Fatalf("rewrote a file with nothing pending:\n got %q\nwant %q", got, odd)
	}
}

func TestCancelPendingParseError(t *testing.T) {
	for _, bad := range []string{"{oops", `{"not":"array"}`, `[1]`} {
		t.Run(bad, func(t *testing.T) {
			ctx := loadCtx(t)
			file := remindersFile(ctx)
			if err := os.WriteFile(file, []byte(bad), 0o644); err != nil {
				t.Fatal(err)
			}
			n, err := CancelPending(ctx, fixedTime)
			if err == nil {
				t.Fatal("want a parse error")
			}
			if n != 0 {
				t.Fatalf("n=%d, want 0 on error", n)
			}
			if got := string(mustRead(t, file)); got != bad {
				t.Fatalf("error rewrote the file:\n got %q\nwant %q", got, bad)
			}
		})
	}
}

func TestTickAfterCancelPendingSendsNothing(t *testing.T) {
	ctx := loadCtx(t)
	file := remindersFile(ctx)
	body := `[{"id":"due","at":"2026-10-03T10:00:00.000Z","platform":"telegram","target":{"chat_id":1},"text":"hi"},{"id":"later","at":"2030-01-01T00:00:00.000Z","text":"x"},{"id":"done","zz":1,"sent":"2026-10-03T09:00:00.000Z"}]`
	if err := os.WriteFile(file, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	n, err := CancelPending(ctx, fixedTime)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("marked %d, want the two pending entries", n)
	}
	afterCancel := string(mustRead(t, file))
	if strings.Count(afterCancel, `"cancelled"`) != 2 {
		t.Fatalf("cancelled markers = %d, want 2:\n%s", strings.Count(afterCancel, `"cancelled"`), afterCancel)
	}
	if !strings.Contains(afterCancel, `"zz": 1`) || !strings.Contains(afterCancel, `"sent": "2026-10-03T09:00:00.000Z"`) {
		t.Fatalf("already-sent entry was not preserved:\n%s", afterCancel)
	}

	capture := filepath.Join(t.TempDir(), "captured-args")
	sleep := newFakeSleeper()
	withHooks(t, fixedTime, writeFakeSay(t, capture), sleep.sleep)
	sink := newChanSink()
	cancel, done := runSource(t, ctx, sink)
	sink.waitLine(t, "LOG reminder scheduler starting")
	sleep.waitCalls(t, 1)
	sleep.nextTick(t)

	if got := readCapture(t, capture); got != "" {
		t.Fatalf("say ran after CancelPending: %q", got)
	}
	for _, ln := range sink.snapshot() {
		if strings.HasPrefix(ln, "REMIND") {
			t.Errorf("unexpected REMIND line after CancelPending: %q", ln)
		}
	}
	if got := string(mustRead(t, file)); got != afterCancel {
		t.Fatalf("tick rewrote cancelled reminders:\n got %q\nwant %q", got, afterCancel)
	}

	cancel()
	if err := waitDone(t, done); err != nil {
		t.Errorf("Run returned %v, want nil after cancel", err)
	}
}

func readCapture(t *testing.T, p string) string {
	t.Helper()
	b, err := os.ReadFile(p)
	if os.IsNotExist(err) {
		return ""
	}
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
