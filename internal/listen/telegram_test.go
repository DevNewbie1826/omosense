package listen

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestTelegramFailureThresholdAndRecovery(t *testing.T) {
	for _, mode := range []string{"api", "decode"} {
		t.Run(mode, func(t *testing.T) {
			c := testCtx(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			count := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				count++
				if count <= 61 {
					if mode == "api" {
						fmt.Fprint(w, `{"ok":false,"description":"unavailable"}`)
					} else {
						fmt.Fprint(w, `not json`)
					}
				} else if count == 62 {
					fmt.Fprint(w, `{"ok":true,"result":[]}`)
				} else {
					<-ctx.Done()
				}
			}))
			defer server.Close()
			t.Setenv("OMOSENSE_TELEGRAM_API", server.URL)
			s := Sources(c)[0].(src)
			sleeps := 0
			s.sleep = func(ctx context.Context, d time.Duration) error {
				if d != 5*time.Second {
					t.Errorf("backoff %v", d)
				}
				sleeps++
				return nil
			}
			sink := newSink()
			done := make(chan error, 1)
			// When: 61 failures, then one successful HTTP response.
			go func() { done <- s.Run(ctx, sink) }()
			first := await(t, sink.signal)
			second := await(t, sink.signal)
			cancel()
			if err := await(t, done); err != nil {
				t.Fatal(err)
			}
			// Then: no transient noise, one alert at 60 and recovery at 61.
			want := "telegram test failing 60x in a row: "
			if mode == "api" {
				want += "getUpdates error unavailable"
			} else {
				want += "poll "
			}
			if first.prefix != "LOG" || !strings.HasPrefix(first.value.(string), want) {
				t.Fatal(first)
			}
			if second.value != "telegram test recovered after 61 failures" {
				t.Fatal(second)
			}
			if len(sink.snapshot()) != 2 || sleeps != 61 {
				t.Fatalf("lines=%#v sleeps=%d", sink.snapshot(), sleeps)
			}
			t.Log("PASS: 61 HTTP failures -> single LOG at 60 -> recovered after 61; cleanup: cancellation joined source, deferred server close")
		})
	}
}

func TestTelegramBotsPollIndependently(t *testing.T) {
	c := testCtx(t)
	c.Profile.Telegram = []string{"test", "test"}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	calls := make(chan struct{}, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls <- struct{}{}; <-ctx.Done() }))
	defer server.Close()
	t.Setenv("OMOSENSE_TELEGRAM_API", server.URL)
	done := make(chan error, 1)
	go func() { done <- Sources(c)[0].Run(ctx, newSink()) }()
	await(t, calls)
	await(t, calls)
	cancel()
	if err := await(t, done); err != nil {
		t.Fatal(err)
	}
}
