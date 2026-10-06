package listen

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestDiscordCaptureResetForUntypedTransportFailure(t *testing.T) {
	cl, sink := newClock(), newSink()
	o := &outage{clock: cl, sink: sink, bot: "test"}
	defer o.stop()
	o.ready("first", false)
	await(t, sink.signal)
	o.capture("Closing and reconnecting in response to Op7")
	o.disconnect()
	first := await(t, cl.armed)
	cl.advance(3 * time.Minute)
	first.fire()
	if r := await(t, sink.signal); r.value != "discord test down 3m+ (last close 1005), still reconnecting" {
		t.Fatal(r)
	}
	o.ready("second", false)
	await(t, sink.signal)
	o.connect()
	o.capture("error reading from gateway %s websocket, %s", "fake", errors.New("transport failure"))
	o.disconnect()
	second := await(t, cl.armed)
	cl.advance(3 * time.Minute)
	second.fire()
	if r := await(t, sink.signal); r.value != "discord test down 3m+ (last close 1006), still reconnecting" {
		t.Fatal("stale close code after Connect", r)
	}
}

func TestDiscordOutageReadyResumedAndShortRecovery(t *testing.T) {
	for _, mode := range []string{"initial", "recovery", "resumed", "short", "resumed-first", "stopped"} {
		t.Run(mode, func(t *testing.T) {
			cl, sink := newClock(), newSink()
			o := &outage{clock: cl, sink: sink, bot: "test"}
			if mode != "initial" && mode != "resumed-first" {
				o.ready("first", false)
				await(t, sink.signal)
			}
			o.capture("", fmt.Errorf("wrapped: %w", &websocket.CloseError{Code: 4001}))
			o.disconnect()
			tm := await(t, cl.armed)
			if mode == "short" || mode == "stopped" {
				if mode == "short" {
					o.ready("second", false)
				} else {
					o.stop()
				}
				tm.fire()
				if len(sink.snapshot()) != 1 {
					t.Fatal(sink.snapshot())
				}
				return
			}
			cl.advance(210 * time.Second)
			tm.fire()
			if r := await(t, sink.signal); r.value != "discord test down 3m+ (last close 4001), still reconnecting" {
				t.Fatal(r)
			}
			o.ready("second", mode == "resumed" || mode == "resumed-first")
			if mode == "resumed-first" {
				if len(sink.snapshot()) != 1 {
					t.Fatal(sink.snapshot())
				}
			} else {
				want := "discord test recovered after 4m down"
				if mode == "initial" {
					want = "discord test ready as second"
				}
				if r := await(t, sink.signal); r.value != want {
					t.Fatal(r)
				}
			}
			o.stop()
		})
	}
}

func TestDiscordGatewaySecondOutageResetAndResume(t *testing.T) {
	c, cl := testCtx(t), newClock()
	c.Profile.Discord.Bot = "test"
	peers, cleanup := fakeGateway(t)
	t.Cleanup(cleanup)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	s := Sources(c)[1].(src)
	s.clock = cl
	backoff := make(chan struct{}, 2)
	reopen := make(chan struct{})
	s.sleep = func(ctx context.Context, d time.Duration) error {
		if d != 5*time.Second {
			t.Errorf("reconnect backoff %v", d)
		}
		backoff <- struct{}{}
		select {
		case <-reopen:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	sink, done := newSink(), make(chan error, 1)
	go func() { defer close(done); done <- s.Run(ctx, sink) }()
	t.Cleanup(func() {
		cancel()
		if err := await(t, done); err != nil {
			t.Error(err)
		}
	})
	first := await(t, peers)
	first.packet(t, 2)
	ready(t, first, "READY")
	await(t, sink.signal)
	first.mu.Lock()
	err := first.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(4000, "fixture"))
	first.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	tm := await(t, cl.armed)
	await(t, backoff)
	cl.advance(3 * time.Minute)
	tm.fire()
	if r := await(t, sink.signal); r.value != "discord test down 3m+ (last close 4000), still reconnecting" {
		t.Fatal(r)
	}
	reopen <- struct{}{}
	second := await(t, peers)
	resume := second.packet(t, 6)
	var payload map[string]any
	if err := json.Unmarshal(resume.Data, &payload); err != nil || payload["session_id"] != "fake-session" {
		t.Fatal("resume session", err)
	}
	ready(t, second, "RESUMED")
	if r := await(t, sink.signal); r.value != "discord test recovered after 3m down" {
		t.Fatal(r)
	}
	// Connect has reset capture before the reader can process this TCP drop.
	second.conn.UnderlyingConn().Close()
	tm = await(t, cl.armed)
	cl.advance(3 * time.Minute)
	tm.fire()
	if r := await(t, sink.signal); r.value != "discord test down 3m+ (last close 1006), still reconnecting" {
		t.Fatal("stale close code", r)
	}
	cancel()
	if err := await(t, done); err != nil {
		t.Fatal(err)
	}
	t.Log("PASS: 4000 -> alert -> explicit reconnect -> Op6 RESUMED -> recovery -> TCP1006; cleanup: source joined and gateway connections closed")
}

func TestDiscordVoiceFlagTranscription(t *testing.T) {
	for _, mode := range []string{"voice", "unflagged", "failed"} {
		t.Run(mode, func(t *testing.T) {
			c := testCtx(t)
			c.Profile.Discord.Roles = map[string]string{"20": "wife"}
			fail := ""
			if mode == "failed" {
				fail = "ffmpeg"
			}
			fakeVoiceTools(t, fail)
			downloads := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				downloads++
				fmt.Fprint(w, "audio")
			}))
			defer server.Close()
			flags := 8192
			if mode == "unflagged" {
				flags = 0
			}
			raw := []byte(fmt.Sprintf(`{"id":"v","channel_id":"ch","guild_id":"guild","author":{"id":"20","username":"u","global_name":null},"content":"original","flags":%d,"attachments":[{"filename":"voice.ogg","url":%q}]}`, flags, server.URL))
			sink := newSink()
			if err := Sources(c)[0].(src).dcHandle(t.Context(), sink, "test", raw); err != nil {
				t.Fatal(err)
			}
			ev := await(t, sink.signal).value.(map[string]any)
			// The role comes from the profile's roles map, not from the id.
			if ev["role"] != "wife" || ev["from"] != "u" || ev["guild_id"] != "guild" {
				t.Fatal(ev)
			}
			switch mode {
			case "voice":
				if ev["text"] != "transcribed words" || ev["transcribed"] != true || downloads != 1 {
					t.Fatal(ev)
				}
			case "unflagged":
				if ev["text"] != "original" || ev["transcribed"] != nil || downloads != 0 {
					t.Fatal(ev)
				}
			case "failed":
				if ev["text"] != "original" || ev["transcribe_error"] == nil {
					t.Fatal(ev)
				}
			}
		})
	}
}
