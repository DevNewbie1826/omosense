package listen

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DevNewbie1826/omosense/internal/core"
	"github.com/bwmarrin/discordgo"
	"github.com/gorilla/websocket"
)

type fakeTimer struct {
	mu      sync.Mutex
	stopped bool
	fn      func()
}

func (t *fakeTimer) Stop() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	was := !t.stopped
	t.stopped = true
	return was
}
func (t *fakeTimer) fire() {
	t.mu.Lock()
	if t.stopped {
		t.mu.Unlock()
		return
	}
	t.stopped = true
	f := t.fn
	t.mu.Unlock()
	f()
}

type fakeClock struct {
	mu    sync.Mutex
	now   time.Time
	armed chan *fakeTimer
}

func newClock() *fakeClock {
	return &fakeClock{now: time.Unix(1000, 0), armed: make(chan *fakeTimer, 20)}
}
func (c *fakeClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *fakeClock) AfterFunc(d time.Duration, f func()) timer {
	if d != 3*time.Minute {
		panic("wrong outage duration")
	}
	t := &fakeTimer{fn: f}
	c.armed <- t
	return t
}
func (c *fakeClock) advance(d time.Duration) { c.mu.Lock(); c.now = c.now.Add(d); c.mu.Unlock() }

type gatewayPacket struct {
	Op   int             `json:"op"`
	Data json.RawMessage `json:"d"`
}
type gatewayPeer struct {
	conn    *websocket.Conn
	mu      sync.Mutex
	packets chan gatewayPacket
	ended   chan struct{}
}

func (p *gatewayPeer) write(t *testing.T, v any) {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.conn.WriteJSON(v); err != nil {
		t.Fatal(err)
	}
}
func (p *gatewayPeer) packet(t *testing.T, op int) gatewayPacket {
	t.Helper()
	for {
		v := await(t, p.packets)
		if v.Op == op {
			return v
		}
		if v.Op == 1 {
			p.write(t, map[string]any{"op": 11, "d": nil})
		} else {
			t.Fatalf("expected op %d got %d", op, v.Op)
		}
	}
}
func fakeGateway(t *testing.T) (<-chan *gatewayPeer, func()) {
	t.Helper()
	peers := make(chan *gatewayPeer, 10)
	var mu sync.Mutex
	var all []*gatewayPeer
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ws/" {
			fmt.Fprintf(w, `{"url":%q}`, strings.Replace(server.URL, "http", "ws", 1)+"/ws")
			return
		}
		up := websocket.Upgrader{}
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		p := &gatewayPeer{conn: conn, packets: make(chan gatewayPacket, 40), ended: make(chan struct{})}
		mu.Lock()
		all = append(all, p)
		mu.Unlock()
		if err := conn.WriteJSON(map[string]any{"op": 10, "d": map[string]any{"heartbeat_interval": 3600000}}); err != nil {
			t.Error(err)
			return
		}
		peers <- p
		defer close(p.ended)
		for {
			var packet gatewayPacket
			if err := conn.ReadJSON(&packet); err != nil {
				return
			}
			p.packets <- packet
		}
	}))
	old, oldBot := discordgo.EndpointGateway, discordgo.EndpointGatewayBot
	discordgo.EndpointGateway = server.URL + "/gateway"
	discordgo.EndpointGatewayBot = server.URL + "/gateway/bot"
	t.Setenv("OMOSENSE_DISCORD_API", server.URL)
	return peers, func() {
		mu.Lock()
		for _, p := range all {
			p.conn.Close()
		}
		mu.Unlock()
		server.Close()
		discordgo.EndpointGateway, discordgo.EndpointGatewayBot = old, oldBot
	}
}
func ready(t *testing.T, p *gatewayPeer, kind string) {
	t.Helper()
	data := map[string]any{"session_id": "fake-session", "user": map[string]any{"id": "bot", "username": "test-bot"}, "guilds": []any{}}
	p.write(t, map[string]any{"op": 0, "t": kind, "s": 1, "d": data})
}
func startDiscord(t *testing.T, cfg *core.Ctx, cl *fakeClock) (*recordingSink, <-chan error, context.CancelFunc) {
	t.Helper()
	cfg.Profile.Discord.Bots = []string{"test"}
	s := Sources(cfg)[1].(src)
	s.clock = cl
	// Reconnect is released explicitly by the test, never by timing luck.
	ctx, cancel := context.WithCancel(t.Context())
	s.sleep = func(ctx context.Context, d time.Duration) error { <-ctx.Done(); return ctx.Err() }
	sink := newSink()
	done := make(chan error, 1)
	go func() { defer close(done); done <- s.Run(ctx, sink) }()
	t.Cleanup(func() {
		cancel()
		if err := await(t, done); err != nil {
			t.Error(err)
		}
	})
	return sink, done, cancel
}

func TestDiscordGatewayIdentifyRawEventsAndGuard(t *testing.T) {
	c := testCtx(t)
	cl := newClock()
	peers, cleanup := fakeGateway(t)
	t.Cleanup(cleanup)
	sink, done, cancel := startDiscord(t, c, cl)
	defer cancel()
	var p *gatewayPeer
	select {
	case p = <-peers:
	case err := <-done:
		t.Fatalf("source stopped before gateway: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("no gateway connection")
	}
	identify := p.packet(t, 2)
	var data map[string]any
	if err := json.Unmarshal(identify.Data, &data); err != nil {
		t.Fatal(err)
	}
	// discordgo v0.29 IdentifyProperties uses legacy $-prefixed wire keys
	// (structs.go:2323-2327); assert the configured values, not TS's keys.
	properties, _ := data["properties"].(map[string]any)
	if data["intents"] != float64(1|512|4096|32768) || properties["$os"] != "darwin" || properties["$browser"] != "omomeow" || properties["$device"] != "omomeow" {
		t.Fatal("identify intents/properties", data["intents"], properties)
	}
	ready(t, p, "READY")
	if line := await(t, sink.signal); line.value != "discord ready as test-bot" {
		t.Fatal(line)
	}
	// A second source must fail without opening a second session.
	if err := Sources(c)[1].Run(t.Context(), newSink()); err == nil || err.Error() != "discord source already active in this process" {
		t.Fatalf("guard: %v", err)
	}
	p.write(t, map[string]any{"op": 0, "t": "MESSAGE_CREATE", "s": 2, "d": map[string]any{"id": "ignored", "channel_id": "channel", "author": map[string]any{"id": "bot", "bot": true}}})
	p.write(t, map[string]any{"op": 0, "t": "MESSAGE_CREATE", "s": 3, "d": map[string]any{"id": "message", "channel_id": "channel", "author": map[string]any{"id": "owner", "username": "username", "global_name": ""}, "message_snapshots": []any{map[string]any{}}, "referenced_message": map[string]any{"id": "reply", "content": "reply body"}, "attachments": []any{map[string]any{"filename": "a", "url": "url"}, map[string]any{"filename": "b", "url": "b-url", "content_type": "image/png"}}}})
	ev := await(t, sink.signal)
	want := map[string]any{"platform": "discord", "kind": "message", "guild_id": nil, "channel_id": "channel", "message_id": "message", "from_id": "owner", "from": "", "role": "owner", "text": "", "forwarded": true, "reply_to": map[string]any{"message_id": "reply", "text": "reply body"}, "attachments": []any{map[string]any{"name": "a", "url": "url"}, map[string]any{"name": "b", "url": "b-url", "type": "image/png"}}}
	if ev.prefix != "EVENT" || !reflect.DeepEqual(ev.value, want) {
		t.Fatalf("got %#v want %#v", ev, want)
	}
	cancel()
	if err := await(t, done); err != nil {
		t.Fatal(err)
	}
	if len(sink.snapshot()) != 2 {
		t.Fatal(sink.snapshot())
	}
	t.Log("PASS: fake gateway identify, raw EVENT, bot skip and second-session guard; cleanup: source canceled and fake gateway closed")
}

func TestDiscordGatewayCloseCodesAndOp9(t *testing.T) {
	for _, mode := range []string{"close4000", "tcp", "op7", "op9"} {
		t.Run(mode, func(t *testing.T) {
			c := testCtx(t)
			cl := newClock()
			peers, cleanup := fakeGateway(t)
			t.Cleanup(cleanup)
			sink, done, cancel := startDiscord(t, c, cl)
			defer cancel()
			var p *gatewayPeer
			select {
			case p = <-peers:
			case err := <-done:
				t.Fatalf("source stopped before gateway: %v", err)
			case <-time.After(10 * time.Second):
				t.Fatal("no gateway")
			}
			p.packet(t, 2)
			ready(t, p, "READY")
			await(t, sink.signal)
			code := 1006
			switch mode {
			case "close4000":
				code = 4000
				p.mu.Lock()
				err := p.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(4000, "test"))
				p.mu.Unlock()
				if err != nil {
					t.Fatal(err)
				}
			case "op7":
				code = 1005
				p.write(t, map[string]any{"op": 7, "d": nil})
			case "op9":
				p.write(t, map[string]any{"op": 9, "d": false})
				p.packet(t, 2)
				// Exact packet reception proves same connection; snapshot proves silence.
				if len(sink.snapshot()) != 1 {
					t.Fatal("Op9 logged", sink.snapshot())
				}
				select {
				case other := <-peers:
					t.Fatalf("Op9 opened a second connection %p", other)
				default:
				}
				p.conn.UnderlyingConn().Close()
			case "tcp":
				p.conn.UnderlyingConn().Close()
			}
			tm := await(t, cl.armed)
			cl.advance(3 * time.Minute)
			tm.fire()
			r := await(t, sink.signal)
			want := fmt.Sprintf("discord down 3m+ (last close %d), still reconnecting", code)
			if r.value != want {
				t.Fatalf("got %#v want %s", r, want)
			}
			cancel()
			if err := await(t, done); err != nil {
				t.Fatal(err)
			}
			t.Logf("PASS: %s -> %s; cleanup: source canceled, fake gateway closed", mode, want)
		})
	}
}
