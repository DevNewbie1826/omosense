package listen

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/DevNewbie1826/omosense/internal/core"
)

type record struct {
	prefix string
	value  any
}

type recordingSink struct {
	mu     sync.Mutex
	lines  []record
	signal chan record
}

func newSink() *recordingSink { return &recordingSink{signal: make(chan record, 200)} }
func (s *recordingSink) Emit(p string, v any) {
	// Round-trip exactly as the real writer does.
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	var value any
	if err := json.Unmarshal(b, &value); err != nil {
		panic(err)
	}
	s.add(record{p, value})
}
func (s *recordingSink) Raw(p, v string) { s.add(record{p, v}) }
func (s *recordingSink) Log(v string)    { s.Raw("LOG", v) }
func (s *recordingSink) add(r record) {
	s.mu.Lock()
	s.lines = append(s.lines, r)
	s.mu.Unlock()
	s.signal <- r
}
func (s *recordingSink) snapshot() []record {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]record(nil), s.lines...)
}
func await[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatal("bounded event wait expired")
		var zero T
		return zero
	}
}
func testCtx(t *testing.T) *core.Ctx {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("OMOMEOW_DIR", filepath.Join(home, "omomeow"))
	t.Setenv("OMOMEOW_STATE", filepath.Join(home, "state"))
	t.Setenv("OMOSENSE_TELEGRAM_API", "http://127.0.0.1:1")
	t.Setenv("OMOSENSE_DISCORD_API", "http://127.0.0.1:1")
	dir := filepath.Join(home, ".config", "agent-messenger")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	for _, file := range []string{"telegrambot-credentials.json", "discordbot-credentials.json"} {
		if err := os.WriteFile(filepath.Join(dir, file), []byte(`{"bots":{"test":{"token":"fake-test-token"}}}`), 0600); err != nil {
			t.Fatal(err)
		}
	}
	state := os.Getenv("OMOMEOW_STATE")
	if err := os.MkdirAll(state, 0700); err != nil {
		t.Fatal(err)
	}
	raw, err := core.ParseJSON([]byte(`{"telegram":{"owner":"12","wife":"13"},"discord":{"owner":"owner","wife":"wife"}}`))
	if err != nil {
		t.Fatal(err)
	}
	return &core.Ctx{State: state, Dir: os.Getenv("OMOMEOW_DIR"), Profile: core.Profile{Name: "test", Telegram: []string{"test"}}, Cfg: &core.Cfg{Raw: raw.(*core.OMap), Discord: core.DiscordCfg{Bot: "test"}}, Flags: map[string]bool{}, Out: core.NewOut(os.Stdout)}
}

func TestTelegramPollPersistsOffsetAndEventShape(t *testing.T) {
	// Given: an existing Bun offset and three wire updates.
	c := testCtx(t)
	offsetFile := filepath.Join(c.State, "tg-offset-test")
	if err := os.WriteFile(offsetFile, []byte("41"), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	requests := make(chan map[string]any, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/botfake-test-token/getFile" {
			fmt.Fprint(w, `{"ok":false,"description":"fixture unavailable"}`)
			return
		}
		if r.Method != "POST" || r.URL.Path != "/botfake-test-token/getUpdates" || r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("unexpected wire request %s %s", r.Method, r.URL.Path)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
			return
		}
		requests <- body
		if body["offset"] == float64(41) {
			fmt.Fprint(w, `{"ok":true,"result":[{"update_id":41,"message":{"message_id":7,"chat":{"id":9,"type":"private"},"text":"" ,"caption":"not text"}},{"update_id":42,"edited_message":{"message_id":8,"chat":{"id":9,"type":"supergroup"},"message_thread_id":2,"from":{"id":12,"username":"user"},"caption":"caption","forward_origin":{},"quote":{"text":"q"},"reply_to_message":{"message_id":6,"from":{"first_name":"missing username"},"caption":"reply"},"photo":[{"file_id":"photo-id","file_unique_id":"unique-photo","width":640,"height":480,"file_size":12}],"document":{"file_id":"document-id","file_name":"report.PDF","mime_type":"application/pdf","file_size":24}}},{"update_id":43,"stopped_message_generation":{"message_id":10}}]}`)
		} else {
			<-ctx.Done()
		}
	}))
	defer server.Close()
	t.Setenv("OMOSENSE_TELEGRAM_API", server.URL)
	sink := newSink()
	done := make(chan error, 1)
	// When: run the real Source against the fake HTTP wire.
	go func() { done <- Sources(c)[0].Run(ctx, sink) }()
	select {
	case err := <-done:
		t.Fatalf("source stopped before polling: %v", err)
	case req := <-requests:
		if req["offset"] != float64(41) || req["timeout"] != float64(50) || !reflect.DeepEqual(req["allowed_updates"], []any{"message", "edited_message", "stopped_message_generation"}) {
			t.Fatalf("poll body: %#v", req)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no poll")
	}
	events := map[string]map[string]any{}
	for range 3 {
		r := await(t, sink.signal)
		if r.prefix != "EVENT" {
			t.Fatalf("unexpected line %#v", r)
		}
		m := r.value.(map[string]any)
		events[m["kind"].(string)] = m
	}
	second := await(t, requests)
	// Then: offsets advance before handlers; absent fields stay absent.
	if second["offset"] != float64(44) {
		t.Fatal(second)
	}
	b, err := os.ReadFile(offsetFile)
	if err != nil || string(b) != "44" {
		t.Fatalf("offset %q %v", b, err)
	}
	want := map[string]any{"platform": "telegram", "bot": "test", "kind": "message", "chat_id": float64(9), "chat_type": "private", "thread_id": nil, "message_id": float64(7), "role": "other", "text": "", "forwarded": false, "quote": nil, "reply_to": nil, "attachments": []any{}}
	if !reflect.DeepEqual(events["message"], want) {
		t.Fatalf("event got %#v want %#v", events["message"], want)
	}
	edit := events["edited"]
	wantAttachments := []any{
		map[string]any{"kind": "photo", "file_id": "photo-id", "name": "tg-test-9-8.jpg", "type": "image/jpeg", "size": float64(12), "error": "getFile error fixture unavailable"},
		map[string]any{"kind": "document", "file_id": "document-id", "name": "report.PDF", "type": "application/pdf", "size": float64(24), "error": "getFile error fixture unavailable"},
	}
	if edit["role"] != "owner" || edit["text"] != "caption" || edit["from"] != "user" || edit["from_id"] != float64(12) || edit["forwarded"] != true || edit["quote"] != "q" || edit["thread_id"] != float64(2) || !reflect.DeepEqual(edit["attachments"], wantAttachments) {
		t.Fatal(edit)
	}
	reply := edit["reply_to"].(map[string]any)
	if _, ok := reply["from"]; ok {
		t.Fatal("reply username should be omitted")
	}
	if reply["text"] != "reply" || reply["message_id"] != float64(6) {
		t.Fatal(reply)
	}
	if !reflect.DeepEqual(events["generation_stopped"], map[string]any{"platform": "telegram", "bot": "test", "kind": "generation_stopped", "data": map[string]any{"message_id": float64(10)}}) {
		t.Fatal(events)
	}
	cancel()
	if err := await(t, done); err != nil {
		t.Fatal(err)
	}
	t.Log("PASS: POST getUpdates 41 -> EVENT message/edited/generation_stopped -> next offset/file 44; cleanup: source canceled, HTTP server closed by defer")
}
