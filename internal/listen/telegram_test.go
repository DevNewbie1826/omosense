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
	"strings"
	"testing"
	"time"
)

func attachmentServer(t *testing.T) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "POST" && r.URL.Path == "/botfake-test-token/getFile":
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
				return
			}
			fmt.Fprintf(w, `{"ok":true,"result":{"file_path":%q}}`, "files/"+body["file_id"]+".wrong")
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/file/botfake-test-token/files/"):
			fmt.Fprint(w, strings.TrimSuffix(filepath.Base(r.URL.Path), ".wrong"))
		default:
			t.Errorf("unexpected wire request %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

func attachmentEvent(t *testing.T, s src, server *httptest.Server, chat, id int64, fields string) map[string]any {
	t.Helper()
	var u telegramUpdate
	if err := json.Unmarshal([]byte(fmt.Sprintf(`{"message":{"chat":{"id":%d},"message_id":%d,"caption":"kept",%s}}`, chat, id, fields)), &u); err != nil {
		t.Fatal(err)
	}
	sink := newSink()
	if err := s.tgHandle(t.Context(), sink, "owo_dm", telegramAPI{server.URL, "fake-test-token", server.Client()}, u); err != nil {
		t.Fatal(err)
	}
	lines := sink.snapshot()
	if len(lines) != 1 || lines[0].prefix != "EVENT" {
		t.Fatalf("expected exactly one EVENT, got %#v", lines)
	}
	return lines[0].value.(map[string]any)
}

func attachmentBytes(t *testing.T, path, want string) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil || string(b) != want {
		t.Fatalf("file %s bytes=%q want=%q err=%v", path, b, want, err)
	}
}

func TestTelegramAttachmentDownloads(t *testing.T) {
	for _, tc := range []struct{ kind, fields, name, mime, ext string }{
		{"photo", `"photo":[{"file_id":"payload","file_unique_id":"unique","width":640,"height":480,"file_size":7}]`, "", "image/jpeg", "jpg"},
		{"document", `"document":{"file_id":"payload","file_name":"report.PDF","mime_type":"text/plain","file_size":7}`, "report.PDF", "text/plain", "pdf"},
		{"video", `"video":{"file_id":"payload","mime_type":"video/quicktime","file_size":7}`, "", "video/quicktime", "mov"},
		{"audio", `"audio":{"file_id":"payload","file_name":"bad.toolongextension","mime_type":"audio/ogg","file_size":7}`, "bad.toolongextension", "audio/ogg", "ogg"},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			// Given: fresh state without inbox and an HTTP file fixture.
			c := testCtx(t)
			server := attachmentServer(t)
			inbox := filepath.Join(c.State, "inbox")
			if _, err := os.Stat(inbox); !os.IsNotExist(err) {
				t.Fatalf("inbox must not pre-exist: %v", err)
			}
			// When: a parsed wire message runs through getFile/download/EVENT.
			ev := attachmentEvent(t, Sources(c)[0].(src), server, 6835736153, 73, tc.fields)
			// Then: exact metadata and bytes at the deterministic absolute path.
			base := "tg-owo_dm-6835736153-73." + tc.ext
			name := tc.name
			if name == "" {
				name = base
			}
			path := filepath.Join(inbox, base)
			want := []any{map[string]any{"kind": tc.kind, "file_id": "payload", "name": name, "type": tc.mime, "size": float64(7), "path": path}}
			if !reflect.DeepEqual(ev["attachments"], want) {
				t.Fatalf("attachments=%#v want=%#v", ev["attachments"], want)
			}
			attachmentBytes(t, path, "payload")
			entries, err := os.ReadDir(inbox)
			if err != nil || len(entries) != 1 || entries[0].Name() != base {
				t.Fatalf("inbox entries=%v err=%v", entries, err)
			}
			t.Logf("PASS: EVENT attachment=%v bytes=payload; cleanup: httptest.Close and t.TempDir removal registered", want)
		})
	}
}

func TestTelegramLargestPhoto(t *testing.T) {
	for _, tc := range []struct{ name, sizes, want string }{
		{"area", `[{"file_id":"medium","width":20,"height":20,"file_size":999},{"file_id":"largest","width":40,"height":40,"file_size":2},{"file_id":"small","width":10,"height":10,"file_size":1000}]`, "largest"},
		{"size", `[{"file_id":"winner","width":40,"height":40,"file_size":9},{"file_id":"loser","width":40,"height":40,"file_size":2},{"file_id":"small","width":10,"height":10,"file_size":100}]`, "winner"},
		{"later", `[{"file_id":"first","width":40,"height":40,"file_size":9},{"file_id":"small","width":10,"height":10},{"file_id":"last","width":40,"height":40,"file_size":9}]`, "last"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given: three non-sorted photo sizes with distinct HTTP payloads.
			c := testCtx(t)
			server := attachmentServer(t)
			// When: the real handler chooses and downloads one photo.
			ev := attachmentEvent(t, Sources(c)[0].(src), server, 2, 1, `"photo":`+tc.sizes)
			// Then: the chosen file ID and saved bytes both prove the selection.
			a := ev["attachments"].([]any)[0].(map[string]any)
			if a["file_id"] != tc.want {
				t.Fatal(a)
			}
			attachmentBytes(t, filepath.Join(c.State, "inbox", "tg-owo_dm-2-1.jpg"), tc.want)
		})
	}
}

func TestTelegramAttachmentFailures(t *testing.T) {
	for _, mode := range []string{"getFile", "HTTP404", "redirect"} {
		t.Run(mode, func(t *testing.T) {
			// Given: a failing wire response, including a token-bearing error.
			c := testCtx(t)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/botfake-test-token/getFile" {
					if mode == "getFile" {
						fmt.Fprint(w, `{"ok":false,"description":"denied fake-test-token"}`)
					} else {
						fmt.Fprint(w, `{"ok":true,"result":{"file_path":"file.bin"}}`)
					}
				} else if mode == "redirect" {
					w.Header().Set("Location", "http://[fake-test-token")
					w.WriteHeader(http.StatusFound)
				} else {
					http.NotFound(w, r)
				}
			}))
			t.Cleanup(server.Close)
			// When: failed download still reaches the EVENT sink.
			ev := attachmentEvent(t, Sources(c)[0].(src), server, 2, 1, `"document":{"file_id":"f"}`)
			// Then: caption preserved, exact error objects, no credential or path.
			a := ev["attachments"].([]any)[0].(map[string]any)
			message, ok := a["error"].(string)
			wire, err := json.Marshal(ev)
			if !ok || message == "" || err != nil || strings.Contains(string(wire), "fake-test-token") || ev["text"] != "kept" {
				t.Fatalf("event=%s err=%v", wire, err)
			}
			want := "getFile error denied [redacted]"
			if mode == "HTTP404" {
				want = "download HTTP 404"
			} else if mode == "redirect" {
				want = `failed to parse Location header "http://[[redacted]": parse "http://[[redacted]": missing ']' in host`
			}
			if !reflect.DeepEqual(a, map[string]any{"kind": "document", "file_id": "f", "name": "tg-owo_dm-2-1.bin", "error": want}) {
				t.Fatal(a)
			}
			if _, err := os.Stat(filepath.Join(c.State, "inbox")); !os.IsNotExist(err) {
				t.Fatalf("failed download created inbox: %v", err)
			}
			t.Logf("PASS: event=%s; cleanup: server.Close and t.TempDir removal registered", wire)
		})
	}
}

func TestTelegramAttachmentFilenameContainment(t *testing.T) {
	// Given: sender-controlled traversal name, isolated home/state.
	c := testCtx(t)
	server := attachmentServer(t)
	// When: the document is downloaded.
	ev := attachmentEvent(t, Sources(c)[0].(src), server, -22, 1, `"document":{"file_id":"safe","file_name":"../../evil.sh"}`)
	// Then: sender name is metadata only; only inbox has a new file.
	path := filepath.Join(c.State, "inbox", "tg-owo_dm--22-1.sh")
	want := []any{map[string]any{"kind": "document", "file_id": "safe", "name": "../../evil.sh", "path": path}}
	if !reflect.DeepEqual(ev["attachments"], want) {
		t.Fatalf("attachments=%#v want=%#v", ev["attachments"], want)
	}
	attachmentBytes(t, path, "safe")
	for _, dir := range []string{c.State, filepath.Dir(c.State)} {
		if _, err := os.Stat(filepath.Join(dir, "evil.sh")); !os.IsNotExist(err) {
			t.Fatalf("escaped file in %s: %v", dir, err)
		}
	}
	entries, err := os.ReadDir(c.State)
	if err != nil || len(entries) != 1 || entries[0].Name() != "inbox" {
		t.Fatalf("state entries=%v err=%v", entries, err)
	}
}

func TestTelegramAttachmentChats(t *testing.T) {
	// Given: shared inbox, same message ID in different chats.
	c := testCtx(t)
	server := attachmentServer(t)
	// When: both real downloads run with different payloads.
	first := attachmentEvent(t, Sources(c)[0].(src), server, 10, 73, `"document":{"file_id":"first"}`)
	second := attachmentEvent(t, Sources(c)[0].(src), server, -20, 73, `"document":{"file_id":"second"}`)
	// Then: paths differ and neither file overwrote the other.
	a := first["attachments"].([]any)[0].(map[string]any)
	b := second["attachments"].([]any)[0].(map[string]any)
	p1 := filepath.Join(c.State, "inbox", "tg-owo_dm-10-73.bin")
	p2 := filepath.Join(c.State, "inbox", "tg-owo_dm--20-73.bin")
	if a["path"] != p1 || b["path"] != p2 {
		t.Fatal(a, b)
	}
	attachmentBytes(t, p1, "first")
	attachmentBytes(t, p2, "second")
}

func TestTelegramAudioDefaultExtension(t *testing.T) {
	// Given: audio without name, MIME or size; remote path has .wrong.
	c := testCtx(t)
	server := attachmentServer(t)
	// When: audio is downloaded.
	ev := attachmentEvent(t, Sources(c)[0].(src), server, 2, 1, `"audio":{"file_id":"audio"}`)
	// Then: kind fallback determines .mp3; unknown metadata is omitted.
	path := filepath.Join(c.State, "inbox", "tg-owo_dm-2-1.mp3")
	want := []any{map[string]any{"kind": "audio", "file_id": "audio", "name": "tg-owo_dm-2-1.mp3", "path": path}}
	if !reflect.DeepEqual(ev["attachments"], want) {
		t.Fatalf("attachments=%#v want=%#v", ev["attachments"], want)
	}
	attachmentBytes(t, path, "audio")
}

func TestTelegramAttachmentCreatesInbox(t *testing.T) {
	// Given: state exists but inbox does not.
	c := testCtx(t)
	server := attachmentServer(t)
	inbox := filepath.Join(c.State, "inbox")
	if _, err := os.Stat(inbox); !os.IsNotExist(err) {
		t.Fatalf("inbox pre-exists: %v", err)
	}
	s := Sources(c)[0].(src)
	empty := attachmentEvent(t, s, server, 2, 1, `"photo":[]`)
	if !reflect.DeepEqual(empty["attachments"], []any{}) {
		t.Fatal(empty)
	}
	if _, err := os.Stat(inbox); !os.IsNotExist(err) {
		t.Fatalf("empty photo created inbox: %v", err)
	}
	// When: first actual media attachment arrives.
	ev := attachmentEvent(t, s, server, 2, 1, `"document":{"file_id":"first"}`)
	// Then: successful EVENT points into newly created inbox with no temp leaks.
	path := filepath.Join(inbox, "tg-owo_dm-2-1.bin")
	a := ev["attachments"].([]any)[0].(map[string]any)
	if a["path"] != path || a["error"] != nil {
		t.Fatal(a)
	}
	attachmentBytes(t, path, "first")
	info, err := os.Stat(inbox)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0755 {
		t.Fatalf("inbox info=%v err=%v", info, err)
	}
	entries, err := os.ReadDir(inbox)
	if err != nil || len(entries) != 1 || entries[0].Name() != filepath.Base(path) {
		t.Fatalf("inbox entries=%v err=%v", entries, err)
	}
}

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
