package say

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestDiscordSendRequestShapeAndExit0(t *testing.T) {
	rec := &recorder{}
	srv := httptest.NewServer(rec.handler(jsonResponder(200, `{"id":"11","nonce":null}`)))
	t.Cleanup(srv.Close)
	t.Setenv("OMOSENSE_DISCORD_API", srv.URL)

	stdout, stderr, code := runSay(t, "discord", "send", `{"channel_id":456,"text":"hi","reply_to":9}`)
	r := rec.last(t)
	if r.method != "POST" {
		t.Errorf("method = %s, want POST", r.method)
	}
	if r.path != "/channels/456/messages" {
		t.Errorf("path = %s, want /channels/456/messages", r.path)
	}
	if auth := r.header.Get("Authorization"); auth != "Bot DCTOK1" {
		t.Errorf("authorization = %q, want Bot DCTOK1", auth)
	}
	if ct := r.header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type = %q, want application/json", ct)
	}
	jsonEqual(t, r.body, `{"content":"hi","message_reference":{"message_id":9}}`)
	if code != 0 {
		t.Errorf("exit = %d, want 0", code)
	}
	if stderr != "" {
		t.Errorf("stderr = %q, want empty", stderr)
	}
	want := "{\"status\":200,\"id\":\"11\",\"nonce\":null}\n"
	if stdout != want {
		t.Errorf("stdout = %q, want %q", stdout, want)
	}
}

func TestDiscordForbiddenPrintsStatusBodyAndExits1(t *testing.T) {
	rec := &recorder{}
	srv := httptest.NewServer(rec.handler(jsonResponder(403, `{"message":"Missing Access"}`)))
	t.Cleanup(srv.Close)
	t.Setenv("OMOSENSE_DISCORD_API", srv.URL)

	stdout, _, code := runSay(t, "discord", "send", `{"channel_id":456,"text":"hi"}`)
	if code != 1 {
		t.Errorf("exit = %d, want 1 on status>=400", code)
	}
	want := "{\"status\":403,\"message\":\"Missing Access\"}\n"
	if stdout != want {
		t.Errorf("stdout = %q, want %q", stdout, want)
	}
}

func TestDiscordTypingEmptyResponseBody(t *testing.T) {
	rec := &recorder{}
	srv := httptest.NewServer(rec.handler(jsonResponder(204, "")))
	t.Cleanup(srv.Close)
	t.Setenv("OMOSENSE_DISCORD_API", srv.URL)

	stdout, _, code := runSay(t, "discord", "typing", `{"channel_id":456}`)
	r := rec.last(t)
	if r.method != "POST" || r.path != "/channels/456/typing" {
		t.Errorf("request = %s %s, want POST /channels/456/typing", r.method, r.path)
	}
	if ct := r.header.Get("Content-Type"); ct != "" {
		t.Errorf("content-type = %q, want none for a bodyless call", ct)
	}
	if len(r.body) != 0 {
		t.Errorf("body = %q, want empty", r.body)
	}
	if code != 0 {
		t.Errorf("exit = %d, want 0", code)
	}
	if stdout != "{\"status\":204}\n" {
		t.Errorf("stdout = %q, want {\"status\":204}", stdout)
	}
}

func TestDiscordReactUnreactPathEmojiEncoding(t *testing.T) {
	cases := []struct {
		name       string
		action     string
		args       string
		wantMethod string
		wantPath   string
	}{
		{"react default emoji", "react", `{"channel_id":456,"message_id":7}`, "PUT", "/channels/456/messages/7/reactions/%F0%9F%91%80/@me"},
		{"react explicit emoji", "react", `{"channel_id":456,"message_id":7,"emoji":"👍"}`, "PUT", "/channels/456/messages/7/reactions/%F0%9F%91%8D/@me"},
		{"unreact default emoji", "unreact", `{"channel_id":456,"message_id":7}`, "DELETE", "/channels/456/messages/7/reactions/%F0%9F%91%80/@me"},
		{"react keeps js unreserved", "react", `{"channel_id":456,"message_id":7,"emoji":"a*b!~"}`, "PUT", "/channels/456/messages/7/reactions/a*b!~/@me"},
		{"react empty emoji kept (?? is nullish)", "react", `{"channel_id":456,"message_id":7,"emoji":""}`, "PUT", "/channels/456/messages/7/reactions//@me"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := &recorder{}
			srv := httptest.NewServer(rec.handler(jsonResponder(204, "")))
			t.Cleanup(srv.Close)
			t.Setenv("OMOSENSE_DISCORD_API", srv.URL)

			_, _, code := runSay(t, "discord", c.action, c.args)
			r := rec.last(t)
			if r.method != c.wantMethod || r.path != c.wantPath {
				t.Errorf("request = %s %s, want %s %s", r.method, r.path, c.wantMethod, c.wantPath)
			}
			if auth := r.header.Get("Authorization"); auth != "Bot DCTOK1" {
				t.Errorf("authorization = %q, want Bot DCTOK1", auth)
			}
			if code != 0 {
				t.Errorf("exit = %d, want 0", code)
			}
		})
	}
}

func TestDiscordFileMultipart(t *testing.T) {
	f := filepath.Join(t.TempDir(), "doc.txt")
	if err := os.WriteFile(f, []byte("FILEDATA"), 0o644); err != nil {
		t.Fatal(err)
	}
	rec := &recorder{}
	srv := httptest.NewServer(rec.handler(jsonResponder(200, `{"id":"12"}`)))
	t.Cleanup(srv.Close)
	t.Setenv("OMOSENSE_DISCORD_API", srv.URL)

	args := `{"channel_id":456,"text":"doc","path":` + strconv.Quote(f) + `}`
	stdout, _, code := runSay(t, "discord", "file", args)
	r := rec.last(t)
	if r.method != "POST" || r.path != "/channels/456/messages" {
		t.Errorf("request = %s %s, want POST /channels/456/messages", r.method, r.path)
	}
	if got := r.fields["payload_json"]; len(got) != 1 || got[0] != `{"content":"doc"}` {
		t.Errorf("payload_json = %v, want [{\"content\":\"doc\"}]", got)
	}
	if got := string(r.files["files[0]"]); got != "FILEDATA" {
		t.Errorf("files[0] = %q, want FILEDATA", got)
	}
	if code != 0 {
		t.Errorf("exit = %d, want 0", code)
	}
	if stdout != "{\"status\":200,\"id\":\"12\"}\n" {
		t.Errorf("stdout = %q", stdout)
	}
}

func TestDiscordActionTable(t *testing.T) {
	cases := []struct {
		name       string
		action     string
		args       string
		wantMethod string
		wantPath   string
		wantBody   string // "" asserts no body at all
	}{
		{"send no reply", "send", `{"channel_id":456,"text":"hi"}`, "POST", "/channels/456/messages", `{"content":"hi"}`},
		{"edit", "edit", `{"channel_id":456,"message_id":7,"text":"t"}`, "PATCH", "/channels/456/messages/7", `{"content":"t"}`},
		{"thread from message", "thread", `{"channel_id":456,"message_id":7,"name":"n"}`, "POST", "/channels/456/messages/7/threads", `{"name":"n"}`},
		{"thread standalone", "thread", `{"channel_id":456,"name":"n"}`, "POST", "/channels/456/threads", `{"name":"n","type":11}`},
		{"thread-edit both", "thread-edit", `{"thread_id":99,"name":"n","archived":true}`, "PATCH", "/channels/99", `{"name":"n","archived":true}`},
		{"thread-edit null archived kept", "thread-edit", `{"thread_id":99,"archived":null}`, "PATCH", "/channels/99", `{"archived":null}`},
		{"thread-edit name only", "thread-edit", `{"thread_id":99,"name":"n2"}`, "PATCH", "/channels/99", `{"name":"n2"}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := &recorder{}
			srv := httptest.NewServer(rec.handler(jsonResponder(204, "")))
			t.Cleanup(srv.Close)
			t.Setenv("OMOSENSE_DISCORD_API", srv.URL)

			_, _, code := runSay(t, "discord", c.action, c.args)
			r := rec.last(t)
			if r.method != c.wantMethod || r.path != c.wantPath {
				t.Errorf("request = %s %s, want %s %s", r.method, r.path, c.wantMethod, c.wantPath)
			}
			if c.wantBody == "" {
				if len(r.body) != 0 {
					t.Errorf("body = %q, want none", r.body)
				}
			} else {
				jsonEqual(t, r.body, c.wantBody)
			}
			if code != 0 {
				t.Errorf("exit = %d, want 0", code)
			}
		})
	}
}

func TestDiscordNetworkErrorExits1(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(http.NotFound))
	url := srv.URL
	srv.Close()
	t.Setenv("OMOSENSE_DISCORD_API", url)

	stdout, stderr, code := runSay(t, "discord", "send", `{"channel_id":456,"text":"x"}`)
	if code != 1 {
		t.Errorf("exit = %d, want 1", code)
	}
	if stdout != "" || !strings.HasPrefix(stderr, `{"error":`) {
		t.Errorf("stdout=%q stderr=%q, want empty stdout + error JSON", stdout, stderr)
	}
}
