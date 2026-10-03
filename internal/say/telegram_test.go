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

// TestTelegramActionTable pins every telegram action's API method and body
// exactly as say.ts builds it (absent keys are omitted, never null).
func TestTelegramActionTable(t *testing.T) {
	cases := []struct {
		name       string
		action     string
		args       string
		wantMethod string
		wantBody   string
	}{
		{"react default emoji", "react", `{"chat_id":1,"message_id":2}`, "setMessageReaction", `{"chat_id":1,"message_id":2,"reaction":[{"type":"emoji","emoji":"👀"}]}`},
		{"react explicit emoji", "react", `{"chat_id":1,"message_id":2,"emoji":"🔥"}`, "setMessageReaction", `{"chat_id":1,"message_id":2,"reaction":[{"type":"emoji","emoji":"🔥"}]}`},
		{"unreact empty reaction", "unreact", `{"chat_id":1,"message_id":2}`, "setMessageReaction", `{"chat_id":1,"message_id":2,"reaction":[]}`},
		{"send plain", "send", `{"chat_id":123,"text":"hi"}`, "sendMessage", `{"chat_id":123,"text":"hi"}`},
		{"send thread reply parse_mode", "send", `{"chat_id":123,"text":"hi","thread_id":77,"reply_to":9,"parse_mode":"HTML"}`, "sendMessage", `{"chat_id":123,"text":"hi","reply_parameters":{"message_id":9},"parse_mode":"HTML","message_thread_id":77}`},
		{"edit", "edit", `{"chat_id":1,"message_id":5,"text":"t"}`, "editMessageText", `{"chat_id":1,"message_id":5,"text":"t"}`},
		{"edit parse_mode", "edit", `{"chat_id":1,"message_id":5,"text":"t","parse_mode":"Markdown"}`, "editMessageText", `{"chat_id":1,"message_id":5,"text":"t","parse_mode":"Markdown"}`},
		{"draft", "draft", `{"chat_id":1,"draft_id":"d1","text":"t"}`, "sendMessageDraft", `{"chat_id":1,"draft_id":"d1","text":"t"}`},
		{"draft thread", "draft", `{"chat_id":1,"draft_id":"d1","text":"t","thread_id":3}`, "sendMessageDraft", `{"chat_id":1,"draft_id":"d1","text":"t","message_thread_id":3}`},
		{"typing", "typing", `{"chat_id":1}`, "sendChatAction", `{"chat_id":1,"action":"typing"}`},
		{"typing thread", "typing", `{"chat_id":1,"thread_id":4}`, "sendChatAction", `{"chat_id":1,"action":"typing","message_thread_id":4}`},
		{"topic", "topic", `{"chat_id":1,"name":"n"}`, "createForumTopic", `{"chat_id":1,"name":"n"}`},
		{"topic-edit", "topic-edit", `{"chat_id":1,"thread_id":9,"name":"n2"}`, "editForumTopic", `{"chat_id":1,"message_thread_id":9,"name":"n2"}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := &recorder{}
			srv := httptest.NewServer(rec.handler(jsonResponder(200, `{"ok":true,"result":{"m":1}}`)))
			t.Cleanup(srv.Close)
			t.Setenv("OMOSENSE_TELEGRAM_API", srv.URL)

			stdout, stderr, code := runSay(t, "telegram", c.action, c.args)
			r := rec.last(t)
			if r.method != "POST" {
				t.Errorf("method = %s, want POST", r.method)
			}
			if r.path != "/botTGTOK1/"+c.wantMethod {
				t.Errorf("path = %s, want /botTGTOK1/%s", r.path, c.wantMethod)
			}
			if ct := r.header.Get("Content-Type"); ct != "application/json" {
				t.Errorf("content-type = %q, want application/json", ct)
			}
			jsonEqual(t, r.body, c.wantBody)
			if code != 0 {
				t.Errorf("exit = %d, want 0 (stderr=%q)", code, stderr)
			}
			if want := "{\"ok\":true,\"result\":{\"m\":1}}\n"; stdout != want {
				t.Errorf("stdout = %q, want %q", stdout, want)
			}
		})
	}
}

func TestTelegramOKFalsePrintsResponseAndExits1(t *testing.T) {
	rec := &recorder{}
	srv := httptest.NewServer(rec.handler(jsonResponder(200, `{"ok":false,"error_code":400,"description":"Bad Request: chat not found"}`)))
	t.Cleanup(srv.Close)
	t.Setenv("OMOSENSE_TELEGRAM_API", srv.URL)

	stdout, stderr, code := runSay(t, "telegram", "send", `{"chat_id":1,"text":"x"}`)
	if code != 1 {
		t.Errorf("exit = %d, want 1 on ok:false", code)
	}
	if stderr != "" {
		t.Errorf("stderr = %q, want empty (response still printed)", stderr)
	}
	want := "{\"ok\":false,\"error_code\":400,\"description\":\"Bad Request: chat not found\"}\n"
	if stdout != want {
		t.Errorf("stdout = %q, want %q", stdout, want)
	}
}

func TestTelegramBotOverrideSelectsBotCred(t *testing.T) {
	rec := &recorder{}
	srv := httptest.NewServer(rec.handler(jsonResponder(200, `{"ok":true}`)))
	t.Cleanup(srv.Close)
	t.Setenv("OMOSENSE_TELEGRAM_API", srv.URL)

	_, _, code := runSay(t, "telegram", "send", `{"bot":"dm2","chat_id":1,"text":"x"}`)
	r := rec.last(t)
	if r.path != "/botTGTOK2/sendMessage" {
		t.Errorf("path = %s, want the a.bot override token (/botTGTOK2/sendMessage)", r.path)
	}
	jsonEqual(t, r.body, `{"chat_id":1,"text":"x"}`)
	if code != 0 {
		t.Errorf("exit = %d, want 0", code)
	}
}

func TestTelegramPhotoAndDocMultipart(t *testing.T) {
	f := filepath.Join(t.TempDir(), "pic.jpg")
	if err := os.WriteFile(f, []byte("JPEGDATA"), 0o644); err != nil {
		t.Fatal(err)
	}

	rec := &recorder{}
	srv := httptest.NewServer(rec.handler(jsonResponder(200, `{"ok":true}`)))
	t.Cleanup(srv.Close)
	t.Setenv("OMOSENSE_TELEGRAM_API", srv.URL)

	t.Run("photo", func(t *testing.T) {
		args := `{"chat_id":123,"caption":"cap \"q\"","thread_id":5,"path":` + strconv.Quote(f) + `}`
		stdout, _, code := runSay(t, "telegram", "photo", args)
		r := rec.last(t)
		if r.path != "/botTGTOK1/sendPhoto" {
			t.Errorf("path = %s, want /botTGTOK1/sendPhoto", r.path)
		}
		// Non-string values are JSON-encoded; strings stay raw.
		if got := r.fields["chat_id"]; len(got) != 1 || got[0] != "123" {
			t.Errorf("chat_id field = %v, want [123] (JSON-encoded number)", got)
		}
		if got := r.fields["message_thread_id"]; len(got) != 1 || got[0] != "5" {
			t.Errorf("message_thread_id field = %v, want [5]", got)
		}
		if got := r.fields["caption"]; len(got) != 1 || got[0] != `cap "q"` {
			t.Errorf("caption field = %v, want the raw string cap \"q\" (never JSON-quoted)", got)
		}
		if got := string(r.files["photo"]); got != "JPEGDATA" {
			t.Errorf("photo file = %q, want JPEGDATA", got)
		}
		if code != 0 || stdout != "{\"ok\":true}\n" {
			t.Errorf("code=%d stdout=%q, want 0/{\"ok\":true}", code, stdout)
		}
	})

	t.Run("doc default caption", func(t *testing.T) {
		args := `{"chat_id":123,"path":` + strconv.Quote(f) + `}`
		_, _, code := runSay(t, "telegram", "doc", args)
		r := rec.last(t)
		if r.path != "/botTGTOK1/sendDocument" {
			t.Errorf("path = %s, want /botTGTOK1/sendDocument", r.path)
		}
		if got := r.fields["caption"]; len(got) != 1 || got[0] != "" {
			t.Errorf("caption field = %v, want [\"\"] (a.caption ?? \"\")", got)
		}
		if got := string(r.files["document"]); got != "JPEGDATA" {
			t.Errorf("document file = %q, want JPEGDATA", got)
		}
		if code != 0 {
			t.Errorf("exit = %d, want 0", code)
		}
	})
}

func TestTelegramNetworkErrorExits1(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(http.NotFound))
	url := srv.URL
	srv.Close()
	t.Setenv("OMOSENSE_TELEGRAM_API", url)

	stdout, stderr, code := runSay(t, "telegram", "send", `{"chat_id":1,"text":"x"}`)
	if code != 1 {
		t.Errorf("exit = %d, want 1 on network error", code)
	}
	if stdout != "" {
		t.Errorf("stdout = %q, want empty", stdout)
	}
	if !strings.HasPrefix(stderr, `{"error":`) {
		t.Errorf("stderr = %q, want error JSON", stderr)
	}
}
