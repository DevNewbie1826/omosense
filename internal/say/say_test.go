package say

import (
	"bytes"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/DevNewbie1826/omosense/internal/core"
)

// sayConfig is the folder's flat config: telegram.bot and discord.bot name
// the default sending bots. The creds file below also holds dm2/d2, which
// the {"bot":...} override tests select.
const sayConfig = `{"telegram":{"bot":"b1"},"discord":{"bot":"d1"}}`

// testEnv points HOME, OMOSENSE_DIR and OMOSENSE_STATE at temp dirs with a
// flat config.json and fake agent-messenger credentials (obviously fake
// tokens).
func testEnv(t *testing.T) (home, dir, state string) {
	return testEnvConfig(t, sayConfig)
}

// testEnvConfig is testEnv with a caller-supplied config.json, for the
// bot-less cases.
func testEnvConfig(t *testing.T, cfg string) (home, dir, state string) {
	t.Helper()
	home = t.TempDir()
	dir = filepath.Join(home, ".omosense")
	state = filepath.Join(dir, "state")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	creds := filepath.Join(home, ".config", "agent-messenger")
	if err := os.MkdirAll(creds, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(cfg), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(creds, "telegrambot-credentials.json"), []byte(`{"bots":{"b1":{"token":"TGTOK1"},"dm2":{"token":"TGTOK2"},"f1":{"token":"TGTOKFAM"}}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(creds, "discordbot-credentials.json"), []byte(`{"bots":{"d1":{"token":"DCTOK1"},"d2":{"token":"DCTOK2"}}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)
	t.Setenv("OMOSENSE_DIR", dir)
	t.Setenv("OMOSENSE_STATE", state)
	return home, dir, state
}

// captureStd swaps the process streams so Run's real surface (the compat
// host writing to os.Stdout/os.Stderr like say.ts console.log) is observable.
func captureStd(t *testing.T) (restore func() (stdout, stderr string)) {
	t.Helper()
	oldOut, oldErr := os.Stdout, os.Stderr
	rOut, wOut, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	rErr, wErr, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout, os.Stderr = wOut, wErr
	return func() (stdout, stderr string) {
		wOut.Close()
		wErr.Close()
		ob, _ := io.ReadAll(rOut)
		eb, _ := io.ReadAll(rErr)
		os.Stdout, os.Stderr = oldOut, oldErr
		return string(ob), string(eb)
	}
}

func runSay(t *testing.T, args ...string) (stdout, stderr string, code int) {
	return runSayConfig(t, sayConfig, args...)
}

func runSayConfig(t *testing.T, cfg string, args ...string) (stdout, stderr string, code int) {
	t.Helper()
	testEnvConfig(t, cfg)
	restore := captureStd(t)
	ctx, err := core.Load(core.ParseArgs(args), false)
	if err != nil {
		stdout, stderr = restore()
		t.Fatalf("load: %v", err)
	}
	code = Run(ctx, args)
	stdout, stderr = restore()
	return
}

type recorded struct {
	method string
	path   string
	header http.Header
	body   []byte
	fields map[string][]string
	files  map[string][]byte
}

type recorder struct {
	mu   sync.Mutex
	reqs []recorded
}

func (rec *recorder) handler(respond func(w http.ResponseWriter)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		rec.mu.Lock()
		defer rec.mu.Unlock()
		// EscapedPath is the wire form; URL.Path would decode the
		// percent-encoded reaction emoji.
		rr := recorded{method: r.Method, path: r.URL.EscapedPath(), header: r.Header.Clone(), body: b}
		if ct := r.Header.Get("Content-Type"); strings.HasPrefix(ct, "multipart/form-data") {
			form, err := multipart.NewReader(bytes.NewReader(b), strings.TrimPrefix(ct, "multipart/form-data; boundary=")).ReadForm(1 << 20)
			if err == nil {
				rr.fields = form.Value
				rr.files = map[string][]byte{}
				for k, fh := range form.File {
					if len(fh) == 0 {
						continue
					}
					f, err := fh[0].Open()
					if err != nil {
						continue
					}
					fb, _ := io.ReadAll(f)
					f.Close()
					rr.files[k] = fb
				}
			}
		}
		rec.reqs = append(rec.reqs, rr)
		respond(w)
	}
}

func (rec *recorder) last(t *testing.T) recorded {
	t.Helper()
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.reqs) == 0 {
		t.Fatal("no request recorded")
	}
	return rec.reqs[len(rec.reqs)-1]
}

func jsonResponder(status int, body string) func(w http.ResponseWriter) {
	return func(w http.ResponseWriter) {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}
}

// jsonEqual compares two JSON documents semantically (key order free).
func jsonEqual(t *testing.T, got []byte, want string) {
	t.Helper()
	var g, w any
	dec := json.NewDecoder(bytes.NewReader(got))
	dec.UseNumber()
	if err := dec.Decode(&g); err != nil {
		t.Fatalf("response body is not JSON: %s (%v)", got, err)
	}
	dec = json.NewDecoder(strings.NewReader(want))
	dec.UseNumber()
	if err := dec.Decode(&w); err != nil {
		t.Fatalf("want is not JSON: %s (%v)", want, err)
	}
	if !reflect.DeepEqual(g, w) {
		t.Errorf("body:\n got %s\nwant %s", got, want)
	}
}

func TestUnknownPlatformExits2(t *testing.T) {
	stdout, stderr, code := runSay(t, "nope", "send", `{}`)
	if code != 2 {
		t.Errorf("exit = %d, want 2", code)
	}
	if stderr != "unknown nope send (platforms: telegram, discord)\n" {
		t.Errorf("stderr = %q, want \"unknown nope send (platforms: telegram, discord)\"", stderr)
	}
	if stdout != "" {
		t.Errorf("stdout = %q, want empty", stdout)
	}
}

func TestUnknownActionExits2(t *testing.T) {
	for _, platform := range []string{"telegram", "discord"} {
		stdout, stderr, code := runSay(t, platform, "nope", `{}`)
		if code != 2 {
			t.Errorf("%s: exit = %d, want 2", platform, code)
		}
		wants := map[string]string{
			"telegram": "unknown telegram nope (actions: send, edit, draft, typing, react, unreact, topic, topic-edit, photo, doc)\n",
			"discord":  "unknown discord nope (actions: send, edit, typing, react, unreact, thread, thread-edit, file)\n",
		}
		if want := wants[platform]; stderr != want {
			t.Errorf("%s: stderr = %q, want %q", platform, stderr, want)
		}
		if stdout != "" {
			t.Errorf("%s: stdout = %q, want empty", platform, stdout)
		}
	}
}

func TestEmptyArgsUnknownPlatformExits2(t *testing.T) {
	_, stderr, code := runSay(t, "telegram")
	// One positional is a known platform and an empty action, so this is the
	// action check (after bot resolution), not the platform check.
	want := "unknown telegram  (actions: send, edit, draft, typing, react, unreact, topic, topic-edit, photo, doc)\n"
	if code != 2 || stderr != want {
		t.Errorf("code=%d stderr=%q, want 2/%q", code, stderr, want)
	}
}

func TestBadJSONArgsErrorExits1(t *testing.T) {
	// say.ts: JSON.parse throws -> unhandled crash (exit 1); the Go port
	// reports the same failure as an error JSON on stderr.
	stdout, stderr, code := runSay(t, "telegram", "send", `{oops`)
	if code != 1 {
		t.Errorf("exit = %d, want 1", code)
	}
	if !strings.HasPrefix(stderr, `{"error":`) {
		t.Errorf("stderr = %q, want error JSON", stderr)
	}
	if stdout != "" {
		t.Errorf("stdout = %q, want empty", stdout)
	}
}

func TestTelegramNetworkErrorRedactsToken(t *testing.T) {
	t.Setenv("OMOSENSE_TELEGRAM_API", "http://127.0.0.1:9")
	stdout, stderr, code := runSay(t, "telegram", "send", `{}`)
	if code != 1 {
		t.Errorf("exit = %d, want 1", code)
	}
	if strings.Contains(stdout+stderr, "TGTOK1") {
		t.Errorf("output contains bot token: stdout=%q stderr=%q", stdout, stderr)
	}
	if !strings.Contains(stdout+stderr, "[redacted]") {
		t.Errorf("output = stdout %q stderr %q, want [redacted]", stdout, stderr)
	}
}

// reflectingAPI serves a 403 whose body echoes the credential-bearing
// request material back, the shape review-1 P1 #2 reproduced against the
// real binary: Telegram reflects the /bot<token>/ URL path, Discord the
// Bot <token> Authorization value.
func reflectingAPI(echo func(r *http.Request) string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		msg := "backend rejected " + echo(r)
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"ok":false,"description":"` + msg + `","message":"` + msg + `"}`))
	}))
}

func TestTelegramAPIFailureResponseRedactsToken(t *testing.T) {
	api := reflectingAPI(func(r *http.Request) string { return r.URL.EscapedPath() })
	t.Cleanup(api.Close)
	t.Setenv("OMOSENSE_TELEGRAM_API", api.URL)

	stdout, stderr, code := runSay(t, "telegram", "send", `{"chat_id":123,"text":"hi"}`)
	if code != 1 {
		t.Errorf("exit = %d, want 1 for ok:false", code)
	}
	if strings.Contains(stdout+stderr, "TGTOK1") {
		t.Errorf("output contains bot token: stdout=%q stderr=%q", stdout, stderr)
	}
	// Structure, echoed description and exit code survive; only the
	// credential is replaced.
	jsonEqual(t, []byte(stdout), `{"ok":false,"description":"backend rejected /bot[redacted]/sendMessage","message":"backend rejected /bot[redacted]/sendMessage"}`)
}

func TestDiscordAPIFailureResponseRedactsToken(t *testing.T) {
	api := reflectingAPI(func(r *http.Request) string { return r.Header.Get("Authorization") })
	t.Cleanup(api.Close)
	t.Setenv("OMOSENSE_DISCORD_API", api.URL)

	stdout, stderr, code := runSay(t, "discord", "send", `{"channel_id":"c1","text":"hi"}`)
	if code != 1 {
		t.Errorf("exit = %d, want 1 for HTTP 403", code)
	}
	if strings.Contains(stdout+stderr, "DCTOK1") {
		t.Errorf("output contains bot token: stdout=%q stderr=%q", stdout, stderr)
	}
	jsonEqual(t, []byte(stdout), `{"status":403,"ok":false,"description":"backend rejected Bot [redacted]","message":"backend rejected Bot [redacted]"}`)
}

func TestMissingJSONArgDefaultsToEmptyObject(t *testing.T) {
	rec := &recorder{}
	srv := httptest.NewServer(rec.handler(jsonResponder(200, `{"ok":true}`)))
	t.Cleanup(srv.Close)
	t.Setenv("OMOSENSE_TELEGRAM_API", srv.URL)

	_, _, code := runSay(t, "telegram", "send")
	r := rec.last(t)
	if r.path != "/botTGTOK1/sendMessage" {
		t.Errorf("path = %s, want sendMessage", r.path)
	}
	jsonEqual(t, r.body, `{}`)
	if code != 0 {
		t.Errorf("exit = %d, want 0", code)
	}
}

// IS-7: a config with no bot for the platform and no {"bot":...} override
// is a usage error (exit 2), not a cred lookup failure.
func TestNoBotsErrorsExits2(t *testing.T) {
	for _, platform := range []string{"telegram", "discord"} {
		stdout, stderr, code := runSayConfig(t, `{"telegram":{"bot":""},"discord":{"bot":""}}`, platform, "send", `{"chat_id":1,"text":"x"}`)
		if code != 2 {
			t.Errorf("%s: exit = %d, want 2", platform, code)
		}
		if want := "no bot: config.json has no " + platform + ".bot; pass {\"bot\":\"name\"} to choose one\n"; stderr != want {
			t.Errorf("%s: stderr = %q, want %q", platform, stderr, want)
		}
		if stdout != "" {
			t.Errorf("%s: stdout = %q, want empty", platform, stdout)
		}
	}
}

// An explicit {"bot":...} override still sends when the config names no bot
// for the platform.
func TestBotOverrideRescuesEmptyBots(t *testing.T) {
	rec := &recorder{}
	srv := httptest.NewServer(rec.handler(jsonResponder(200, `{"ok":true}`)))
	t.Cleanup(srv.Close)
	t.Setenv("OMOSENSE_TELEGRAM_API", srv.URL)

	_, _, code := runSayConfig(t, `{"telegram":{"bot":""}}`, "telegram", "send", `{"bot":"b1","chat_id":1,"text":"x"}`)
	r := rec.last(t)
	if r.path != "/botTGTOK1/sendMessage" {
		t.Errorf("path = %s, want the override token (/botTGTOK1/sendMessage)", r.path)
	}
	jsonEqual(t, r.body, `{"chat_id":1,"text":"x"}`)
	if code != 0 {
		t.Errorf("exit = %d, want 0", code)
	}
}

// The folder's config.json supplies the default bot: telegram.bot is the
// token say sends with, with no flag needed.
func TestConfigSelectsDefaultBot(t *testing.T) {
	rec := &recorder{}
	srv := httptest.NewServer(rec.handler(jsonResponder(200, `{"ok":true}`)))
	t.Cleanup(srv.Close)
	t.Setenv("OMOSENSE_TELEGRAM_API", srv.URL)

	_, _, code := runSayConfig(t, `{"telegram":{"bot":"f1"}}`, "telegram", "send", `{"chat_id":1,"text":"x"}`)
	r := rec.last(t)
	if r.path != "/botTGTOKFAM/sendMessage" {
		t.Errorf("path = %s, want the configured bot (/botTGTOKFAM/sendMessage)", r.path)
	}
	if code != 0 {
		t.Errorf("exit = %d, want 0", code)
	}
}

// TestDiscordContentAlias pins Discord's content key: send and edit copy a
// present text key (even null) and otherwise a present content key; file
// uses text ?? content ?? "". text wins when both are set.
func TestDiscordContentAlias(t *testing.T) {
	f := filepath.Join(t.TempDir(), "doc.txt")
	if err := os.WriteFile(f, []byte("FILEDATA"), 0o644); err != nil {
		t.Fatal(err)
	}
	quotedPath := strconv.Quote(f)
	rec := &recorder{}
	srv := httptest.NewServer(rec.handler(jsonResponder(200, `{"id":"1"}`)))
	t.Cleanup(srv.Close)
	t.Setenv("OMOSENSE_DISCORD_API", srv.URL)

	cases := []struct {
		name   string
		action string
		args   string
		file   bool
		want   string
	}{
		{"send content only", "send", `{"channel_id":"1","content":"hi"}`, false, `{"content":"hi"}`},
		{"edit content only", "edit", `{"channel_id":"1","message_id":"2","content":"e"}`, false, `{"content":"e"}`},
		{"file content only", "file", `{"channel_id":"1","content":"f","path":` + quotedPath + `}`, true, `{"content":"f"}`},
		{"send text wins", "send", `{"channel_id":"1","text":"T","content":"C"}`, false, `{"content":"T"}`},
		{"edit text wins", "edit", `{"channel_id":"1","message_id":"2","text":"T","content":"C"}`, false, `{"content":"T"}`},
		{"file text wins", "file", `{"channel_id":"1","text":"T","content":"C","path":` + quotedPath + `}`, true, `{"content":"T"}`},
		{"send text null is presence", "send", `{"channel_id":"1","text":null,"content":"x"}`, false, `{"content":null}`},
		{"edit text null is presence", "edit", `{"channel_id":"1","message_id":"2","text":null,"content":"x"}`, false, `{"content":null}`},
		{"file text null uses content", "file", `{"channel_id":"1","text":null,"content":"kept","path":` + quotedPath + `}`, true, `{"content":"kept"}`},
		{"send neither key omits content", "send", `{"channel_id":"1"}`, false, `{}`},
		{"file neither key is empty", "file", `{"channel_id":"1","path":` + quotedPath + `}`, true, `{"content":""}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, stderr, code := runSay(t, "discord", c.action, c.args)
			r := rec.last(t)
			if c.file {
				got := r.fields["payload_json"]
				if len(got) != 1 {
					t.Fatalf("payload_json = %v, want one field", got)
				}
				jsonEqual(t, []byte(got[0]), c.want)
			} else {
				jsonEqual(t, r.body, c.want)
			}
			if code != 0 {
				t.Errorf("exit = %d, want 0 (stderr=%q)", code, stderr)
			}
		})
	}
}

// actionInHelp reports whether Help has a line naming action as a field list.
func actionInHelp(action string) bool {
	prefix := action + ":"
	for _, line := range strings.Split(Help, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), prefix) {
			return true
		}
	}
	return false
}

// TestActionListsMatchDispatch pins tgActions/dcActions to the dispatch
// switches, to Help, and to rejection of a name that is not a case.
func TestActionListsMatchDispatch(t *testing.T) {
	// Hand-typed from the switch cases in tgCallFor and dcCallFor, in the
	// order the unknown-action error prints. Not derived from the slices.
	tgProbe := []string{"send", "edit", "draft", "typing", "react", "unreact", "topic", "topic-edit", "photo", "doc"}
	dcProbe := []string{"send", "edit", "typing", "react", "unreact", "thread", "thread-edit", "file"}
	empty := core.NewOMap()

	for _, action := range tgActions {
		if _, ok := tgCallFor(action, empty); !ok {
			t.Errorf("tgActions %q is rejected by tgCallFor", action)
		}
		if !actionInHelp(action) {
			t.Errorf("tgActions %q does not appear in Help", action)
		}
	}
	for _, action := range tgProbe {
		if !slices.Contains(tgActions, action) {
			t.Errorf("tgCallFor case %q is not in tgActions %v", action, tgActions)
		}
	}
	if !reflect.DeepEqual(tgActions, tgProbe) {
		t.Errorf("tgActions = %v, want %v", tgActions, tgProbe)
	}

	for _, action := range dcActions {
		if _, ok := dcCallFor(action, empty); !ok {
			t.Errorf("dcActions %q is rejected by dcCallFor", action)
		}
		if !actionInHelp(action) {
			t.Errorf("dcActions %q does not appear in Help", action)
		}
	}
	for _, action := range dcProbe {
		if !slices.Contains(dcActions, action) {
			t.Errorf("dcCallFor case %q is not in dcActions %v", action, dcActions)
		}
	}
	if !reflect.DeepEqual(dcActions, dcProbe) {
		t.Errorf("dcActions = %v, want %v", dcActions, dcProbe)
	}

	const bogus = "not-a-real-action"
	if _, ok := tgCallFor(bogus, empty); ok {
		t.Errorf("tgCallFor accepted %q", bogus)
	}
	if _, ok := dcCallFor(bogus, empty); ok {
		t.Errorf("dcCallFor accepted %q", bogus)
	}
	if slices.Contains(tgActions, bogus) || slices.Contains(dcActions, bogus) {
		t.Errorf("%q is listed in an action slice", bogus)
	}
	if strings.Contains(Help, bogus) {
		t.Errorf("Help lists %q", bogus)
	}
	if strings.Contains(strings.Join(tgActions, ", "), bogus) || strings.Contains(strings.Join(dcActions, ", "), bogus) {
		t.Errorf("printed action list contains %q", bogus)
	}
}
