package listen

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/DevNewbie1826/omosense/internal/core"
)

// TestConfiguredTranscriber is the IS-11 guard: a configured transcriber is
// actually run, every {audio} is replaced (or the ogg path is appended),
// and a non-zero exit or start error is surfaced as transcribe_error
// "transcriber: <err>: <stderr trimmed, 200 runes>".
func TestConfiguredTranscriber(t *testing.T) {
	t.Run("telegram_substitutes_audio", func(t *testing.T) {
		argvPath, bodyPath, script := recordingTranscriber(t, "printf '  hello \\n'\n")
		pipeline := pipelineSentinel(t)
		tmp := voiceTempRoot(t)
		argv := []string{script, "--in", "{audio}", "x{audio}y", "pre-{audio}-mid-{audio}-end"}
		ev := telegramVoiceEvent(t, argv)
		t.Logf("reached telegram event %#v", ev)
		if ev["text"] != "hello" || ev["transcribed"] != true {
			t.Fatalf("configured transcriber ignored: %#v", ev)
		}
		if _, exists := ev["transcribe_error"]; exists {
			t.Fatalf("configured transcriber ignored: %#v", ev)
		}
		got := readArgv(t, argvPath)
		t.Logf("transcriber argv %q body %q", got, osRead(t, bodyPath))
		if strings.Contains(strings.Join(got, "\n"), "{audio}") {
			t.Fatalf("{audio} not substituted: %q", got)
		}
		if len(got) != 4 {
			t.Fatalf("{audio} not substituted: argc %d argv %q", len(got), got)
		}
		ogg := got[1]
		if !strings.HasSuffix(ogg, "/omosense-voice.ogg") || got[0] != "--in" {
			t.Fatalf("{audio} not substituted: %q", got)
		}
		if got[2] != "x"+ogg+"y" || got[3] != "pre-"+ogg+"-mid-"+ogg+"-end" {
			t.Fatalf("{audio} not substituted: %q", got)
		}
		if osRead(t, bodyPath) != "audio" {
			t.Fatalf("downloaded audio not passed: %q", osRead(t, bodyPath))
		}
		if _, err := os.Stat(pipeline); !os.IsNotExist(err) {
			t.Fatalf("default pipeline ran: %v", err)
		}
		assertVoiceTempGone(t, tmp)
	})

	t.Run("discord_substitutes_audio", func(t *testing.T) {
		argvPath, bodyPath, script := recordingTranscriber(t, "printf '  hello \\n'\n")
		pipeline := pipelineSentinel(t)
		argv := []string{script, "{audio}"}
		ev := discordVoiceEvent(t, argv)
		t.Logf("reached discord event %#v", ev)
		if ev["text"] != "hello" || ev["transcribed"] != true {
			t.Fatalf("configured transcriber ignored: %#v", ev)
		}
		got := readArgv(t, argvPath)
		t.Logf("transcriber argv %q body %q", got, osRead(t, bodyPath))
		if len(got) != 1 || strings.Contains(got[0], "{audio}") || !strings.HasSuffix(got[0], "/omosense-voice.ogg") {
			t.Fatalf("{audio} not substituted: %q", got)
		}
		if osRead(t, bodyPath) != "audio" {
			t.Fatalf("downloaded audio not passed: %q", osRead(t, bodyPath))
		}
		if _, err := os.Stat(pipeline); !os.IsNotExist(err) {
			t.Fatalf("default pipeline ran: %v", err)
		}
	})

	t.Run("telegram_appends_audio", func(t *testing.T) {
		argvPath, bodyPath, script := recordingTranscriber(t, "printf 'appended\\n'\n")
		ev := telegramVoiceEvent(t, []string{script, "--file", "keep"})
		t.Logf("reached telegram append event %#v", ev)
		if ev["text"] != "appended" || ev["transcribed"] != true {
			t.Fatalf("configured transcriber ignored: %#v", ev)
		}
		got := readArgv(t, argvPath)
		t.Logf("transcriber argv %q", got)
		if len(got) != 3 || got[0] != "--file" || got[1] != "keep" || !strings.HasSuffix(got[2], "/omosense-voice.ogg") {
			t.Fatalf("path not appended: %q", got)
		}
		if osRead(t, bodyPath) != "audio" {
			t.Fatalf("appended path is not the download: %q", osRead(t, bodyPath))
		}
	})

	t.Run("telegram_failure_stderr", func(t *testing.T) {
		script := exitTranscriber(t, "printf 'boom-fail\\n' >&2\nexit 9\n")
		ev := telegramVoiceEvent(t, []string{script, "{audio}"})
		t.Logf("reached telegram failure event %#v", ev)
		const want = "transcriber: exit status 9: boom-fail"
		if ev["text"] != "original" || ev["transcribed"] != nil || ev["transcribe_error"] != want {
			t.Fatalf("failure not surfaced as transcribe_error: %#v", ev)
		}
	})

	t.Run("discord_failure_stderr", func(t *testing.T) {
		script := exitTranscriber(t, "printf 'boom-fail\\n' >&2\nexit 9\n")
		ev := discordVoiceEvent(t, []string{script, "{audio}"})
		t.Logf("reached discord failure event %#v", ev)
		const want = "transcriber: exit status 9: boom-fail"
		if ev["text"] != "original" || ev["transcribed"] != nil || ev["transcribe_error"] != want {
			t.Fatalf("failure not surfaced as transcribe_error: %#v", ev)
		}
	})

	t.Run("telegram_failure_empty_stderr", func(t *testing.T) {
		script := exitTranscriber(t, "exit 3\n")
		ev := telegramVoiceEvent(t, []string{script, "{audio}"})
		t.Logf("reached empty-stderr event %#v", ev)
		const want = "transcriber: exit status 3"
		if ev["transcribe_error"] != want {
			t.Fatalf("failure not surfaced as transcribe_error: %#v", ev)
		}
	})

	t.Run("telegram_failure_whitespace_stderr", func(t *testing.T) {
		script := exitTranscriber(t, "printf '\\n  \\t\\n' >&2\nexit 5\n")
		ev := telegramVoiceEvent(t, []string{script, "{audio}"})
		t.Logf("reached whitespace-stderr event %#v", ev)
		const want = "transcriber: exit status 5"
		if ev["transcribe_error"] != want {
			t.Fatalf("failure not surfaced as transcribe_error: %#v", ev)
		}
	})

	t.Run("telegram_failure_redacts_token", func(t *testing.T) {
		script := exitTranscriber(t, "printf 'leak fake-test-token leak\\n' >&2\nexit 4\n")
		ev := telegramVoiceEvent(t, []string{script, "{audio}"})
		t.Logf("reached redaction event %#v", ev)
		const want = "transcriber: exit status 4: leak [redacted] leak"
		if ev["transcribe_error"] != want {
			t.Fatalf("failure not surfaced as transcribe_error: %#v", ev)
		}
	})

	t.Run("telegram_start_error", func(t *testing.T) {
		missing := filepath.Join(t.TempDir(), "missing-transcriber")
		ev := telegramVoiceEvent(t, []string{missing, "{audio}"})
		t.Logf("reached start-error event %#v", ev)
		want := "transcriber: fork/exec " + missing + ": no such file or directory"
		if ev["text"] != "original" || ev["transcribe_error"] != want {
			t.Fatalf("failure not surfaced as transcribe_error: %#v", ev)
		}
	})

	t.Run("telegram_stderr_trimmed_200_runes", func(t *testing.T) {
		noise := strings.Repeat("가", 250)
		script := exitTranscriber(t, "printf '%s' '"+noise+"' >&2\nexit 1\n")
		ev := telegramVoiceEvent(t, []string{script, "{audio}"})
		t.Logf("reached trimmed-stderr event error %q", ev["transcribe_error"])
		want := "transcriber: exit status 1: " + strings.Repeat("가", 200)
		if ev["transcribe_error"] != want {
			t.Fatalf("failure not surfaced as transcribe_error: %#v", ev)
		}
	})
}

// TestTranscriberRespectsContext cancels the handler context after the
// configured transcriber has started and requires the event path to return
// because that process was killed.
func TestTranscriberRespectsContext(t *testing.T) {
	dir := t.TempDir()
	pidPath := filepath.Join(dir, "pid")
	readyPath := filepath.Join(dir, "ready")
	script := exitTranscriber(t, fmt.Sprintf("echo $$ > %s\necho started > %s\nwhile :; do :; done\n", shellQuote(pidPath), shellQuote(readyPath)))
	c := testCtx(t)
	c.Profile.Transcriber = []string{script, "{audio}"}
	server := audioServer(t)
	defer server.Close()
	var u telegramUpdate
	if err := json.Unmarshal([]byte(`{"message":{"message_id":1,"chat":{"id":2,"type":"private"},"caption":"original","voice":{"file_id":"voice-id"}}}`), &u); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var pid int
	t.Cleanup(func() {
		if pid > 0 {
			if p, err := os.FindProcess(pid); err == nil {
				_ = p.Signal(syscall.SIGKILL)
			}
		}
	})
	type outcome struct {
		ev  map[string]any
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		sink := newSink()
		err := sourceByName(t, c, "telegram").tgHandle(ctx, sink, "test", telegramAPI{server.URL, "fake-test-token", server.Client()}, u)
		if err != nil {
			done <- outcome{err: err}
			return
		}
		select {
		case rec := <-sink.signal:
			ev, _ := rec.value.(map[string]any)
			done <- outcome{ev: ev}
		case <-time.After(5 * time.Second):
			done <- outcome{err: fmt.Errorf("no event after transcriber start")}
		}
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(readyPath); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("transcriber did not reach the guarded running state")
		}
	}
	raw, err := os.ReadFile(pidPath)
	if err != nil {
		t.Fatal(err)
	}
	pid, err = strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("transcriber running pid %d; cancelling context", pid)
	cancel()
	var ev map[string]any
	select {
	case out := <-done:
		if out.err != nil {
			t.Fatal(out.err)
		}
		ev = out.ev
	case <-time.After(5 * time.Second):
		t.Fatal("context cancel did not stop the transcriber")
	}
	t.Logf("reached cancelled event %#v", ev)
	errorText, _ := ev["transcribe_error"].(string)
	if ev["text"] != "original" || !strings.HasPrefix(errorText, "transcriber:") {
		t.Fatalf("failure not surfaced as transcribe_error: %#v", ev)
	}
	if processAlive(pid) {
		t.Fatalf("transcriber pid %d still alive after cancel", pid)
	}
}

// subscribeFifo subscribes to a FIFO BEFORE the transcriber starts. The
// goroutine blocks in open() until the child opens the FIFO for writing,
// reports the pid line the child writes (readiness), then blocks until the
// last writer closes and reports that (io.EOF) - which happens exactly when
// the child dies. Both waits are OS events on the pipe; neither polls.
func subscribeFifo(t *testing.T, path string) (<-chan string, <-chan error) {
	t.Helper()
	pids := make(chan string, 1)
	gones := make(chan error, 1)
	go func() {
		f, err := os.OpenFile(path, os.O_RDONLY, 0)
		if err != nil {
			pids <- ""
			gones <- err
			return
		}
		defer f.Close()
		br := bufio.NewReader(f)
		line, err := br.ReadString('\n')
		if err != nil {
			pids <- ""
			gones <- err
			return
		}
		pids <- strings.TrimSpace(line)
		_, err = br.ReadByte()
		gones <- err
	}()
	return pids, gones
}

// awaitFifoPid waits (bounded) for the child's readiness line.
func awaitFifoPid(t *testing.T, pids <-chan string, bound time.Duration) int {
	t.Helper()
	select {
	case line := <-pids:
		n, err := strconv.Atoi(line)
		if err != nil || n <= 0 {
			t.Fatalf("transcriber child pid line %q: %v", line, err)
		}
		return n
	case <-time.After(bound):
		t.Fatalf("the transcriber child never announced readiness through the FIFO within %s", bound)
		return 0
	}
}

// awaitFifoGone waits (bounded) for the FIFO's last writer to close. The
// child holds the FIFO open for writing, so a surviving child keeps this
// read blocked and the bound is what fails.
func awaitFifoGone(t *testing.T, gones <-chan error, bound time.Duration) {
	t.Helper()
	select {
	case err := <-gones:
		if !errors.Is(err, io.EOF) {
			t.Fatalf("FIFO read ended with %v, want EOF once the child died", err)
		}
	case <-time.After(bound):
		t.Fatalf("the transcriber child still holds the FIFO %s after the cancel: it survived the kill", bound)
	}
}

// childProcessGone confirms FIFO EOF without waiting for an orphan's
// reaping. A zombie has exited; a running child still fails this check.
func childProcessGone(pid int) bool {
	if !processAlive(pid) {
		return true
	}
	state, err := exec.Command("ps", "-p", strconv.Itoa(pid), "-o", "stat=").Output()
	return (err == nil && strings.HasPrefix(strings.TrimSpace(string(state)), "Z")) ||
		!processAlive(pid)
}

// TestTranscriberCancelKillsBackgroundChild pins the transcriber's
// process-group contract (core.SourceCommand/RunSource): the configured
// transcriber starts an ordinary child WITHOUT exec, so that child stays in
// the transcriber's process group and inherits its stdout/stderr pipes. The
// child opens a FIFO for writing (the test subscribes to it before the
// source starts), announces its pid, then blocks holding both the FIFO and
// the inherited pipes. Cancelling the handler context must make the source
// return - so the host can release listen.lock.json - AND close the FIFO's
// last writer by killing that child. A bare exec.CommandContext kills only
// the transcriber and then blocks forever draining pipes the surviving child
// still holds.
func TestTranscriberCancelKillsBackgroundChild(t *testing.T) {
	dir := t.TempDir()
	fifoPath := filepath.Join(dir, "child.fifo")
	child := exitTranscriber(t, fmt.Sprintf("exec 9>%s\nprintf '%%s\\n' \"$$\" >&9\nwhile :; do :; done\n", shellQuote(fifoPath)))
	script := exitTranscriber(t, fmt.Sprintf("%s &\nwait\n", shellQuote(child)))
	c := testCtx(t)
	c.Profile.Transcriber = []string{script, "{audio}"}
	server := audioServer(t)
	defer server.Close()
	var u telegramUpdate
	if err := json.Unmarshal([]byte(`{"message":{"message_id":1,"chat":{"id":2,"type":"private"},"caption":"original","voice":{"file_id":"voice-id"}}}`), &u); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(fifoPath, 0o600); err != nil {
		t.Fatal(err)
	}
	pids, gones := subscribeFifo(t, fifoPath)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var pid int
	t.Cleanup(func() {
		if pid > 0 && processAlive(pid) {
			if p, err := os.FindProcess(pid); err == nil {
				_ = p.Signal(syscall.SIGKILL)
			}
		}
	})
	type outcome struct {
		ev  map[string]any
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		sink := newSink()
		err := sourceByName(t, c, "telegram").tgHandle(ctx, sink, "test", telegramAPI{server.URL, "fake-test-token", server.Client()}, u)
		if err != nil {
			done <- outcome{err: err}
			return
		}
		select {
		case rec := <-sink.signal:
			ev, _ := rec.value.(map[string]any)
			done <- outcome{ev: ev}
		case <-time.After(5 * time.Second):
			done <- outcome{err: fmt.Errorf("no event after transcriber start")}
		}
	}()
	pid = awaitFifoPid(t, pids, 5*time.Second)
	t.Logf("transcriber child pid %d is holding the pipes; cancelling the context", pid)
	cancel()
	var ev map[string]any
	select {
	case out := <-done:
		if out.err != nil {
			t.Fatal(out.err)
		}
		ev = out.ev
	case <-time.After(5 * time.Second):
		t.Fatal("context cancel did not stop the transcriber while its child held the pipes")
	}
	t.Logf("reached cancelled event %#v", ev)
	errorText, _ := ev["transcribe_error"].(string)
	if ev["text"] != "original" || !strings.HasPrefix(errorText, "transcriber:") {
		t.Fatalf("failure not surfaced as transcribe_error: %#v", ev)
	}
	awaitFifoGone(t, gones, 3*time.Second)
	if !childProcessGone(pid) {
		t.Fatalf("transcriber child pid %d still alive after cancel: the transcriber does not own its process group", pid)
	}
}

func recordingTranscriber(t *testing.T, trailer string) (argvPath, bodyPath, script string) {
	t.Helper()
	dir := t.TempDir()
	argvPath = filepath.Join(dir, "argv")
	bodyPath = filepath.Join(dir, "body")
	script = exitTranscriber(t, fmt.Sprintf(`printf '%%s\n' "$@" > %s
: > %s
for a in "$@"; do
  if [ -f "$a" ]; then
    cat -- "$a" > %s
  fi
done
%s`, shellQuote(argvPath), shellQuote(bodyPath), shellQuote(bodyPath), trailer))
	return argvPath, bodyPath, script
}

func exitTranscriber(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tr.sh")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body), 0700); err != nil {
		t.Fatal(err)
	}
	return path
}

func pipelineSentinel(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	mark := filepath.Join(dir, "pipeline-ran")
	bin := filepath.Join(dir, "bin")
	if err := os.Mkdir(bin, 0700); err != nil {
		t.Fatal(err)
	}
	script := "#!/bin/sh\ntouch " + shellQuote(mark) + "\nexit 9\n"
	for _, name := range []string{"ffmpeg", "mlx_whisper"} {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(script), 0700); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	return mark
}

func voiceTempRoot(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("TMPDIR", dir)
	return dir
}

func assertVoiceTempGone(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "omosense-voice-") {
			t.Fatalf("voice temporary artifacts leaked: %s", e.Name())
		}
	}
}

func telegramVoiceEvent(t *testing.T, argv []string) map[string]any {
	t.Helper()
	c := testCtx(t)
	c.Profile.Transcriber = argv
	server := audioServer(t)
	t.Cleanup(server.Close)
	var u telegramUpdate
	if err := json.Unmarshal([]byte(`{"message":{"message_id":1,"chat":{"id":2,"type":"private"},"caption":"original","voice":{"file_id":"voice-id"}}}`), &u); err != nil {
		t.Fatal(err)
	}
	sink := newSink()
	if err := sourceByName(t, c, "telegram").tgHandle(context.Background(), sink, "test", telegramAPI{server.URL, "fake-test-token", server.Client()}, u); err != nil {
		t.Fatal(err)
	}
	return await(t, sink.signal).value.(map[string]any)
}

func discordVoiceEvent(t *testing.T, argv []string) map[string]any {
	t.Helper()
	c := testCtx(t)
	c.Profile.Discord.Roles = map[string]string{"20": "wife"}
	c.Profile.Transcriber = argv
	server := audioServer(t)
	t.Cleanup(server.Close)
	raw := []byte(fmt.Sprintf(`{"id":"v","channel_id":"ch","guild_id":"guild","author":{"id":"20","username":"u"},"content":"original","flags":8192,"attachments":[{"filename":"voice.ogg","url":%q}]}`, server.URL))
	sink := newSink()
	if err := sourceByName(t, c, "discord").dcHandle(context.Background(), sink, "test", raw); err != nil {
		t.Fatal(err)
	}
	return await(t, sink.signal).value.(map[string]any)
}

func audioServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/getFile") {
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body["file_id"] != "voice-id" {
				t.Errorf("getFile body %v %v", body, err)
			}
			fmt.Fprint(w, `{"ok":true,"result":{"file_path":"voices/file.ogg"}}`)
			return
		}
		fmt.Fprint(w, "audio")
	}))
}

func sourceByName(t *testing.T, c *core.Ctx, name string) src {
	t.Helper()
	for _, source := range Sources(c) {
		if source.Name() == name {
			return source.(src)
		}
	}
	t.Fatalf("no %s source", name)
	return src{}
}

func readArgv(t *testing.T, path string) []string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	text := strings.TrimSuffix(string(b), "\n")
	if text == "" {
		return nil
	}
	return strings.Split(text, "\n")
}

func osRead(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

func processAlive(pid int) bool {
	p, err := os.FindProcess(pid)
	if err != nil || pid <= 0 {
		return false
	}
	return p.Signal(syscall.Signal(0)) == nil
}
