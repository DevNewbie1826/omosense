package listen

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func fakeVoiceTools(t *testing.T, fail string) string {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("TMPDIR", dir)
	bin := filepath.Join(dir, "bin")
	if err := os.Mkdir(bin, 0700); err != nil {
		t.Fatal(err)
	}
	scripts := map[string]string{
		"ffmpeg": `#!/bin/sh
test "$1" = -loglevel && test "$2" = error && test "$3" = -y && test "$4" = -i || exit 20
test "$(cat "$5")" = audio || exit 21
test "$6" = -ar && test "$7" = 16000 && test "$8" = -ac && test "$9" = 1 || exit 22
cp "$5" "${10}"
`,
		"mlx_whisper": `#!/bin/sh
test "$(cat "$1")" = audio || exit 23
test "$2" = --model && test "$3" = mlx-community/whisper-large-v3-turbo || exit 24
test "$4" = --output-format && test "$5" = txt && test "$6" = --output-dir || exit 25
test "$8" = --verbose && test "$9" = False || exit 26
printf '  transcribed words \n' > "$7/$(basename "${1%.wav}").txt"
`,
	}
	if fail != "" {
		scripts[fail] = "#!/bin/sh\nprintf 'fixture failure' >&2\nexit 7\n"
	}
	for name, script := range scripts {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(script), 0700); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	return dir
}

func TestTelegramVoiceTranscriptionAndCleanup(t *testing.T) {
	for _, mode := range []string{"voice", "video_note", "ffmpeg", "mlx_whisper", "getFile"} {
		t.Run(mode, func(t *testing.T) {
			c := testCtx(t)
			fail := ""
			if mode == "ffmpeg" || mode == "mlx_whisper" {
				fail = mode
			}
			tmp := fakeVoiceTools(t, fail)
			fileRequests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/botfake-test-token/getFile":
					var body map[string]string
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body["file_id"] != "voice-id" {
						t.Errorf("getFile body %v %v", body, err)
					}
					if mode == "getFile" {
						fmt.Fprint(w, `{"ok":false,"description":"missing file"}`)
					} else {
						fmt.Fprint(w, `{"ok":true,"result":{"file_path":"voices/file.ogg"}}`)
					}
				case "/file/botfake-test-token/voices/file.ogg":
					fileRequests++
					fmt.Fprint(w, "audio")
				default:
					t.Errorf("unexpected request %s", r.URL.Path)
				}
			}))
			defer server.Close()
			field := "voice"
			if mode == "video_note" {
				field = mode
			}
			var u telegramUpdate
			if err := json.Unmarshal([]byte(fmt.Sprintf(`{"message":{"message_id":1,"chat":{"id":2,"type":"private"},"caption":"original",%q:{"file_id":"voice-id"}}}`, field)), &u); err != nil {
				t.Fatal(err)
			}
			sink := newSink()
			// When: real HTTP getFile/download and real fake-PATH subprocesses.
			err := Sources(c)[0].(src).tgHandle(context.Background(), sink, "test", telegramAPI{server.URL, "fake-test-token", server.Client()}, u)
			if err != nil {
				t.Fatal(err)
			}
			ev := await(t, sink.signal).value.(map[string]any)
			if fail == "" && mode != "getFile" {
				if ev["text"] != "transcribed words" || ev["transcribed"] != true || fileRequests != 1 {
					t.Fatal(ev, fileRequests)
				}
				if _, exists := ev["transcribe_error"]; exists {
					t.Fatal(ev)
				}
			} else {
				if ev["text"] != "original" || ev["transcribed"] != nil || ev["transcribe_error"] == nil {
					t.Fatal(ev)
				}
			}
			entries, err := os.ReadDir(tmp)
			if err != nil || len(entries) != 1 || entries[0].Name() != "bin" {
				t.Fatalf("voice temporary artifacts leaked: %v %v", entries, err)
			}
			t.Log("PASS: getFile/download -> ffmpeg -> mlx_whisper -> EVENT; cleanup: no voice files remain, HTTP server closed by defer")
		})
	}
}

func TestTelegramVoiceErrorRedactsCredential(t *testing.T) {
	c := testCtx(t)
	var u telegramUpdate
	if err := json.Unmarshal([]byte(`{"message":{"chat":{"id":1},"voice":{"file_id":"v"}}}`), &u); err != nil {
		t.Fatal(err)
	}
	sink := newSink()
	if err := Sources(c)[0].(src).tgHandle(t.Context(), sink, "test", telegramAPI{"http://127.0.0.1:1", "fake-test-token", &http.Client{}}, u); err != nil {
		t.Fatal(err)
	}
	ev := await(t, sink.signal).value.(map[string]any)
	errorText, ok := ev["transcribe_error"].(string)
	if !ok || strings.Contains(errorText, "fake-test-token") {
		t.Fatal(ev)
	}
}
