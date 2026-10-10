package rpc

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/DevNewbie1826/omosense/internal/core"
)

// TestMissingSocketNotLogged guards the quiet start: dialing a socket path that
// does not exist yet logs nothing on the tick path or the stream path, while
// another dial error (here a path under a regular file) is still logged.
func TestMissingSocketNotLogged(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "file")
	if err := os.WriteFile(file, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name, sock string
		logged     bool
	}{
		{"missing socket", filepath.Join(dir, "none.sock"), false},
		{"other dial error", filepath.Join(file, "x.sock"), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dial(context.Background(), tc.sock)
			if err == nil {
				t.Fatal("dial succeeded")
			}
			var buf bytes.Buffer
			w := &watcher{sink: core.NewOut(&buf), errs: map[string]string{}, streamErrors: map[string]bool{}}
			w.noteError("list", err)
			w.applyStream(context.Background(), streamItem{err: err})
			got := strings.TrimSpace(buf.String())
			if !tc.logged {
				if got != "" {
					t.Fatalf("missing socket logged %q, want nothing", got)
				}
				return
			}
			want := "LOG rpc " + err.Error() + "\nLOG rpc stream " + err.Error()
			if got != want {
				t.Fatalf("logged %q, want %q", got, want)
			}
		})
	}
}
