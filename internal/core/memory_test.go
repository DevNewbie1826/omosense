package core

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestMemoryAgentsDir(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("OMO_MEMORY_AGENTS", "")
	if d, err := MemoryAgentsDir(); err != nil || d != filepath.Join(home, ".omo", "memory", "agents") {
		t.Fatalf("MemoryAgentsDir = %q %v, want the home default", d, err)
	}
	override := filepath.Join(t.TempDir(), "agents")
	t.Setenv("OMO_MEMORY_AGENTS", override)
	if d, err := MemoryAgentsDir(); err != nil || d != override {
		t.Fatalf("MemoryAgentsDir = %q %v, want the override to win", d, err)
	}
}

func TestMemoryRepoPath(t *testing.T) {
	agents := filepath.Join(t.TempDir(), "agents")
	t.Setenv("OMO_MEMORY_AGENTS", agents)
	p, err := MemoryRepoPath("some-id")
	if err != nil || p != filepath.Join(agents, "some-id", "repo") {
		t.Fatalf("MemoryRepoPath = %q %v, want <agents>/some-id/repo", p, err)
	}
}

// TestCheckMemory pins the IS-14 host-start check: a configured memory id
// without a repo directory errors naming the key and path, a present repo
// passes, and an unset memory logs exactly one warning only with tidy
// enabled.
func TestCheckMemory(t *testing.T) {
	agents := filepath.Join(t.TempDir(), "agents")
	t.Setenv("OMO_MEMORY_AGENTS", agents)
	if err := os.MkdirAll(filepath.Join(agents, "ok", "repo"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(agents, "plain"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name    string
		profile Profile
		wantErr string
		wantLog string
	}{
		{
			name:    "missing repo",
			profile: Profile{Memory: "nope"},
			wantErr: fmt.Sprintf("config.json: memory %q: repo not found at %s", "nope", filepath.Join(agents, "nope", "repo")),
		},
		{
			name:    "repo path is a file",
			profile: Profile{Memory: "plain"},
			wantErr: fmt.Sprintf("config.json: memory %q: repo not found at %s", "plain", filepath.Join(agents, "plain", "repo")),
		},
		{name: "present repo", profile: Profile{Memory: "ok"}},
		{
			name:    "unset with tidy",
			profile: Profile{Tidy: TidyCfg{Enabled: true}},
			wantLog: "LOG memory is not set; tidy has no own memory repo to skip\n",
		},
		{name: "unset without tidy", profile: Profile{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			c := &Ctx{Profile: tc.profile, Out: NewOut(&buf)}
			err := CheckMemory(c)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("err = %v, want nil", err)
				}
			} else if err == nil || err.Error() != tc.wantErr {
				t.Fatalf("err = %v, want %q", err, tc.wantErr)
			}
			if got := buf.String(); got != tc.wantLog {
				t.Errorf("output = %q, want %q", got, tc.wantLog)
			}
		})
	}
}
