package daemon

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime/debug"
	"testing"
)

func TestProtocolUnknownSourceExitsTwo(t *testing.T) {
	// Given no config, an unknown source must fail at the argument boundary.
	if code := RunAttach([]string{"invalid"}); code != 2 {
		t.Fatalf("unknown source exit = %d, want 2", code)
	}
}

func TestProtocolAttachOptions(t *testing.T) {
	// Given selectors with profile, output filter and an explicit name.
	h, err := parseAttach([]string{"listen", "--profile=family", "--only", "EVENT,LOG", "--name", "inbox"}, "v1")
	if err != nil {
		t.Fatal(err)
	}
	// Then interest is source identity, not the output prefixes.
	if h.Profile != "family" || h.Name != "inbox" || h.Version != "v1" ||
		!reflect.DeepEqual(h.Sources, []string{"telegram", "discord"}) ||
		!reflect.DeepEqual(h.Only, []string{"EVENT", "LOG"}) {
		t.Fatalf("hello: %+v", h)
	}
	all, err := parseAttach([]string{"all"}, "v1")
	if err != nil || all.Name != "all-main" || all.Only != nil ||
		!reflect.DeepEqual(all.Sources, []string{"telegram", "discord", "google", "remind", "herdr", "rpc", "tidy"}) {
		t.Fatalf("all: %+v, %v", all, err)
	}
	for _, args := range [][]string{{}, {"herdr", "--only"}, {"herdr", "--wat"}, {"herdr", "--profile"}, {"herdr", "extra"}} {
		if _, err := parseAttach(args, "v1"); err == nil {
			t.Errorf("accepted malformed args %q", args)
		}
	}
}

func TestRoutingSourceAndOnlyAreIndependent(t *testing.T) {
	h := hello{Profile: "main", Sources: []string{"herdr"}, Only: []string{"LOG"}}
	for _, tt := range []struct {
		profile, source, line string
		want                  bool
	}{
		{"main", "herdr", "LOG own", true},
		{"main", "google", "LOG foreign", false},
		{"family", "herdr", "LOG other profile", false},
		{"main", "herdr", "HERDR {}", false},
		{"main", "", "LOG omosense daemon notice", true},
	} {
		if got := matches(h, tt.profile, tt.source, tt.line); got != tt.want {
			t.Errorf("%+v matched = %v", tt, got)
		}
	}
	h.Only = nil
	if !matches(h, "main", "herdr", "HERDR {}") || matches(h, "main", "telegram", "EVENT {}") {
		t.Fatal("unfiltered client lost source isolation")
	}
}

func TestProtocolNDJSONPreservesLine(t *testing.T) {
	want := frame{Line: "LOG 한글 <&>\u2028value"}
	var buf bytes.Buffer
	if err := writeFrame(&buf, want); err != nil {
		t.Fatal(err)
	}
	if bytes.Count(buf.Bytes(), []byte("\n")) != 1 {
		t.Fatalf("not one NDJSON record: %q", buf.String())
	}
	var got frame
	if err := newFramer(&buf).read(&got); err != nil || got != want {
		t.Fatalf("round trip = %+v, %v", got, err)
	}
}

func TestProtocolVersionRevisionOrExecutableDigest(t *testing.T) {
	info := &debug.BuildInfo{Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "abc"}, {Key: "vcs.modified", Value: "true"}}}
	if got, err := buildVersion(info, "missing"); err != nil || got != "abc+modified" {
		t.Fatalf("vcs version = %q, %v", got, err)
	}
	path := filepath.Join(t.TempDir(), "bin")
	if err := os.WriteFile(path, []byte("executable"), 0o600); err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf("%x", sha256.Sum256([]byte("executable")))
	if got, err := buildVersion(nil, path); err != nil || got != want {
		t.Fatalf("fallback version = %q, %v", got, err)
	}
}
