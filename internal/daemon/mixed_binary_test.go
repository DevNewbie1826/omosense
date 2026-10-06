package daemon

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// stageOldBinaryEnv points the pre-generalize binary — which reads only
// OMOMEOW_* and only the old config shape — at the same sandbox dir and
// state as HEAD, and stages the old-shape config its lenient parser needs
// to register the discord source. Same dir keeps the shared socket,
// journal, spawn and lifetime locks, so the upgrade race protection
// holds across versions.
func stageOldBinaryEnv(t *testing.T, f *processFixture) {
	t.Helper()
	var state string
	for _, e := range f.env {
		if v, ok := strings.CutPrefix(e, "OMOSENSE_STATE="); ok {
			state = v
		}
	}
	if state == "" {
		t.Fatal("fixture env carries no OMOSENSE_STATE")
	}
	f.env = append(f.env, "OMOMEOW_DIR="+f.p.dir, "OMOMEOW_STATE="+state)
	stageOldConfig(t, f)
}

func stageOldConfig(t *testing.T, f *processFixture) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(f.p.dir, "config.json"),
		[]byte(`{"profiles":{"main":{"discord":true},"family":{"discord":false}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
}

// stageNewConfig restores the new profile shape before a HEAD daemon
// spawns: config and binary swap together, exactly like the deploy
// procedure the runbook describes.
func stageNewConfig(t *testing.T, f *processFixture) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(f.p.dir, "config.json"),
		[]byte(`{"profiles":{"main":{"discord":{"bots":["d1"]}},"family":{"discord":{"bots":[]}}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestMixedBinaryUpgrade(t *testing.T) {
	// Given actual pre-fix production sources, not HEAD with an old label.
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	oldTree := filepath.Join(t.TempDir(), "e8164ef")
	run := func(dir string, args ...string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		cmd := exec.CommandContext(ctx, args[0], args[1:]...)
		cmd.Dir = dir
		t.Logf("RUN %v (dir %s)", args, dir)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("%v: %v\n%s", args, err, out)
		}
	}
	run(root, "git", "worktree", "add", "--detach", oldTree, "e8164ef")
	t.Cleanup(func() {
		run(root, "git", "worktree", "remove", oldTree)
		t.Log("cleanup: detached e8164ef worktree removed")
	})
	oldBin := filepath.Join(t.TempDir(), "omosense-old")
	bin := filepath.Join(t.TempDir(), "omosense-head")
	run(oldTree, "go", "build", "-o", oldBin, "./cmd/omosense")
	run(root, "go", "build", "-race", "-o", bin, "./cmd/omosense")

	t.Run("pending_replay", func(t *testing.T) {
		// When HEAD upgrades OLD, then replay precedes live output exactly once.
		testUpgradeJournalReplayFrom(t, bin, oldBin)
	})
	t.Run("old_client_resumes", func(t *testing.T) {
		// Given an actual old attach connected to its old daemon.
		f := subprocessFixture(t, bin)
		stageOldBinaryEnv(t, f)
		f.bin = oldBin
		peer := f.attach("v1", "main")
		old := f.streamPID(peer, 0)
		f.bin = bin
		stageNewConfig(t, f)

		// When HEAD upgrades the daemon.
		a := f.attach("v2", "main")
		next := f.streamPID(a, old)

		// Then the old hello-only client receives replacement events.
		if got := f.streamPID(peer, old); got != next {
			t.Fatalf("old client reconnected to %d, want replacement %d", got, next)
		}
		f.waitEvent("exit", old)
		if count := f.spawnCount(); count != 2 {
			t.Fatalf("upgrade spawns=%d, want 2", count)
		}
		f.stop(peer, a)
	})
}
