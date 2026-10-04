package daemon

import (
	"context"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

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
		f.bin = oldBin
		peer := f.attach("v1", "main")
		old := f.streamPID(peer, 0)
		f.bin = bin

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
