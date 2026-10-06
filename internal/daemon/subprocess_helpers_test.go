package daemon

import (
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

type processEvent struct {
	Event string `json:"event"`
	PID   int    `json:"pid"`
}

type processFixture struct {
	t      *testing.T
	bin    string
	p      paths
	env    []string
	events chan processEvent
	seen   []processEvent
	ln     net.Listener
	wg     sync.WaitGroup
	spawns string
}

func subprocessFixture(t *testing.T, bin string) *processFixture {
	t.Helper()
	p := socketPaths(t)
	root := t.TempDir()
	f := &processFixture{t: t, bin: bin, p: p, events: make(chan processEvent, 1024)}
	f.spawns = filepath.Join(root, "spawns")
	f.env = append(os.Environ(),
		"HOME="+root, "OMOSENSE_DIR="+p.dir, "OMOSENSE_STATE="+filepath.Join(root, "state"),
		"OMOSENSE_SOCK="+p.socket, "OMOSENSE_TEST_REGISTRY=fake",
		"OMOSENSE_TEST_VERSION=v1", "OMOSENSE_TEST_READY_GATE=",
		"OMOSENSE_TEST_SPAWN_LOG="+f.spawns,
		"OMOSENSE_TEST_EVENTS="+filepath.Join(p.dir, "events.sock"))
	if err := os.WriteFile(filepath.Join(p.dir, "config.json"),
		[]byte(`{"profiles":{"main":{"discord":{"bots":["d1"]}},"family":{"discord":{"bots":[]}}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	var err error
	f.ln, err = net.Listen("unix", filepath.Join(p.dir, "events.sock"))
	if err != nil {
		t.Fatal(err)
	}
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		for {
			conn, err := f.ln.Accept()
			if err != nil {
				return
			}
			f.wg.Add(1)
			go func() {
				defer f.wg.Done()
				defer conn.Close()
				reader := newFramer(conn)
				pid := 0
				for {
					var e processEvent
					if err := reader.read(&e); err != nil {
						if pid != 0 {
							f.events <- processEvent{Event: "exit", PID: pid}
						}
						return
					}
					pid = e.PID
					f.events <- e
				}
			}()
		}
	}()
	t.Cleanup(func() {
		// Attach cleanups run first. A failed scenario still owns its daemon.
		for {
			select {
			case e := <-f.events:
				f.seen = append(f.seen, e)
			default:
				goto drained
			}
		}
	drained:
		spawns, err := os.ReadFile(f.spawns)
		if err != nil && !os.IsNotExist(err) {
			t.Error(err)
		}
		for _, value := range strings.Fields(string(spawns)) {
			pid, err := strconv.Atoi(value)
			if err != nil {
				t.Error(err)
				continue
			}
			if !f.has("exit", pid) {
				if err := syscall.Kill(pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
					t.Error(err)
				}
				f.waitEvent("exit", pid)
			}
		}
		f.ln.Close()
		f.wg.Wait()
		log, err := os.ReadFile(f.p.log)
		if err != nil && !os.IsNotExist(err) {
			t.Error(err)
		}
		if strings.Contains(string(log), "WARNING: DATA RACE") {
			t.Errorf("subprocess daemon race:\n%s", log)
		}
		t.Log("cleanup: all owned daemon processes exited; event listener and readers joined")
	})
	return f
}

func (f *processFixture) has(event string, pid int) bool {
	for _, e := range f.seen {
		if e.Event == event && (pid == 0 || pid == e.PID) {
			return true
		}
	}
	return false
}

func (f *processFixture) waitEvent(event string, pid int) int {
	f.t.Helper()
	for _, e := range f.seen {
		if e.Event == event && (pid == 0 || pid == e.PID) {
			return e.PID
		}
	}
	for {
		e := await(f.t, f.events)
		f.seen = append(f.seen, e)
		if e.Event == event && (pid == 0 || pid == e.PID) {
			return e.PID
		}
	}
}

func (f *processFixture) run(args ...string) (string, int) {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, f.bin, args...)
	cmd.Env = f.env
	f.t.Logf("RUN %s %s", f.bin, strings.Join(args, " "))
	b, err := cmd.CombinedOutput()
	code := 0
	if err != nil {
		var exited *exec.ExitError
		if !errors.As(err, &exited) {
			f.t.Fatal(err)
		}
		code = exited.ExitCode()
	}
	f.t.Logf("EXIT %d: %s", code, b)
	return string(b), code
}

func (f *processFixture) spawnCount() int {
	f.t.Helper()
	b, err := os.ReadFile(f.spawns)
	if err != nil {
		f.t.Fatal(err)
	}
	return len(strings.Fields(string(b)))
}
