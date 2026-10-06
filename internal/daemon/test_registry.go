package daemon

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/DevNewbie1826/omosense/internal/core"
)

// These env hooks are TEST-ONLY and compiled into the package so real-binary
// subprocess QA can never require a bot, a gateway or a real poller:
//
//	OMOSENSE_TEST_REGISTRY=fake selects channel-driven fake sources.
//	OMOSENSE_TEST_VERSION overrides build identity only with that registry.
//	OMOSENSE_TEST_SPAWN_LOG appends every foreground daemon start's pid.
//	OMOSENSE_TEST_READY_GATE is documented next to waitReadyGate.
//	OMOSENSE_TEST_EVENTS sends NDJSON lifecycle signals on a Unix connection
//	held until process exit; its EOF lets tests observe death without polling.
//
// All hook paths must point at sandbox files, never real user state.
var testEventConn struct {
	sync.Mutex
	conn net.Conn
}

func testNotify(event string) error {
	path := os.Getenv("OMOSENSE_TEST_EVENTS")
	if os.Getenv("OMOSENSE_TEST_REGISTRY") != "fake" || path == "" {
		return nil
	}
	testEventConn.Lock()
	defer testEventConn.Unlock()
	if testEventConn.conn == nil {
		conn, err := net.DialTimeout("unix", path, time.Second)
		if err != nil {
			return fmt.Errorf("test event connection: %w", err)
		}
		testEventConn.conn = conn
	}
	if err := testEventConn.conn.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
		return err
	}
	return writeFrame(testEventConn.conn, struct {
		Event string `json:"event"`
		PID   int    `json:"pid"`
	}{event, os.Getpid()})
}

func cliVersion() (string, error) {
	if os.Getenv("OMOSENSE_TEST_REGISTRY") == "fake" {
		if v := os.Getenv("OMOSENSE_TEST_VERSION"); v != "" {
			return v, nil
		}
	}
	return Version()
}

func testSpawnLog() error {
	path := os.Getenv("OMOSENSE_TEST_SPAWN_LOG")
	if path == "" {
		return nil
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = fmt.Fprintln(f, os.Getpid())
	if err != nil {
		return err
	}
	return testNotify("start")
}

type fakeSource struct {
	name, profile, prefix, lock, legacy string
	always                              bool
	pulse                               chan struct{}
}

func (s *fakeSource) Name() string               { return s.name }
func (s *fakeSource) Prefixes() []string         { return []string{s.prefix} }
func (s *fakeSource) AlwaysOn() bool             { return s.always }
func (s *fakeSource) LockName() (string, string) { return s.lock, s.legacy }

func (s *fakeSource) attached() {
	select {
	case s.pulse <- struct{}{}:
	default:
	}
}

func (s *fakeSource) Run(ctx context.Context, sink core.Sink) error {
	emit := func() {
		sink.Log("fake " + s.name)
		sink.Emit(s.prefix, map[string]any{"source": s.name, "profile": s.profile, "pid": os.Getpid()})
	}
	if s.always {
		emit()
	}
	if err := testNotify("source:" + s.profile + ":" + s.name); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.pulse:
			emit()
		}
	}
}

func fakeRegistry(c *core.Ctx) []core.Source {
	var sources []core.Source
	for _, spec := range []struct {
		name, prefix, lock, legacy string
		always                     bool
	}{
		{"telegram", "EVENT", "listen", "listen", false},
		{"discord", "EVENT", "listen", "listen", true},
		{"google", "CAL", "watch-google", "watch-google", false},
		{"remind", "REMIND", "remind", "remind", true},
		{"herdr", "HERDR", "watch-herdr", "", false},
		{"tidy", "TIDY", "memory-tidy", "", false},
	} {
		if spec.name == "discord" && len(c.Profile.Discord.Bots) == 0 {
			continue
		}
		sources = append(sources, &fakeSource{name: spec.name, prefix: spec.prefix,
			profile: c.Profile.Name, lock: spec.lock + "-" + c.Profile.Name,
			legacy: spec.legacy, always: spec.always, pulse: make(chan struct{}, 32)})
	}
	return sources
}
