package daemon

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
)

func socketPaths(t *testing.T) paths {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "os-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	})
	return pathsFor(dir, filepath.Join(dir, "s.sock"))
}

func TestSingletonLockPrecedesSocketAndLivesUntilClose(t *testing.T) {
	p := socketPaths(t)
	owner, err := acquireLifetime(p)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.close()
	ln, err := owner.bind()
	if err != nil {
		t.Fatal(err)
	}
	_, err = acquireLifetime(p)
	var held *alreadyRunningError
	if !errors.As(err, &held) || held.PID != os.Getpid() {
		t.Fatalf("second owner = %v", err)
	}
	conn, err := net.Dial("unix", p.socket)
	if err != nil {
		t.Fatalf("second acquire touched first socket: %v", err)
	}
	conn.Close()
	fi, err := os.Stat(p.socket)
	if err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("socket permissions: %v %v", fi, err)
	}
	if err := owner.close(); err != nil {
		t.Fatal(err)
	}
	if _, err := ln.Accept(); err == nil {
		t.Fatal("closed listener accepted")
	}
	for _, path := range []string{p.socket, p.pid} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("not cleaned %s: %v", path, err)
		}
	}
	next, err := acquireLifetime(p)
	if err != nil {
		t.Fatalf("flock not released: %v", err)
	}
	next.close()
}

func TestSingletonReclaimsStaleSocketButPreservesReplacement(t *testing.T) {
	p := socketPaths(t)
	stale, err := net.ListenUnix("unix", &net.UnixAddr{Name: p.socket, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	stale.SetUnlinkOnClose(false)
	stale.Close()
	owner, err := acquireLifetime(p)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.bind(); err != nil {
		t.Fatalf("stale socket not reclaimed: %v", err)
	}
	// Replace the rendezvous path after this generation has bound.
	if err := os.Remove(p.socket); err != nil {
		t.Fatal(err)
	}
	next, err := net.ListenUnix("unix", &net.UnixAddr{Name: p.socket, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close()
	if err := owner.close(); err != nil {
		t.Fatal(err)
	}
	c, err := net.Dial("unix", p.socket)
	if err != nil {
		t.Fatalf("old owner unlinked replacement: %v", err)
	}
	c.Close()
}

func TestSingletonRefusesNonSocketAndLiveForeignEndpoint(t *testing.T) {
	for _, live := range []bool{false, true} {
		p := socketPaths(t)
		if live {
			ln, err := net.Listen("unix", p.socket)
			if err != nil {
				t.Fatal(err)
			}
			defer ln.Close()
		} else if err := os.WriteFile(p.socket, []byte("not a socket"), 0o600); err != nil {
			t.Fatal(err)
		}
		owner, err := acquireLifetime(p)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := owner.bind(); err == nil {
			t.Fatal("replaced non-stale endpoint")
		}
		owner.close()
		if _, err := os.Stat(p.socket); err != nil {
			t.Fatalf("foreign path was removed: %v", err)
		}
	}
}
