// Command lockprobe is a test helper that behaves like a compat host: it
// loads a profile, takes a lock through Ctx.Acquire, prints "acquired", and
// holds the lock until stdin closes or a terminating signal arrives.
package main

import (
	"fmt"
	"io"
	"os"

	"github.com/DevNewbie1826/omosense/internal/core"
)

func main() {
	pa := core.ParseArgs(os.Args[1:])
	ctx, err := core.Load(pa, true)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	name := os.Getenv("PROBE_LOCK")
	if name == "" {
		fmt.Println("acquired")
		return
	}
	release := ctx.Acquire(name, os.Getenv("PROBE_LEGACY"))
	fmt.Println("acquired")
	io.Copy(io.Discard, os.Stdin)
	release()
}
