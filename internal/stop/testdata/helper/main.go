// Command helper is the stop test's stand-in for a host process: it sleeps
// until a terminating signal and can be told to remove a lock file on the
// way out (the way a real host releases its own locks) or to ignore SIGTERM
// so a stop attempt times out.
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	ignore := flag.Bool("ignore-sigterm", false, "ignore SIGTERM and keep running")
	lock := flag.String("lock", "", "lock file to remove on SIGTERM")
	flag.Parse()

	if *ignore {
		// A process that never leaves, so a stop attempt must time out and
		// report its pid.
		signal.Ignore(syscall.SIGTERM, syscall.SIGINT)
		fmt.Println("ready")
		for {
			time.Sleep(time.Hour)
		}
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	fmt.Println("ready")
	<-sig
	if *lock != "" {
		_ = os.Remove(*lock)
	}
}
