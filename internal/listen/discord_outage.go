package listen

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/DevNewbie1826/omosense/internal/core"
	"github.com/gorilla/websocket"
)

type outage struct {
	mu         sync.Mutex
	clock      clock
	sink       core.Sink
	bot        string
	code       int
	downSince  time.Time
	everReady  bool
	alerted    bool
	timer      timer
	generation uint64
	stopped    bool
}

// tag names the bot every LOG line belongs to: the SDK logger is shared by
// every session in the process, so an unattributed line is ambiguous.
func (o *outage) tag() string {
	return "discord " + o.bot
}

func (o *outage) connect() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.code = 0
}

func (o *outage) capture(format string, args ...any) {
	o.mu.Lock()
	defer o.mu.Unlock()
	// discordgo v0.29 wsapi.go:609 and :620 exact informational formats.
	// Op9 ("sending identify packet to gateway in response to Op9") is
	// deliberately ignored: its re-identify stays on the same connection.
	if format == "Closing and reconnecting in response to Op7" {
		o.code = 1005
	}
	for _, arg := range args {
		if err, ok := arg.(error); ok {
			var closeErr *websocket.CloseError
			if errors.As(err, &closeErr) {
				o.code = closeErr.Code
			}
		}
	}
}

func (o *outage) disconnect() {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.stopped || !o.downSince.IsZero() {
		return
	}
	o.downSince = o.clock.Now()
	code := o.code
	if code == 0 {
		code = 1006
	}
	o.generation++
	generation := o.generation
	o.timer = o.clock.AfterFunc(3*time.Minute, func() {
		o.mu.Lock()
		defer o.mu.Unlock()
		if o.stopped || generation != o.generation {
			return
		}
		o.alerted = true
		o.sink.Log(fmt.Sprintf("%s down 3m+ (last close %d), still reconnecting", o.tag(), code))
	})
}

func (o *outage) ready(username string, resumed bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.stopped {
		return
	}
	o.cancelTimer()
	if !o.everReady {
		if !resumed {
			o.sink.Log(o.tag() + " ready as " + username)
		}
	} else if o.alerted {
		minutes := int(math.Floor(o.clock.Now().Sub(o.downSince).Minutes() + 0.5))
		o.sink.Log(fmt.Sprintf("%s recovered after %dm down", o.tag(), minutes))
	}
	o.everReady = true
	o.downSince = time.Time{}
	o.alerted = false
}

func (o *outage) cancelTimer() {
	o.generation++
	if o.timer != nil {
		o.timer.Stop()
		o.timer = nil
	}
}

func (o *outage) stop() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.stopped = true
	o.cancelTimer()
}
