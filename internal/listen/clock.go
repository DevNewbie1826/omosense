package listen

import "time"

type timer interface{ Stop() bool }
type clock interface {
	Now() time.Time
	AfterFunc(time.Duration, func()) timer
}
type wallClock struct{}

func (wallClock) Now() time.Time                            { return time.Now() }
func (wallClock) AfterFunc(d time.Duration, f func()) timer { return time.AfterFunc(d, f) }
