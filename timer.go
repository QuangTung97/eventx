package eventx

import "time"

type simpleTimer struct {
	duration time.Duration
	timer    *time.Timer
}

var _ Timer = simpleTimer{}

func newTimer(d time.Duration) simpleTimer {
	return simpleTimer{
		duration: d,
		timer:    time.NewTimer(d),
	}
}

// Reset resets the timer
func (t simpleTimer) Reset() {
	if !t.timer.Stop() {
		<-t.timer.C
	}
	t.timer.Reset(t.duration)
}

// ResetAfterChan resets the timer right after recv from channel
func (t simpleTimer) ResetAfterChan() {
	t.timer.Reset(t.duration)
}

// Chan returns the timer channel
func (t simpleTimer) Chan() <-chan time.Time {
	return t.timer.C
}
