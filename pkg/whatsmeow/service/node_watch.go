package whatsmeow_service

import (
	"sync"
	"time"

	waLog "go.mau.fi/whatsmeow/util/log"
)

// unhandledNodeMsg is the debug line whatsmeow writes for a node tag it has no
// handler for (client.go). Such a node is not acked, so when WhatsApp starts
// sending a new tag the offline queue can stop behind it: in 2026-11 the new
// <status> nodes held an instance's messages for two days with no error logged.
const unhandledNodeMsg = "Didn't handle WhatsApp node %s"

// nodeWatchLogger wraps the whatsmeow client logger and reports unhandled
// nodes, whatever the configured log level.
type nodeWatchLogger struct {
	waLog.Logger
	onUnhandled func(tag string)
}

func (l *nodeWatchLogger) Debugf(msg string, args ...any) {
	if msg == unhandledNodeMsg && len(args) == 1 && l.onUnhandled != nil {
		if tag, ok := args[0].(string); ok {
			l.onUnhandled(tag)
		}
	}
	l.Logger.Debugf(msg, args...)
}

// nodeThrottle lets each tag through once per window, so a flood of unhandled
// nodes becomes one report per tag.
type nodeThrottle struct {
	mu     sync.Mutex
	window time.Duration
	last   map[string]time.Time
}

func newNodeThrottle(window time.Duration) *nodeThrottle {
	return &nodeThrottle{window: window, last: make(map[string]time.Time)}
}

func (t *nodeThrottle) allow(tag string, now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if last, ok := t.last[tag]; ok && now.Sub(last) < t.window {
		return false
	}
	t.last[tag] = now
	return true
}
