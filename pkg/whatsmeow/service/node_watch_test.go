package whatsmeow_service

import (
	"testing"
	"time"

	waLog "go.mau.fi/whatsmeow/util/log"
)

func TestNodeWatchLoggerReportsUnhandledNodes(t *testing.T) {
	var got []string
	l := &nodeWatchLogger{Logger: waLog.Noop, onUnhandled: func(tag string) { got = append(got, tag) }}

	l.Debugf("Didn't handle WhatsApp node %s", "status")
	l.Debugf("Didn't handle WhatsApp node %s", "status")
	l.Debugf("Handling %s", "message")
	l.Infof("Didn't handle WhatsApp node %s", "ignored-level")

	if len(got) != 2 || got[0] != "status" || got[1] != "status" {
		t.Fatalf("got %v, want [status status]", got)
	}
}

func TestNodeWatchLoggerWithoutCallbackStillLogs(t *testing.T) {
	l := &nodeWatchLogger{Logger: waLog.Noop}
	l.Debugf("Didn't handle WhatsApp node %s", "status") // must not panic
}

func TestNodeThrottleAllowsOncePerWindowPerTag(t *testing.T) {
	th := newNodeThrottle(time.Hour)
	t0 := time.Date(2026, 11, 8, 0, 0, 0, 0, time.UTC)

	if !th.allow("status", t0) {
		t.Fatal("first status must pass")
	}
	if th.allow("status", t0.Add(30*time.Minute)) {
		t.Fatal("second status within the hour must be throttled")
	}
	if !th.allow("other", t0.Add(time.Minute)) {
		t.Fatal("another tag is independent")
	}
	if !th.allow("status", t0.Add(61*time.Minute)) {
		t.Fatal("status after the window must pass again")
	}
}
