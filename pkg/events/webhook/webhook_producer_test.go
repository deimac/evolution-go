package webhook_producer

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/evolution-foundation/evolution-go/pkg/config"
	logger_wrapper "github.com/evolution-foundation/evolution-go/pkg/logger"
)

func testLogger(t *testing.T) *logger_wrapper.LoggerManager {
	return logger_wrapper.NewLoggerManager(&config.Config{LogDirectory: t.TempDir()})
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("timed out")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A burst of events (an offline queue) must never hit the receiver with more
// than `workers` requests at once, and every event must be delivered.
func TestBurstIsBoundedAndDelivered(t *testing.T) {
	var inFlight, maxInFlight, done int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&inFlight, 1)
		for {
			m := atomic.LoadInt32(&maxInFlight)
			if n <= m || atomic.CompareAndSwapInt32(&maxInFlight, m, n) {
				break
			}
		}
		time.Sleep(2 * time.Millisecond)
		atomic.AddInt32(&inFlight, -1)
		atomic.AddInt32(&done, 1)
	}))
	defer srv.Close()

	p := newWebhookProducer("", testLogger(t), 3, 10)
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = p.Produce("inst.message", []byte(`{}`), srv.URL, "inst") }()
	}
	wg.Wait()
	waitFor(t, func() bool { return atomic.LoadInt32(&done) == 200 })
	if m := atomic.LoadInt32(&maxInFlight); m > 3 {
		t.Fatalf("max concurrent requests = %d, want <= 3", m)
	}
}

// 5xx is retried with the backoff until it succeeds.
func TestServerErrorIsRetriedWithBackoff(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&calls, 1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	defer srv.Close()

	p := newWebhookProducer("", testLogger(t), 1, 10)
	var mu sync.Mutex
	var waits []time.Duration
	p.backoff = func(a int) time.Duration { return time.Duration(a) * time.Second }
	p.sleep = func(d time.Duration) { mu.Lock(); waits = append(waits, d); mu.Unlock() }

	_ = p.Produce("inst.message", []byte(`{}`), srv.URL, "inst")
	waitFor(t, func() bool { return atomic.LoadInt32(&calls) == 3 })
	mu.Lock()
	defer mu.Unlock()
	if len(waits) != 2 || waits[0] != time.Second || waits[1] != 2*time.Second {
		t.Fatalf("waits = %v, want [1s 2s]", waits)
	}
}

// A definitive 4xx is not retried.
func TestClientErrorIsNotRetried(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	p := newWebhookProducer("", testLogger(t), 1, 10)
	p.sleep = func(time.Duration) { t.Error("must not wait for a retry") }
	_ = p.Produce("inst.message", []byte(`{}`), srv.URL, "inst")
	_ = p.Produce("inst.message", []byte(`{}`), srv.URL, "inst")
	waitFor(t, func() bool { return atomic.LoadInt32(&calls) == 2 })
	time.Sleep(20 * time.Millisecond)
	if c := atomic.LoadInt32(&calls); c != 2 {
		t.Fatalf("calls = %d, want 2", c)
	}
}

func TestBackoffGrowsAndIsCapped(t *testing.T) {
	for a, want := range map[int]time.Duration{1: 2 * time.Second, 2: 4 * time.Second, 5: 32 * time.Second, 20: maxBackoff} {
		got := expBackoff(a)
		if got < want || got > want+want/5 {
			t.Fatalf("expBackoff(%d) = %s, want %s..+20%%", a, got, want)
		}
	}
}

func TestQueueNameWithoutEventIsIgnored(t *testing.T) {
	p := newWebhookProducer("", testLogger(t), 1, 1)
	if err := p.Produce("noevent", []byte(`{}`), "http://unused", "inst"); err != nil || len(p.jobs) != 0 {
		t.Fatalf("err=%v queued=%d", err, len(p.jobs))
	}
}
