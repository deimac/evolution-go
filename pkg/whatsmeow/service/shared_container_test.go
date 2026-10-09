package whatsmeow_service

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/evolution-foundation/evolution-go/pkg/config"
)

// Every StartClient must reuse one container: creating one per (re)connection
// leaked a database pool each time.
func TestGetSharedContainerReusesSingleContainer(t *testing.T) {
	sharedContainerMu.Lock()
	sharedContainer = nil
	sharedContainerMu.Unlock()
	t.Cleanup(func() {
		sharedContainerMu.Lock()
		sharedContainer = nil
		sharedContainerMu.Unlock()
	})

	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "dbdata"), 0o755); err != nil {
		t.Fatal(err)
	}
	w := whatsmeowService{config: &config.Config{}, exPath: dir}

	first, err := w.getSharedContainer()
	if err != nil {
		t.Fatalf("first call: %v", err)
	}

	const calls = 50
	got := make([]any, calls)
	var wg sync.WaitGroup
	for i := 0; i < calls; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, err := w.getSharedContainer()
			if err != nil {
				t.Errorf("call %d: %v", i, err)
				return
			}
			got[i] = c
		}(i)
	}
	wg.Wait()

	for i, c := range got {
		if c != any(first) {
			t.Fatalf("call %d returned a different container", i)
		}
	}
}

// A failed creation must not be cached, so a later StartClient can retry.
func TestGetSharedContainerDoesNotCacheFailure(t *testing.T) {
	sharedContainerMu.Lock()
	sharedContainer = nil
	sharedContainerMu.Unlock()
	t.Cleanup(func() {
		sharedContainerMu.Lock()
		sharedContainer = nil
		sharedContainerMu.Unlock()
	})

	dir := t.TempDir() // no dbdata directory yet: opening the SQLite file fails
	w := whatsmeowService{config: &config.Config{}, exPath: dir}

	if _, err := w.getSharedContainer(); err == nil {
		t.Fatal("expected an error without the dbdata directory")
	}
	if err := os.MkdirAll(filepath.Join(dir, "dbdata"), 0o755); err != nil {
		t.Fatal(err)
	}
	if c, err := w.getSharedContainer(); err != nil || c == nil {
		t.Fatalf("retry after failure: container=%v err=%v", c, err)
	}
}
