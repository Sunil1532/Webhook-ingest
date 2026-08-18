package stats_test

import (
	"sync"
	"testing"

	"github.com/convin/webhook-ingest/internal/stats"
)

func TestCacheRecordAccumulates(t *testing.T) {
	c := stats.NewCache()

	c.Record("acc_1", 30)
	c.Record("acc_1", 12)
	c.Record("acc_2", 5)

	got := c.Get("acc_1")
	if got.CallCount != 2 || got.TotalDurationSec != 42 {
		t.Fatalf("acc_1: got %+v, want CallCount=2 TotalDurationSec=42", got)
	}

	other := c.Get("acc_2")
	if other.CallCount != 1 || other.TotalDurationSec != 5 {
		t.Fatalf("acc_2: got %+v, want CallCount=1 TotalDurationSec=5", other)
	}
}

func TestCacheGetUnknownAccountIsZero(t *testing.T) {
	c := stats.NewCache()
	if got := c.Get("nobody"); got.CallCount != 0 || got.TotalDurationSec != 0 {
		t.Fatalf("got %+v, want zero value", got)
	}
}

// TestCacheRecordIsSafeForConcurrentUse pins down the behaviour the service
// actually relies on: Record is called from every in-flight webhook handler at
// once, so it must be safe under concurrency and must not lose increments.
func TestCacheRecordIsSafeForConcurrentUse(t *testing.T) {
	const (
		writers   = 8
		perWriter = 500
		duration  = 3
	)

	c := stats.NewCache()

	var start, done sync.WaitGroup
	start.Add(1)
	done.Add(writers)

	for i := 0; i < writers; i++ {
		go func() {
			defer done.Done()
			start.Wait() // release every writer at the same moment
			for j := 0; j < perWriter; j++ {
				c.Record("acc_concurrent", duration)
			}
		}()
	}

	start.Done()
	done.Wait()

	got := c.Get("acc_concurrent")
	wantCalls := int64(writers * perWriter)
	if got.CallCount != wantCalls {
		t.Fatalf("CallCount = %d, want %d (increments were lost)", got.CallCount, wantCalls)
	}
	if want := wantCalls * duration; got.TotalDurationSec != want {
		t.Fatalf("TotalDurationSec = %d, want %d", got.TotalDurationSec, want)
	}
}