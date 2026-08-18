package ingest_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/convin/webhook-ingest/internal/store"
	"github.com/convin/webhook-ingest/internal/testutil"
)

// eventJSON builds a well-formed call-completion payload.
func eventJSON(eventID, callID, accountID string) string {
	return fmt.Sprintf(`{
	  "event_id":      %q,
	  "call_id":       %q,
	  "account_id":    %q,
	  "status":        "completed",
	  "duration_sec":  143,
	  "recording_url": "https://recordings.example.com/%s.wav",
	  "occurred_at":   "2026-08-13T09:12:00Z"
	}`, eventID, callID, accountID, callID)
}

func post(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestWebhookStoresEventAndCall(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	exists, err := st.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if !exists {
		t.Fatal("expected the event to be stored")
	}

	var gotAccount string
	row := st.Pool().QueryRow(ctx, `SELECT account_id FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&gotAccount); err != nil {
		t.Fatalf("expected a call record for %s: %v", callID, err)
	}
	if gotAccount != accountID {
		t.Fatalf("call belongs to %q, want %q", gotAccount, accountID)
	}
}

func TestDuplicateDeliveryIsIgnored(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	for i := 0; i < 3; i++ {
		if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
			t.Fatalf("delivery %d: got %d, want 200", i, resp.StatusCode)
		}
	}

	var n int
	row := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&n); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if n != 1 {
		t.Fatalf("stored %d copies of %s, want 1", n, eventID)
	}
}

// waitForRecordingProcessed polls until the call is flagged processed, or the
// deadline passes. Processing is asynchronous, so polling is the honest way to
// assert on it; the timeout is generous relative to the 50ms of simulated work.
func waitForRecordingProcessed(t *testing.T, st *store.Store, callID string) bool {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(3 * time.Second)

	for time.Now().Before(deadline) {
		var processed bool
		row := st.Pool().QueryRow(ctx,
			`SELECT recording_processed FROM calls WHERE call_id = $1`, callID)
		if err := row.Scan(&processed); err == nil && processed {
			return true
		}
		time.Sleep(25 * time.Millisecond)
	}
	return false
}

// TestRecordingIsMarkedProcessed covers the ops report that recordings never
// get marked processed and nothing shows up in the logs.
func TestRecordingIsMarkedProcessed(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)

	body := eventJSON(eventID, callID, accountID)
	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	if !waitForRecordingProcessed(t, st, callID) {
		t.Fatalf("recording for %s was never marked processed", callID)
	}
}

// TestConcurrentDuplicateDeliveriesCountOnce reproduces the ops report
// directly: duplicate rows in the dashboard and account counts drifting high.
//
// The provider delivers at least once and retries aggressively, so redeliveries
// of one event_id arrive *in parallel*, not neatly one after another. Ingest
// asks EventExists and then inserts as two separate statements, with only a
// non-unique index on events.event_id -- so every racing delivery sees
// "absent" and every one of them inserts and increments.
//
// The window between the check and the insert is sub-millisecond, so the test
// gives each caller its own already-connected client before the starting gun:
// otherwise TCP setup staggers the deliveries by more than the window and the
// race never opens. Several rounds run so one lucky round cannot hide the bug.
func TestConcurrentDuplicateDeliveriesCountOnce(t *testing.T) {
	const (
		deliveries = 16
		rounds     = 5
	)

	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	// One client per caller, each holding its own warm connection. Sharing a
	// client would serialise them behind a small idle-connection pool.
	clients := make([]*http.Client, deliveries)
	for i := range clients {
		c := &http.Client{Transport: &http.Transport{}}
		clients[i] = c
		t.Cleanup(c.CloseIdleConnections)

		resp, err := c.Get(srv.URL + "/healthz")
		if err != nil {
			t.Fatalf("warm up client %d: %v", i, err)
		}
		_ = resp.Body.Close()
	}

	for r := 0; r < rounds; r++ {
		roundEventID := fmt.Sprintf("%s_r%d", eventID, r)
		roundCallID := fmt.Sprintf("%s_r%d", callID, r)
		body := eventJSON(roundEventID, roundCallID, accountID)

		var start, done sync.WaitGroup
		start.Add(1)
		done.Add(deliveries)

		codes := make([]int, deliveries)
		for i := 0; i < deliveries; i++ {
			go func(i int) {
				defer done.Done()
				start.Wait() // fire every redelivery at once
				resp, err := clients[i].Post(srv.URL+"/webhooks/calls",
					"application/json", strings.NewReader(body))
				if err != nil {
					codes[i] = -1
					return
				}
				defer func() { _ = resp.Body.Close() }()
				codes[i] = resp.StatusCode
			}(i)
		}
		start.Done()
		done.Wait()

		for i, code := range codes {
			if code != http.StatusOK {
				t.Fatalf("round %d delivery %d returned %d, want 200 (a non-2xx makes the provider retry forever)", r, i, code)
			}
		}

		var rows int
		if err := st.Pool().QueryRow(ctx,
			`SELECT count(*) FROM events WHERE event_id = $1`, roundEventID).Scan(&rows); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if rows != 1 {
			t.Fatalf("round %d stored %d copies of %s, want 1", r, rows, roundEventID)
		}
	}

	// One increment per round, not one per delivery.
	got, err := st.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("AccountStats: %v", err)
	}
	if got.CallCount != rounds || got.TotalDurationSec != int64(rounds*143) {
		t.Fatalf("durable stats = %+v, want CallCount=%d TotalDurationSec=%d",
			got, rounds, rounds*143)
	}
}