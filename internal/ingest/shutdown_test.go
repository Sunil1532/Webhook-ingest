package ingest_test

import (
	"context"
	"testing"
	"time"

	"github.com/convin/webhook-ingest/internal/ingest"
	"github.com/convin/webhook-ingest/internal/testutil"
)

// newEvent builds a well-formed event for tests that drive the service
// directly rather than over HTTP.
func newEvent(eventID, callID, accountID string) ingest.Event {
	return ingest.Event{
		EventID:      eventID,
		CallID:       callID,
		AccountID:    accountID,
		Status:       "completed",
		DurationSec:  143,
		RecordingURL: "https://recordings.example.com/" + callID + ".wav",
		OccurredAt:   time.Date(2026, 8, 13, 9, 12, 0, 0, time.UTC),
	}
}

// TestShutdownWaitsForInFlightRecordingWork covers the deploy symptom: work
// that had been accepted but not finished vanished when the process exited.
//
// Shutdown must return only once the recording job has finished, so the
// assertion needs no polling -- if it had to poll, shutdown would not be
// waiting.
func TestShutdownWaitsForInFlightRecordingWork(t *testing.T) {
	svc, st := testutil.NewService(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	if err := svc.Ingest(ctx, newEvent(eventID, callID, accountID)); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := svc.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	var processed bool
	row := st.Pool().QueryRow(ctx,
		`SELECT recording_processed FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&processed); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !processed {
		t.Fatal("shutdown returned before the in-flight recording was processed")
	}
}

// TestShutdownIsPromptWhenIdle guards against the opposite mistake: a drain
// that blocks when there is nothing to drain would stall every deploy.
func TestShutdownIsPromptWhenIdle(t *testing.T) {
	svc, _ := testutil.NewService(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- svc.Shutdown(ctx) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown blocked with no work in flight")
	}
}