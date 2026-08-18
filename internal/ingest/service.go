// Package ingest accepts call-completion webhooks and processes them.
package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
)

// recordingWork stands in for downloading and transcoding a recording.
const (
	recordingWork    = 50 * time.Millisecond
	recordingTimeout = 30 * time.Second
)

// Service ingests webhook deliveries.
type Service struct {
	store    *store.Store
	cache    *stats.Cache
	rdb      *redis.Client
	log      *slog.Logger
	bgCtx    context.Context
	bgCancel context.CancelFunc
	bg       sync.WaitGroup
	mu       sync.Mutex
	closing  bool
}

// New builds a Service.
func New(s *store.Store, c *stats.Cache, rdb *redis.Client, log *slog.Logger) *Service {
	bgCtx, bgCancel := context.WithCancel(context.Background())
	return &Service{
		store:    s,
		cache:    c,
		rdb:      rdb,
		log:      log,
		bgCtx:    bgCtx,
		bgCancel: bgCancel,
	}
}

// Stats returns the cached totals for an account.
func (s *Service) Stats(accountID string) stats.AccountStats {
	return s.cache.Get(accountID)
}

// Ingest stores a delivery and kicks off processing. Processing runs
// asynchronously so the provider gets a fast acknowledgement.
func (s *Service) Ingest(ctx context.Context, evt Event) error {
	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	rec := store.Event{
		EventID:      evt.EventID,
		CallID:       evt.CallID,
		AccountID:    evt.AccountID,
		Status:       evt.Status,
		DurationSec:  evt.DurationSec,
		RecordingURL: evt.RecordingURL,
		OccurredAt:   evt.OccurredAt,
		Payload:      payload,
	}

	// One transaction decides, atomically, whether this delivery is new and
	// applies every consequence of it. Asking the database "have I seen this?"
	// and then acting on the answer is what let concurrent redeliveries
	// through.
	first, err := s.store.RecordDelivery(ctx, rec)
	if err != nil {
		return err
	}
	if !first {
		// Not an error: the provider is allowed to redeliver, and it needs a
		// 2xx or it will keep trying.
		s.log.Info("duplicate delivery ignored", "event_id", evt.EventID)
		return nil
	}

	// Only after the transaction committed, so the in-memory view cannot count
	// a delivery that Postgres rolled back.
	s.cache.Record(rec.AccountID, rec.DurationSec)
	// Recordings are slow to fetch, so that part does not block the provider.
	if rec.RecordingURL != "" {
		s.startRecordingWork(rec)
	}

	return nil
}

// startRecordingWork runs processRecording off the request path, tracked so
// that shutdown can wait for it.
func (s *Service) startRecordingWork(rec store.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closing {
		// Shutdown is already draining. Say so loudly rather than starting
		// work nobody is waiting for.
		s.log.Warn("recording not started, service is shutting down",
			"event_id", rec.EventID, "call_id", rec.CallID)
		return
	}

	s.bg.Add(1)
	go func() {
		defer s.bg.Done()

		ctx, cancel := context.WithTimeout(s.bgCtx, recordingTimeout)
		defer cancel()

		if err := s.processRecording(ctx, rec); err != nil {
			s.log.Error("process recording",
				"event_id", rec.EventID, "call_id", rec.CallID, "err", err)
		}
	}()
}

// Shutdown stops accepting new background work and waits for what is already
// running to finish, giving up when ctx expires.
//
// Callers should shut the HTTP server down first: that way no new deliveries
// can arrive, and this only has to drain what is genuinely in flight.
func (s *Service) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.closing = true
	s.mu.Unlock()

	// bg.Wait blocks, so it is moved off this goroutine to keep the ctx
	// deadline meaningful.
	drained := make(chan struct{})
	go func() {
		s.bg.Wait()
		close(drained)
	}()

	select {
	case <-drained:
		s.bgCancel()
		return nil
	case <-ctx.Done():
		// Out of time. Cancel so the stragglers unwind rather than being
		// killed mid-statement by process exit.
		s.bgCancel()
		return ctx.Err()
	}
}

// processRecording downloads and transcodes the call recording, then marks
// the call as done.
func (s *Service) processRecording(ctx context.Context, rec store.Event) error {
	select {
	case <-time.After(recordingWork):
	case <-ctx.Done():
		return ctx.Err()
	}
	return s.store.MarkRecordingProcessed(ctx, rec.CallID)
}
