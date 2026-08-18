# SOLUTION

## What was broken, and why

**Duplicate rows and inflated counts.** `Ingest` called `EventExists` and then
`InsertEvent` — two separate trips to the database with nothing in between to
stop a second delivery. An at-least-once provider retries in parallel, so
concurrent copies of one `event_id` all read "not seen" and all inserted and
incremented. The schema made it worse: a plain index on `events.event_id`, not
a unique one, so the database did not stop them either.

**The three writes were not atomic.** `InsertEvent`, `UpsertCall` and
`IncrementAccountStats` ran as independent statements. If the first succeeded
and a later one failed, the handler returned 500, the provider retried, and
`EventExists` said "already seen" — so the retry was skipped. The event existed
but was never counted, permanently. This was not in the incident report, and I
think it is worse than the double-counting that was.

**The stats cache had no write lock.** `Get` took a read lock; `Record`, which
mutates the map and the counters, held nothing. Eight concurrent writers doing
500 increments each recorded 3374 of 4000 — and writing the map from two
goroutines can kill the process outright.

**Recording work inherited a dead context, and failed silently.** The goroutine
captured `r.Context()`, which Go cancels the moment the handler returns, so the
`UPDATE` fifty milliseconds later always failed with `context.Canceled`. The
error was then discarded by an empty `if err != nil { // TODO }` body — which is
why there was nothing in the logs. The silence was the more dangerous half.

**In-flight work died on deploy.** The recording goroutines were unowned.
`srv.Shutdown` drains HTTP connections and nothing else, so once it returned
`main` fell off the end and the process exited on top of anything still running.

## The fixes

Uniqueness now lives in the schema (migration 002, which also clears the
duplicates already present). `store.RecordDelivery` inserts with `ON CONFLICT
DO NOTHING` and uses `RowsAffected` to report whether this delivery was the
first, with all three writes in one transaction. `Cache.Record` takes the write
lock. Background work derives from a service-lifetime context with its own
timeout and logs its failures. `Service` tracks background jobs in a
`WaitGroup` and exposes `Shutdown`, which `main` calls after the HTTP server is
down.

Each has a test that fails on the original code and passes after:
`TestCacheRecordIsSafeForConcurrentUse`, `TestRecordingIsMarkedProcessed`,
`TestConcurrentDuplicateDeliveriesCountOnce`,
`TestShutdownWaitsForInFlightRecordingWork`.

## Why Postgres for deduplication

The marker that says "I have seen this event" and the data it protects have to
move together, so I put the marker in the same place as the data and let one
transaction decide both. A unique index on `events.event_id` makes Postgres the
arbiter: two racing deliveries cannot both win, because under READ COMMITTED
the second blocks on the first's row lock and then sees zero rows affected.

I considered Redis `SETNX` and rejected it as the authority. The marker and the
write would live in two systems with no shared transaction, and there is no safe
order. Set the marker first and crash before the commit, and the event is
suppressed forever — a call silently lost, worse than the bug I am fixing.
Commit first and crash before the marker, and the retry double-counts, which is
the bug I am fixing. A TTL bounds how long you are exposed but does not remove
the window.

An in-process set of seen IDs was also rejected: it dies with the process and is
wrong the moment there is a second replica.

## At 10,000 webhooks/second

The transaction-per-webhook shape is the first thing to go.

- **Accept and queue.** The handler validates, appends to a durable log, and
  returns 200. Ingestion becomes consumers reading a partitioned stream, which
  decouples provider latency from database throughput.
- **Redis in front of Postgres.** A `SETNX` with a TTL covering the retry window
  turns the common redelivery into one memory hit instead of a transaction.
  Postgres stays the authority; Redis only says "definitely a duplicate" or
  "ask the database".
- **Stop incrementing `account_stats` per event.** At this rate a busy account
  is one contended row. Aggregate over a short window and apply one increment
  per account per window.

## Not done

`account_stats` still carries the inflation from before the fix — migration 002
removes the duplicate `events` rows but not the aggregate, because that is a
backfill and wants to run once, supervised, against production. I also left the
in-memory stats cache unseeded at startup: `GET /accounts/{id}/stats` reads only
the cache, so it reports zero after a restart until new webhooks arrive. The fix
is small, but it is not one of the reported symptoms and I would rather submit
fixes I can defend.