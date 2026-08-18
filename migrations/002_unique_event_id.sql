-- Idempotent ingestion needs the database to enforce "one row per event_id".
-- The original schema only had a non-unique index, so nothing stopped two
-- concurrent redeliveries of the same event from both inserting.

-- Clear out the duplicates the check-then-insert race already let in, keeping
-- the earliest copy of each event_id. On a fresh volume this is a no-op; on the
-- production database it is the reason the index below can be created.
DELETE FROM events a
USING events b
WHERE a.event_id = b.event_id
  AND a.id > b.id;

-- The unique index subsumes the old lookup index, so drop that rather than
-- maintaining two indexes over the same column.
DROP INDEX IF EXISTS idx_events_event_id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_events_event_id_unique ON events (event_id);