-- Index trip stops for ordered per-trip lookups during PostgreSQL hydration.
-- Safe to run repeatedly; no public API contract changes.
BEGIN;

CREATE INDEX IF NOT EXISTS idx_trip_stops_trip_id_sequence
    ON public.trip_stops (trip_id, sequence);

COMMIT;
