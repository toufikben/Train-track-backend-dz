-- Migration 012: public corridor monitor sessions
-- A public session may be bound to a line and direction without a specific trip/train.
-- Existing train-bound sessions remain valid and unchanged.

ALTER TABLE public.monitor_sessions
    ADD COLUMN IF NOT EXISTS line_id TEXT,
    ADD COLUMN IF NOT EXISTS direction TEXT;

ALTER TABLE public.gps_observations
    ADD COLUMN IF NOT EXISTS line_id TEXT,
    ADD COLUMN IF NOT EXISTS direction TEXT;

CREATE INDEX IF NOT EXISTS idx_monitor_sessions_line_direction_status
    ON public.monitor_sessions(line_id, direction, status);

CREATE INDEX IF NOT EXISTS idx_gps_observations_session_line_direction
    ON public.gps_observations(session_id, line_id, direction, observed_at DESC);

ALTER TABLE public.monitor_sessions
    DROP CONSTRAINT IF EXISTS monitor_sessions_direction_check;

ALTER TABLE public.monitor_sessions
    ADD CONSTRAINT monitor_sessions_direction_check
    CHECK (direction IS NULL OR direction IN ('INBOUND', 'OUTBOUND', 'BOTH'));

ALTER TABLE public.gps_observations
    DROP CONSTRAINT IF EXISTS gps_observations_direction_check;

ALTER TABLE public.gps_observations
    ADD CONSTRAINT gps_observations_direction_check
    CHECK (direction IS NULL OR direction IN ('INBOUND', 'OUTBOUND', 'BOTH'));

COMMENT ON COLUMN public.monitor_sessions.line_id IS
    'UI/backend corridor identifier; required for public corridor sessions.';
COMMENT ON COLUMN public.monitor_sessions.direction IS
    'INBOUND, OUTBOUND, or BOTH; public sessions may omit trip_id/train_id.';
COMMENT ON COLUMN public.gps_observations.line_id IS
    'Corridor identifier copied from the validated monitor session.';
COMMENT ON COLUMN public.gps_observations.direction IS
    'Direction copied from the validated monitor session.';

-- Backfill legacy train-bound sessions where the canonical trip row has line/direction.
UPDATE public.monitor_sessions AS ms
SET line_id = t.line_id,
    direction = t.direction
FROM public.trips AS t
WHERE ms.trip_id = t.id
  AND (ms.line_id IS NULL OR ms.direction IS NULL);

-- No public grants are changed by this migration. Writes remain server-controlled.
