-- Migration 011: bind community reports to monitor sessions when provided.
-- Backward compatible: legacy reports may keep session_id NULL.

ALTER TABLE public.community_reports
    ADD COLUMN IF NOT EXISTS session_id UUID;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'community_reports_session_id_fkey'
          AND conrelid = 'public.community_reports'::regclass
    ) THEN
        ALTER TABLE public.community_reports
            ADD CONSTRAINT community_reports_session_id_fkey
            FOREIGN KEY (session_id)
            REFERENCES public.monitor_sessions(id)
            ON DELETE SET NULL;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_community_reports_session
    ON public.community_reports(session_id);

CREATE OR REPLACE FUNCTION public.validate_community_report_session()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    session_trip_id UUID;
    session_train_id UUID;
    session_status TEXT;
BEGIN
    IF NEW.session_id IS NULL THEN
        RETURN NEW;
    END IF;

    SELECT trip_id, train_id, status
      INTO session_trip_id, session_train_id, session_status
      FROM public.monitor_sessions
     WHERE id = NEW.session_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'session_not_found'
            USING ERRCODE = '23503';
    END IF;

    IF session_status = 'ENDED'
       OR session_trip_id IS DISTINCT FROM NEW.trip_id
       OR session_train_id IS DISTINCT FROM NEW.train_id THEN
        RAISE EXCEPTION 'session_binding_mismatch'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS community_reports_session_binding ON public.community_reports;

CREATE TRIGGER community_reports_session_binding
BEFORE INSERT OR UPDATE OF session_id, trip_id, train_id
ON public.community_reports
FOR EACH ROW
EXECUTE FUNCTION public.validate_community_report_session();

COMMENT ON COLUMN public.community_reports.session_id IS
    'Optional monitor session that produced this report; when present it must match trip_id and train_id.';
COMMENT ON FUNCTION public.validate_community_report_session() IS
    'Rejects reports whose session_id is missing, ended, or bound to another trip/train.';

-- Rollback:
-- DROP TRIGGER IF EXISTS community_reports_session_binding ON public.community_reports;
-- DROP FUNCTION IF EXISTS public.validate_community_report_session();
-- DROP INDEX IF EXISTS public.idx_community_reports_session;
-- ALTER TABLE public.community_reports DROP CONSTRAINT IF EXISTS community_reports_session_id_fkey;
-- ALTER TABLE public.community_reports DROP COLUMN IF EXISTS session_id;

