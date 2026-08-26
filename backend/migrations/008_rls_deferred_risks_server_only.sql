-- WinRah RLS deferred-risk hardening.
-- LOCAL REVIEW ONLY: do not apply to Supabase until explicitly approved.
-- Assumptions approved for this draft:
--   1) community_reports are server-only until redaction/moderation exists.
--   2) data_sources are server-only until metadata exposure is reviewed.
--   3) no Supabase Auth user model is enabled for favorites/notifications yet.

BEGIN;

-- Public clients must not read user-submitted report text directly.
REVOKE SELECT ON TABLE public.community_reports, public.data_sources
FROM PUBLIC, anon, authenticated;

DROP POLICY IF EXISTS community_reports_public_read ON public.community_reports;
DROP POLICY IF EXISTS data_sources_public_read ON public.data_sources;
DROP POLICY IF EXISTS community_reports_server_read ON public.community_reports;
DROP POLICY IF EXISTS data_sources_server_read ON public.data_sources;

-- Keep sensitive operational/user tables inaccessible to anon/authenticated.
REVOKE ALL ON TABLE public.monitor_sessions, public.gps_observations,
    public.monitor_reputation, public.favorites, public.notifications,
    public.audit_logs
FROM PUBLIC, anon, authenticated;

-- Remove any accidentally-created broad policies from prior/manual changes.
DROP POLICY IF EXISTS monitor_sessions_public_read ON public.monitor_sessions;
DROP POLICY IF EXISTS gps_observations_public_read ON public.gps_observations;
DROP POLICY IF EXISTS monitor_reputation_public_read ON public.monitor_reputation;
DROP POLICY IF EXISTS favorites_public_read ON public.favorites;
DROP POLICY IF EXISTS notifications_public_read ON public.notifications;
DROP POLICY IF EXISTS audit_logs_public_read ON public.audit_logs;
DROP POLICY IF EXISTS monitor_sessions_authenticated_read ON public.monitor_sessions;
DROP POLICY IF EXISTS gps_observations_authenticated_read ON public.gps_observations;
DROP POLICY IF EXISTS monitor_reputation_authenticated_read ON public.monitor_reputation;
DROP POLICY IF EXISTS favorites_authenticated_read ON public.favorites;
DROP POLICY IF EXISTS notifications_authenticated_read ON public.notifications;
DROP POLICY IF EXISTS audit_logs_authenticated_read ON public.audit_logs;

-- No INSERT/UPDATE/DELETE is granted to public, anon, or authenticated.
-- Existing SECURITY DEFINER write APIs remain server-controlled as hardened in 003.

COMMIT;
