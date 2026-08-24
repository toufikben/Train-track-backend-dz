-- Emergency rollback for 003_rls_least_privilege.sql.
-- Do not run automatically: this restores broad direct client access and must
-- only be used after confirming the replacement policies are ready.
BEGIN;

DROP POLICY IF EXISTS countries_public_read ON public.countries;
DROP POLICY IF EXISTS regions_public_read ON public.regions;
DROP POLICY IF EXISTS railway_networks_public_read ON public.railway_networks;
DROP POLICY IF EXISTS railway_lines_public_read ON public.railway_lines;
DROP POLICY IF EXISTS stations_public_read ON public.stations;
DROP POLICY IF EXISTS station_aliases_public_read ON public.station_aliases;
DROP POLICY IF EXISTS trains_public_read ON public.trains;
DROP POLICY IF EXISTS trips_public_read ON public.trips;
DROP POLICY IF EXISTS trip_stops_public_read ON public.trip_stops;
DROP POLICY IF EXISTS railway_segments_public_read ON public.railway_segments;
DROP POLICY IF EXISTS aggregated_positions_public_read ON public.aggregated_train_positions;
DROP POLICY IF EXISTS station_events_public_read ON public.station_events;
DROP POLICY IF EXISTS community_reports_public_read ON public.community_reports;
DROP POLICY IF EXISTS data_sources_public_read ON public.data_sources;

ALTER TABLE public.countries DISABLE ROW LEVEL SECURITY;
ALTER TABLE public.regions DISABLE ROW LEVEL SECURITY;
ALTER TABLE public.railway_networks DISABLE ROW LEVEL SECURITY;
ALTER TABLE public.railway_lines DISABLE ROW LEVEL SECURITY;
ALTER TABLE public.stations DISABLE ROW LEVEL SECURITY;
ALTER TABLE public.station_aliases DISABLE ROW LEVEL SECURITY;
ALTER TABLE public.trains DISABLE ROW LEVEL SECURITY;
ALTER TABLE public.trips DISABLE ROW LEVEL SECURITY;
ALTER TABLE public.trip_stops DISABLE ROW LEVEL SECURITY;
ALTER TABLE public.railway_segments DISABLE ROW LEVEL SECURITY;
ALTER TABLE public.monitor_sessions DISABLE ROW LEVEL SECURITY;
ALTER TABLE public.gps_observations DISABLE ROW LEVEL SECURITY;
ALTER TABLE public.aggregated_train_positions DISABLE ROW LEVEL SECURITY;
ALTER TABLE public.station_events DISABLE ROW LEVEL SECURITY;
ALTER TABLE public.community_reports DISABLE ROW LEVEL SECURITY;
ALTER TABLE public.monitor_reputation DISABLE ROW LEVEL SECURITY;
ALTER TABLE public.favorites DISABLE ROW LEVEL SECURITY;
ALTER TABLE public.notifications DISABLE ROW LEVEL SECURITY;
ALTER TABLE public.data_sources DISABLE ROW LEVEL SECURITY;
ALTER TABLE public.audit_logs DISABLE ROW LEVEL SECURITY;

COMMIT;
