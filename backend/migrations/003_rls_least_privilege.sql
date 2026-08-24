-- Security migration for qitari-dz.
-- Enable RLS on every exposed public table and keep only the minimum
-- anonymous/authenticated access required for reference and public read APIs.
-- Backend connections using the database owner/service role are unaffected.
BEGIN;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'anon') THEN
        CREATE ROLE anon NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'authenticated') THEN
        CREATE ROLE authenticated NOLOGIN;
    END IF;
END
$$;

GRANT USAGE ON SCHEMA public TO anon, authenticated;

ALTER TABLE public.countries ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.regions ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.railway_networks ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.railway_lines ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.stations ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.station_aliases ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.trains ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.trips ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.trip_stops ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.railway_segments ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.monitor_sessions ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.gps_observations ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.aggregated_train_positions ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.station_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.community_reports ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.monitor_reputation ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.favorites ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.notifications ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.data_sources ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.audit_logs ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON TABLE public.countries, public.regions,
    public.railway_networks, public.railway_lines, public.stations,
    public.station_aliases, public.trains, public.trips, public.trip_stops,
    public.railway_segments, public.monitor_sessions, public.gps_observations,
    public.aggregated_train_positions, public.station_events,
    public.community_reports, public.monitor_reputation, public.favorites,
    public.notifications, public.data_sources, public.audit_logs
FROM PUBLIC, anon, authenticated;

GRANT SELECT ON TABLE public.countries, public.regions, public.railway_networks,
    public.railway_lines, public.stations, public.station_aliases, public.trains,
    public.trips, public.trip_stops, public.railway_segments,
    public.aggregated_train_positions, public.station_events,
    public.community_reports, public.data_sources
TO anon, authenticated;

DO $$
DECLARE
    table_name text;
BEGIN
    FOR table_name IN SELECT unnest(ARRAY[
        'countries', 'regions', 'railway_networks',
        'railway_lines', 'stations', 'station_aliases', 'trains', 'trips',
        'trip_stops', 'railway_segments', 'monitor_sessions',
        'gps_observations', 'aggregated_train_positions', 'station_events',
        'community_reports', 'monitor_reputation', 'favorites', 'notifications',
        'data_sources', 'audit_logs'
    ])
    LOOP
        EXECUTE format('DROP POLICY IF EXISTS %I_public_read ON public.%I', table_name, table_name);
    END LOOP;
END
$$;

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
DROP POLICY IF EXISTS monitor_reputation_public_read ON public.monitor_reputation;
DROP POLICY IF EXISTS data_sources_public_read ON public.data_sources;

CREATE POLICY countries_public_read
    ON public.countries FOR SELECT TO anon, authenticated USING (deleted_at IS NULL);
CREATE POLICY regions_public_read
    ON public.regions FOR SELECT TO anon, authenticated USING (deleted_at IS NULL);
CREATE POLICY railway_networks_public_read
    ON public.railway_networks FOR SELECT TO anon, authenticated USING (deleted_at IS NULL);
CREATE POLICY railway_lines_public_read
    ON public.railway_lines FOR SELECT TO anon, authenticated USING (deleted_at IS NULL);
CREATE POLICY stations_public_read
    ON public.stations FOR SELECT TO anon, authenticated USING (deleted_at IS NULL);
CREATE POLICY station_aliases_public_read
    ON public.station_aliases FOR SELECT TO anon, authenticated USING (true);
CREATE POLICY trains_public_read
    ON public.trains FOR SELECT TO anon, authenticated USING (deleted_at IS NULL);
CREATE POLICY trips_public_read
    ON public.trips FOR SELECT TO anon, authenticated USING (deleted_at IS NULL);
CREATE POLICY trip_stops_public_read
    ON public.trip_stops FOR SELECT TO anon, authenticated USING (true);
CREATE POLICY railway_segments_public_read
    ON public.railway_segments FOR SELECT TO anon, authenticated USING (true);
CREATE POLICY aggregated_positions_public_read
    ON public.aggregated_train_positions FOR SELECT TO anon, authenticated
    USING (truth IS NULL OR truth <> 'UNKNOWN');
CREATE POLICY station_events_public_read
    ON public.station_events FOR SELECT TO anon, authenticated USING (true);
CREATE POLICY community_reports_public_read
    ON public.community_reports FOR SELECT TO anon, authenticated USING (true);
CREATE POLICY data_sources_public_read
    ON public.data_sources FOR SELECT TO anon, authenticated USING (true);

DO $$
DECLARE
    proc text;
BEGIN
    FOR proc IN SELECT unnest(ARRAY[
        'public.api_stations()',
        'public.api_station_by_id(uuid)',
        'public.api_trains()',
        'public.api_train_by_id(uuid)',
        'public.api_train_live(uuid)',
        'public.api_trips()',
        'public.api_trip_by_id(uuid)',
        'public.api_trip_stops(uuid)',
        'public.api_trip_live(uuid)',
        'public.api_nearby_trains(double precision,double precision,double precision)',
        'public.api_reports_for_train(uuid)',
        'public.api_favorites()',
        'public.api_create_monitor_session(uuid,uuid,text,text,text)',
        'public.api_end_monitor_session(uuid)',
        'public.api_insert_observation(uuid,uuid,uuid,double precision,double precision,double precision,double precision,double precision,timestamp with time zone)',
        'public.api_insert_report(uuid,uuid,uuid,text,text)',
        'public.api_insert_station_event(uuid,uuid,uuid,text,text)',
        'public.api_insert_favorite(text,text)',
        'public.api_delete_favorite(uuid)',
        'public.sql_proxy_execute(text,jsonb)'
    ])
    LOOP
        IF to_regprocedure(proc) IS NOT NULL THEN
            EXECUTE format('ALTER FUNCTION %s SET search_path = public, pg_catalog', proc);
        END IF;
    END LOOP;
END
$$;

DO $$
DECLARE
    proc text;
BEGIN
    FOR proc IN SELECT unnest(ARRAY[
        'public.api_favorites()',
        'public.api_create_monitor_session(uuid,uuid,text,text,text)',
        'public.api_end_monitor_session(uuid)',
        'public.api_insert_observation(uuid,uuid,uuid,double precision,double precision,double precision,double precision,double precision,timestamp with time zone)',
        'public.api_insert_report(uuid,uuid,uuid,text,text)',
        'public.api_insert_station_event(uuid,uuid,uuid,text,text)',
        'public.api_insert_favorite(text,text)',
        'public.api_delete_favorite(uuid)',
        'public.sql_proxy_execute(text,jsonb)'
    ])
    LOOP
        IF to_regprocedure(proc) IS NOT NULL THEN
            EXECUTE format('REVOKE EXECUTE ON FUNCTION %s FROM PUBLIC, anon, authenticated', proc);
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'service_role') THEN
                EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO service_role', proc);
            END IF;
        END IF;
    END LOOP;
END
$$;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
    REVOKE ALL ON TABLES FROM anon, authenticated;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC, anon, authenticated;

COMMIT;
