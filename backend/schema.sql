CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE countries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name_ar TEXT NOT NULL,
    name_fr TEXT,
    name_en TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    deleted_at TIMESTAMPTZ
);

CREATE TABLE regions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    country_id UUID REFERENCES countries(id),
    name_ar TEXT NOT NULL,
    name_fr TEXT,
    name_en TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    deleted_at TIMESTAMPTZ
);

CREATE TABLE railway_networks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    region_id UUID REFERENCES regions(id),
    name_ar TEXT NOT NULL,
    name_fr TEXT,
    name_en TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    deleted_at TIMESTAMPTZ
);

CREATE TABLE railway_lines (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    network_id UUID REFERENCES railway_networks(id),
    name_ar TEXT NOT NULL,
    name_fr TEXT,
    name_en TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    deleted_at TIMESTAMPTZ
);

CREATE TABLE stations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name_ar TEXT NOT NULL,
    name_fr TEXT,
    name_en TEXT,
    location GEOMETRY(Point, 4326) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    deleted_at TIMESTAMPTZ
);

CREATE TABLE station_aliases (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    station_id UUID REFERENCES stations(id) ON DELETE CASCADE,
    alias TEXT NOT NULL,
    language TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE trains (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    train_number TEXT NOT NULL UNIQUE,
    line_id UUID REFERENCES railway_lines(id),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    deleted_at TIMESTAMPTZ
);

CREATE TABLE trips (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    train_id UUID REFERENCES trains(id),
    line_id UUID REFERENCES railway_lines(id),
    direction TEXT CHECK (direction IN ('OUTBOUND','INBOUND')),
    scheduled_departure TIMESTAMPTZ,
    scheduled_arrival TIMESTAMPTZ,
    status TEXT DEFAULT 'SCHEDULED',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    deleted_at TIMESTAMPTZ
);

CREATE TABLE trip_stops (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    trip_id UUID REFERENCES trips(id) ON DELETE CASCADE,
    station_id UUID REFERENCES stations(id),
    sequence INTEGER NOT NULL,
    scheduled_arrival TIMESTAMPTZ,
    scheduled_departure TIMESTAMPTZ,
    actual_arrival TIMESTAMPTZ,
    actual_departure TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE railway_segments (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    line_id UUID REFERENCES railway_lines(id),
    start_station_id UUID REFERENCES stations(id),
    end_station_id UUID REFERENCES stations(id),
    geometry GEOMETRY(LineString, 4326) NOT NULL,
    distance_meters DOUBLE PRECISION,
    direction TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE monitor_sessions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    trip_id UUID REFERENCES trips(id) ON DELETE CASCADE,
    train_id UUID REFERENCES trains(id),
    anonymous_monitor_id TEXT,
    status TEXT DEFAULT 'STARTING',
    started_at TIMESTAMPTZ DEFAULT NOW(),
    ended_at TIMESTAMPTZ,
    last_observation_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE gps_observations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id UUID REFERENCES monitor_sessions(id) ON DELETE CASCADE,
    trip_id UUID REFERENCES trips(id),
    train_id UUID REFERENCES trains(id),
    location GEOMETRY(Point, 4326) NOT NULL,
    accuracy_meters FLOAT,
    speed_mps FLOAT,
    heading_deg FLOAT,
    observed_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    is_valid BOOLEAN DEFAULT FALSE,
    rejection_reason TEXT,
    validation_score DOUBLE PRECISION CHECK (validation_score IS NULL OR validation_score BETWEEN 0 AND 1)
);

CREATE TABLE aggregated_train_positions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    train_id UUID REFERENCES trains(id),
    trip_id UUID REFERENCES trips(id) UNIQUE,
    location GEOMETRY(Point, 4326) NOT NULL,
    estimated_speed_mps FLOAT,
    heading_deg FLOAT,
    confidence TEXT CHECK (confidence IN ('HIGH','MEDIUM','LOW','UNKNOWN')),
    confidence_score DOUBLE PRECISION CHECK (confidence_score IS NULL OR confidence_score BETWEEN 0 AND 1),
    freshness TEXT CHECK (freshness IS NULL OR freshness IN ('LIVE','RECENT','AGING','STALE','UNKNOWN')),
    truth TEXT CHECK (truth IS NULL OR truth IN ('OBSERVED','ESTIMATED','UNKNOWN')),
    source_count INTEGER DEFAULT 0,
    next_station_id UUID REFERENCES stations(id),
    next_station_name_ar TEXT,
    station_event TEXT,
    eta_station_id UUID REFERENCES stations(id),
    eta_min_sec INTEGER,
    eta_max_sec INTEGER,
    eta_confidence TEXT,
    wait_decision TEXT,
    wait_reason_ar TEXT,
    last_observed_at TIMESTAMPTZ,
    last_estimated_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE station_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    train_id UUID REFERENCES trains(id),
    trip_id UUID REFERENCES trips(id),
    station_id UUID REFERENCES stations(id),
    event_type TEXT CHECK (event_type IN ('ARRIVING','AT_STATION','DEPARTED')),
    confidence TEXT,
    source TEXT DEFAULT 'monitor',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE community_reports (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    train_id UUID REFERENCES trains(id),
    trip_id UUID REFERENCES trips(id),
    station_id UUID REFERENCES stations(id),
    report_type TEXT CHECK (report_type IN ('TRAIN_MOVING','TRAIN_STOPPED','ARRIVED_STATION','DEPARTED_STATION','DELAYED','PROBLEM','OTHER')),
    description TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE monitor_reputation (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    anonymous_monitor_id TEXT NOT NULL,
    reliability_score FLOAT DEFAULT 0.0,
    total_observations INTEGER DEFAULT 0,
    accepted_observations INTEGER DEFAULT 0,
    rejected_observations INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE favorites (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id TEXT,
    type TEXT CHECK (type IN ('STATION','ROUTE','TRIP')),
    value TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE notifications (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id TEXT,
    title TEXT,
    body TEXT,
    data JSONB,
    sent_at TIMESTAMPTZ,
    read_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE data_sources (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_type TEXT,
    reliability_score FLOAT,
    last_updated TIMESTAMPTZ,
    metadata JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE audit_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id TEXT,
    action TEXT,
    entity_type TEXT,
    entity_id TEXT,
    details JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_stations_location ON stations USING GIST (location);
CREATE INDEX idx_railway_segments_geometry ON railway_segments USING GIST (geometry);
CREATE INDEX idx_gps_observations_location ON gps_observations USING GIST (location);
CREATE INDEX idx_gps_observations_session ON gps_observations(session_id);
CREATE INDEX idx_gps_observations_trip ON gps_observations(trip_id);
CREATE INDEX idx_aggregated_train_positions_location ON aggregated_train_positions USING GIST (location);
CREATE INDEX idx_aggregated_train_positions_trip ON aggregated_train_positions(trip_id);
CREATE INDEX idx_community_reports_train ON community_reports(train_id);
