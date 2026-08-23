-- Local-first migration for the canonical observation/aggregate contract.
-- Safe to run repeatedly. Legacy columns are intentionally not dropped here.
BEGIN;

ALTER TABLE public.gps_observations
    ADD COLUMN IF NOT EXISTS validation_score DOUBLE PRECISION;

DO $$
DECLARE duplicate_trip_ids BIGINT;
BEGIN
    SELECT COUNT(*) INTO duplicate_trip_ids
    FROM (
        SELECT trip_id
        FROM public.aggregated_train_positions
        WHERE trip_id IS NOT NULL
        GROUP BY trip_id
        HAVING COUNT(*) > 1
    ) duplicates;
    IF duplicate_trip_ids > 0 THEN
        RAISE EXCEPTION
            'canonical migration blocked: aggregated_train_positions has % duplicate trip_id group(s); resolve them before adding uniqueness',
            duplicate_trip_ids;
    END IF;
END
$$;

ALTER TABLE public.aggregated_train_positions
    ADD COLUMN IF NOT EXISTS confidence_score DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS freshness TEXT,
    ADD COLUMN IF NOT EXISTS truth TEXT,
    ADD COLUMN IF NOT EXISTS next_station_id UUID,
    ADD COLUMN IF NOT EXISTS next_station_name_ar TEXT,
    ADD COLUMN IF NOT EXISTS station_event TEXT,
    ADD COLUMN IF NOT EXISTS eta_station_id UUID,
    ADD COLUMN IF NOT EXISTS eta_min_sec INTEGER,
    ADD COLUMN IF NOT EXISTS eta_max_sec INTEGER,
    ADD COLUMN IF NOT EXISTS eta_confidence TEXT,
    ADD COLUMN IF NOT EXISTS wait_decision TEXT,
    ADD COLUMN IF NOT EXISTS wait_reason_ar TEXT;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'gps_observations_validation_score_check'
    ) THEN
        ALTER TABLE public.gps_observations
            ADD CONSTRAINT gps_observations_validation_score_check
            CHECK (validation_score IS NULL OR validation_score BETWEEN 0 AND 1);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'aggregated_train_positions_confidence_score_check'
    ) THEN
        ALTER TABLE public.aggregated_train_positions
            ADD CONSTRAINT aggregated_train_positions_confidence_score_check
            CHECK (confidence_score IS NULL OR confidence_score BETWEEN 0 AND 1);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'aggregated_train_positions_freshness_check'
    ) THEN
        ALTER TABLE public.aggregated_train_positions
            ADD CONSTRAINT aggregated_train_positions_freshness_check
            CHECK (freshness IS NULL OR freshness IN ('LIVE','RECENT','AGING','STALE','UNKNOWN'));
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'aggregated_train_positions_truth_check'
    ) THEN
        ALTER TABLE public.aggregated_train_positions
            ADD CONSTRAINT aggregated_train_positions_truth_check
            CHECK (truth IS NULL OR truth IN ('OBSERVED','ESTIMATED','UNKNOWN'));
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'aggregated_train_positions_next_station_id_fkey'
    ) THEN
        ALTER TABLE public.aggregated_train_positions
            ADD CONSTRAINT aggregated_train_positions_next_station_id_fkey
            FOREIGN KEY (next_station_id) REFERENCES public.stations(id);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'aggregated_train_positions_eta_station_id_fkey'
    ) THEN
        ALTER TABLE public.aggregated_train_positions
            ADD CONSTRAINT aggregated_train_positions_eta_station_id_fkey
            FOREIGN KEY (eta_station_id) REFERENCES public.stations(id);
    END IF;
END
$$;

CREATE UNIQUE INDEX IF NOT EXISTS uq_aggregated_train_positions_trip_id
    ON public.aggregated_train_positions (trip_id);

COMMIT;
