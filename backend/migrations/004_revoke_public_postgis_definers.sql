-- Remove direct API execution of PostGIS SECURITY DEFINER helpers flagged by
-- Supabase advisors. Internal database code and the database owner remain able
-- to use these functions.
BEGIN;

DO $$
DECLARE
    proc text;
BEGIN
    FOR proc IN SELECT unnest(ARRAY[
        'public.st_estimatedextent(text,text)',
        'public.st_estimatedextent(text,text,text)',
        'public.st_estimatedextent(text,text,text,boolean)'
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

COMMIT;
