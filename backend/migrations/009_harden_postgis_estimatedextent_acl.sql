-- WinRah PostGIS ACL hardening.
-- LOCAL REVIEW FIRST: revoke only the exposed SECURITY DEFINER
-- st_estimatedextent overloads from PUBLIC/anon/authenticated.
-- Do not enable RLS on spatial_ref_sys and do not move the PostGIS extension.
-- Map-safe design: ST_AsGeoJSON/ST_Transform/ST_SetSRID/ST_MakeEnvelope
-- are intentionally not changed by this migration.

BEGIN;

REVOKE EXECUTE ON FUNCTION public.st_estimatedextent(text, text)
    FROM PUBLIC, anon, authenticated;
REVOKE EXECUTE ON FUNCTION public.st_estimatedextent(text, text, text)
    FROM PUBLIC, anon, authenticated;
REVOKE EXECUTE ON FUNCTION public.st_estimatedextent(text, text, text, boolean)
    FROM PUBLIC, anon, authenticated;

-- Keep execution available to the backend/service role if it is explicitly
-- used by an internal maintenance task. No grant is made here.

COMMIT;
