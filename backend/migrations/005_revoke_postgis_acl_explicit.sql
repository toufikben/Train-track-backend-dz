-- Direct ACL cleanup for PostGIS helper functions. This is intentionally
-- explicit because extension-owned ACLs may not be changed by a dynamic loop.
BEGIN;
REVOKE EXECUTE ON FUNCTION public.st_estimatedextent(text, text) FROM PUBLIC, anon, authenticated;
REVOKE EXECUTE ON FUNCTION public.st_estimatedextent(text, text, text) FROM PUBLIC, anon, authenticated;
REVOKE EXECUTE ON FUNCTION public.st_estimatedextent(text, text, text, boolean) FROM PUBLIC, anon, authenticated;
COMMIT;
