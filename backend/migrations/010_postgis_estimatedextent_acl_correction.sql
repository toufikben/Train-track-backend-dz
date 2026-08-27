-- Corrective PostGIS ACL hardening.
-- Migration 009 was recorded, but live ACL inspection showed explicit
-- anon/authenticated EXECUTE grants still present. Use explicit ALL revoke.
BEGIN;

REVOKE ALL PRIVILEGES ON FUNCTION public.st_estimatedextent(text, text)
    FROM anon, authenticated, PUBLIC;
REVOKE ALL PRIVILEGES ON FUNCTION public.st_estimatedextent(text, text, text)
    FROM anon, authenticated, PUBLIC;
REVOKE ALL PRIVILEGES ON FUNCTION public.st_estimatedextent(text, text, text, boolean)
    FROM anon, authenticated, PUBLIC;

COMMIT;
