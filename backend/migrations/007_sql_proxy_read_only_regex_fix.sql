-- Correct the read-only SQL proxy regex without backslash word-boundary ambiguity.
-- This changes no data; it rejects all non-SELECT/CTE statements.
BEGIN;

CREATE OR REPLACE FUNCTION public.sql_proxy_execute(q text, p jsonb DEFAULT '[]'::jsonb)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $function$
DECLARE
    result jsonb;
    normalized text;
BEGIN
    normalized := btrim(q);
    IF normalized = '' OR normalized IS NULL THEN
        RAISE EXCEPTION 'query required';
    END IF;
    IF position(';' IN normalized) > 0
       OR normalized !~* '^(select|with)([[:space:]]|$)'
       OR normalized ~* '(^|[^a-z])(insert|update|delete|merge|drop|create|alter|truncate|grant|revoke|copy|call|do|execute|vacuum|refresh|comment)([^a-z]|$)'
    THEN
        RAISE EXCEPTION 'read_only_query_required';
    END IF;
    EXECUTE format('select coalesce(jsonb_agg(row_to_json(x)), ''[]''::jsonb) from (%s) x', normalized) INTO result;
    RETURN coalesce(result, '[]'::jsonb);
END;
$function$;

REVOKE EXECUTE ON FUNCTION public.sql_proxy_execute(text, jsonb)
    FROM PUBLIC, anon, authenticated;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'service_role') THEN
        GRANT EXECUTE ON FUNCTION public.sql_proxy_execute(text, jsonb) TO service_role;
    END IF;
END
$$;

COMMIT;
