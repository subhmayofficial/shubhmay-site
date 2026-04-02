-- PostgREST / Data API access for schema v2 (from Supabase docs: using custom schemas)
-- Still add "v2" under Dashboard → Project Settings → API → Exposed schemas when you see it.

GRANT USAGE ON SCHEMA v2 TO anon, authenticated, service_role;
GRANT ALL ON ALL TABLES IN SCHEMA v2 TO anon, authenticated, service_role;
GRANT ALL ON ALL ROUTINES IN SCHEMA v2 TO anon, authenticated, service_role;
GRANT ALL ON ALL SEQUENCES IN SCHEMA v2 TO anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA v2 GRANT ALL ON TABLES TO anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA v2 GRANT ALL ON ROUTINES TO anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA v2 GRANT ALL ON SEQUENCES TO anon, authenticated, service_role;
