-- Searchable text form of lead id (PostgREST cannot ilike on uuid type).
ALTER TABLE v2.leads
  ADD COLUMN IF NOT EXISTS id_text text GENERATED ALWAYS AS (id::text) STORED;

COMMENT ON COLUMN v2.leads.id_text IS 'Mirrors id::text for admin search (ilike on lead id / lead code).';
