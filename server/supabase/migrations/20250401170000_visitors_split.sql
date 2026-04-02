-- Run in Supabase SQL editor (schema v2 must match SUPABASE_SCHEMA in .env).
-- Enables: visitors table, visitor_events, links to leads.

CREATE TABLE IF NOT EXISTS v2.visitors (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  session_id text NOT NULL UNIQUE,
  email text,
  name text,
  phone text,
  source_page text,
  landing_path text,
  referrer text,
  document_referrer text,
  utm_source text,
  utm_medium text,
  utm_campaign text,
  utm_content text,
  utm_term text,
  user_agent text,
  client_language text,
  screen_width int,
  screen_height int,
  first_seen_at timestamptz,
  last_seen_at timestamptz,
  converted_lead_id uuid,
  conversion_at timestamptz,
  conversion_source jsonb DEFAULT '{}'::jsonb,
  meta jsonb,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

ALTER TABLE v2.leads ADD COLUMN IF NOT EXISTS visitor_id uuid REFERENCES v2.visitors(id) ON DELETE SET NULL;

ALTER TABLE v2.visitors DROP CONSTRAINT IF EXISTS visitors_converted_lead_fk;
ALTER TABLE v2.visitors
  ADD CONSTRAINT visitors_converted_lead_fk
  FOREIGN KEY (converted_lead_id) REFERENCES v2.leads(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_visitors_session ON v2.visitors(session_id);
CREATE INDEX IF NOT EXISTS idx_visitors_last_seen ON v2.visitors(last_seen_at);
CREATE INDEX IF NOT EXISTS idx_visitors_converted ON v2.visitors(converted_lead_id);
CREATE INDEX IF NOT EXISTS idx_leads_visitor ON v2.leads(visitor_id);

CREATE TABLE IF NOT EXISTS v2.visitor_events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  visitor_id uuid NOT NULL REFERENCES v2.visitors(id) ON DELETE CASCADE,
  session_id text NOT NULL,
  event_type text NOT NULL,
  event_name text,
  path text,
  referrer text,
  document_referrer text,
  utm_source text,
  utm_medium text,
  utm_campaign text,
  meta jsonb,
  created_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_visitor_events_visitor ON v2.visitor_events(visitor_id);
CREATE INDEX IF NOT EXISTS idx_visitor_events_created ON v2.visitor_events(created_at);
CREATE INDEX IF NOT EXISTS idx_visitor_events_path ON v2.visitor_events(path);
