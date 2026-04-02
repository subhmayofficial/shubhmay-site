-- ShubhMay v2 — lead activity timeline
-- Create v2.lead_events to capture every significant action for a lead/session.

CREATE TABLE IF NOT EXISTS v2.lead_events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  lead_id uuid REFERENCES v2.leads (id) ON DELETE CASCADE,
  session_id text NOT NULL,
  event_type text NOT NULL, -- e.g. 'lead', 'checkout'
  event_name text NOT NULL, -- e.g. 'lead_upsert', 'page_view', 'form_submit', 'payment_opened', 'converted'
  stage text,               -- optional finer-grained stage (e.g. checkout stage)
  path text,
  referrer text,
  document_referrer text,
  utm_source text,
  utm_medium text,
  utm_campaign text,
  utm_content text,
  utm_term text,
  meta jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS lead_events_lead_id_idx ON v2.lead_events (lead_id, created_at DESC);
CREATE INDEX IF NOT EXISTS lead_events_session_id_idx ON v2.lead_events (session_id, created_at DESC);
ALTER TABLE v2.lead_events ENABLE ROW LEVEL SECURITY;
COMMENT ON TABLE v2.lead_events IS 'Per-lead activity timeline (page views, checkout stages, conversions).';
