-- =============================================================================
-- Master schema (all migrations concatenated in filename order).
-- Use on a fresh Supabase project: SQL Editor → paste → Run once.
-- After that, configure keys in Admin → Settings (encrypted on server) or .env.
-- =============================================================================

-- === FILE: 20250331000000_v2_commerce.sql ===
-- ShubhMay commerce — schema v2 ONLY (nothing new in public)
-- Applied via: npx supabase db push (linked project) or supabase db reset (local)

CREATE SCHEMA IF NOT EXISTS v2;
CREATE OR REPLACE FUNCTION v2.set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TABLE v2.customers (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  email text NOT NULL,
  name text,
  phone text,
  is_paying_customer boolean NOT NULL DEFAULT false,
  first_paid_at timestamptz,
  total_spent_paise bigint NOT NULL DEFAULT 0,
  notes text,
  meta jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX customers_email_lower_uidx ON v2.customers (lower(email));
CREATE INDEX customers_created_at_idx ON v2.customers (created_at DESC);
CREATE INDEX customers_phone_idx ON v2.customers (phone);
CREATE TRIGGER customers_set_updated_at
  BEFORE UPDATE ON v2.customers
  FOR EACH ROW EXECUTE FUNCTION v2.set_updated_at();
CREATE TABLE v2.leads (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  session_id text NOT NULL,
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
  lead_status text NOT NULL DEFAULT 'new',
  converted_order_id uuid,
  first_seen_at timestamptz NOT NULL DEFAULT now(),
  last_seen_at timestamptz NOT NULL DEFAULT now(),
  meta jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT leads_session_id_key UNIQUE (session_id)
);
CREATE INDEX leads_email_idx ON v2.leads (lower(email));
CREATE INDEX leads_created_at_idx ON v2.leads (created_at DESC);
CREATE INDEX leads_utm_campaign_idx ON v2.leads (utm_campaign);
CREATE TRIGGER leads_set_updated_at
  BEFORE UPDATE ON v2.leads
  FOR EACH ROW EXECUTE FUNCTION v2.set_updated_at();
CREATE TABLE v2.abandoned_checkouts (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  checkout_session_id text NOT NULL,
  lead_id uuid REFERENCES v2.leads (id) ON DELETE SET NULL,
  email text,
  name text,
  phone text,
  product_slug text NOT NULL DEFAULT 'premium_kundli_report',
  stage text NOT NULL DEFAULT 'page_view',
  razorpay_order_id text,
  amount_paise int,
  currency text DEFAULT 'INR',
  utm_source text,
  utm_medium text,
  utm_campaign text,
  utm_content text,
  utm_term text,
  referrer text,
  landing_path text,
  last_event_at timestamptz NOT NULL DEFAULT now(),
  abandoned_at timestamptz,
  converted_order_id uuid,
  converted_at timestamptz,
  meta jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT abandoned_checkouts_session_key UNIQUE (checkout_session_id)
);
CREATE INDEX abandoned_checkouts_lead_id_idx ON v2.abandoned_checkouts (lead_id);
CREATE INDEX abandoned_checkouts_stage_idx ON v2.abandoned_checkouts (stage);
CREATE INDEX abandoned_checkouts_created_at_idx ON v2.abandoned_checkouts (created_at DESC);
CREATE TRIGGER abandoned_checkouts_set_updated_at
  BEFORE UPDATE ON v2.abandoned_checkouts
  FOR EACH ROW EXECUTE FUNCTION v2.set_updated_at();
CREATE TABLE v2.orders (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  customer_id uuid REFERENCES v2.customers (id) ON DELETE SET NULL,
  lead_id uuid REFERENCES v2.leads (id) ON DELETE SET NULL,
  abandoned_checkout_id uuid REFERENCES v2.abandoned_checkouts (id) ON DELETE SET NULL,
  product_slug text NOT NULL DEFAULT 'premium_kundli_report',
  razorpay_order_id text NOT NULL,
  razorpay_payment_id text,
  receipt text,
  amount_paise int NOT NULL,
  currency text NOT NULL DEFAULT 'INR',
  payment_status text NOT NULL DEFAULT 'paid',
  order_status text NOT NULL DEFAULT 'new',
  dob date,
  tob time,
  birth_place text,
  language text,
  coupon text,
  razorpay_notes jsonb,
  paid_at timestamptz NOT NULL DEFAULT now(),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT orders_razorpay_order_id_key UNIQUE (razorpay_order_id)
);
CREATE INDEX orders_customer_id_idx ON v2.orders (customer_id);
CREATE INDEX orders_lead_id_idx ON v2.orders (lead_id);
CREATE INDEX orders_created_at_idx ON v2.orders (created_at DESC);
CREATE INDEX orders_payment_status_idx ON v2.orders (payment_status);
CREATE TRIGGER orders_set_updated_at
  BEFORE UPDATE ON v2.orders
  FOR EACH ROW EXECUTE FUNCTION v2.set_updated_at();
ALTER TABLE v2.leads
  ADD CONSTRAINT leads_converted_order_id_fkey
  FOREIGN KEY (converted_order_id) REFERENCES v2.orders (id) ON DELETE SET NULL;
ALTER TABLE v2.abandoned_checkouts
  ADD CONSTRAINT abandoned_checkouts_converted_order_id_fkey
  FOREIGN KEY (converted_order_id) REFERENCES v2.orders (id) ON DELETE SET NULL;
ALTER TABLE v2.customers ENABLE ROW LEVEL SECURITY;
ALTER TABLE v2.leads ENABLE ROW LEVEL SECURITY;
ALTER TABLE v2.abandoned_checkouts ENABLE ROW LEVEL SECURITY;
ALTER TABLE v2.orders ENABLE ROW LEVEL SECURITY;
COMMENT ON TABLE v2.customers IS 'Paying and prospect customers';
COMMENT ON TABLE v2.leads IS 'Funnel + UTM tracking';
COMMENT ON TABLE v2.abandoned_checkouts IS 'Checkout drop-off';
COMMENT ON TABLE v2.orders IS 'Paid Razorpay orders';
GRANT USAGE ON SCHEMA v2 TO postgres, anon, authenticated, service_role;
GRANT ALL ON ALL TABLES IN SCHEMA v2 TO postgres, service_role;
GRANT ALL ON ALL SEQUENCES IN SCHEMA v2 TO postgres, service_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA v2 GRANT ALL ON TABLES TO postgres, service_role;

-- === FILE: 20250331000001_v2_postgrest_grants.sql ===
-- PostgREST / Data API access for schema v2 (from Supabase docs: using custom schemas)
-- Still add "v2" under Dashboard → Project Settings → API → Exposed schemas when you see it.

GRANT USAGE ON SCHEMA v2 TO anon, authenticated, service_role;
GRANT ALL ON ALL TABLES IN SCHEMA v2 TO anon, authenticated, service_role;
GRANT ALL ON ALL ROUTINES IN SCHEMA v2 TO anon, authenticated, service_role;
GRANT ALL ON ALL SEQUENCES IN SCHEMA v2 TO anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA v2 GRANT ALL ON TABLES TO anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA v2 GRANT ALL ON ROUTINES TO anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA v2 GRANT ALL ON SEQUENCES TO anon, authenticated, service_role;

-- === FILE: 20250331000002_v2_lead_events.sql ===
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

-- === FILE: 20250331000003_v2_consultancy_bookings.sql ===
-- Consultancy booking funnel (v2 schema only)

CREATE TABLE IF NOT EXISTS v2.consultancy_bookings (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  lead_id uuid REFERENCES v2.leads (id) ON DELETE SET NULL,
  session_id text,
  name text NOT NULL,
  email text NOT NULL,
  phone text NOT NULL,
  topic text,
  notes text,
  slot_start timestamptz NOT NULL,
  slot_end timestamptz NOT NULL,
  timezone text NOT NULL DEFAULT 'Asia/Kolkata',
  status text NOT NULL DEFAULT 'confirmed', -- confirmed | cancelled | completed
  meta jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS consultancy_bookings_slot_start_uidx
  ON v2.consultancy_bookings (slot_start)
  WHERE status = 'confirmed';
CREATE INDEX IF NOT EXISTS consultancy_bookings_email_idx ON v2.consultancy_bookings (lower(email));
CREATE INDEX IF NOT EXISTS consultancy_bookings_created_at_idx ON v2.consultancy_bookings (created_at DESC);
CREATE TRIGGER consultancy_bookings_set_updated_at
  BEFORE UPDATE ON v2.consultancy_bookings
  FOR EACH ROW EXECUTE FUNCTION v2.set_updated_at();
ALTER TABLE v2.consultancy_bookings ENABLE ROW LEVEL SECURITY;
COMMENT ON TABLE v2.consultancy_bookings IS 'Consultancy slot bookings from /products/consultancy-checkout';

-- === FILE: 20250331000004_v2_consultancy_payment_fields.sql ===
-- Consultancy payment + plan fields

ALTER TABLE v2.consultancy_bookings
  ADD COLUMN IF NOT EXISTS plan_code text,
  ADD COLUMN IF NOT EXISTS plan_name text,
  ADD COLUMN IF NOT EXISTS duration_minutes int,
  ADD COLUMN IF NOT EXISTS amount_paise int,
  ADD COLUMN IF NOT EXISTS currency text DEFAULT 'INR',
  ADD COLUMN IF NOT EXISTS razorpay_order_id text,
  ADD COLUMN IF NOT EXISTS razorpay_payment_id text,
  ADD COLUMN IF NOT EXISTS payment_status text DEFAULT 'paid';
CREATE UNIQUE INDEX IF NOT EXISTS consultancy_bookings_razorpay_order_uidx
  ON v2.consultancy_bookings (razorpay_order_id)
  WHERE razorpay_order_id IS NOT NULL;

-- === FILE: 20250331120000_v2_analytics_snapshots.sql ===
-- Persisted dashboard analytics snapshots for historical tracking and conversion trends.

CREATE TABLE IF NOT EXISTS v2.analytics_snapshots (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  created_at timestamptz NOT NULL DEFAULT now(),
  period_start timestamptz NOT NULL,
  period_end timestamptz NOT NULL,
  preset text,
  payload jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_analytics_snapshots_created ON v2.analytics_snapshots (created_at DESC);

-- === FILE: 20250401170000_visitors_split.sql ===
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

-- === FILE: 20250402120000_intent_scoring.sql ===
-- Intent score + tier on leads and visitors (v2)

ALTER TABLE v2.leads ADD COLUMN IF NOT EXISTS intent_score integer NOT NULL DEFAULT 0;
ALTER TABLE v2.leads ADD COLUMN IF NOT EXISTS intent_tier text NOT NULL DEFAULT 'low';

ALTER TABLE v2.visitors ADD COLUMN IF NOT EXISTS intent_score integer NOT NULL DEFAULT 0;
ALTER TABLE v2.visitors ADD COLUMN IF NOT EXISTS intent_tier text NOT NULL DEFAULT 'low';

CREATE INDEX IF NOT EXISTS idx_leads_intent_score ON v2.leads (intent_score DESC);
CREATE INDEX IF NOT EXISTS idx_leads_intent_tier ON v2.leads (intent_tier);

COMMENT ON COLUMN v2.leads.intent_score IS 'Sales intent points (first visit, contact, unique pages)';
COMMENT ON COLUMN v2.leads.intent_tier IS 'low | medium | high — derived from intent_score';

-- === FILE: 20260403120000_leads_id_text_search.sql ===
-- Searchable text form of lead id (PostgREST cannot ilike on uuid type).
ALTER TABLE v2.leads
  ADD COLUMN IF NOT EXISTS id_text text GENERATED ALWAYS AS (id::text) STORED;

COMMENT ON COLUMN v2.leads.id_text IS 'Mirrors id::text for admin search (ilike on lead id / lead code).';

-- === FILE: 20260404120000_v2_person_profiles.sql ===
-- Person-centric identity layer for linking visitors, leads, checkouts, orders, and bookings.

CREATE TABLE IF NOT EXISTS v2.person_profiles (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  id_text text GENERATED ALWAYS AS (id::text) STORED,
  canonical_name text,
  canonical_email text,
  canonical_phone text,
  first_seen_at timestamptz,
  last_seen_at timestamptz,
  first_touch_path text,
  first_touch_source text,
  first_touch_referrer text,
  last_touch_path text,
  last_touch_source text,
  last_touch_referrer text,
  lead_status text NOT NULL DEFAULT 'new',
  lifecycle_stage text NOT NULL DEFAULT 'visitor',
  total_orders integer NOT NULL DEFAULT 0,
  total_revenue_paise bigint NOT NULL DEFAULT 0,
  merged_session_ids jsonb NOT NULL DEFAULT '[]'::jsonb,
  merged_session_ids_text text GENERATED ALWAYS AS (merged_session_ids::text) STORED,
  merged_visitor_ids jsonb NOT NULL DEFAULT '[]'::jsonb,
  merged_lead_ids jsonb NOT NULL DEFAULT '[]'::jsonb,
  merged_checkout_ids jsonb NOT NULL DEFAULT '[]'::jsonb,
  merged_order_ids jsonb NOT NULL DEFAULT '[]'::jsonb,
  merged_booking_ids jsonb NOT NULL DEFAULT '[]'::jsonb,
  meta jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_person_profiles_created_at ON v2.person_profiles (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_person_profiles_last_seen ON v2.person_profiles (last_seen_at DESC);
CREATE INDEX IF NOT EXISTS idx_person_profiles_email ON v2.person_profiles (lower(canonical_email));
CREATE INDEX IF NOT EXISTS idx_person_profiles_phone ON v2.person_profiles (canonical_phone);
CREATE TRIGGER person_profiles_set_updated_at
  BEFORE UPDATE ON v2.person_profiles
  FOR EACH ROW EXECUTE FUNCTION v2.set_updated_at();

ALTER TABLE v2.visitors ADD COLUMN IF NOT EXISTS person_profile_id uuid REFERENCES v2.person_profiles(id) ON DELETE SET NULL;
ALTER TABLE v2.leads ADD COLUMN IF NOT EXISTS person_profile_id uuid REFERENCES v2.person_profiles(id) ON DELETE SET NULL;
ALTER TABLE v2.lead_events ADD COLUMN IF NOT EXISTS person_profile_id uuid REFERENCES v2.person_profiles(id) ON DELETE SET NULL;
ALTER TABLE v2.visitor_events ADD COLUMN IF NOT EXISTS person_profile_id uuid REFERENCES v2.person_profiles(id) ON DELETE SET NULL;
ALTER TABLE v2.abandoned_checkouts ADD COLUMN IF NOT EXISTS person_profile_id uuid REFERENCES v2.person_profiles(id) ON DELETE SET NULL;
ALTER TABLE v2.orders ADD COLUMN IF NOT EXISTS person_profile_id uuid REFERENCES v2.person_profiles(id) ON DELETE SET NULL;
ALTER TABLE v2.consultancy_bookings ADD COLUMN IF NOT EXISTS person_profile_id uuid REFERENCES v2.person_profiles(id) ON DELETE SET NULL;
ALTER TABLE v2.customers ADD COLUMN IF NOT EXISTS person_profile_id uuid REFERENCES v2.person_profiles(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_visitors_person_profile ON v2.visitors (person_profile_id);
CREATE INDEX IF NOT EXISTS idx_leads_person_profile ON v2.leads (person_profile_id);
CREATE INDEX IF NOT EXISTS idx_lead_events_person_profile ON v2.lead_events (person_profile_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_visitor_events_person_profile ON v2.visitor_events (person_profile_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_abandoned_person_profile ON v2.abandoned_checkouts (person_profile_id);
CREATE INDEX IF NOT EXISTS idx_orders_person_profile ON v2.orders (person_profile_id);
CREATE INDEX IF NOT EXISTS idx_bookings_person_profile ON v2.consultancy_bookings (person_profile_id);
CREATE INDEX IF NOT EXISTS idx_customers_person_profile ON v2.customers (person_profile_id);

ALTER TABLE v2.person_profiles ENABLE ROW LEVEL SECURITY;
COMMENT ON TABLE v2.person_profiles IS 'Merged real-person profile linked to visitors, leads, checkouts, bookings, orders, and customer records.';
