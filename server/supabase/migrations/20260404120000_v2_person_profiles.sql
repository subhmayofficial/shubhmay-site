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
