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
