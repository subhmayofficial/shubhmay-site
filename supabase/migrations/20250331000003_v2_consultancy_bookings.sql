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
