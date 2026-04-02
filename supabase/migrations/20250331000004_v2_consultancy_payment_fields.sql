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
