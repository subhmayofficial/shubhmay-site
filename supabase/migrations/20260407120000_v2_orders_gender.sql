-- Kundli checkout: persist customer gender on paid orders
ALTER TABLE v2.orders ADD COLUMN IF NOT EXISTS gender text;

COMMENT ON COLUMN v2.orders.gender IS 'Customer gender for report orders (male, female, other).';
