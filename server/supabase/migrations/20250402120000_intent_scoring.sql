-- Intent score + tier on leads and visitors (v2)

ALTER TABLE v2.leads ADD COLUMN IF NOT EXISTS intent_score integer NOT NULL DEFAULT 0;
ALTER TABLE v2.leads ADD COLUMN IF NOT EXISTS intent_tier text NOT NULL DEFAULT 'low';

ALTER TABLE v2.visitors ADD COLUMN IF NOT EXISTS intent_score integer NOT NULL DEFAULT 0;
ALTER TABLE v2.visitors ADD COLUMN IF NOT EXISTS intent_tier text NOT NULL DEFAULT 'low';

CREATE INDEX IF NOT EXISTS idx_leads_intent_score ON v2.leads (intent_score DESC);
CREATE INDEX IF NOT EXISTS idx_leads_intent_tier ON v2.leads (intent_tier);

COMMENT ON COLUMN v2.leads.intent_score IS 'Sales intent points (first visit, contact, unique pages)';
COMMENT ON COLUMN v2.leads.intent_tier IS 'low | medium | high — derived from intent_score';
