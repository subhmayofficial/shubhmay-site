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
