-- Add callable RPC to rebuild + merge person profiles on demand.

CREATE OR REPLACE FUNCTION v2.norm_email(t text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT NULLIF(lower(trim(t)), '');
$$;

CREATE OR REPLACE FUNCTION v2.norm_phone(t text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT NULLIF(regexp_replace(coalesce(t, ''), '[^0-9]+', '', 'g'), '');
$$;

CREATE OR REPLACE FUNCTION v2.rebuild_profiles_now()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  rec record;
BEGIN
  -- Normalize stored contact keys.
  UPDATE v2.person_profiles p
  SET
    canonical_email = v2.norm_email(p.canonical_email),
    canonical_phone = v2.norm_phone(p.canonical_phone),
    updated_at = now()
  WHERE p.canonical_email IS DISTINCT FROM v2.norm_email(p.canonical_email)
     OR p.canonical_phone IS DISTINCT FROM v2.norm_phone(p.canonical_phone);

  -- Pull canonical fields from linked rows.
  UPDATE v2.person_profiles p
  SET
    canonical_email = COALESCE(p.canonical_email, x.email_n),
    canonical_phone = COALESCE(p.canonical_phone, x.phone_n),
    canonical_name = COALESCE(p.canonical_name, x.name_v),
    updated_at = now()
  FROM (
    SELECT
      l.person_profile_id AS pid,
      max(v2.norm_email(l.email)) FILTER (WHERE v2.norm_email(l.email) IS NOT NULL) AS email_n,
      max(v2.norm_phone(l.phone)) FILTER (WHERE v2.norm_phone(l.phone) IS NOT NULL) AS phone_n,
      max(NULLIF(trim(l.name), '')) FILTER (WHERE NULLIF(trim(l.name), '') IS NOT NULL) AS name_v
    FROM v2.leads l
    WHERE l.person_profile_id IS NOT NULL
    GROUP BY l.person_profile_id
  ) x
  WHERE p.id = x.pid;

  UPDATE v2.person_profiles p
  SET
    canonical_email = COALESCE(p.canonical_email, x.email_n),
    canonical_phone = COALESCE(p.canonical_phone, x.phone_n),
    canonical_name = COALESCE(p.canonical_name, x.name_v),
    updated_at = now()
  FROM (
    SELECT
      v.person_profile_id AS pid,
      max(v2.norm_email(v.email)) FILTER (WHERE v2.norm_email(v.email) IS NOT NULL) AS email_n,
      max(v2.norm_phone(v.phone)) FILTER (WHERE v2.norm_phone(v.phone) IS NOT NULL) AS phone_n,
      max(NULLIF(trim(v.name), '')) FILTER (WHERE NULLIF(trim(v.name), '') IS NOT NULL) AS name_v
    FROM v2.visitors v
    WHERE v.person_profile_id IS NOT NULL
    GROUP BY v.person_profile_id
  ) x
  WHERE p.id = x.pid;

  -- Merge by email.
  FOR rec IN
    WITH d AS (
      SELECT
        p.canonical_email AS key_value,
        (
          SELECT p2.id
          FROM v2.person_profiles p2
          WHERE p2.canonical_email = p.canonical_email
          ORDER BY p2.last_seen_at DESC NULLS LAST, p2.created_at DESC NULLS LAST, p2.id
          LIMIT 1
        ) AS winner_id,
        array_agg(p.id) AS ids
      FROM v2.person_profiles p
      WHERE p.canonical_email IS NOT NULL
      GROUP BY p.canonical_email
      HAVING count(*) > 1
    )
    SELECT winner_id, ids FROM d
  LOOP
    UPDATE v2.visitors SET person_profile_id = rec.winner_id, updated_at = now()
      WHERE person_profile_id = ANY(rec.ids) AND person_profile_id <> rec.winner_id;
    UPDATE v2.leads SET person_profile_id = rec.winner_id, updated_at = now()
      WHERE person_profile_id = ANY(rec.ids) AND person_profile_id <> rec.winner_id;
    UPDATE v2.abandoned_checkouts SET person_profile_id = rec.winner_id, updated_at = now()
      WHERE person_profile_id = ANY(rec.ids) AND person_profile_id <> rec.winner_id;
    UPDATE v2.orders SET person_profile_id = rec.winner_id, updated_at = now()
      WHERE person_profile_id = ANY(rec.ids) AND person_profile_id <> rec.winner_id;
    UPDATE v2.consultancy_bookings SET person_profile_id = rec.winner_id, updated_at = now()
      WHERE person_profile_id = ANY(rec.ids) AND person_profile_id <> rec.winner_id;
    UPDATE v2.customers SET person_profile_id = rec.winner_id, updated_at = now()
      WHERE person_profile_id = ANY(rec.ids) AND person_profile_id <> rec.winner_id;
    UPDATE v2.visitor_events SET person_profile_id = rec.winner_id
      WHERE person_profile_id = ANY(rec.ids) AND person_profile_id <> rec.winner_id;
    UPDATE v2.lead_events SET person_profile_id = rec.winner_id
      WHERE person_profile_id = ANY(rec.ids) AND person_profile_id <> rec.winner_id;
    DELETE FROM v2.person_profiles WHERE id = ANY(rec.ids) AND id <> rec.winner_id;
  END LOOP;

  -- Merge by phone.
  FOR rec IN
    WITH d AS (
      SELECT
        p.canonical_phone AS key_value,
        (
          SELECT p2.id
          FROM v2.person_profiles p2
          WHERE p2.canonical_phone = p.canonical_phone
          ORDER BY p2.last_seen_at DESC NULLS LAST, p2.created_at DESC NULLS LAST, p2.id
          LIMIT 1
        ) AS winner_id,
        array_agg(p.id) AS ids
      FROM v2.person_profiles p
      WHERE p.canonical_phone IS NOT NULL
      GROUP BY p.canonical_phone
      HAVING count(*) > 1
    )
    SELECT winner_id, ids FROM d
  LOOP
    UPDATE v2.visitors SET person_profile_id = rec.winner_id, updated_at = now()
      WHERE person_profile_id = ANY(rec.ids) AND person_profile_id <> rec.winner_id;
    UPDATE v2.leads SET person_profile_id = rec.winner_id, updated_at = now()
      WHERE person_profile_id = ANY(rec.ids) AND person_profile_id <> rec.winner_id;
    UPDATE v2.abandoned_checkouts SET person_profile_id = rec.winner_id, updated_at = now()
      WHERE person_profile_id = ANY(rec.ids) AND person_profile_id <> rec.winner_id;
    UPDATE v2.orders SET person_profile_id = rec.winner_id, updated_at = now()
      WHERE person_profile_id = ANY(rec.ids) AND person_profile_id <> rec.winner_id;
    UPDATE v2.consultancy_bookings SET person_profile_id = rec.winner_id, updated_at = now()
      WHERE person_profile_id = ANY(rec.ids) AND person_profile_id <> rec.winner_id;
    UPDATE v2.customers SET person_profile_id = rec.winner_id, updated_at = now()
      WHERE person_profile_id = ANY(rec.ids) AND person_profile_id <> rec.winner_id;
    UPDATE v2.visitor_events SET person_profile_id = rec.winner_id
      WHERE person_profile_id = ANY(rec.ids) AND person_profile_id <> rec.winner_id;
    UPDATE v2.lead_events SET person_profile_id = rec.winner_id
      WHERE person_profile_id = ANY(rec.ids) AND person_profile_id <> rec.winner_id;
    DELETE FROM v2.person_profiles WHERE id = ANY(rec.ids) AND id <> rec.winner_id;
  END LOOP;

  -- Session-based linking fallback.
  UPDATE v2.leads l
  SET person_profile_id = v.person_profile_id, updated_at = now()
  FROM v2.visitors v
  WHERE l.person_profile_id IS NULL
    AND v.person_profile_id IS NOT NULL
    AND l.session_id IS NOT NULL
    AND l.session_id = v.session_id;

  UPDATE v2.visitors v
  SET person_profile_id = l.person_profile_id, updated_at = now()
  FROM v2.leads l
  WHERE v.person_profile_id IS NULL
    AND l.person_profile_id IS NOT NULL
    AND v.session_id IS NOT NULL
    AND v.session_id = l.session_id;

  -- Keep profile aggregates current.
  UPDATE v2.person_profiles p
  SET
    merged_session_ids = COALESCE((
      SELECT to_jsonb(array_agg(DISTINCT s) FILTER (WHERE s IS NOT NULL))
      FROM (
        SELECT v.session_id AS s FROM v2.visitors v WHERE v.person_profile_id = p.id
        UNION ALL
        SELECT l.session_id AS s FROM v2.leads l WHERE l.person_profile_id = p.id
      ) z
    ), '[]'::jsonb),
    merged_visitor_ids = COALESCE((
      SELECT to_jsonb(array_agg(v.id ORDER BY v.last_seen_at DESC))
      FROM v2.visitors v WHERE v.person_profile_id = p.id
    ), '[]'::jsonb),
    merged_lead_ids = COALESCE((
      SELECT to_jsonb(array_agg(l.id ORDER BY l.last_seen_at DESC))
      FROM v2.leads l WHERE l.person_profile_id = p.id
    ), '[]'::jsonb),
    lifecycle_stage = CASE
      WHEN p.canonical_email IS NOT NULL OR p.canonical_phone IS NOT NULL THEN 'lead'
      ELSE COALESCE(p.lifecycle_stage, 'visitor')
    END,
    lead_status = CASE
      WHEN p.canonical_email IS NOT NULL OR p.canonical_phone IS NOT NULL THEN COALESCE(NULLIF(p.lead_status, ''), 'new')
      ELSE 'anonymous'
    END,
    updated_at = now();
END;
$$;

SELECT v2.rebuild_profiles_now();
