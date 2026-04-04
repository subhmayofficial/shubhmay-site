-- Strong person profile dedupe + backfill across all main sections.
-- Normalization: lower(trim(email)) and digits-only phone.

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
  -- Make sure every profile stores normalized contact keys.
  UPDATE v2.person_profiles p
  SET
    canonical_email = v2.norm_email(p.canonical_email),
    canonical_phone = v2.norm_phone(p.canonical_phone),
    updated_at = now()
  WHERE
    p.canonical_email IS DISTINCT FROM v2.norm_email(p.canonical_email)
    OR p.canonical_phone IS DISTINCT FROM v2.norm_phone(p.canonical_phone);

  -- Backfill from leads (contact-aware only) into existing linked profiles.
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
      AND (v2.norm_email(l.email) IS NOT NULL OR v2.norm_phone(l.phone) IS NOT NULL)
    GROUP BY l.person_profile_id
  ) x
  WHERE p.id = x.pid;

  -- Backfill from visitors when profiles are still empty.
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
  WHERE p.id = x.pid
    AND (p.canonical_email IS NULL OR p.canonical_phone IS NULL OR p.canonical_name IS NULL);

  -- Merge duplicate profiles by normalized email (latest seen wins).
  FOR rec IN
    WITH d AS (
      SELECT
        canonical_email AS key_email,
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
    -- Repoint all linked rows to winner.
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

    DELETE FROM v2.person_profiles
      WHERE id = ANY(rec.ids) AND id <> rec.winner_id;
  END LOOP;

  -- Merge duplicate profiles by normalized phone (again latest seen wins).
  FOR rec IN
    WITH d AS (
      SELECT
        canonical_phone AS key_phone,
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

    DELETE FROM v2.person_profiles
      WHERE id = ANY(rec.ids) AND id <> rec.winner_id;
  END LOOP;

  -- Assign profile_id to unmatched leads with contact.
  INSERT INTO v2.person_profiles (
    canonical_name, canonical_email, canonical_phone, first_seen_at, last_seen_at,
    first_touch_path, first_touch_source, first_touch_referrer,
    last_touch_path, last_touch_source, last_touch_referrer,
    lead_status, lifecycle_stage, meta, created_at, updated_at
  )
  SELECT
    NULLIF(trim(l.name), '') AS canonical_name,
    v2.norm_email(l.email) AS canonical_email,
    v2.norm_phone(l.phone) AS canonical_phone,
    l.first_seen_at,
    l.last_seen_at,
    l.landing_path,
    l.source_page,
    l.referrer,
    l.landing_path,
    l.source_page,
    l.referrer,
    COALESCE(NULLIF(trim(l.lead_status), ''), 'new') AS lead_status,
    'lead' AS lifecycle_stage,
    '{}'::jsonb,
    now(),
    now()
  FROM v2.leads l
  WHERE l.person_profile_id IS NULL
    AND (v2.norm_email(l.email) IS NOT NULL OR v2.norm_phone(l.phone) IS NOT NULL);

  UPDATE v2.leads l
  SET person_profile_id = p.id, updated_at = now()
  FROM v2.person_profiles p
  WHERE l.person_profile_id IS NULL
    AND (
      (v2.norm_email(l.email) IS NOT NULL AND p.canonical_email = v2.norm_email(l.email))
      OR (v2.norm_phone(l.phone) IS NOT NULL AND p.canonical_phone = v2.norm_phone(l.phone))
    );

  -- Assign profile_id to unmatched visitors if they have contact.
  INSERT INTO v2.person_profiles (
    canonical_name, canonical_email, canonical_phone, first_seen_at, last_seen_at,
    first_touch_path, first_touch_source, first_touch_referrer,
    last_touch_path, last_touch_source, last_touch_referrer,
    lead_status, lifecycle_stage, meta, created_at, updated_at
  )
  SELECT
    NULLIF(trim(v.name), '') AS canonical_name,
    v2.norm_email(v.email) AS canonical_email,
    v2.norm_phone(v.phone) AS canonical_phone,
    v.first_seen_at,
    v.last_seen_at,
    v.landing_path,
    v.source_page,
    v.referrer,
    v.landing_path,
    v.source_page,
    v.referrer,
    'anonymous',
    'visitor',
    '{}'::jsonb,
    now(),
    now()
  FROM v2.visitors v
  WHERE v.person_profile_id IS NULL
    AND (v2.norm_email(v.email) IS NOT NULL OR v2.norm_phone(v.phone) IS NOT NULL);

  UPDATE v2.visitors v
  SET person_profile_id = p.id, updated_at = now()
  FROM v2.person_profiles p
  WHERE v.person_profile_id IS NULL
    AND (
      (v2.norm_email(v.email) IS NOT NULL AND p.canonical_email = v2.norm_email(v.email))
      OR (v2.norm_phone(v.phone) IS NOT NULL AND p.canonical_phone = v2.norm_phone(v.phone))
    );

  -- Session-based backfill when visitor/lead already profile-linked.
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

  -- Propagate profile links from leads/visitors into events.
  UPDATE v2.lead_events e
  SET person_profile_id = l.person_profile_id
  FROM v2.leads l
  WHERE e.person_profile_id IS NULL
    AND e.lead_id = l.id
    AND l.person_profile_id IS NOT NULL;

  UPDATE v2.visitor_events e
  SET person_profile_id = v.person_profile_id
  FROM v2.visitors v
  WHERE e.person_profile_id IS NULL
    AND e.visitor_id = v.id
    AND v.person_profile_id IS NOT NULL;

  -- Propagate links into abandoned_checkouts/orders/bookings/customers via lead/session/contact.
  UPDATE v2.abandoned_checkouts a
  SET person_profile_id = l.person_profile_id, updated_at = now()
  FROM v2.leads l
  WHERE a.person_profile_id IS NULL
    AND a.lead_id = l.id
    AND l.person_profile_id IS NOT NULL;

  UPDATE v2.orders o
  SET person_profile_id = l.person_profile_id, updated_at = now()
  FROM v2.leads l
  WHERE o.person_profile_id IS NULL
    AND o.lead_id = l.id
    AND l.person_profile_id IS NOT NULL;

  UPDATE v2.consultancy_bookings b
  SET person_profile_id = l.person_profile_id, updated_at = now()
  FROM v2.leads l
  WHERE b.person_profile_id IS NULL
    AND b.lead_id = l.id
    AND l.person_profile_id IS NOT NULL;

  UPDATE v2.customers c
  SET person_profile_id = p.id, updated_at = now()
  FROM v2.person_profiles p
  WHERE c.person_profile_id IS NULL
    AND (
      (v2.norm_email(c.email) IS NOT NULL AND p.canonical_email = v2.norm_email(c.email))
      OR (v2.norm_phone(c.phone) IS NOT NULL AND p.canonical_phone = v2.norm_phone(c.phone))
    );

  -- Final denormalized arrays + lifecycle/lead status refresh.
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
    merged_checkout_ids = COALESCE((
      SELECT to_jsonb(array_agg(a.id ORDER BY a.last_event_at DESC))
      FROM v2.abandoned_checkouts a WHERE a.person_profile_id = p.id
    ), '[]'::jsonb),
    merged_order_ids = COALESCE((
      SELECT to_jsonb(array_agg(o.id ORDER BY coalesce(o.paid_at, o.created_at) DESC))
      FROM v2.orders o WHERE o.person_profile_id = p.id
    ), '[]'::jsonb),
    merged_booking_ids = COALESCE((
      SELECT to_jsonb(array_agg(b.id ORDER BY coalesce(b.slot_start, b.created_at) DESC))
      FROM v2.consultancy_bookings b WHERE b.person_profile_id = p.id
    ), '[]'::jsonb),
    total_orders = COALESCE((
      SELECT count(*)::int FROM (
        SELECT o.id FROM v2.orders o WHERE o.person_profile_id = p.id
        UNION ALL
        SELECT b.id FROM v2.consultancy_bookings b WHERE b.person_profile_id = p.id
      ) t
    ), 0),
    total_revenue_paise = COALESCE((
      SELECT sum(vv)::bigint FROM (
        SELECT coalesce(o.amount_paise,0)::bigint AS vv
        FROM v2.orders o
        WHERE o.person_profile_id = p.id
          AND lower(coalesce(o.payment_status,'')) = 'paid'
        UNION ALL
        SELECT coalesce(b.amount_paise,0)::bigint AS vv
        FROM v2.consultancy_bookings b
        WHERE b.person_profile_id = p.id
          AND lower(coalesce(b.payment_status,'')) = 'paid'
      ) t
    ), 0),
    first_seen_at = COALESCE((
      SELECT min(ts) FROM (
        SELECT v.first_seen_at AS ts FROM v2.visitors v WHERE v.person_profile_id = p.id
        UNION ALL
        SELECT l.first_seen_at AS ts FROM v2.leads l WHERE l.person_profile_id = p.id
      ) t
    ), p.first_seen_at),
    last_seen_at = COALESCE((
      SELECT max(ts) FROM (
        SELECT v.last_seen_at AS ts FROM v2.visitors v WHERE v.person_profile_id = p.id
        UNION ALL
        SELECT l.last_seen_at AS ts FROM v2.leads l WHERE l.person_profile_id = p.id
      ) t
    ), p.last_seen_at),
    lifecycle_stage = CASE
      WHEN EXISTS (
        SELECT 1 FROM v2.orders o
        WHERE o.person_profile_id = p.id
          AND lower(coalesce(o.payment_status,'')) = 'paid'
      ) OR EXISTS (
        SELECT 1 FROM v2.consultancy_bookings b
        WHERE b.person_profile_id = p.id
          AND lower(coalesce(b.payment_status,'')) = 'paid'
      ) THEN 'customer'
      WHEN EXISTS (
        SELECT 1 FROM v2.leads l
        WHERE l.person_profile_id = p.id
          AND (v2.norm_email(l.email) IS NOT NULL OR v2.norm_phone(l.phone) IS NOT NULL)
      ) THEN 'lead'
      WHEN EXISTS (
        SELECT 1 FROM v2.abandoned_checkouts a
        WHERE a.person_profile_id = p.id
          AND (v2.norm_email(a.email) IS NOT NULL OR v2.norm_phone(a.phone) IS NOT NULL)
      ) THEN 'checkout_contact'
      ELSE 'visitor'
    END,
    lead_status = CASE
      WHEN EXISTS (
        SELECT 1 FROM v2.leads l
        WHERE l.person_profile_id = p.id
          AND (v2.norm_email(l.email) IS NOT NULL OR v2.norm_phone(l.phone) IS NOT NULL)
      )
      THEN COALESCE((
        SELECT l2.lead_status
        FROM v2.leads l2
        WHERE l2.person_profile_id = p.id
          AND (v2.norm_email(l2.email) IS NOT NULL OR v2.norm_phone(l2.phone) IS NOT NULL)
        ORDER BY l2.last_seen_at DESC NULLS LAST, l2.created_at DESC NULLS LAST
        LIMIT 1
      ), 'new')
      ELSE 'anonymous'
    END,
    updated_at = now();
END;
$$;

SELECT v2.rebuild_profiles_now();
