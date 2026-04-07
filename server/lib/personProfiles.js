import { supabaseRequest } from './supabase.js';
import { normalizeEmail, normalizePhone, strTrim } from './strings.js';

function jsonRows(res) {
  if (!res || res.code < 200 || res.code >= 300) return [];
  try {
    const rows = JSON.parse(res.body || '[]');
    return Array.isArray(rows) ? rows : [];
  } catch {
    return [];
  }
}

function firstNonEmpty(...vals) {
  for (const v of vals) {
    const s = strTrim(v, 1000);
    if (s) return s;
  }
  return null;
}

/** All profiles matching normalized email or phone (used to merge duplicates). */
export async function findPersonProfilesByContact(cfg, emailRaw, phoneRaw) {
  const email = normalizeEmail(emailRaw);
  const phone = normalizePhone(phoneRaw);
  if (!email && !phone) return [];

  const parts = [];
  if (email) parts.push(`canonical_email=eq.${encodeURIComponent(email)}`);
  if (phone) parts.push(`canonical_phone=eq.${encodeURIComponent(phone)}`);
  const path =
    `person_profiles?select=id,canonical_email,canonical_phone,first_seen_at,last_seen_at,lifecycle_stage&or=(${parts.join(',')})` +
    '&order=last_seen_at.desc&limit=25';
  return jsonRows(await supabaseRequest(cfg, 'GET', path));
}

export async function findPersonProfileByContact(cfg, emailRaw, phoneRaw) {
  const rows = await findPersonProfilesByContact(cfg, emailRaw, phoneRaw);
  return rows[0] || null;
}

const MERGE_RELATED_TABLES = [
  'visitors',
  'leads',
  'visitor_events',
  'lead_events',
  'abandoned_checkouts',
  'orders',
  'consultancy_bookings',
  'customers',
];

/** Repoint all rows from duplicate profiles onto `winnerId`, merge meta, delete losers. */
export async function mergePersonProfilesIntoWinner(cfg, winnerId, loserIdsRaw) {
  const winner = String(winnerId || '').trim();
  if (!winner) return;
  const loserIds = [...new Set((loserIdsRaw || []).map(String).filter((id) => id && id !== winner))];
  if (!loserIds.length) return;

  const now = new Date().toISOString();
  for (const lid of loserIds) {
    for (const tbl of MERGE_RELATED_TABLES) {
      const extra = tbl === 'visitor_events' || tbl === 'lead_events' ? {} : { updated_at: now };
      await supabaseRequest(
        cfg,
        'PATCH',
        `${tbl}?person_profile_id=eq.${encodeURIComponent(lid)}`,
        JSON.stringify({ person_profile_id: winner, ...extra }),
        'return=minimal'
      );
    }
  }

  const winnerRow = jsonRows(
    await supabaseRequest(cfg, 'GET', `person_profiles?id=eq.${encodeURIComponent(winner)}&select=meta&limit=1`)
  )[0];
  let meta = winnerRow?.meta && typeof winnerRow.meta === 'object' ? { ...winnerRow.meta } : {};
  const hist = Array.isArray(meta.merge_history) ? [...meta.merge_history] : [];
  hist.push({ at: now, merged_profile_ids: [...loserIds] });
  meta.merge_history = hist;

  for (const lid of loserIds) {
    const lr = jsonRows(
      await supabaseRequest(cfg, 'GET', `person_profiles?id=eq.${encodeURIComponent(lid)}&select=meta&limit=1`)
    )[0];
    if (lr?.meta && typeof lr.meta === 'object') {
      for (const [k, v] of Object.entries(lr.meta)) {
        if (k === 'merge_history') continue;
        if (meta[k] == null || meta[k] === '') meta[k] = v;
      }
    }
    await supabaseRequest(cfg, 'DELETE', `person_profiles?id=eq.${encodeURIComponent(lid)}`, null, 'return=minimal');
  }

  await supabaseRequest(
    cfg,
    'PATCH',
    `person_profiles?id=eq.${encodeURIComponent(winner)}`,
    JSON.stringify({ meta, updated_at: now }),
    'return=minimal'
  );
  await refreshPersonProfileStats(cfg, winner);
}

export async function ensurePersonProfile(cfg, seed = {}) {
  const email = normalizeEmail(seed.email);
  const phone = normalizePhone(seed.phone);
  let matches = email || phone ? await findPersonProfilesByContact(cfg, email, phone) : [];
  if (matches.length > 1) {
    matches = matches.slice().sort((a, b) => new Date(b.last_seen_at || 0) - new Date(a.last_seen_at || 0));
    const win = String(matches[0].id);
    const losers = matches.slice(1).map((m) => String(m.id)).filter(Boolean);
    await mergePersonProfilesIntoWinner(cfg, win, losers);
    matches = await findPersonProfilesByContact(cfg, email, phone);
  }
  const existing = matches[0] || null;
  const now = new Date().toISOString();
  const payload = {
    canonical_name: strTrim(seed.name, 500),
    canonical_email: email,
    canonical_phone: phone,
    first_seen_at: seed.first_seen_at || now,
    last_seen_at: seed.last_seen_at || now,
    first_touch_path: strTrim(seed.first_touch_path, 1000),
    first_touch_source: strTrim(seed.first_touch_source, 500),
    first_touch_referrer: strTrim(seed.first_touch_referrer, 2000),
    last_touch_path: strTrim(seed.last_touch_path, 1000),
    last_touch_source: strTrim(seed.last_touch_source, 500),
    last_touch_referrer: strTrim(seed.last_touch_referrer, 2000),
    lead_status: strTrim(seed.lead_status, 64) || 'new',
    lifecycle_stage: strTrim(seed.lifecycle_stage, 64) || 'visitor',
    total_orders: seed.total_orders != null ? parseInt(String(seed.total_orders), 10) || 0 : 0,
    total_revenue_paise:
      seed.total_revenue_paise != null ? parseInt(String(seed.total_revenue_paise), 10) || 0 : 0,
    merged_session_ids: Array.isArray(seed.merged_session_ids) ? seed.merged_session_ids : [],
    merged_visitor_ids: Array.isArray(seed.merged_visitor_ids) ? seed.merged_visitor_ids : [],
    merged_lead_ids: Array.isArray(seed.merged_lead_ids) ? seed.merged_lead_ids : [],
    merged_checkout_ids: Array.isArray(seed.merged_checkout_ids) ? seed.merged_checkout_ids : [],
    merged_order_ids: Array.isArray(seed.merged_order_ids) ? seed.merged_order_ids : [],
    merged_booking_ids: Array.isArray(seed.merged_booking_ids) ? seed.merged_booking_ids : [],
    meta: seed.meta && typeof seed.meta === 'object' ? seed.meta : {},
  };

  if (existing?.id) {
    const patch = {
      canonical_name: firstNonEmpty(existing.canonical_name, payload.canonical_name),
      canonical_email: firstNonEmpty(existing.canonical_email, payload.canonical_email),
      canonical_phone: firstNonEmpty(existing.canonical_phone, payload.canonical_phone),
      first_seen_at: existing.first_seen_at || payload.first_seen_at,
      first_touch_path: firstNonEmpty(existing.first_touch_path, payload.first_touch_path),
      first_touch_source: firstNonEmpty(existing.first_touch_source, payload.first_touch_source),
      first_touch_referrer: firstNonEmpty(existing.first_touch_referrer, payload.first_touch_referrer),
      last_seen_at: payload.last_seen_at,
      last_touch_path: firstNonEmpty(payload.last_touch_path, existing.last_touch_path),
      last_touch_source: firstNonEmpty(payload.last_touch_source, existing.last_touch_source),
      last_touch_referrer: firstNonEmpty(payload.last_touch_referrer, existing.last_touch_referrer),
      lead_status: payload.lead_status,
      lifecycle_stage: payload.lifecycle_stage,
      updated_at: now,
    };
    await supabaseRequest(
      cfg,
      'PATCH',
      `person_profiles?id=eq.${encodeURIComponent(String(existing.id))}`,
      JSON.stringify(patch),
      'return=minimal'
    );
    return String(existing.id);
  }

  const ins = await supabaseRequest(cfg, 'POST', 'person_profiles', JSON.stringify(payload), 'return=representation');
  const rows = jsonRows(ins);
  if (rows[0]?.id) return String(rows[0].id);

  const refetched = (await findPersonProfilesByContact(cfg, email, phone))[0];
  return refetched?.id ? String(refetched.id) : null;
}

export async function patchPersonProfile(cfg, profileId, patch) {
  if (!profileId || !patch || typeof patch !== 'object') return;
  const payload = { ...patch, updated_at: new Date().toISOString() };
  await supabaseRequest(
    cfg,
    'PATCH',
    `person_profiles?id=eq.${encodeURIComponent(String(profileId))}`,
    JSON.stringify(payload),
    'return=minimal'
  );
}

export async function attachProfileToRecord(cfg, table, recordId, profileId) {
  if (!table || !recordId || !profileId) return;
  const patch = { person_profile_id: profileId };
  if (table !== 'lead_events' && table !== 'visitor_events') {
    patch.updated_at = new Date().toISOString();
  }
  await supabaseRequest(
    cfg,
    'PATCH',
    `${table}?id=eq.${encodeURIComponent(String(recordId))}`,
    JSON.stringify(patch),
    'return=minimal'
  );
}

export async function attachProfileByFilter(cfg, table, filterQuery, profileId) {
  if (!table || !filterQuery || !profileId) return;
  const sep = filterQuery.includes('?') ? '&' : '?';
  const patch = { person_profile_id: profileId };
  if (table !== 'lead_events' && table !== 'visitor_events') {
    patch.updated_at = new Date().toISOString();
  }
  await supabaseRequest(
    cfg,
    'PATCH',
    `${table}${sep}${filterQuery}`,
    JSON.stringify(patch),
    'return=minimal'
  );
}

export async function refreshPersonProfileStats(cfg, profileId) {
  if (!profileId) return;
  const pid = encodeURIComponent(String(profileId));
  const [orders, bookings, visitors, leads, profileCore] = await Promise.all([
    supabaseRequest(cfg, 'GET', `orders?person_profile_id=eq.${pid}&select=id,amount_paise,paid_at&payment_status=eq.paid&limit=5000`),
    supabaseRequest(cfg, 'GET', `consultancy_bookings?person_profile_id=eq.${pid}&select=id,amount_paise,created_at,payment_status&limit=5000`),
    supabaseRequest(cfg, 'GET', `visitors?person_profile_id=eq.${pid}&select=id,session_id,first_seen_at,last_seen_at,landing_path,source_page,referrer&limit=5000`),
    supabaseRequest(cfg, 'GET', `leads?person_profile_id=eq.${pid}&select=id,session_id,lead_status,first_seen_at,last_seen_at,email,name,phone,landing_path,source_page,referrer,converted_order_id&limit=5000`),
    supabaseRequest(cfg, 'GET', `person_profiles?id=eq.${pid}&select=canonical_email,canonical_phone&limit=1`),
  ]);

  const orderRows = jsonRows(orders);
  const bookingRows = jsonRows(bookings);
  const visitorRows = jsonRows(visitors);
  const leadRows = jsonRows(leads);
  const core = jsonRows(profileCore)[0] || null;

  let totalRevenuePaise = 0;
  for (const row of orderRows) totalRevenuePaise += parseInt(row.amount_paise ?? 0, 10) || 0;
  for (const row of bookingRows) {
    if (String(row.payment_status || '').toLowerCase() === 'paid') {
      totalRevenuePaise += parseInt(row.amount_paise ?? 0, 10) || 0;
    }
  }

  const allSeen = []
    .concat(visitorRows.map((r) => r.first_seen_at).filter(Boolean))
    .concat(leadRows.map((r) => r.first_seen_at).filter(Boolean))
    .sort();
  const allLastSeen = []
    .concat(visitorRows.map((r) => r.last_seen_at).filter(Boolean))
    .concat(leadRows.map((r) => r.last_seen_at).filter(Boolean))
    .sort();

  const leadWithContact = leadRows.find((r) => normalizePhone(r.phone) || normalizeEmail(r.email)) || null;
  const hasRealLead = Boolean(leadWithContact);
  const hasProfileContact = Boolean(normalizePhone(core?.canonical_phone) || normalizeEmail(core?.canonical_email));
  const hasCheckoutContact = bookingRows.length > 0;
  const firstTouchVisitor = visitorRows
    .slice()
    .sort((a, b) => new Date(a.first_seen_at || 0) - new Date(b.first_seen_at || 0))[0];
  const latestTouch = visitorRows
    .concat(leadRows)
    .slice()
    .sort((a, b) => new Date(b.last_seen_at || 0) - new Date(a.last_seen_at || 0))[0];

  const lifecycleStage =
    orderRows.length || bookingRows.length
      ? 'customer'
      : hasRealLead || hasProfileContact
        ? 'lead'
        : hasCheckoutContact
          ? 'checkout_contact'
          : visitorRows.length
            ? 'visitor'
            : 'unknown';

  const patch = {
    canonical_name: firstNonEmpty(leadWithContact?.name, core?.canonical_name),
    canonical_email: normalizeEmail(firstNonEmpty(leadWithContact?.email, core?.canonical_email)),
    canonical_phone: normalizePhone(firstNonEmpty(leadWithContact?.phone, core?.canonical_phone)),
    first_seen_at: allSeen[0] || null,
    last_seen_at: allLastSeen[allLastSeen.length - 1] || null,
    first_touch_path: firstNonEmpty(firstTouchVisitor?.landing_path, leadWithContact?.landing_path),
    first_touch_source: firstNonEmpty(firstTouchVisitor?.source_page, leadWithContact?.source_page),
    first_touch_referrer: firstNonEmpty(firstTouchVisitor?.referrer, leadWithContact?.referrer),
    last_touch_path: firstNonEmpty(latestTouch?.landing_path),
    last_touch_source: firstNonEmpty(latestTouch?.source_page),
    last_touch_referrer: firstNonEmpty(latestTouch?.referrer),
    lead_status: firstNonEmpty(leadWithContact?.lead_status) || (hasRealLead || hasProfileContact ? 'new' : 'anonymous'),
    lifecycle_stage: lifecycleStage,
    total_orders: orderRows.length + bookingRows.length,
    total_revenue_paise: totalRevenuePaise,
    merged_session_ids: [...new Set(visitorRows.concat(leadRows).map((r) => r.session_id).filter(Boolean))],
    merged_visitor_ids: visitorRows.map((r) => r.id).filter(Boolean),
    merged_lead_ids: leadRows.map((r) => r.id).filter(Boolean),
    merged_order_ids: orderRows.map((r) => r.id).filter(Boolean),
    merged_booking_ids: bookingRows.map((r) => r.id).filter(Boolean),
  };
  await patchPersonProfile(cfg, profileId, patch);
}
